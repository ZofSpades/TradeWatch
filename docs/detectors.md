# Detectors

The `detector` service turns the sequenced `market_events` stream into
explainable alerts. For each of five kinds of market abuse, this document
covers what the detector looks for, the rule parameters that decide it, what
each alert contains, and the false positives and blind spots that are known.

All market data is simulated (see [simulator.md](simulator.md)). The abuse
the detectors are tested against is injected by TradeWatch itself
([manipulation.md](manipulation.md)). Detection quality is measured against
the simulator's ground truth by `make eval`; the results are summarized in
[Evaluation](#evaluation) and reported in full in
[docs/results/eval](results/eval/README.md).

## How detection runs

The detection engine, `internal/detect`, is a pure library with no Kafka,
clock or randomness inside it. Its output depends only on its input, so any
program that embeds it, the detector service or an offline tool, produces
the same alerts for the same events.

The engine keeps one state per (session, symbol):

- **A market view** shared by every detector:
  - Depth and the best bid and offer, taken from the latest book snapshot.
    Snapshots arrive every second and are authoritative.
  - A per-second history of mid, last price and top of book.
  - The recent orders, each with the depth context at the time it arrived:
    its size relative to the mean displayed level, and whether it was priced
    inside the spread.

  Order memory is bounded by age (`TW_DETECTOR_ORDER_HORIZON`, 15 min) and by
  count (`TW_DETECTOR_MAX_ORDERS`, 200,000 per symbol). Older orders are
  unknown to every detector, the same way with or without a restart.
- **One detector instance per enabled rule.**

Each event is handled in four steps:

1. **Deduplicate.** A sequence number that was already processed is
   dropped. A jump in the sequence, or a `SequenceGap` marker from ingest, is
   counted as a gap. Events of a session that has already closed are dropped
   as stale.
2. **Advance the watermark.** Every window is measured in event time. A
   symbol's watermark is the latest event time it has seen. An event stamped
   earlier is processed at the watermark and counted as late, so time never
   runs backwards for a detector.
3. **Update the market view**, then hand the event to every detector of the
   symbol in rule ID order.
4. **Complete and suppress.** The engine completes the drafts that detectors
   emit into alerts. A repeat of the same rule and suppression key within the
   rule's `suppress_for` is suppressed. By default the key is the accounts
   involved.

The `CLOSED` snapshot ends a session's state. A session can also simply stop,
for example when marketsim is restarted mid-session. Once the stream's newest
event was ingested more than `TW_DETECTOR_IDLE_AFTER` (10 min) later than a
sequence's newest event, that sequence's state is dropped. Event time cannot
reveal an abandoned session, because the clocks of two sessions are
unrelated. Ingest time can, and because it travels with the events the
decision is the same in every run over the same input.

### What an alert contains

| Field | Content |
| --- | --- |
| `alert_key` | The first 32 hex digits of SHA-256 over rule ID, session, symbol, sorted accounts and an *anchor* derived from the events, such as the trade ID that closed a ring. Reprocessing produces the same key, which makes delivery idempotent. The rule version is deliberately not part of the key: the same pattern instance seen under a newer rule version is the same alert. |
| `rule_id`, `rule_version`, `rule_digest`, `taxonomy_code` | The exact rule that fired and its regulatory mapping |
| `severity`, `score` | Severity from the rule. It is raised to critical when the score reaches the rule's `escalate_at_score`. |
| `account_ids`, `beneficial_owner_ids` | Who was involved |
| `window_start_ns`, `window_end_ns`, `detected_at_ns` | The pattern's span and when it was recognised (all event time) |
| `explanation` | One paragraph rendered from the rule's template |
| `evidence.features` | Every measured value with its threshold, comparison, unit and whether it breached |
| `evidence.order_ids`, `evidence.trade_ids`, `evidence.timeline` | The orders and trades that make up the pattern, with their role (for example `layer`, `layer_cancel`, `genuine_fill`) |
| `evidence.price_context` | Up to 300 points of last price and top of book, from one minute before the window to detection, for the alert's chart |

**Score.** A feature with a threshold contributes its margin past the
threshold, capped at twice the threshold. If every thresholded feature sits
exactly at its threshold, the alert scores 50. If every one is at least twice
past its threshold, it scores 100. Informational features do not count. The
score orders the triage queue. It is not a probability.

## Rules

Each rule is a versioned YAML file in [`rules/`](../rules). The file names
the detector it configures, its taxonomy code, severity, owner and
description, and in `spec` its parameters, suppression period, escalation
score and explanation template. [`rules/taxonomy.yaml`](../rules/taxonomy.yaml)
maps the codes to provisions of the EU Market Abuse Regulation. The mapping
is indicative and for engineering purposes only. It is not legal advice.

A rule set is checked in four layers before it can run:

1. **Schema.** An embedded JSON Schema checks the file. Parameters are
   checked per detector, and an unknown or misspelled parameter is an error.
2. **Decoding.** Parameters are decoded strictly into the detector's
   parameter struct.
3. **Consistency.** Each detector checks relationships the schema cannot
   express, for example that `max_entry_gap` is shorter than `window`.
4. **Rendering.** The explanation template is rendered with
   `missingkey=error` against sample inputs for every detector. A template
   that refers to a feature the detector does not produce fails at load
   time, not in front of an analyst.

`make validate-rules` (or `detector validate-rules [dir]`) runs the same
checks from the command line, and `TestLoadRepositoryRules` runs them in CI.

**Hot reload.** The service polls the rules directory every
`TW_DETECTOR_RELOAD_INTERVAL` (5 s) and compares content digests:

- **A set that fails any check** is rejected with an error log and a
  `rule_reloads_total{result="error"}` count. The previous set stays in
  force.
- **A valid set** is applied between two poll batches:
  - A rule that remains keeps its detector state, and its new parameters
    apply from the next event.
  - A removed rule loses its state.
  - A new rule starts empty.

Every loaded set is published to the compacted `rules_audit` topic, keyed by
digest, with each rule's source and digest. Every alert records the rule
version and digest that produced it.

Templates can use `join`, `int`, `num`, `pct`, `bps`, `ms` and `secs` to
format values. They see the symbol, accounts, owners, the feature values
(`.F`) and string facts (`.Facts`), such as a side or a window length taken
from the rule's own parameters.

## Wash trading: `TW-WASH-001` (MAR-WASH)

**Abuse.** Trades that change no beneficial ownership. They put volume on
the tape without any transfer of risk, which falsely signals supply, demand
or liquidity. Two forms are covered:

- **Direct.** One beneficial owner trades with itself across two of its
  accounts.
- **Ring.** Several owners pass shares around a closed cycle: A to B, B to C,
  and C back to A.

**How it decides.** For every trade, the two beneficial owners come from the
enrichment ingest added. A trade looks *pre-arranged* when both of the
following hold:

- the aggressive order arrived within `max_entry_gap` of the resting order
  it hit;
- the two sizes match to at least `min_size_match` (smaller divided by
  larger).

The two forms are then decided as follows:

- **Direct.** Both sides have the same owner, and the trade is either
  pre-arranged or at least the owner's `min_repeat`-th same-owner trade
  within `window`. When repetition triggers the alert, the entry gap and
  size match are reported for context only.
- **Ring.** Pre-arranged trades between different owners whose resting
  order was priced inside the spread become directed edges, seller to buyer.
  When a new edge closes a cycle of `min_ring_length` to `max_ring_length`
  distinct owners, with every edge later than the one before, the ring is
  reported. The smallest leg must be at least `min_ring_balance` of the
  largest.

| Parameter | Default | Meaning |
| --- | --- | --- |
| `window` | 10m | How long same-owner trades and ring edges are remembered |
| `max_entry_gap` | 1s | Longest gap between the two orders' entries for a pre-arranged trade |
| `min_size_match` | 0.9 | Smallest size match (min/max) for a pre-arranged trade |
| `min_repeat` | 3 | Same-owner trades within the window that alert without pre-arrangement |
| `min_ring_length`, `max_ring_length` | 3, 4 | Owners in a ring |
| `min_ring_balance` | 0.5 | Smallest leg relative to the largest |

Features: `same_owner_trades`, `entry_gap_ms`, `size_match` and
`inside_spread` for the direct form; `ring_length`, `ring_balance` and
`ring_duration_s` for rings. Alerts are suppressed for 15 minutes per owner
(direct) or per set of owners (ring).

**When it fires.** On the trade that completes the pattern.

**False positives and blind spots.**

- Two desks of one firm cross incidentally in normal flow. Such a cross is
  neither pre-arranged nor frequent, so it is not reported
  (`TestWashIncidentalSameOwnerCrossIsNotAlerted`). A firm whose desks
  genuinely internalise often enough to reach `min_repeat` within the window
  *would* be reported.
- The detector is only as good as the ownership data. A trade with an
  account whose owner is unknown is skipped.
- It misses:
  - rings longer than four owners, or whose legs span more than the
    ten-minute window;
  - ring legs whose resting order was not priced inside the spread;
  - pre-arrangement with entries more than one second apart, unless it
    repeats;
  - rings whose resting orders are older than the order memory.

## Spoofing and layering: `TW-SPOOF-001` (MAR-SPOOF)

**Abuse.** Large orders are placed on one side of the book to suggest
supply or demand that is not there. The spoofer executes a genuine order on
the other side and then pulls the large orders.

**How it decides.** A user cancel counts as a *layer* when both of these
hold:

- the cancelled order was at least `min_size_vs_depth` times the mean
  displayed quantity of the top five levels on its side when it arrived;
- it lived no longer than `max_order_lifetime`.

Layers that one account cancels on one side within `cancel_group_window` of
the first form a group. The group is evaluated once that window has passed,
so every layer pulled together ends up in one alert. It is reported when all
of the following hold:

- it has at least `min_layers` layers;
- the account had a fill on the opposite side while the layers rested;
- the last such fill came no more than `max_cancel_after_fill` before the
  first cancel;
- the account cancelled at least `min_cancel_ratio` of the quantity it
  placed on the layer side within `window`.

| Parameter | Default | Meaning |
| --- | --- | --- |
| `window` | 5m | Lookback for fills and for placed and cancelled quantity |
| `min_size_vs_depth` | 3 | Order size relative to the mean displayed level |
| `max_order_lifetime` | 3m | Longest life of a layer |
| `max_cancel_after_fill` | 2s | Longest delay from the genuine fill to the first cancel |
| `min_layers` | 2 | Layers in a group |
| `cancel_group_window` | 500ms | Cancels this close to the first belong to one group |
| `min_cancel_ratio` | 0.8 | Share of placed quantity cancelled on the layer side |

Features: `layers`, `size_vs_depth`, `cancel_after_fill_ms`,
`max_lifetime_s`, `cancel_ratio` and `opposite_fill_qty`.

**When it fires.** At the first event after the cancel group's window has
passed.

**False positives and blind spots.**

- A market maker cancels and replaces constantly, but its orders are about
  the size of the levels it makes up, so they are not layers
  (`TestSpoofingIgnoresOrdersSizedLikeTheBook`).
- An institution may cancel a large order, but unless a fill on the other
  side came just before, it is not reported (`TestSpoofingNeedsAnOppositeFill`).
- A trader who quotes large on both sides and pulls one side after the
  other is hit can look like a spoofer, and would be reported.
- The detector requires an execution on the other side. A spoofing attempt
  whose genuine order never fills is not reported.
- A single large order is not reported (`min_layers`).
- Layers pulled more than 500 ms apart form separate groups, so cancels
  staggered that far apart evade it.

## Momentum ignition: `TW-MOMO-001` (MAR-MOMENTUM)

**Abuse.** A burst of aggressive orders starts a price move that other
participants then chase, and the instigator trades out into it.

**How it decides.**

1. **Impulse.** For each aggressive trade, the detector collects the
   aggressor's aggressive trades in the same direction within
   `impulse_window`. All of the following must hold:
   - they come from at least `min_impulse_orders` distinct orders;
   - the price moved at least `min_move_bps` in that direction. The move
     is measured from the last price **before the account's own first
     aggressive trade**, so a trader who joins a move already under way
     measures a small move and is not mistaken for its instigator
     (`TestMomentumFollowerIsNotAlerted`);
   - the move is at least `min_move_sigma` times the volatility expected
     over the burst's duration. Volatility is estimated from thirty-second
     returns of the mid over the `volatility_lookback` before the burst,
     because one-second returns are dominated by quote flicker;
   - the account made at least `min_aggressive_share` of all aggressive
     volume in that direction since its first trade.
2. **Unwind.** The account's trades in the other direction within
   `reversal_window` count as unwinding, whether they are aggressive or
   passive. The pattern is complete when they reach `min_unwind_ratio` of
   the impulse volume.

| Parameter | Default | Meaning |
| --- | --- | --- |
| `impulse_window` | 60s | Span of the aggressive burst |
| `min_impulse_orders` | 3 | Distinct aggressive orders in the burst |
| `min_move_bps` | 20 | Move from the account's entry, in basis points. Version 2 lowered it from 30; see [Evaluation](#evaluation). |
| `min_move_sigma` | 4 | Move relative to the volatility expected over the burst |
| `volatility_lookback` | 30m | History used to estimate volatility |
| `min_aggressive_share` | 0.4 | Account's share of aggressive volume in that direction |
| `reversal_window` | 5m | Time allowed for the unwind |
| `min_unwind_ratio` | 0.5 | Unwound share of the impulse volume |

Features: `impulse_orders`, `move_bps`, `move_sigma`, `aggressive_share`,
`unwind_ratio`, `impulse_qty` and `impulse_s`.

**When it fires.** On the trade that completes the unwind.

**False positives and blind spots.**

- An urgent genuine order that moves the price and is then partly reversed
  within five minutes, such as a mistaken trade being unwound, matches the
  pattern.
- Impulses that move the price less than `min_move_bps` are not reported.
  The evaluation's sweep of this threshold shows how recall falls as it
  rises.
- It also misses:
  - ignition split across several accounts, each below the share
    threshold;
  - unwinds slower than five minutes;
  - bursts in the first minutes of a session, before ten volatility
    returns exist.

## Quote stuffing: `TW-STUFF-001` (MAR-STUFF)

**Abuse.** A participant floods the book with orders and cancels to slow
down or confuse other participants and the venue, with orders never meant
to trade.

**How it decides.** For each account, the detector counts order, replace
and user-cancel messages within `burst_window`. A burst is reported when all
of the following hold:

- there are at least `min_messages` messages;
- the per-second rate is at least `min_rate_multiple` times the baseline.
  The baseline is the 95th percentile, over every other account active in
  the last `baseline_window`, of each account's busiest second. It is at
  least one message per second. The judged account is left out, so its
  burst cannot raise its own bar. A percentile alone would not prevent
  that: with twenty accounts or fewer it is the maximum. A burst that
  started late in a second would then make that second's count the bar for
  the rest of the burst. The end-to-end test caught the detector missing a
  burst for exactly this reason, 25 seconds into a session
  (`TestQuoteStuffingBurstAcrossASecondBoundaryIsAlerted`);
- the account's fills in the window are at most `max_fill_ratio` of its new
  orders;
- the orders it cancelled in the window lived a median of at most
  `max_median_lifetime`.

| Parameter | Default | Meaning |
| --- | --- | --- |
| `burst_window` | 1s | Window messages are counted in |
| `min_messages` | 100 | Messages in the window |
| `min_rate_multiple` | 10 | Rate relative to the baseline |
| `baseline_window` | 10m | History of the baseline |
| `max_fill_ratio` | 0.02 | Fills per new order in the window |
| `max_median_lifetime` | 250ms | Median life of cancelled orders |

Features: `messages`, `rate_multiple`, `fill_ratio`, `median_lifetime_ms`
and `baseline_rate`. The timeline is bounded to the first and last twenty
orders of the burst.

**When it fires.** On the message that completes the burst.

**False positives and blind spots.**

- A very fast market maker repricing in a fast market is the main risk.
  It sends many short-lived orders and trades little. The baseline is
  relative to the busiest normal participants, so a market-wide burst of
  repricing raises it. A busy account whose orders trade is not reported
  (`TestQuoteStuffingIgnoresBusyAccountsThatTrade`).
- The detector works per symbol. It misses:
  - stuffing spread over many symbols at a moderate rate in each;
  - stuffing split across several accounts;
  - bursts below 100 messages per second.

## Marking the close: `TW-CLOSE-001` (MAR-CLOSE)

**Abuse.** Trading at the close to move the closing price, which values
portfolios, settles derivatives and benchmarks funds.

**How it decides.** From the first closing-phase snapshot on, the detector
evaluates the aggressor on every aggressive trade, and every account once
more at `CLOSED`. Within the closing phase, all of the following must hold:

- the account sent at least `min_orders` aggressive orders;
- its share of the volume traded from its own first closing-phase trade on
  is at least `min_volume_share`;
- at least `min_aggressive_fraction` of its closing-phase volume was
  aggressive in its dominant direction;
- the last price moved at least `min_move_bps` its way from the mid when
  the closing phase began;
- at least `min_concentration` of its volume over the closing phase and the
  `lookback_before_close` before it fell in the closing phase.

The concentration test separates the manipulator from an execution
algorithm that has been working an order all afternoon and finishes at the
close (`TestExecutionAlgorithmFinishingAtTheCloseIsNotAlerted`).

| Parameter | Default | Meaning |
| --- | --- | --- |
| `lookback_before_close` | 30m | Trading before the closing phase used for concentration |
| `min_volume_share` | 0.15 | Share of the volume traded from the account's first closing trade |
| `min_aggressive_fraction` | 0.8 | Aggressive share of the account's closing volume |
| `min_concentration` | 0.6 | Closing-phase share of the account's volume |
| `min_move_bps` | 10 | Move its way since the closing phase began |
| `min_orders` | 3 | Aggressive orders in the closing phase |

Features: `orders`, `volume_share`, `aggressive_fraction`, `concentration`
and `move_bps`. Alerts are suppressed for one hour, so an account is
reported at most once per close.

**When it fires.** On the aggressive closing-phase trade that meets every
threshold, or at `CLOSED`.

**False positives and blind spots.**

- A genuine large order worked entirely in the closing phase by an account
  that did not trade earlier looks the same. Index rebalancing flow is the
  classic example.
- The simulator has a continuous closing phase rather than a closing
  auction, so auction-based marking (imbalance games, late
  cancellations) is out of scope.
- It misses manipulation spread over the lookback before the closing phase,
  and manipulation done with passive orders.

## Evaluation

`make eval` runs `cmd/evaluator`, which works in three steps:

1. **Simulate.** It simulates 20 sessions with injected abuse and 5
   without, each 2 hours long with a 10-minute closing phase.
2. **Detect.** It runs every event through the ingest stage and the
   detection engine, as the services do.
3. **Score.** It matches the alerts against the ground-truth labels.

The run is deterministic. A native run and a run in Docker on another Go
patch version produced byte-identical reports. The full report, with
confidence intervals, time to detect, recall by scenario and the parameter
sweeps, is [docs/results/eval](results/eval/README.md). The table below
quotes it.

| Abuse type | Episodes | Recall | Precision | False alerts per 100 symbol-hours without abuse | Baseline recall | Baseline precision | Baseline false alerts |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| wash_trading | 252 | 0.93 | 0.92 | 8.08 | 0.63 | 0.13 | 319.62 |
| spoofing | 267 | 0.80 | 1.00 | 0.00 | 0.86 | 0.01 | 6236.54 |
| momentum_ignition | 255 | 0.67 | 0.99 | 0.00 | 0.81 | 0.43 | 55.38 |
| quote_stuffing | 264 | 0.93 | 1.00 | 0.00 | 0.93 | 1.00 | 0.00 |
| marking_the_close | 85 | 0.68 | 0.91 | 0.00 | 0.46 | 0.11 | 83.08 |

All market data and all abuse are simulated. These figures say how well the
detectors find the patterns TradeWatch injects into its own synthetic
market, not how they would perform on a real venue.

What the comparison with the naive baselines shows:

- **Wash trading.** Flagging every same-owner trade catches the direct
  episodes as well as the production rule does (0.98 recall each). It finds
  none of the 90 rings, which the production rule recovers at 0.83 recall.
  It also flags every incidental cross between desks of one firm. The
  production rule's false alerts on sessions without abuse come from such
  crosses repeating at least three times within ten minutes; none of them
  looked pre-arranged.
- **Spoofing.** A high cancel ratio catches slightly more episodes (0.86
  against 0.80), but market makers cancel constantly. It raises 21,730
  alerts, at a precision of 0.01. Order size against the book, a fill on
  the other side and cancel timing are what make the production rule
  usable.
- **Momentum ignition.** Aggressive trading into a move catches more
  episodes, but with precision 0.43: momentum traders who join a move look
  the same. The production rule's misses are mostly small impulses (see the
  `min_move_bps` sweep in the report).
- **Quote stuffing.** The baseline matches the production rule exactly,
  because no normal participant in the simulator ever sends 100 messages a
  second. The production rule's extra conditions are not exercised here. On
  a venue with fast market makers they would be:
  `TestBaselinesFlagWhatTheProductionRulesLeaveAlone` shows the baseline
  flagging a busy account whose orders trade.
- **Marking the close.** A large share of closing volume alone flags
  execution algorithms finishing at the close. Requiring aggression,
  direction, a price move and concentration in the closing phase gives
  0.91 precision against 0.11.

**Time to detect.**
- **Immediate:** wash trading and quote stuffing (median 0 s), because the
  alert fires on the completing trade or message.
- **Spoofing:** 12 s.
- **Momentum ignition:** 46 s, because the rule waits for the unwind.
- **Marking the close:** 25 s.

All are in simulated market time.

**How a threshold was changed.** The parameter sweeps showed that
`TW-MOMO-001` gained recall below 30 bps without losing precision or
raising false alerts. Version 2 of the rule lowered `min_move_bps` to 20,
not to the 10–15 bps the sweep would allow, to stay conservative about
volatility the simulator may understate. The decision used three disjoint
seed ranges:

1. **Development:** the change was chosen on seeds 7000–7024.
2. **Validation:** it was confirmed on seeds 9000–9024.
3. **Test:** the figures above use seeds 1000–1024, which played no part.
   On those seeds the change raised recall from 0.61 to 0.67, with
   precision at 0.99 against 1.00.

The sweeps suggest smaller gains elsewhere, such as a higher
`min_volume_share` for marking the close. They were not adopted, to avoid
tuning the rules to the simulator.

**The baselines** are the rules in [`rules/baselines`](../rules/baselines),
which the live detector never loads. Each looks only at the single most
obvious signal of its abuse type: every same-owner trade, a high cancel
ratio, aggressive trading into a move, a message rate, or a share of
closing volume. Each uses the production rule's window, thresholds and
suppression period where the signals overlap.

### Backtesting a rule change

marketsim records every session it publishes into the compose stack's
`recordings` volume, including the reference data and the ground-truth
labels. `make replay` lists the recordings. `make replay REC=<file>` reruns
detection over one of them offline, exactly as the services would, and
scores the alerts against the session's labels.

With `COMPARE=<rules dir>`, it also replays the session under a second
rules directory and lists, by alert key, the alerts only one of the two
raises. That shows what a rule change would have added or removed on
recorded history before the change is deployed. `TestReplayReproducesTheLiveAlerts`
checks that a replay raises exactly the alerts of the live engine, and
that a stricter threshold can only remove alerts.

## Sample explanations

These are rendered from the inputs each detector ships for template
validation, not from a detection run. They show the wording; the values are
made up.

> **TW-WASH-001 (direct).** Accounts ACC-0001, ACC-0002 share the beneficial
> owner BO-0001 and traded QZAP with each other: 2 same-owner trade(s) in the
> window. The two orders of the latest trade were entered 12 ms apart at a
> size match of 100%, at a price inside the prevailing spread, so no
> beneficial ownership changed.

> **TW-SPOOF-001.** Account ACC-0001 cancelled 4 large buy orders in QZAP, up
> to 6.5x the displayed depth per level, 180 ms after its own sell order
> executed for 400 shares. The cancelled orders lived at most 21 s, and the
> account cancelled 96% of the quantity it placed on the buy side in the
> window.

> **TW-STUFF-001.** Account ACC-0001 sent 310 order and cancel messages for
> QZAP within 1 s, a rate 44 times the busiest normal per-account rate over
> the last 10 min (7 messages per second). Its orders lived a median 5 ms,
> and 0% of them traded.

## The detector service

Each instance of `detector` joins one consumer group on `market_events`
(cooperative-sticky assignment) and runs one engine per owned partition.
Alerts go to the `alerts` topic, keyed by alert key.

**Delivery.** Before committing, the service flushes the producer and
checks every alert it produced. If any was not delivered, it commits nothing
and stops. Alerts are therefore delivered at least once, and the alert key
makes repeats idempotent.

**Restarts and handovers.** The engine's state is too large for commit
metadata. Instead, it is rebuilt by deterministic replay
([ADR-0005](adr/0005-detector-state-by-replay.md)):

1. **Checkpoint.** The commit metadata stores three things: the offset
   from which reprocessing rebuilds the current state (*low-water*), the
   committed offset (*frontier*), and, for each open sequence, the sequence
   number its replay starts from.
2. **Replay.** A new owner starts at the low-water offset and reprocesses up
   to the frontier without producing the alerts again. Those alerts were
   already acknowledged by the previous owner.
3. **Resume.** After the frontier it continues as normal.

With the default rules the engine horizon is 61 minutes of event time. It
reaches further back while a suppression is active, because the alert behind
that suppression must be regenerated to rebuild it.

| Variable | Default | Meaning |
| --- | --- | --- |
| `TW_DETECTOR_GROUP` | `detector` | Consumer group |
| `TW_DETECTOR_RULES_DIR` | `rules` (`/etc/tradewatch/rules` in the image) | Rules directory |
| `TW_DETECTOR_RELOAD_INTERVAL` | 5s | Rules directory polling period |
| `TW_DETECTOR_COMMIT_INTERVAL` | 1s | Flush and commit period |
| `TW_DETECTOR_INSTANCE` | host name | Instance name recorded in alerts and rule snapshots |
| `TW_DETECTOR_MAX_ORDERS` | 200000 | Orders remembered per symbol |
| `TW_DETECTOR_ORDER_HORIZON` | 15m | Age after which orders are forgotten |
| `TW_DETECTOR_IDLE_AFTER` | 10m | Ingest-time idleness after which an unclosed session's state is dropped |
| `TW_HTTP_ADDR` | `:8092` | Admin server (metrics, health) |

Metrics (prefix `tradewatch_detector_`):

| Metric | Meaning |
| --- | --- |
| `events_total` | Market events processed |
| `alerts_total{abuse_type,rule}` | Alerts produced |
| `replayed_alerts_total` | Alerts regenerated while rebuilding state, not produced again |
| `suppressed_alerts_total` | Detections silenced by a suppression period |
| `duplicate_events_total`, `sequence_gaps_total`, `late_events_total` | Input quality as the engine saw it |
| `orders_evicted_total`, `abandoned_symbols_total`, `open_symbols` | State bounds at work |
| `event_latency_seconds` | Wall time from marketsim publishing an event to the detector processing it |
| `process_seconds` | Time to run one event through every detector |
| `consumer_lag_records`, `assigned_partitions` | Consumption progress |
| `commits_total{result}`, `commit_duration_seconds`, `rule_reloads_total{result}` | Commits and rule reloads |

## Performance

`make bench-micro` runs `BenchmarkEngine`. It replays a seeded session
through a fresh engine and saves raw results to
[`results/microbench/detect.txt`](results/microbench/detect.txt), with a
benchstat summary (median of 10 runs, 95% confidence interval) in
[`detect.benchstat.txt`](results/microbench/detect.benchstat.txt).

- **Workload:** a 20-minute session with a 3-minute close, 52,029 events.
  Every kind of abuse is injected four times per symbol per hour, so each
  detector has work to do.
- **Machine:** the same as the matching engine's
  ([matching-engine.md](matching-engine.md#performance)).

| Rules enabled | Time per event | Allocations per event |
| --- | --- | --- |
| All five, as the service runs them | 1.64 µs ± 16% | 4.2 (580 bytes) |
| Wash trading only | 506 ns ± 17% | 1.3 |
| Spoofing only | 674 ns ± 7% | 1.7 |
| Momentum ignition only | 616 ns ± 17% | 1.5 |
| Quote stuffing only | 1.20 µs ± 17% | 3.2 |
| Marking the close only | 453 ns ± 9% | 1.5 |

A single-rule run still pays for the engine's shared bookkeeping: the
market and order state of every symbol. Each such row is that base plus
one detector, not the detector alone. Quote stuffing costs the most. At
1.64 µs per event, one core runs every detector over about 610,000 events
per second.

## Tests

| Test | What it checks |
| --- | --- |
| Scenario tests (`internal/detect/detectors_test.go`) | For each detector, at least one hand-built event stream that must alert, and the look-alike normal behaviour that must not. Every alert is checked: thresholded features breached, explanation rendered, identity fields present. |
| `TestEngineIsDeterministic` | Two engines over the same simulated session produce identical alerts with unique keys |
| `TestRestartFromCheckpointReproducesEveryAlert` | A run that crashes at 25, 50, 75 or 93% of a 150-minute session resumes from its checkpoint and emits exactly the alerts of an uninterrupted run: none lost, none extra, none different |
| `TestEngineForgetsAbandonedSessions` | An abandoned session's state is dropped after ten minutes of ingest time and stops holding the checkpoint back, even though its event-time clock is ahead of the new session's |
| `TestSetRulesKeepsStateAndAppliesNewParameters` | A hot reload keeps detector state and applies new thresholds from the next event |
| `TestDetectorsAgainstInjectedSessions` | Three seeded two-hour sessions with injected abuse. Each detector's recall and precision against the ground truth must stay above a floor (recall: wash 0.8, spoofing 0.6, momentum 0.35, stuffing 0.9, close 0.35; precision 0.9 for all), guarding against regressions. The floors are not results. The measured figures come from the evaluator. |
| `internal/rules` tests | The repository rules load; unknown parameters, bad templates and broken taxonomy references are rejected; the watcher keeps the last good set |
| `TestLivePipelineProducesTheInProcessAlerts` (integration) | marketsim → Kafka → ingest → Kafka → detector produces exactly the alerts of running the engine in-process on the same session |
| `TestDetectorHandoversLoseAndDuplicateNothing` (integration) | A stop/start handover and a mid-stream rebalance lose no alert and duplicate none |
| `TestStopDuringReplayKeepsThePreviousCheckpoint` (integration) | A detector stopped while it is still rebuilding state leaves the previous owner's checkpoint in place, and its successor produces no duplicates |

The last integration test was added after a code review found that a
partition still replaying committed its partial position. That moved the
frontier backwards, and the next owner would produce alerts a second time.
With the fix disabled, both of the test's assertions fail.
