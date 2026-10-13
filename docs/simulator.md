# Market simulator

`marketsim` simulates a small exchange and publishes its feeds to Kafka. The
symbols, accounts, owners and prices are all synthetic. The simulator exists
to produce order flow that is realistic enough for surveillance logic to be
tested against, including legitimate behaviour that *looks* abusive. It does
not model any real venue.

The simulation (`internal/sim`) is a library with no I/O. The service
(`internal/marketsim`, `cmd/marketsim`) adds Kafka publishing, pacing against
the wall clock, recordings and a control API.

## Determinism

A session is a discrete-event simulation on one virtual clock shared by all
symbols. Agents (participants and session machinery) wake up at scheduled
times. Wake-ups at the same instant run in scheduling order. Every agent
draws from its own PCG stream, seeded from the session seed and a stable
label such as `mm/ACC-0123/QZAP`. Adding an agent therefore never changes
another agent's draws. Nothing in the simulation reads the wall clock.

For a given configuration (seed, open time, duration, symbols, activity),
a session therefore always produces the same events. This is tested in three
places:

- `TestSessionsAreReproducible`: the hash of the full encoded event stream is
  identical across runs.
- `TestPublishedSessionMatchesInProcessSimulation`: a session published
  through Kafka is byte-for-byte equal to the in-process run. The only
  exception is `published_at`, which is wall-clock by design.
- `TestDeterministicUnderSeed` in the order book covers the engine itself.

Pacing only sleeps between events and never alters them, so `realtime`,
`rate` and `fast` runs of the same configuration contain identical events.
A live demo session opens at the wall-clock time it starts, so its event
times differ from run to run. Set `TW_SIM_START` to make a run reproducible.

One caveat: the simulator uses floating point for its random
distributions, and Go allows fused multiply-add on some architectures. A
session simulated on arm64 may therefore diverge from the same session on
amd64. The published results were produced on linux/amd64.

## Universe

Eight fictional symbols in three liquidity tiers. The tick size is 0.01 and
the lot size is 1 share.

| Symbol | Tier | Opening reference | Daily volatility of fair value |
| --- | --- | --- | --- |
| QZAP | high | 125.40 | 1.8% |
| VNTR | high | 48.20 | 2.2% |
| ORBT | medium | 77.50 | 2.5% |
| KRYL | medium | 22.10 | 2.8% |
| MOSS | medium | 310.40 | 1.6% |
| TALX | low | 9.85 | 3.5% |
| WREN | low | 14.60 | 3.2% |
| YUMA | low | 63.30 | 3.0% |

The participants are:

- 3 market-making firms, with one account each.
- 6 institutional firms, with two desks (accounts) each. The two desks share
  a beneficial owner.
- 4 proprietary trading firms.
- 300 retail clients, each its own beneficial owner, trading through one of
  three brokers.

Account, owner and entity IDs are assigned through a fixed permutation. An
ID such as `ACC-0137` says nothing about the account's role, and the
simulator's internal role is never published. Reference data carries only
the account kind, owner and entity, as a venue's would.

## Participants

Each participant type is there either to create realistic order flow or to
be a *legitimate lookalike* of an abuse pattern, which is a source of false
positives for the detectors.

| Participant | Behaviour | Lookalike of |
| --- | --- | --- |
| Fundamentals (one per symbol) | The fair value follows a geometric random walk at the symbol's daily volatility, updated every 500 ms. A news jump of ±0.4–1.2% happens about once every four trading hours. | Momentum ignition: large moves with no manipulator behind them |
| Market makers (1–3 per symbol, by tier) | Quote both sides around fair value with a tier-dependent half-spread and lognormal sizes. Quotes are shaded towards top-of-book imbalance and skewed against inventory, within an inventory limit. They re-quote on exponential wake-ups (mean 0.4–1.2 s), mostly by cancel/replace, and never cross the book. | Spoofing and quote stuffing: very high cancel and replace rates |
| Noise traders (one pool per symbol) | Poisson arrivals from 300 retail accounts. 45% are marketable (a market IOC or a marketable limit IOC). The rest are passive DAY limits at or near the touch, cancelled after a random lifetime (mean 45 s) if unfilled. | Background flow |
| Institutional parents (1–3 per symbol) | One desk works a parent order of lognormal size over 15–60% of the session on a TWAP or VWAP schedule. VWAP follows a U-shaped intraday volume curve. The desk posts passive children at the touch, crosses the spread when behind schedule or when a child has rested too long, and finishes aggressively. A quarter of parents end at the close. | Marking the close, momentum ignition: sustained one-sided aggressive flow |
| Momentum traders (prop, 1–2 per symbol) | When the mid has moved more than 5σ (lookback volatility, at least 3 ticks) over 30 s, the trader joins the move with an aggressive order. It exits after a holding period (mean 2 min) and then cools down for 10 minutes. | Momentum ignition: one account buying aggressively and later selling |

Because market makers shade quotes towards book imbalance, displayed depth
moves prices. This is the mechanism spoofing exploits, so injected spoofing
(Phase 5) actually works in the simulation rather than being a pattern in
isolation.

Some beneficial owners hold several accounts (the institutional desks). Two
desks of one firm can therefore occasionally trade with each other by
accident. This is a realistic false-positive source for wash-trading
detection.

## Session lifecycle

1. **Open.** The phase becomes `CONTINUOUS`, and every symbol publishes a
   snapshot at that sequence position.
2. **Closing phase.** For the last `TW_SIM_CLOSING_WINDOW` of the session,
   the phase is `CLOSING`. This is the reference-price window that
   marking-the-close detection watches.
3. **Close.** Every resting order is cancelled (`SESSION_END`), and a final
   `CLOSED` snapshot ends each symbol's stream.

Snapshots (10 levels per side) are also published every
`TW_SIM_SNAPSHOT_INTERVAL` (1 s by default). Each snapshot takes a sequence
number, so it reflects exactly the events before it.

## Feeds

| Topic | Message | Record key | Headers |
| --- | --- | --- | --- |
| `orders` | `OrderEvent` (NEW, REPLACE) | symbol | `tw-schema`, `tw-session`, `tw-seq` |
| `cancels` | `CancelEvent` | symbol | same |
| `trades` | `TradeEvent` | symbol | same |
| `book_snapshots` | `BookSnapshot` | symbol | same |
| `refdata` | `RefDataRecord` | `account/…`, `instrument/…`, `session/…` | `tw-schema` |

Reference data is published, and acknowledged, before the first market
event. A `TradingSession` record is published at the start of every
session. Sequence numbers are per (session, symbol) and shared across the
four market feeds ([ADR-0003](adr/0003-partitioning-and-ordering.md)).

## Recordings

When `TW_SIM_RECORD_DIR` is set, each session is also written to
`<session>.twrec.gz` (`internal/recording`). A recording holds a header with
the simulator configuration, the reference data, every market event in
emission order, and (from Phase 5) the ground-truth labels. Recordings keep
`published_at` at zero, so recordings of the same configuration are
identical. The writer flushes every two seconds. A crash therefore loses at
most the last two seconds, and everything before that remains readable.

## Control API

The admin server (`TW_HTTP_ADDR`, default `:8090`) serves `/metrics`,
`/healthz` and `/readyz`, plus:

| Method and path | Effect |
| --- | --- |
| `GET /api/v1/sim/status` | State, session, simulated time, events published, pace |
| `POST /api/v1/sim/start` | Start (or resume with a new session) |
| `POST /api/v1/sim/stop` | End the current session early and pause |
| `PUT /api/v1/sim/pace` | `{"mode": "realtime"\|"fast"\|"rate", "speed": 1, "rate": 10000}` |

In compose, this port is reachable only on the internal network. The
dashboard reaches it through the authenticated API (Phase 7).

## Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `TW_SIM_SEED` | 1 | Seed of the first session; session *n* uses seed + *n* |
| `TW_SIM_SESSIONS` | 0 | Number of sessions to run, 0 = forever |
| `TW_SIM_START` | unset | RFC 3339 open time of the first session (later sessions open 24 h apart). Unset = open at wall-clock start |
| `TW_SIM_SESSION_PREFIX` | derived | `live-<start time>` or `seed<seed>` |
| `TW_SIM_SESSION_DURATION` | 6h30m | Continuous trading length |
| `TW_SIM_CLOSING_WINDOW` | 10m | Length of the closing phase |
| `TW_SIM_SNAPSHOT_INTERVAL` | 1s | Book snapshot period |
| `TW_SIM_ACTIVITY` | 1 | Multiplier on participant arrival rates |
| `TW_SIM_SYMBOLS` | all | Comma-separated subset of symbols |
| `TW_SIM_PACE` | realtime | `realtime`, `fast` or `rate` |
| `TW_SIM_SPEED` | 1 | Simulated seconds per wall second (`realtime`) |
| `TW_SIM_RATE` | 10000 | Events per wall second (`rate`) |
| `TW_SIM_RECORD_DIR` | unset | Directory for session recordings |
| `TW_SIM_MALFORMED_RATE` | 0 | Fraction of published records corrupted on purpose (garbage values, stripped headers, negative quantities, unknown accounts, off-tick prices) to exercise validation and the dead-letter topic; recordings are never corrupted |
| `TW_SIM_AUTOSTART` | true | Start the first session immediately |
| `TW_KAFKA_*` | | See `internal/kafka.LoadConfig` |

The compose stack runs 30-minute sessions with a 3-minute closing window in
real time, so every phase of the session is visible within a demo.
