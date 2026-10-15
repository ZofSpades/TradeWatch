# Injected market abuse and ground truth

The simulator can inject five kinds of market abuse into otherwise normal
sessions. Every injected episode produces a **ground-truth label**: which
accounts did it, on which symbol and when, and exactly which orders and
trades it consisted of. Labels go to the `groundtruth` topic, and into
session recordings, as separate entries. They are for scoring only: the
evaluator and the replay tool use them, and the detectors never see them.
The detectors work from the same market data any participant could observe,
plus reference data.

All of this is synthetic. The patterns are simplified renditions of
behaviour described in market-abuse rules (for example, EU MAR Annex I).
They are not a catalogue of real cases.

## Planning

Rates are configured per abuse type (`TW_SIM_INJECT_*`, or live through
`PUT /api/v1/sim/injection`):

- **Continuous behaviours** (wash trading, spoofing, momentum ignition,
  quote stuffing) are measured in expected episodes per symbol per trading
  hour. Every simulated minute, a planner draws the next minute's episodes
  from a Poisson process per (symbol, type), with uniformly random start
  times. Nothing is planned in the first minute, while books fill, or in the
  closing phase. Live rate changes take effect at the next planning minute.
- **Marking the close** is the probability that a symbol's close is marked
  in a session. It is decided when the session starts.

At most one episode of a given type runs on a symbol at a time, and an
account is used by one episode at a time. Episodes that cannot get a slot
or an account are skipped. Every episode draws from its own seeded random
stream, so a seeded session always contains the same episodes.

Episodes run from a dedicated pool of accounts: four wash firms with two
accounts each, three rings of three, four spoofers, four momentum igniters,
three quote stuffers and eight close markers. In reference data they look
like any other account. Their kinds are ordinary (proprietary, retail or
institutional), and their IDs come from the same permutation as everyone
else's.

## Scenarios

### `wash.direct` and `wash.ring`: wash trading

| | |
| --- | --- |
| Mechanics | Each leg posts a passive limit at a price **strictly inside the spread**, so no other order rests there. The counterparty account hits it 1–150 ms later with an IOC for the same quantity. If the spread is too tight to fit a price inside it, the episode waits, retrying every 2 s for up to 2 minutes. |
| Direct | The two accounts of one beneficial owner alternate as buyer and seller, for 3–8 legs. |
| Ring | Three colluding accounts with **three different owners** trade in a circle (A to B to C to A), for 2–4 cycles. No single-owner check can see this. |
| Labelled trades | Only trades with an episode account on *both* sides. For rings, only **complete cycles**: a ring that breaks off after A to B and B to C is not circular trading, so those trades are not labelled. |
| Legitimate lookalikes | Two desks of one institutional firm (same owner) occasionally trade with each other by accident. |

### `spoofing.layering`: spoofing and layering

| | |
| --- | --- |
| Mechanics | A **genuine** order joins the touch on one side. 100–300 ms later, 3–5 **layers** are placed 1–5 ticks behind the touch on the other side, each 5–12 times the typical displayed level size. Market makers shade their quotes towards displayed depth, so the layers make the genuine order more likely to execute. When it executes, the layers are cancelled 50–400 ms later. If it has not executed after 2 minutes, everything is cancelled. |
| Label | The genuine order first, then the layers. Trades are the genuine order's fills, plus any layer fills. `params.outcome` is `executed` or `not_executed`, and `cancel_latency_ms` is set when it executed. |
| Legitimate lookalikes | Market makers cancel and replace constantly and pull quotes after fills. Their orders are small, though, and they do not stack many large orders behind the touch. |

### `momentum.ignition`: momentum ignition (pump and dump)

| | |
| --- | --- |
| Mechanics | 6–14 aggressive IOC orders, 0.5–2.5 s apart, each taking the best two opposite levels, until the mid has moved by the target (0.4–1.2%, at least 6 ticks). The trader then holds for 10–45 s while momentum traders follow, and trades out of the position aggressively in the other direction. |
| Label | All of the account's orders and trades in both phases. `peak_move_bps` records how far the mid actually moved. |
| Legitimate lookalikes | Momentum traders, which join a move and exit later. News jumps in fair value. Institutional parents that fall behind schedule and cross the spread repeatedly. |

Price moves are possible because aggressive flow has **permanent price
impact** in the simulator. Each aggressive share moves the fair value by a
tier-dependent fraction (4e-7, 1e-6 and 2.5e-6 for the high, medium and
low tiers), signed by the aggressor's side.

### `quote_stuffing.burst`: quote stuffing

| | |
| --- | --- |
| Mechanics | For 2–6 s, the account sends an order and cancels its previous one every few milliseconds, at 150–400 messages per second. The orders are priced 3–8 ticks behind the touch, or behind fair value if that side of the book is empty, so they never trade. |
| Label | Every order of the burst. Trades are normally none. |
| Legitimate lookalikes | Market makers re-quoting, at a few messages per second per symbol. |

### `marking_the_close`: marking the close

| | |
| --- | --- |
| Mechanics | Starting 60–150 s before the close, inside the closing phase, the account sends 6–15 aggressive IOC orders in one direction, each taking the best opposite level. They are evenly spaced until 2 s before the close, with no unwind. |
| Label | All of the account's orders and trades. `move_bps` is the change in mid from the first order to the last. |
| Legitimate lookalikes | Institutional parents scheduled to finish at the close, which trade aggressively in the closing phase. Their activity is spread over the day, though. |

## Label accuracy

A label describes what *happened*, not what was attempted.

- An episode that never produced its defining pattern is not labelled.
  Examples are a wash leg that matched someone else, a spoof whose layers
  were never placed, and a momentum episode with no trades.
- Trade IDs are restricted to the trades that constitute the pattern.
- The window `[start_time_ns, end_time_ns]` spans the episode's first order
  to its last order, cancel or trade.

`internal/sim/inject_test.go` checks this against the event stream for
every scenario:

- Every labelled order and trade exists, belongs to a labelled account and
  lies inside the window.
- Every direct wash trade is between two accounts with the same owner.
- Ring members both buy and sell, with three distinct owners.
- Spoofing layers are on the opposite side of the genuine order, larger
  than it, and pulled within a second of its first fill.
- Momentum igniters trade both ways.
- Stuffing bursts exceed 100 messages per second without trading.
- Marking the close is one-directional, aggressive and inside the closing
  phase.

A Kafka integration test checks that the labels on the `groundtruth` topic
are identical to those of an in-process run of the same session.

## Live triggers

`POST /api/v1/sim/scenarios {"scenario": "spoofing.layering", "symbol": "QZAP"}`
starts a scenario at the current simulated time. Marking the close starts
90 s before the close. The command reaches the running simulation through
its command queue and is applied between two simulation steps. The dashboard's
Simulator page uses this for its labelled demo scenarios. So does `make demo`,
which then reads each episode's label from the `groundtruth` topic and finds
the alert that matches it by the evaluator's rule (`eval.Matches`).

Triggered episodes are labelled like planned ones. A run that receives
triggers depends on when they arrive, so evaluation runs never use them.
