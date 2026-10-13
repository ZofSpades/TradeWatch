# Matching engine

`internal/orderbook` is the limit order book behind every simulated symbol.
It is a library with no I/O. The simulator drives one `Book` per symbol from
a single goroutine and publishes the resulting events to Kafka.

## Rules

| Rule | Behaviour |
| --- | --- |
| Priority | Best price first. At equal prices, the earliest-entered order first (FIFO). |
| Execution price | Always the resting order's price. |
| Limit protection | A limit order never trades at a price worse than its limit. |
| DAY limit | Trades what it can, and the remainder rests on the book. |
| IOC limit | Trades what it can, and the remainder is cancelled (`IOC_REMAINDER`). |
| FOK | Fills in full immediately or is cancelled with no trades (`FOK_UNFILLED`). Fillability is checked within the limit price. |
| Market | Sweeps the opposite side, and any remainder is cancelled (`NO_LIQUIDITY`). Must be IOC or FOK. It never rests. |
| Cancel | Removes the order's entire open quantity. Only the owning account can cancel. |
| Replace | `Qty` is the new *open* quantity. Reducing size at the same price keeps time priority. Any other change re-enters the order at the back of its new price level, and it trades if the new price crosses. The original entry time is kept. |
| Session close | Every resting order is cancelled (`SESSION_END`): bids before asks, each side from the best price outward, each level in queue order. |
| Validation | Side, type, time in force, price (positive, on tick, bounded) and quantity (positive, on lot, bounded) are checked in that order. A rejected request changes nothing, emits nothing and uses no sequence number. |
| Self-trades | Allowed on purpose. The simulator needs them to produce wash trades. |

## Event model

Every state change appends an `Event` to a caller-supplied slice. Events
carry a sequence number that starts at 1, has no gaps, and is shared by all
event kinds. A book snapshot also takes a number, so it reflects exactly the
events before it. The simulator converts events to the protobuf messages
published on the four raw topics (`orders`, `cancels`, `trades`,
`book_snapshots`). The shared sequence number is what lets ingest rebuild the
exact exchange order across those topics ([ADR-0003](adr/0003-partitioning-and-ordering.md)).

A new order's events always appear in this order: `Accepted`, then zero or
more `Trade`s, then at most one `Cancelled` for an unfilled remainder. A
trade reports the open quantity left on both orders (`BuyLeaves`,
`SellLeaves`). A downstream consumer can therefore track every order's state
from the stream alone.

## Data structures

- Each side keeps its price levels in a slice sorted from worst to best
  price. Reaching or consuming the best level is O(1). Inserting or removing
  an inner level is O(levels), which is cheap at realistic book depths and
  keeps the hot path cache-friendly.
- A map from price to level gives O(1) access to a price level.
- Each level is an intrusive doubly-linked FIFO of orders, plus an aggregate
  quantity and count.
- A map from order ID to order makes cancel and replace O(1) once the order
  is found. An inner level that empties costs O(levels) to remove.

## How it is tested

| Test | What it establishes |
| --- | --- |
| Scenario tests (`book_test.go`) | Each rule in the table above, including the edge cases: FOK liquidity beyond the limit, a replace that crosses, a replace of a partially filled order, and the session-close order. |
| Differential property test (`TestBookMatchesReferenceModel`) | Random sequences of submit, cancel, replace, snapshot and close, including invalid requests, run against `Book` and an independently written naive model that scans all orders. Event streams, errors, order IDs and sequence numbers must be identical. |
| Structural invariants, checked after every step | The book is never crossed. Levels are sorted and indexed. Aggregates match their orders. Every resting quantity is greater than 0. Queues are in priority order. The map and the levels agree. |
| Event-stream ledger, checked after every step | Each order's quantity is conserved across fills, cancels and replaces. Reported leaves are correct. Every trade executes at the resting price and never trades through the aggressor's limit. IOC, FOK and market orders never rest. FOK is all-or-nothing. |
| Determinism (`TestDeterministicUnderSeed`) | The same seed produces an identical event stream, and a different seed produces a different one. |
| Soak (`TestRandomizedSoak`) | 200,000 seeded operations, with invariant checks along the way. |

The suite was mutation-checked before it was committed. Seven bugs were
injected by hand: wrong trade price, priority kept on a size increase, a FOK
off-by-one, inverted level ordering, overfilling a resting order, trading
through a limit, and stale level aggregates. The suite caught every one.

## Performance

These numbers come from `make bench-micro`, which saves raw results to
[`results/microbench/orderbook.txt`](results/microbench/orderbook.txt) and a
benchstat summary (median of 10 runs, 95% confidence interval) to
[`results/microbench/orderbook.benchstat.txt`](results/microbench/orderbook.benchstat.txt).
Machine: AMD Ryzen 7 4800H laptop, Docker Desktop VM on WSL2, Go 1.26.9.

| Benchmark | Median | Allocations |
| --- | --- | --- |
| Add a passive order and cancel the oldest (1,000 resting) | 226.7 ns ± 1% | 1 per op |
| IOC order filled against one resting order (plus re-posting it) | 298.0 ns ± 3% | 2 per op |
| Market order sweeping 25 orders over 5 levels (plus rebuilding them) | 5.41 µs ± 0% | 30 per op |
| Simulator-like mix: 55% limit, 5% market, 22% cancel, 15% replace, 3% snapshot | 290.4 ns/op ± 13%, i.e. 3.44 M ops/s or 4.07 M events/s | 0 per op (53 B/op on average) |

Each resting order and each new price level is one heap allocation. Phase 10
profiles the full pipeline to decide which hotspot is worth fixing first.
