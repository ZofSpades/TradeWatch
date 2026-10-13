# ADR-0003: Partition by symbol and restore cross-topic order with sequence numbers

- Status: Accepted
- Date: 2026-10-13

## Context

The simulated exchange publishes four raw feeds: `orders`, `cancels`, `trades`
and `book_snapshots`. Detection needs the exchange's exact order of events
**for one symbol across all four feeds**. Here is an example. A spoofing
pattern is "a large order is cancelled within 300 ms *after* a trade on the
other side". If the cancel is processed before the trade, the pattern is
missed.

Kafka orders records only within a partition of one topic. Keying by symbol
gives per-symbol order inside each topic. It says nothing about the order
*between* topics. A consumer reading `orders` and `trades` sees them
interleaved according to fetch timing. When the simulator runs faster than
real time, that skew can be minutes of event time.

Evaluation also requires that the same seeded session always yields the same
alerts. Ordering by event time with watermarks alone does not give this.
Which events count as "late" would depend on fetch timing.

## Decision

1. **Key every market record by symbol.** The four raw topics and
   `market_events` share one partition count (`TW_KAFKA_PARTITIONS`,
   default 6). `kafka.EnsureTopics` refuses to start if an existing topic
   has a different count. The default murmur2 key partitioner then places a
   symbol on the same partition number *p* in every topic, which an
   integration test verifies.
2. **One gap-free sequence per (session, symbol) across all four feeds.**
   The matching engine assigns `EventHeader.seq` to every event it emits,
   whichever topic the event goes to. Book snapshots take a sequence number
   too, so a consumer knows exactly which events a snapshot reflects.
3. **Ingest re-sequences.** Ingest consumes the four raw topics with the
   Range balancer. Since the partition counts are equal, Range assigns
   partition *p* of every topic to the same instance. Per (session, symbol),
   ingest releases events to `market_events` strictly in `seq` order.
   Duplicates are dropped and early arrivals are buffered.
   - A record rejected to the dead-letter topic still carries its `tw-seq`
     header. Ingest emits a `SequenceGap` in its place, so downstream
     consumers see a contiguous sequence and know exactly what is missing.
   - A gap with no explanation, such as a lost record without headers, is
     filled with a `SequenceGap` after a bounded wait or buffer size, and is
     counted in metrics. This is the only non-deterministic path, and it
     never triggers in normal operation.
   - When one partition of a co-partitioned group gets too far ahead, fetching
     from it is paused. This keeps the reorder buffer bounded.
4. **Detectors are per symbol.** Every detector consumes `market_events`,
   which is in per-symbol order, and keeps state per symbol. Cross-symbol
   interleaving therefore cannot change any detection result, which keeps
   live processing, replay and evaluation equivalent.
5. **Alerts are keyed by their deterministic alert key.** This spreads sink
   load evenly. The sink is idempotent on that key.

## Consequences

- One symbol is the unit of parallelism. A very hot symbol cannot be split
  across instances. With P partitions, at most P ingest or detector
  instances do useful work.
- Detectors cannot see one account's activity across symbols in a single
  instance, because an account's orders for different symbols land on
  different partitions. Cross-symbol, per-account analysis would need a
  second stage that repartitions by account. It is out of scope and listed
  as a limitation.
- Ingest becomes stateful, since it holds the next expected sequence per
  symbol. Its checkpoint lives in offset-commit metadata (ADR-0001).
- Increasing the partition count changes the symbol-to-partition mapping. It
  is an offline operation: drain the pipeline, recreate the topics, restart.

## Alternatives considered

- **A single raw topic for all event types.** This preserves order with no
  extra work. It was rejected because consumers that need only one feed
  should not have to read all of them, and because separate feeds mirror how
  exchanges publish data.
- **Event-time merge with watermarks only.** This is non-deterministic under
  arbitrary fetch skew, as argued above. Detectors still track event time
  and close windows by it (see `docs/detectors.md`). Ordering comes from the
  sequence number.
