# ADR-0001: Use franz-go as the Kafka client

- Status: Accepted
- Date: 2026-10-13

## Context

Every TradeWatch service talks to Kafka. The stateful consumers (ingest and
the detector) need more control than "read, process, auto-commit":

- **Manual offset commits** that happen only after outputs are durably
  written (at-least-once with idempotent sinks, see the delivery-semantics
  ADR).
- **Rebalance hooks** to flush outputs and checkpoint state before a partition
  moves to another instance, and a guarantee that a rebalance cannot happen
  while a batch is half processed.
- **Offset commit metadata.** Ingest and the detector store a small
  checkpoint (sequencer position, state low-water offset) next to the
  committed offset, so a new owner of the partition can resume exactly.
- **Range assignment** so that partition *p* of the four co-partitioned raw
  topics is owned by the same ingest instance (ADR-0003), and
  cooperative-sticky assignment elsewhere to keep rebalances cheap.
- **Fetch pausing** for backpressure, an **idempotent producer**, and an
  admin API for topic creation and lag inspection.
- Pure Go, so binaries are static, images are distroless, and the race
  detector runs without a C library.

## Decision

Use [`github.com/twmb/franz-go`](https://github.com/twmb/franz-go) (`kgo`,
plus `kadm` for admin and `kmsg` for raw protocol requests).

It covers every requirement above directly:

| Need | franz-go facility |
| --- | --- |
| Rebalance only between polls | `kgo.BlockRebalanceOnPoll()` + `Client.AllowRebalance()` |
| Flush and checkpoint on revocation | `kgo.OnPartitionsRevoked`, `OnPartitionsLost`, `OnPartitionsAssigned` |
| No implicit commits | `kgo.DisableAutoCommit()` |
| Co-partitioned assignment | `kgo.RangeBalancer()` |
| Cheap rebalances | `kgo.CooperativeStickyBalancer()` |
| Backpressure | `Client.PauseFetchPartitions` / `ResumeFetchPartitions` |
| Seeking to a checkpoint | `Client.SetOffsets` |
| Idempotent producer | enabled by default |
| Admin (topics, offsets, lag) | `kadm.Client` |
| Commit metadata | raw `kmsg.OffsetCommitRequest` sent with `Client.Request`, using `Client.GroupMetadata()` for member ID and generation |

The last row needs a note. `kgo`'s own commit path always writes the member ID
as the commit metadata (`consumer_group.go` in v1.22.2), so custom metadata
is committed with a raw `OffsetCommit` request. `Client.Request` routes group
requests to the group coordinator and retries retryable errors. From
`OffsetCommit` v10 onwards topics are identified by ID instead of by name, so
the request sets both `Topic` and `TopicID`. Integration tests cover this
path.

## Alternatives considered

- **segmentio/kafka-go.** Smaller, approachable API. As of this writing it
  offers only eager rebalancing (range, round-robin, rack affinity), and its
  group commits take offsets without custom metadata. Its writer has no
  idempotent mode. The checkpointing design would have to work around the
  client.
- **confluent-kafka-go (librdkafka).** Very mature, but needs cgo: no static
  binaries, a heavier build image, and C code in the race detector's blind
  spot.
- **IBM/sarama.** Mature and widely used, but its consumer group API is
  callback-heavy and more error-prone for manual commit flows. We did not
  benchmark the clients against each other. Performance was not a deciding
  factor.

## Consequences

- One client library for producing, consuming and admin, with no cgo.
- The raw `OffsetCommit` path depends on Kafka protocol details (topic IDs
  from v10). This is isolated in one function in `internal/kafka` and
  exercised against a real broker by `make test`.
- franz-go's API surface is large. Services use it through a few helpers in
  `internal/kafka`, not ad hoc.
