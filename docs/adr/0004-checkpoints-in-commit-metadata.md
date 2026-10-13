# ADR-0004: Stateful consumers checkpoint in offset-commit metadata

- Status: Accepted
- Date: 2026-10-13

## Context

Ingest is stateful. Its sequencer remembers, for every open
(session, symbol), the next sequence number to release. When a partition
moves to another instance, after a rebalance, a restart or a crash, the new
owner must resume with state that is consistent with the committed offsets.
Two failures must be impossible:

- Waiting forever for a sequence number that was already released, and
  eventually declaring it lost.
- Releasing an event a second time *as if it were new*.

The consistency rule is the following. Every raw record whose sequence number
is at or above a key's next value must sit at or after its feed's committed
offset. The committable offset of a feed is therefore the lowest offset whose
record has not been released yet. The checkpoint is a snapshot of every
key's next value taken at the same moment.

## Decision

Store the checkpoint in the **metadata string of the offset commit itself**.

- The service commits all four co-partitioned feeds of a partition in one
  `OffsetCommit` request. Every feed carries the same JSON checkpoint:
  version, commit time, next sequence per (session, symbol), and the most
  recently closed sessions.
- Before committing, ingest flushes the producer. If any output failed to be
  delivered, it does not commit at all and exits. The process restarts and
  reprocesses from the previous commit. At-least-once delivery can never
  silently turn into at-most-once.
- On assignment, ingest fetches the group's committed offsets and metadata.
  It restores the newest checkpoint among the partition's four feeds and
  starts each feed at its committed offset.
- Commits use the member ID and generation the client currently holds.
  The coordinator therefore rejects a commit from an instance that has been
  fenced by a rebalance.
- The checkpoint is kept under 3,800 bytes, below Kafka's default
  `offset.metadata.max.bytes` of 4,096, by dropping the oldest remembered
  closed sessions first.

franz-go's commit API always writes the member ID as metadata, so the commit
is sent as a raw `kmsg.OffsetCommitRequest` (ADR-0001).

## Alternatives considered

- **A changelog or checkpoint topic** (the Kafka Streams approach). This is
  robust and scales to large state. It needs an extra compacted topic, a
  restore step that reads it on every assignment, and fencing logic of its
  own. That is a lot of machinery for a state of a few dozen integers per
  partition.
- **An external store** (Postgres or Redis). Writing to it is not atomic
  with the offset commit, so it reintroduces the very inconsistency this
  ADR exists to avoid. It also puts another dependency on the hot path.
- **Kafka transactions** (consume-transform-produce exactly once). These
  would remove duplicates from `market_events`. franz-go's transactional
  group session does not expose commit metadata, so the sequencer state
  would still need a separate home. Transactions also add per-batch
  latency. Downstream consumers already have to tolerate duplicates after
  crashes, and they deduplicate cheaply by (session, symbol, seq).

## Consequences

- Delivery into `market_events` is **at-least-once**. After a graceful
  shutdown or a rebalance there are no duplicates, because the service
  flushes and then commits on revocation. The integration test
  `TestIngestResumesFromItsCheckpoint` checks this with a stop/restart
  handover and a mid-stream rebalance.
- Duplicates can only appear after a crash between a flush and the next
  commit (at most `TW_INGEST_COMMIT_INTERVAL`, 1 s by default). They are
  exact copies with the same (session, symbol, seq).
- State stays small and bounded. The metadata size limit caps how many
  sequences can be open on one partition: hundreds, far more than this
  system uses.
- Standard lag tooling keeps working, because committed offsets track real
  progress.
