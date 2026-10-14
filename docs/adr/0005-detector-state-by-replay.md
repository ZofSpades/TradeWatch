# ADR-0005: Detector state is rebuilt by deterministic replay

- Status: Accepted
- Date: 2026-10-13

## Context

A detector partition's state is large and keeps changing:

- the recent orders of every symbol, with their depth context;
- a second-by-second market history;
- per-account windows of fills, cancels, impulses and closing volume;
- the active suppressions.

When a partition moves to another instance after a rebalance, a restart or
a crash, the new owner must continue with the same state. Otherwise patterns
that straddle the handover are missed, or reported differently than they
would have been without it.

Two properties make the problem tractable:

- **Determinism.** The engine (`internal/detect`) reads no clock and no
  randomness. Its windows are measured in event time, and its input,
  `market_events`, is in per-symbol sequence order
  ([ADR-0003](0003-partitioning-and-ordering.md)). The same events produce
  the same state and the same alerts.
- **Bounded memory.** Every detector declares a horizon: how much
  event-time history can influence its decisions. Order memory has an age
  bound as well. With the default rules the engine horizon is 61 minutes.

## Decision

Do not persist detector state. Rebuild it by reprocessing `market_events`,
and checkpoint only what the rebuild needs, in the offset-commit metadata
([ADR-0004](0004-checkpoints-in-commit-metadata.md)):

| Field | Meaning |
| --- | --- |
| `fr` (frontier) | The committed offset: every alert of an earlier event was produced and acknowledged before the commit |
| `lw` (low-water) | The offset that reprocessing starts from to rebuild the current state |
| `next` | For each open (session, symbol), the sequence number its replay starts from |
| `closed` | Recently closed sessions, so that their late duplicates stay dropped |

**The low-water offset.** The engine records, at most once per second of
event time, an input position for each symbol. For each open symbol, the
low-water is the latest such mark at or before that symbol's *replay cut*.
The overall low-water is the earliest of these. A symbol's replay cut is its
watermark minus the horizon. It reaches further back while a suppression is
active. A suppression exists because an alert fired, so to rebuild it, the
replay must regenerate that alert, starting from the earliest event the
alert could depend on.

**Handover.** A new owner restores `next` and `closed`, then seeks to the
low-water offset. It reprocesses events up to the frontier and drops the
alerts they produce, since the previous owner already delivered them. From
the frontier on, it produces alerts normally.

**Commits.** These follow the same rules as ingest:

- Before committing, the producer is flushed. A failed alert stops the
  service before anything is committed.
- Commits carry the member's generation, so a fenced instance cannot
  commit.
- A partition that is still replaying is not committed, so the previous
  owner's checkpoint stays in force until the rebuild has reached it.

**Abandoned sessions.** A session that stops without its `CLOSED` snapshot,
as a marketsim restart causes, would otherwise hold the low-water at its old
positions forever. Its state is dropped once the stream's newest event was
ingested more than `TW_DETECTOR_IDLE_AFTER` (10 min) after the session's
newest event. Ingest time is used because event time cannot tell an
abandoned session from a live one: two sessions' clocks are unrelated. The
ingest timestamps travel with the events, so replays make the same decision.

## Alternatives considered

- **Snapshot the state to a compacted changelog topic**, as Kafka Streams
  state stores do. Restores would be fast and independent of the horizon,
  but it would cost four things:
  - a serialization format for every detector's internal state;
  - a schema-evolution story for that format, every time a detector
    changes;
  - a second write path per event;
  - fencing between the changelog and the offsets.

  Each detector would also become harder to change than its rule.
- **Snapshot to Redis or Postgres.** This adds all the costs above. On top
  of that, the snapshot write is not atomic with the offset commit.
- **Start each new owner empty.** This is simple, but every handover would
  lose in-flight patterns, and alerts would depend on when rebalances
  happened. Results would then not be reproducible, which defeats the
  evaluation.

## Consequences

- **Exact rebuilds.**
  - **In process:** `TestRestartFromCheckpointReproducesEveryAlert`
    crashes a run at four points of a 150-minute session, restores a fresh
    engine from each checkpoint, and requires exactly the alerts of the
    uninterrupted run.
  - **Live, against Kafka:** the integration tests require the same of
    graceful handovers, a mid-stream rebalance, and a replay interrupted by
    a stop.
- **Delivery.** Alerts are delivered at least once. A crash between a flush
  and the next commit can re-produce alerts. These are identical, with the
  same alert key, so the alert sink stores them idempotently.
- **Handover cost.** A handover costs a replay of up to the horizon of the
  partition's events, and longer while suppressions are active. Its cost is
  measured by the benchmarks rather than estimated here.
- **Rules as of the replay.** A replay runs under the rule set in force at
  that time. If the rules changed between the original processing and the
  handover, the rebuilt state reflects the new rules. Alerts before the
  frontier are never re-produced, so this can change only what is detected
  after the handover. The rules a session ran under are recorded on
  `rules_audit`.
- **The engine must stay deterministic.** A detector whose decisions depend
  on anything beyond its declared horizon, or on wall-clock time, breaks
  rebuilds. The restart test guards this property for every detector in the
  repository.
