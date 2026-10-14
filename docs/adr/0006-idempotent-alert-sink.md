# ADR-0006: Alerts are stored once through an idempotent upsert, not exactly-once delivery

- Status: Accepted
- Date: 2026-10-14

## Context

Detectors deliver alerts at least once. Graceful handovers produce no
duplicates ([ADR-0005](0005-detector-state-by-replay.md)). A detector that
crashes between flushing its producer and committing its offsets, however,
produces the same alerts again when its successor replays. The alert sink
can also crash anywhere in its own read, store and commit cycle.

Analysts need exactly one row per alert. That row carries a workflow status
and an audit trail, and a duplicate would split both. The live views (counts
and the recent list) should not count an alert twice either.

## Decision

1. **The alert key is the primary key.** The detector derives it
   deterministically from the rule, session, symbol, accounts and the
   pattern's anchor event, so a regenerated alert has the same key.
2. **Store, then commit.** The sink stores a polled batch in one Postgres
   transaction with `INSERT … ON CONFLICT (alert_key) DO NOTHING`. Only after
   that transaction commits does it commit the Kafka offsets. For a key that
   is already stored, it records the duplicate by incrementing `deliveries`
   and updating `last_seen_at`. It never changes the alert's content or its
   workflow status: the first delivery's content stands. An alert is
   recorded as raised in the audit trail only when it is inserted.
3. **Redis is derived.** The sink updates the live views and publishes the
   raise event only for alerts inserted for the first time, after the
   commit. Every sink rebuilds the views from Postgres when it starts, which
   repairs anything a crash between the commit and the Redis update left
   out.
4. **Poison records are isolated.** A record that is not a valid alert, or
   an alert that Postgres rejects (as opposed to a transient failure), goes
   to the dead-letter topic with a reason. The rest of its batch is stored,
   and the sink does not crash-loop on it.

## Alternatives considered

- **Store consumer offsets in Postgres, in the same transaction**, and seek
  to them on assignment. This gives exactly-once *processing* of the alerts
  topic. It would not remove duplicates a detector produced upstream,
  because those are separate records with new offsets, so the key-based
  upsert would still be needed. The upsert alone covers both cases.
- **Kafka transactions end to end.** Postgres cannot take part in a Kafka
  transaction, so the sink would still need an idempotent write or a
  two-phase commit. Transactions would also add per-batch latency upstream
  and complicate the detector's checkpointing (see ADR-0004 on why commit
  metadata is used).
- **Deduplicating in Redis** with a set of seen keys. Redis runs without
  persistence and with LRU eviction, so it cannot be the guard of record.
- **Last write wins.** Overwriting the content on each delivery would let a
  replay under a newer rule version silently change an alert an analyst has
  already worked. Keeping the first delivery, and recording redeliveries,
  is auditable.

## Consequences

- **Exactly one row per alert**, however often an alert is delivered. This
  is verified with in-batch duplicates, redeliveries across a sink restart,
  and concurrent deliveries of the same key from several connections.
- **Duplicates stay visible.** They are counted per alert (`deliveries`) and
  in total (`tradewatch_alertsink_alert_duplicates_total`), so the cost of
  at-least-once delivery can be measured rather than hidden.
- **Live counts can lag.** After a sink crash they under-count until the
  restarted sink has rebuilt them. They never over-count.
- **One sink instance for the live views.** Postgres correctness holds with
  several instances, but one instance's rebuild could race another's
  increments. Compose runs one.
