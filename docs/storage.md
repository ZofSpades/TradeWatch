# Storage and the alert sink

Postgres is TradeWatch's system of record for alerts, their workflow and
audit trail, and the rule versions detectors have loaded. Redis holds live
views derived from it. `alertsink` writes both. Its delivery guarantees are
in [delivery-semantics.md](delivery-semantics.md) and
[ADR-0006](adr/0006-idempotent-alert-sink.md).

## Postgres schema

The schema changes only through goose migrations embedded in the binaries
([`internal/store/migrations`](../internal/store/migrations)). In compose,
the one-shot `migrate` service runs `alertsink migrate` before alertsink
and the API start. Migrations take a Postgres advisory lock, so concurrent
runs apply each migration exactly once (`TestMigrateAppliesOnceUnderConcurrency`).

| Table | Contents |
| --- | --- |
| `alerts` | One row per alert key. Holds the alert's searchable fields, its full protobuf as first received (`payload`, which includes the evidence), the workflow `status`, and delivery bookkeeping (`deliveries`, `first_seen_at`, `last_seen_at`). |
| `alert_audit` | Append-only history per alert: `raised`, each `transition` (from, to, note) and each `comment`, with the actor |
| `rule_versions` | Every rule version a detector loaded, identified by rule ID and source digest, with the YAML source |
| `rule_sets`, `rule_set_members` | Which rule versions made up each loaded rule set |
| `rule_set_loads` | Every load of a rule set by a detector instance, and when |

Event times are stored as the protobuf's nanosecond integers. Bookkeeping
times are `timestamptz`. Alerts are indexed for the API's listing: by
detection time, status, symbol, session and account (a GIN index on the
account array). Paging is by keyset, on (detection time, key).

`rule_versions` makes one governance check possible. If a rule's source
changes without its version number changing, two digests share a rule ID
and version, and the API reports `version_reused: true` for them.

## Redis views

Redis runs without persistence and with LRU eviction, so it holds nothing
that cannot be rebuilt from Postgres:

| Key (prefix `tw:`) | Type | Contents |
| --- | --- | --- |
| `live:counts` | hash | `total`, `type:<abuse type>`, `symbol:<symbol>` |
| `live:recent` | sorted set | The 500 most recently stored alert keys, scored by arrival time |
| `live:alert:<key>` | string | The alert's summary as JSON, expiring after 24 hours |
| `live:events` | pub/sub channel | `alert.raised` and `alert.updated` events for the API's event stream |

When `alertsink` starts, it rebuilds the counts and the recent list from
Postgres. It builds the new keys under temporary names and renames them in
one MULTI/EXEC, so readers never see a half-built view. While running, it
updates the views only for alerts stored for the first time, after the
database commit. If an update fails, the views are marked stale and rebuilt
once Redis answers again.

## alertsink

`alertsink` consumes `alerts` and `rules_audit` in one consumer group. For
each polled batch, it:

1. decodes the records, dead-lettering any that are not valid alerts or
   rule sets;
2. stores the alerts in one transaction with an idempotent upsert, and
   records the rule sets. Transient database errors are retried with
   exponential backoff. If the database rejects the batch outright, the
   alerts are stored one at a time, so only the offending alert is
   dead-lettered;
3. updates the Redis views and publishes the raise events for alerts stored
   for the first time;
4. commits the batch's offsets.

It also measures pipeline latency: the time from marketsim publishing the
event that triggered an alert to the alert being stored.

| Variable | Default | Meaning |
| --- | --- | --- |
| `TW_ALERTSINK_GROUP` | `alertsink` | Consumer group |
| `TW_POSTGRES_URL` | required | Postgres connection URL |
| `TW_REDIS_ADDR` | `localhost:16379` | Redis address |
| `TW_REDIS_PREFIX` | `tw:` | Prefix of the live-view keys and channel |
| `TW_ALERTSINK_RECENT_ALERTS` | 500 | Alerts kept in the recent list |
| `TW_ALERTSINK_MAX_BATCH` | 500 | Records per poll and per transaction |
| `TW_HTTP_ADDR` | `:8093` | Admin server (metrics, health) |

| Metric (prefix `tradewatch_alertsink_`) | Meaning |
| --- | --- |
| `records_total{topic}` | Records consumed |
| `alerts_raised_total{abuse_type}` | Alerts stored for the first time |
| `alert_duplicates_total` | Deliveries of alerts already stored |
| `rule_sets_total` | Rule-set snapshots recorded |
| `dead_letters_total{reason}` | Records dead-lettered: `decode_error`, `store_rejected` |
| `store_retries_total`, `store_seconds` | Database retries and batch write time |
| `pipeline_latency_seconds` | From marketsim publishing the triggering event to the alert being stored |
| `live_errors_total`, `live_rebuilds_total` | Redis view failures and rebuilds |
| `commits_total{result}`, `consumer_lag_records` | Offset commits and lag |

## Tests

| Test | What it checks |
| --- | --- |
| `internal/store` integration tests | Migrations under concurrency, idempotent upserts (in-batch duplicates, redeliveries with different content, concurrent deliveries of one key), the workflow and its errors, serialized concurrent transitions, filters and keyset paging, counts, and idempotent rule-set records with the version-reuse check |
| `internal/live` integration tests | Raise and update events, live counts and the recent list, and a rebuild that repairs a lost counter and alerts a sink stored without updating Redis |
| `TestSinkStoresEachAlertOnceAcrossDuplicatesAndRestarts` | The sink against real Kafka, Postgres and Redis: duplicates, a sink restart, a garbage record and an alert Postgres rejects. The result must be exactly one row per alert, exact live counts, and two dead letters with the right reasons. |
