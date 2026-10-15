# Ingest

`ingest` is the boundary between the raw exchange feeds and everything
downstream. It reads the four raw topics and does five things:

1. Decodes and validates every record.
2. Normalizes it.
3. Enriches it with reference data: beneficial owner, entity and account
   kind for every account involved, plus the instrument's tick size and
   liquidity tier.
4. Sends rejects to the dead-letter topic with a reason.
5. Publishes `market_events` in exact per-symbol sequence order.

## Validation

`ingest.Parse` checks a record against the schema, its routing headers and
the reference data. A record that fails is never repaired. Repairing data
silently would hide upstream defects from surveillance. Instead, the record is
wrapped in a `DeadLetter` along with its source topic, partition, offset, key,
value and headers, and sent to `dlq` with one of these reason codes:

| Reason | Meaning |
| --- | --- |
| `decode_error` | The value is not a valid protobuf message for the feed |
| `schema_mismatch` | The `tw-schema` header names another message type |
| `missing_field` | A required field is empty (session, symbol, seq, IDs) |
| `invalid_field` | A value is out of range: non-positive price or quantity, unknown enum value, market order with a price, crossed or unsorted book, cancel entered after itself, a trade of an order with itself, negative leaves |
| `key_mismatch` | The record key is not the payload's symbol |
| `header_mismatch` | `tw-seq` or `tw-session` disagree with the payload |
| `unknown_symbol` / `unknown_account` / `unknown_session` | Not in reference data |
| `price_off_tick` / `quantity_off_lot` | Not a multiple of the instrument's tick or lot |

Normalization is deliberately limited to canonical forms that cannot change
meaning: symbols are upper-cased, and identifiers are trimmed.

Reference data is published, and acknowledged, before the events that use
it. An unknown identifier therefore usually means ingest's reference-data
tail is behind. Before rejecting for that reason, ingest catches up to the
refdata topic's current end offset (bounded by `TW_INGEST_REFDATA_TIMEOUT`)
and validates again.

The parser is covered by table tests for every reason and by a fuzz test
(`FuzzParse`). The fuzz test checks two properties: nothing panics, and
re-encoding an accepted event parses to an identical event.

## Sequencing

Each instance owns partition *p* of all four raw feeds (Range assignment,
[ADR-0003](adr/0003-partitioning-and-ordering.md)). It keeps one
`Sequencer` per partition. For each (session, symbol), the sequencer
releases records strictly in `seq` order:

- **Early records** are held until every lower sequence number has been
  released.
- **Duplicates** are dropped. A duplicate is a sequence number that has
  already been released or is already held, or any record of a session that
  has already closed.
- **Dead-lettered records** whose position is known from their `tw-seq`
  header are released as a `SequenceGap` at exactly that position, with the
  dead-letter reason. Downstream consumers therefore see a contiguous
  sequence and know what is missing.
- **Lost records**, which never arrived (for example a record whose headers
  are corrupt as well), are declared with a `SequenceGap` (reason `lost`) in
  one of two ways:
  - **Exactly**, as soon as every feed has delivered a later record of the
    key. Each feed is ordered per key, so the missing number can no longer
    arrive.
  - **By timeout**, after `TW_INGEST_GAP_TIMEOUT` (5 s), but only once every
    feed is caught up to its high watermark, checked against the brokers. A
    lagging feed is never mistaken for a lost record. Once one gap of a key
    has timed out this way, later gaps of the same key are declared in the
    same pass.
- **Session close.** The `CLOSED` book snapshot ends a session. Its state is
  dropped, and only its final sequence number is remembered, so that late
  duplicates can be recognised.
- **Abandoned sessions.** A session can also just stop, for example when
  marketsim restarts mid-session. Its keys would then stay in the checkpoint
  forever, and after enough restarts the checkpoint would outgrow the
  commit-metadata limit and commits would fail. A key is therefore dropped
  once all three of these hold:
  - nothing of it is held;
  - it has received no new sequence number for `TW_INGEST_IDLE_AFTER`
    (10 min of wall time);
  - none of its records sits at or above a committable offset, so a restart
    from a later commit cannot read them again.

  If a dropped key does resume, it is treated as a new key. A gap marker
  from sequence number 1 reports its earlier records lost, like any other
  gap.

**Backpressure.** If a partition holds more than `TW_INGEST_MAX_HELD`
records (20,000), ingest pauses the feeds that have already delivered a
record *beyond* the missing position of every blocked key. None of the
missing records can be on those feeds, so pausing them lets the others catch
up without the buffer growing. A paused feed resumes in two cases: the
buffer drains below half the limit, or a newly blocked key might be waiting
for a record on that feed. An earlier version checked only one blocked key
per feed. With two symbols on a partition, it paused a feed that the other
symbol still needed, and that symbol starved. The detector integration test
found this deadlock, and `TestFeedNeededByAnyBlockedKeyIsNotPausable`
reproduces it.

The sequencer is pure logic, with no Kafka dependency. Its property test,
`TestSequencerProperties`, generates random productions across symbols and
sessions, random consumption orders over the four feeds, duplicates,
dead-lettered and lost records, and a crash with restore from an encoded
checkpoint at a random point. It then checks that the output is exactly the
produced sequence per key. It found one real defect during development:
consecutive gaps in one key used to wait one full timeout each.

## Commits, checkpoints and delivery semantics

Every `TW_INGEST_COMMIT_INTERVAL` (1 s), on partition revocation and on
shutdown, ingest takes these steps:

1. **Flush.** It waits until every produced `market_events` and `dlq` record
   is acknowledged. If any failed, it does not commit and exits, so a
   restart reprocesses from the previous commit.
2. **Commit.** It commits, per feed, the lowest offset whose record has not
   been released yet, together with a JSON checkpoint of the sequencer as
   offset-commit metadata ([ADR-0004](adr/0004-checkpoints-in-commit-metadata.md)).

On assignment, a new owner restores the newest checkpoint and resumes at the
committed offsets.

The resulting guarantee for `market_events` is at-least-once delivery, with
every symbol's events in sequence order and no event skipped without a gap
marker. Graceful handovers produce no duplicates, which the integration tests
verify. A crash can re-emit what was released after the last commit, as
exact copies with the same (session, symbol, seq). Consumers deduplicate on
that triple.

## Tests against real Kafka

| Test | What it checks |
| --- | --- |
| `TestIngestSequencesASimulatedSession` | A marketsim session comes out complete, in order per key, unaltered, enriched with the correct beneficial owners, with no dead letters and no duplicates |
| `TestIngestResumesFromItsCheckpoint` | marketsim publishes at a fixed rate while one instance stops, another starts and a third joins mid-stream. Nothing is missing, reordered or replaced by a gap. A failed checkpoint restore would show up as an invented gap |
| `TestIngestDeadLettersBadRecordsAndFillsGaps` | Corrupt bytes, an unknown account, a lost record and a headerless record produce exactly `1 order, gap(decode_error), gap(unknown_account), 4 cancel, gap(lost), 6 book, 7 trade`, plus the matching dead letters |
| `TestCommitWithMetadataRoundTrip` (`internal/kafka`) | The raw commit path stores and returns metadata, and refuses to commit outside a group |

In the compose stack, marketsim corrupts about one record in 2,000
(`TW_SIM_MALFORMED_RATE`, see [simulator.md](simulator.md)). The dead-letter
and gap paths are therefore always active.

## Performance

`make bench-micro` saves ingest's micro-benchmarks to
[`results/microbench/ingest.txt`](results/microbench/ingest.txt), with a
benchstat summary (median of 10 runs, 95% confidence interval) in
[`ingest.benchstat.txt`](results/microbench/ingest.benchstat.txt). The
workload is the same seeded session as the detectors' benchmark: 52,029
events, encoded and labelled exactly as marketsim publishes them.

| Benchmark | Median | Allocations |
| --- | --- | --- |
| `Parse`: decode, validate and enrich one raw record | 1.55 µs ± 14% | 12 per record (650 bytes) |
| `SequencerInOrder`: release events that arrive in order, the common case | 409 ns ± 8% per event | 0.13 per event |

## Metrics

| Metric | Meaning |
| --- | --- |
| `tradewatch_ingest_records_total{feed,outcome}` | Raw records accepted or rejected |
| `tradewatch_ingest_dead_letters_total{reason}` | Dead letters by reason |
| `tradewatch_ingest_events_released_total{kind}` | Events published to `market_events` |
| `tradewatch_ingest_sequence_gaps_total{reason}` | Gap markers (`dead_letter`, `lost`) |
| `tradewatch_ingest_duplicates_total` | Records dropped as duplicates or stale |
| `tradewatch_ingest_abandoned_sequences_total` | Sequences dropped after going idle without their session closing |
| `tradewatch_ingest_held_records` | Records waiting for an earlier sequence number |
| `tradewatch_ingest_paused_feeds` | Feed partitions paused for backpressure |
| `tradewatch_ingest_consumer_lag_records` | High watermark minus position, over owned partitions |
| `tradewatch_ingest_commits_total{result}`, `..._commit_duration_seconds` | Commit outcomes and timing |
| `tradewatch_ingest_event_latency_seconds` | Wall time from marketsim publishing an event to ingest releasing it |

## Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `TW_INGEST_GROUP` | `ingest` | Consumer group |
| `TW_INGEST_COMMIT_INTERVAL` | 1s | Flush and commit period |
| `TW_INGEST_GAP_TIMEOUT` | 5s | Wait before declaring a missing record lost (feeds caught up) |
| `TW_INGEST_MAX_HELD` | 20000 | Held records per partition before pausing feeds that are ahead |
| `TW_INGEST_IDLE_AFTER` | 10m | Idle time after which an unclosed session's sequences are dropped; `0` keeps them |
| `TW_INGEST_REFDATA_TIMEOUT` | 2s | Bound on a reference-data catch-up |
| `TW_HTTP_ADDR` | `:8091` | Admin server |
| `TW_KAFKA_*` | | See `internal/kafka.LoadConfig` |
