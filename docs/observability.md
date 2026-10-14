# Observability

Every TradeWatch service exports Prometheus metrics and can be profiled in
place. The compose stack runs Prometheus and Grafana with dashboards and
alerting rules provisioned from this repository.

| What | Where (compose) | Configuration |
| --- | --- | --- |
| Grafana dashboards | `http://localhost:3001`. Anyone who can reach the port may view; changes need `admin` and `GRAFANA_ADMIN_PASSWORD` from `.env` | [`deploy/compose/grafana`](../deploy/compose/grafana) |
| Prometheus, with the alerting rules under Alerts | `http://localhost:9090` | [`deploy/compose/prometheus`](../deploy/compose/prometheus) |
| Each service's `/metrics`, `/healthz`, `/readyz` and `/debug/pprof/` | admin ports `:8090`–`:8094`, on the compose network only | [architecture.md](architecture.md#services) |

## Metrics

Each service registers its series under `tradewatch_<service>_` and also
exports the Go runtime (`go_*`) and process (`process_*`) collectors.
Prometheus finds every replica through Docker's DNS, which answers a
service name with all of its addresses, so scaling a service needs no
configuration change. It scrapes every five seconds.

The series that answer the usual questions:

| Question | Series |
| --- | --- |
| How much is flowing through each stage? | `tradewatch_marketsim_events_published_total`, `tradewatch_ingest_events_released_total`, `tradewatch_detector_events_total`, `tradewatch_alertsink_alerts_raised_total` |
| Is a stage falling behind? | `tradewatch_{ingest,detector,alertsink}_consumer_lag_records` |
| How long does an event take to reach each stage? | `tradewatch_ingest_event_latency_seconds` (published → released by ingest), `tradewatch_detector_event_latency_seconds` (published → processed by a detector), `tradewatch_alertsink_pipeline_latency_seconds` (published → its alert stored in Postgres) |
| How expensive is detection? | `tradewatch_detector_process_seconds`, one event through every detector |
| What was rejected or repaired? | `tradewatch_ingest_dead_letters_total{reason}`, `tradewatch_ingest_sequence_gaps_total{reason}`, `tradewatch_alertsink_dead_letters_total{reason}` |
| What did deduplication absorb? | `tradewatch_ingest_duplicates_total`, `tradewatch_detector_duplicate_events_total`, `tradewatch_alertsink_alert_duplicates_total` |
| Did a handover replay state? | `tradewatch_detector_replayed_alerts_total`, `tradewatch_detector_assigned_partitions` |

Latencies are wall-clock differences between timestamps taken by
different containers. That is valid here because every container reads
the same host clock. Across machines it would also include clock skew.
The three event-latency histograms share bucket bounds (`metrics.LatencyBuckets`)
that are at most 1.5 times apart, so a quantile read from them is within
that factor of the true value. `make bench` measures end-to-end latency
exactly instead, from the stored alerts.

## Dashboards

**TradeWatch pipeline** (the home dashboard) shows:

- **Overview:** events per second published and processed, detector lag,
  the 95th-percentile time from an event to its stored alert, alerts and
  dead letters in the last hour.
- **Throughput:** each stage's event rate on one chart, so a stage falling
  behind shows as a gap between lines, and alerts per minute by abuse type.
- **Latency:** p50, p95 and p99 from publication to ingest, to the
  detectors and to the stored alert; the detectors' processing time per
  event; and commit and store times.
- **Kafka consumers:** lag per consumer group and instance, partitions owned
  (a rebalance shows as a step), and offset commits by result.
- **Data quality and recovery:** dead letters by reason, sequence gaps
  marked, duplicates absorbed by each stage, the sequencer's held records,
  open detector state, handover replays and suppressed repeats.
- **API:** requests and p95 latency by route, live-feed clients and server
  errors.

**TradeWatch services: Go runtime** shows CPU, resident memory, heap,
allocation rate, goroutines, garbage collection and open file descriptors
for every instance, filterable by service.

Dashboards are provisioned read-only from the JSON in
`deploy/compose/grafana/dashboards`; changes made in the UI are not kept.
`TestDashboardsQueryExportedMetrics` parses every query and fails if it
names a metric that no service exports, so renaming a metric cannot
silently empty a panel. The same test also checks panel layout and data
sources.

## Alerting rules

[`rules/tradewatch.yml`](../deploy/compose/prometheus/rules/tradewatch.yml)
defines ten rules:

| Rule | Fires when | Severity |
| --- | --- | --- |
| `TradeWatchServiceDown` | a service instance has not answered scrapes for a minute | critical |
| `TradeWatchConsumerLagHigh` | a consumer group stays more than 100,000 records behind for five minutes | warning |
| `TradeWatchPipelineLatencyHigh` | p99 time from event to stored alert stays above 5 s for ten minutes | warning |
| `TradeWatchOffsetCommitsFailing` | a consumer group's commits keep failing for five minutes | warning |
| `TradeWatchProduceErrors` | marketsim failed to deliver a record | critical |
| `TradeWatchDeadLetterRateHigh` | more than 1% of raw records are rejected for five minutes | warning |
| `TradeWatchEventsLost` | more than 0.1% of released sequence numbers are gaps for records that never arrived | warning |
| `TradeWatchAlertDeadLettered` | the alert sink dead-lettered an alert or rule set | critical |
| `TradeWatchRuleReloadRejected` | a detector refused a rule change | warning |
| `TradeWatchLiveViewsFailing` | the Redis live views failed to update | warning |

The compose stack corrupts about one raw record in 2,000 on purpose. The
two data-quality thresholds sit well above that rate, so the demo does not
alert. The stack runs no Alertmanager, so rules show in Prometheus but
notify nobody.

`make lint-monitoring` (part of `make lint`, and a CI job) runs
`promtool check config` and the rules' unit tests in
[`tests/tradewatch_test.yml`](../deploy/compose/prometheus/tests/tradewatch_test.yml).
The tests cover:

- the firing delays;
- the per-group lag sums;
- the latency quantile;
- the dead-letter and lost-event ratios at the demo's corruption rate and
  above their thresholds;
- the rendered summaries.

## Profiling

Every admin server serves the Go profiler. From another container on the
compose network, for example the toolbox:

```sh
docker compose run --rm --no-deps toolbox \
  go tool pprof -top -seconds 30 http://detector:8092/debug/pprof/profile
```

Heap, allocation, goroutine, block and mutex profiles, and execution
traces, are under the same prefix. Admin ports are not published outside
the compose network.
