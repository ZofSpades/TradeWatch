# Observability

Every TradeWatch service exports Prometheus metrics and OpenTelemetry
traces, and can be profiled in place. The compose stack runs Prometheus and
Grafana, with dashboards and alerting rules provisioned from this
repository, and Jaeger for traces.

| What | Where (compose) | Configuration |
| --- | --- | --- |
| Grafana dashboards | `http://localhost:3001`. Anyone who can reach the port may view; changes need `admin` and `GRAFANA_ADMIN_PASSWORD` from `.env` | [`deploy/compose/grafana`](../deploy/compose/grafana) |
| Prometheus, with the alerting rules under Alerts | `http://localhost:9090` | [`deploy/compose/prometheus`](../deploy/compose/prometheus) |
| Jaeger, with each alert's trace and a sample of market events | `http://localhost:16686` | [`deploy/compose/jaeger`](../deploy/compose/jaeger) |
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

The analyst dashboard's System page shows the essentials from Prometheus
without leaving the app: throughput, consumer lag, latency percentiles,
dead letters and service health ([dashboard.md](dashboard.md), and
`GET /api/v1/system` in [api.md](api.md#system-status)).

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

## Tracing

Every service exports OpenTelemetry spans over OTLP/HTTP to Jaeger
(`http://localhost:16686` in compose). Trace context crosses Kafka as a
W3C `traceparent` record header and crosses Redis pub/sub inside the live
event. Two kinds of trace follow work through the pipeline:

| Trace | Spans | Started for |
| --- | --- | --- |
| A market event | marketsim `send <feed>` (until the broker acknowledges it) → ingest `process <feed>` (until the sequencer releases the event) → ingest `send market_events` → detector `process market_events` | a sample of events, `TW_TRACE_EVENT_SAMPLE_RATIO` (compose: 1 in 500) |
| An alert | detector `raise <rule>` (until acknowledged) → alertsink `store alert` (Postgres, then the live views) → api `stream alert` (handed to every connected client) | every alert |

- **Linked traces.** If the event that triggered an alert was sampled, the
  alert's trace continues the event's, from the exchange to the analyst's
  screen.
- **Spans record:**
  - Kafka topic, partition, offset and key, by OpenTelemetry's messaging
    conventions;
  - the alert's key, rule, score and symbol;
  - whether a delivery was a duplicate the alert key absorbed;
  - how many clients the alert was streamed to.
- **The alert keeps its trace ID** (`trace_id`). The dashboard links each
  alert to its trace in Jaeger. A redelivered alert keeps the first
  delivery's trace, and the duplicate's own trace shows it absorbed.
- **API requests.** Every request except the event stream is a trace, named
  by its route. Postgres queries made for a request become its child
  spans, and so do the Prometheus queries behind `GET /api/v1/system`.
  Queries outside a request, such as the alert sink's batch writes, are
  not traced.

**What tracing never changes.** Trace context lives only in record headers
and in the alert's `trace_id`. The deterministic alert key does not
include `trace_id`, so detection and deduplication are unaffected.
`TestTracesFollowSampledEventsAndEveryAlert` runs the pipeline with 2% of
events sampled and checks four things against real Kafka:

- the alerts are exactly the in-process alerts;
- every alert record carries the trace of the span that raised it;
- every sampled event forms a complete parent-child chain from marketsim
  through ingest to a detector;
- alerts raised by sampled events continue those events' traces.

**Bounded cost.**
- An untraced record costs one scan of its headers.
- Ingest holds at most 10,000 sampled events' spans while they wait for
  release. A span never released, such as a duplicate's, ends after two
  minutes.
- Jaeger keeps the most recent 20,000 traces in memory.

Tracing is off unless `TW_OTLP_ENDPOINT` is set, and then the default
no-op tracer costs almost nothing.

| Variable | Default | Meaning |
| --- | --- | --- |
| `TW_OTLP_ENDPOINT` | none: tracing off | OTLP/HTTP base URL, such as `http://jaeger:4318` |
| `TW_TRACE_EVENT_SAMPLE_RATIO` | `0.001` | Fraction of market events marketsim traces (marketsim only) |
| `TW_API_TRACES_URL` | none | Jaeger UI as a browser reaches it; the API turns an alert's `trace_id` into a link |

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
