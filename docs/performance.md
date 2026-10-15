# Performance

All market data is simulated. This page summarizes what TradeWatch's
benchmarks measured on one laptop, and how to reproduce every figure. Each
figure comes from a file under [`results/`](results), written by one of
these commands:

| What | Command | Results |
| --- | --- | --- |
| The whole pipeline under rising load | `make bench` | [results/bench](results/bench/README.md) |
| Components in isolation | `make bench-micro` | [results/microbench](results/microbench) |
| CPU profiles of every service under load | `make profile` | [results/profiling](results/profiling/README.md) |
| A change's effect on a benchmark | `make bench-compare` | [results/profiling](results/profiling/README.md) |

**Machine.** An AMD Ryzen 7 4800H laptop with Docker Desktop on WSL2. Docker
sees 16 logical CPUs and 15.3 GiB of memory. All of these share it:

- the load generator (marketsim);
- Kafka, Postgres and Redis;
- every TradeWatch service;
- Prometheus, Grafana and Jaeger.

## The pipeline

`make bench` offers marketsim's events at rising fixed rates. At each rate
it measures:

- the throughput of every stage;
- consumer lag;
- the exact time from an event's publication to its alert's insert into
  Postgres.

The [latest run](results/bench/README.md), at commit `344ffd7`:

| Offered, events/s | p50, event to stored alert | p99 | Alerts measured | Verdict |
| ---: | ---: | ---: | ---: | --- |
| 10,000 | 33.2 ms | 44.9 ms | 354 | sustained |
| 40,000 | 37.9 ms | 81.8 ms | 1,439 | sustained |
| 60,000 | 41.5 ms | 81.6 ms | 2,201 | sustained |
| 80,000 | 52.3 ms | 177 ms | 2,884 | sustained |
| 100,000 | 3.84 s | 6.02 s | 3,491 | fell behind |

- **Sustained:** 80,000 events/s. The detectors processed what was
  published, and no consumer group built up lag.
- **At the limit:** 100,000 events/s. The run before the routing-header fix
  ([bench-before](results/profiling/bench-before/README.md)) kept up there,
  with a p99 of 1.41 s. The latest run fell behind: ingest ended the window
  220,922 records behind.

A rate at which runs differ like this has no headroom left. Abuse is
injected ten times as often as in the demo, so every step stores enough
alerts to measure.

### What limits throughput

Ingest, the detectors and marketsim each process their records in one loop
per instance. The compose stack runs one ingest, one marketsim and two
detectors.

- **Ingest saturates first.** Ingest restores per-symbol order by reading
  all four raw feeds of a partition together, in one loop over every
  partition it owns. Between 80,000 and 100,000 events/s, ingest's CPU
  stopped rising (1.61 to 1.49 cores in the latest run, 1.63 to 1.57 in the
  one before), while its lag grew.
- **At 100,000 its loop needs about a whole core.** The profile at 80,000
  events/s puts the loop at 0.79 of a core, 9.8 µs per event
  ([profiling](results/profiling/README.md)). At 100,000 events/s that is
  0.98 of a core.
- **Not the machine as a whole.** At 100,000 events/s the TradeWatch
  services together used 4.92 cores of the 16 logical CPUs (marketsim
  1.43, ingest 1.49, the detectors 1.75, the alert sink 0.22, the API
  0.03). The bench does not measure Kafka, Postgres or the monitoring
  stack. The limit is the one loop, not total CPU.

Ingest is a consumer group, so more instances would split the partitions
between them. That has not been measured here.

### Where a record's time goes

The micro-benchmarks put each component's work far below what the
services spend per record:

- one simulator-like order-book operation: 290 ns
  ([matching-engine.md](matching-engine.md#performance));
- every detector over one event: 1.84 µs
  ([detectors.md](detectors.md#performance));
- parsing one raw record: 1.48 µs, and sequencing it: 398 ns
  ([ingest.md](ingest.md#performance)).

The rest of ingest's 9.8 µs per event goes to:

- re-encoding each event for `market_events`;
- the Kafka client's batching and network I/O;
- garbage collection;
- metrics;
- the routing headers.

Profiling under load shows all of this; micro-benchmarks cannot.

## The hotspot fixed

The routing headers (schema, session and sequence number) were built for
every record produced and parsed for every record consumed:

- six allocations to build, and two to parse, per record;
- 7.6% of ingest's CPU and 7.3% of marketsim's at 80,000 events/s.

Commit `a03893f` builds them in one allocation and parses them in none.

| Measure | Before | After |
| --- | --- | --- |
| Building one record's headers | 356 ns, 6 allocations | 185 ns, 1 allocation |
| Parsing them | 93 ns, 2 allocations | 62 ns, none |
| Share of ingest's CPU at 80,000 events/s | 7.6% | 3.8% |
| Share of marketsim's CPU | 7.3% | 3.7% |
| Ingest's processing loop, per event | 10.6 µs | 9.8 µs |

The first two rows come from 10 runs each with benchstat; the shares from
one profile of each version. The fix made every record cheaper, but the
two pipeline runs do not show it raising the ceiling. At 100,000
events/s, where the loop has no headroom left, they went opposite ways.
[results/profiling](results/profiling/README.md) has the whole account.

## What these numbers are not

- **One machine, simulated data.** These are measurements of one laptop
  running everything, on simulated data. A real venue's message mix,
  payload sizes and burstiness would differ.
- **No network or replication costs.** Kafka is one broker without
  replication, and every hop stays inside one host. Replicated, multi-host
  Kafka would add network and acknowledgement latency.
- **Same-clock latencies.** Latencies compare timestamps from one host
  clock. Across machines they would also include clock skew.
