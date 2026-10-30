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

The [latest run](results/bench/README.md), at commit `e55dee4`:

| Offered, events/s | p50, event to stored alert | p99 | Alerts measured | Verdict |
| ---: | ---: | ---: | ---: | --- |
| 10,000 | 32.2 ms | 41.7 ms | 353 | sustained |
| 40,000 | 34.7 ms | 47.6 ms | 1,443 | sustained |
| 60,000 | 36.6 ms | 51.9 ms | 2,198 | sustained |
| 80,000 | 38.3 ms | 491 ms | 2,871 | sustained |
| 100,000 | 40.5 ms | 75.9 ms | 3,531 | sustained |
| 120,000 | 46.4 ms | 193 ms | 4,399 | sustained |
| 150,000 | 418 ms | 10.91 s | 4,821 | fell behind |

- **Sustained:** 120,000 events/s. The detectors processed what was
  published, and no consumer group built up lag.
- **At the limit:** 150,000 events/s. Ingest released 134,709 of the
  149,807 events/s published and ended the window 810,068 records behind.
- **One slow step.** At 80,000 events/s the p99 was 491 ms, although the
  step kept up and the next one had a p99 of 75.9 ms. One run does not say
  what stalled.

Abuse is injected ten times as often as in the demo, so every step stores
enough alerts to measure.

**Earlier runs on the same laptop sustained less:** 100,000 events/s at
`a1234cb` ([bench-before](results/profiling/bench-before/README.md)) and
80,000 at `344ffd7` ([bench-after](results/profiling/bench-after/README.md)).
Nothing on ingest's processing path changed between `344ffd7` and
`e55dee4`, but the machine did. The WSL kernel went from 6.18.33 to
6.18.40, and ingest used 1.12 cores at 80,000 events/s instead of 1.61. A
ceiling here describes the machine's state as much as the code.

### What limits throughput

Ingest, the detectors and marketsim each process their records in one loop
per instance. The compose stack runs one ingest, one marketsim and two
detectors.

- **Ingest saturates first.** Ingest restores per-symbol order by reading
  all four raw feeds of a partition together, in one loop over every
  partition it owns. Between 120,000 and 150,000 events/s its CPU stopped
  rising, at 1.58 cores, while its lag grew. The CPU profiles in
  [results/profiling](results/profiling/README.md) show that loop as the
  largest part of ingest's work.
- **Not the machine as a whole.** At 150,000 events/s the TradeWatch
  services together used 5.40 cores of the 16 logical CPUs (marketsim
  1.58, ingest 1.58, the detectors 1.96, the alert sink 0.24, the API
  0.04). The bench does not measure Kafka, Postgres or the monitoring
  stack.

Ingest is a consumer group, so more instances would split the partitions
between them. That has not been measured here.

### Where a record's time goes

The micro-benchmarks put each component's work far below what the
services spend per record:

- one simulator-like order-book operation: 290 ns
  ([matching-engine.md](matching-engine.md#performance));
- every detector over one event: 1.64 µs
  ([detectors.md](detectors.md#performance));
- parsing one raw record: 1.55 µs, and sequencing it: 409 ns
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
