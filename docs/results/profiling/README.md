# Profiling: the routing-header hotspot

All market data is simulated. Every number below comes from a file in
this directory, produced by `make profile`, `make bench-compare` or
`make bench`. The machine is an AMD Ryzen 7 4800H laptop running Docker
Desktop on WSL2, with 16 logical CPUs; the load generator and every
service share it.

## Finding it

The first full `make bench` ([bench-before](bench-before/README.md)) at
commit `a1234cb` sustained every rate it offered, up to 100,000 events/s.
But latency rose steeply above 60,000:

| Offered | p99, event to stored alert |
| ---: | ---: |
| 60,000/s | 79 ms |
| 80,000/s | 319 ms |
| 100,000/s | 1.41 s |

At 100,000/s, ingest's lag reached 29,403 records within a window. Over
the same steps, ingest's CPU stopped rising (1.63 cores at 80,000/s, 1.57
at 100,000/s). Ingest processes every record in one loop, so that loop
was close to saturating one core.

`make profile PROFILE_RATE=80000` took 30-second CPU profiles of every
service at once ([before](before), `README.txt` has the measured rate:
80,385 events/s). In ingest's profile ([`ingest.cum.txt`](before/ingest.cum.txt)),
the processing loop (`ingest.(*Service).Run`) accounts for 25.63 s of
45.77 s of CPU. That is 0.85 of a core, or 10.6 µs per event.
`go tool pprof -list` on it gave this breakdown:

- **Decoding, validating and enriching** each record: about a quarter.
- **Re-encoding** the event for `market_events`: about a fifth.
- **The sequencer's bookkeeping.**
- **Routing headers**, built for every record ingest produces and parsed
  for every record it consumes:
  - `kafka.EventMeta.Headers` took 2.34 s, 5.1% of all of ingest's CPU;
  - `kafka.ParseEventMeta` took 1.15 s, 2.5%;
  - with the allocations they caused, that is the largest cost that is
    not inherent in the work.

The same header code was 7.3% of marketsim's CPU
([`marketsim.cum.txt`](before/marketsim.cum.txt)).

`EventMeta.Headers` grew its header slice twice and copied each value into
a new byte slice, six allocations per record. `ParseEventMeta` allocated
new strings for values that are the same on every record of a session.

## The fix

Commit `a03893f`:

- **One allocation per record.** `Headers` builds a record's headers and
  their values in a single block, and the values still never overlap.
- **No allocations when parsing.** `ParseEventMeta` takes schema names and
  session IDs from a small bounded intern table and parses the sequence
  number in place.
- **Behaviour pinned.** The benchmarks and the edge-case tests were
  committed first (`d09f956`) and pass unchanged.

## Before and after

**The functions themselves:** `make bench-compare NAME=headers OLD=d09f956 NEW=a03893f PKG=./internal/kafka BENCH=EventMeta`
([`headers.benchstat.txt`](headers.benchstat.txt), medians of 10 runs):

| Benchmark | Before | After |
| --- | --- | --- |
| Build a record's headers | 356 ns ± 5%, 6 allocations, 360 B | 185 ns ± 12%, 1 allocation, 224 B |
| Parse a record's headers | 93 ns ± 6%, 2 allocations, 64 B | 62 ns ± 8%, no allocation |

**The running services, at the same rate:** `make profile PROFILE_RATE=80000`
at `a03893f` ([after](after); 80,375 events/s):

| Profile, 30 s at 80,000 events/s | Before | After |
| --- | ---: | ---: |
| ingest, all CPU | 45.77 s | 42.63 s |
| ingest's processing loop | 25.63 s (10.6 µs per event) | 23.63 s (9.8 µs per event) |
| `EventMeta.Headers` in ingest | 2.34 s | 0.91 s |
| `ParseEventMeta` in ingest | 1.15 s | 0.71 s |
| `runtime.mallocgc` in ingest | 7.04 s | 5.95 s |
| marketsim, all CPU | 33.75 s | 30.85 s |
| `EventMeta.Headers` in marketsim | 2.47 s | 1.14 s |

The microseconds per event are the loop's CPU divided by the events
published during the profile.

- **The header code's share halved.** Within each profile, it was 7.6% of
  ingest's CPU before (5.11% + 2.51%) and 3.8% after (2.13% + 1.67%). In
  marketsim it went from 7.3% to 3.7%. Shares within a profile do not
  depend on how busy the machine was.
- **The totals are less certain.** Two single 30-second profiles can
  differ by a few percent for other reasons, so the drop in total CPU and
  in the loop's cost per event is less certain than the drop in share.

**The pipeline.** `make bench` ran before the fix, at `a1234cb`
([bench-before](bench-before/README.md)), and after it, at `344ffd7`
([results/bench](../bench/README.md)):

| Offered | p99 event to stored alert, before | after | ingest CPU, before | after |
| ---: | ---: | ---: | ---: | ---: |
| 60,000/s | 79.0 ms | 81.6 ms | 1.37 cores | 1.32 cores |
| 80,000/s | 319 ms | 177 ms | 1.63 cores | 1.61 cores |
| 100,000/s | 1.41 s, kept up | 6.02 s, fell behind | 1.57 cores | 1.49 cores |

These runs do not show the fix raising the pipeline's ceiling.

- **Two rates are inside the noise.** At 60,000/s the latencies are the
  same, and the ingest CPU figures differ by less than single runs vary.
- **100,000/s is at the limit of ingest's single processing loop.** The
  run before the fix kept up there; the run after fell behind. Neither
  run had headroom to recover from a hiccup.

The fix makes each record cheaper; raising the ceiling needs more than
one processing loop ([performance.md](../../performance.md#what-limits-throughput)).

Reproduce the profiles with `make profile PROFILE_RATE=80000` at each
commit. The summaries in `before/` and `after/` were produced from the
saved profiles:

```sh
go tool pprof -top -cum -nodecount=200 <profile>.pb.gz
```
