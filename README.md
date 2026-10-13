# TradeWatch

TradeWatch is a real-time market-surveillance platform written in Go on
Kafka. A simulated exchange with a price-time-priority matching engine
generates realistic order flow with labelled market abuse mixed in: wash
trading, spoofing/layering, momentum ignition, quote stuffing and marking the
close. A streaming pipeline restores per-symbol order, enriches events with
beneficial-ownership data and runs stateful, rule-driven detectors. Every
alert carries a full evidence trail. Because the abuse is labelled, detection
quality is measured as precision, recall and time-to-detect rather than
asserted.

> **All market data in this repository is simulated.** Symbols, accounts and
> prices are synthetic. Nothing here is compliance advice.

## Status

The project is being built in phases. This table tracks what exists today.

| Phase | Scope | State |
| --- | --- | --- |
| 1 | Repo skeleton, compose (Kafka/Postgres/Redis), CI, Makefile, schemas | done |
| 2 | Order book and matching engine | done |
| 3 | `marketsim` normal participants | planned |
| 4 | `ingest` validation, enrichment, DLQ | planned |
| 5 | Manipulation injector with ground truth | planned |
| 6 | `detector` framework, rule engine, five detectors | planned |
| 7 | `alertsink`, Postgres, `api` | planned |
| 8 | `evaluator` and first results | planned |
| 9 | Dashboard | planned |
| 10 | Observability, benchmarks, chaos tests, profiling | planned |
| 11 | Docs, polish, `make demo` | planned |

## Quick start

Requirements: Docker with the compose plugin, and GNU make. On Windows, use
WSL2.

```sh
make infra   # start Kafka (KRaft), Postgres and Redis
make test    # unit, race and integration tests, run inside Docker
make lint    # gofmt, go vet, staticcheck
make proto   # lint protobuf schemas and regenerate Go code
make bench-micro    # Go micro-benchmarks, saved under docs/results/microbench
make mutation-test  # check the order book tests catch injected engine bugs
make help    # list all targets
```

## Documentation

- [Architecture](docs/architecture.md)
- [Matching engine](docs/matching-engine.md)
- [Architecture decision records](docs/adr/README.md)
