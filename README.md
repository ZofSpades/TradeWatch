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

## Results

`make eval` simulates 25 seeded sessions (20 with injected abuse, 5
without), runs the detectors exactly as the services do, and scores them
against the ground truth. The table compares each production rule with a
naive baseline that looks only at the most obvious signal. The full report
has confidence intervals, time to detect, recall by scenario and threshold
sweeps: [docs/results/eval](docs/results/eval/README.md).

| Abuse type | Recall | Precision | Baseline recall | Baseline precision |
| --- | ---: | ---: | ---: | ---: |
| Wash trading | 0.93 | 0.92 | 0.63 | 0.13 |
| Spoofing / layering | 0.80 | 1.00 | 0.86 | 0.01 |
| Momentum ignition | 0.67 | 0.99 | 0.81 | 0.43 |
| Quote stuffing | 0.93 | 1.00 | 0.93 | 1.00 |
| Marking the close | 0.68 | 0.91 | 0.46 | 0.11 |

These figures measure detection of the abuse TradeWatch injects into its own
simulated market, not performance on a real venue.
[detectors.md](docs/detectors.md#evaluation) explains what the comparison
shows, including where the baseline does as well.

## Status

The project is being built in phases. This table tracks what exists today.

| Phase | Scope | State |
| --- | --- | --- |
| 1 | Repo skeleton, compose (Kafka/Postgres/Redis), CI, Makefile, schemas | done |
| 2 | Order book and matching engine | done |
| 3 | `marketsim` normal participants | done |
| 4 | `ingest` validation, enrichment, DLQ | done |
| 5 | Manipulation injector with ground truth | done |
| 6 | `detector` framework, rule engine, five detectors | done |
| 7 | `alertsink`, Postgres, `api` | done |
| 8 | `evaluator` and first results | done |
| 9 | Dashboard | done |
| 10 | Observability, benchmarks, chaos tests, profiling | planned |
| 11 | Docs, polish, `make demo` | planned |

## Quick start

Requirements: Docker with the compose plugin, and GNU make. On Windows, use
WSL2.

```sh
make up      # build and start the stack; the dashboard is then on http://localhost:3000
make infra   # start only Kafka (KRaft), Postgres and Redis
make test    # Go unit, race and integration tests and the dashboard tests, in Docker
make lint    # gofmt, go vet, staticcheck, and the Prometheus config and rule tests
make proto   # lint protobuf schemas and regenerate Go code
make validate-rules # check the detection rules in rules/
make eval    # measure detection quality; writes docs/results/eval
make replay  # list session recordings; REC=<file> [COMPARE=<rules dir>] replays one offline
make web-test       # dashboard type-check, tests and build (also part of make test)
make web-e2e        # drive the running dashboard in a real browser (changes data)
make bench-micro    # Go micro-benchmarks, saved under docs/results/microbench
make mutation-test  # check the order book tests catch injected engine bugs
make help    # list all targets
```

Once the stack is up, open the dashboard on `http://localhost:3000` and sign
in as `analyst` or `admin`. `make env` generated their passwords into `.env`.
Grafana's pipeline dashboards are on `http://localhost:3001`, Prometheus is
on `http://localhost:9090`, and Jaeger, with every alert's trace, is on
`http://localhost:16686`. The API itself listens on
`http://localhost:8080`:

```sh
PW=$(grep '^TW_API_ANALYST_PASSWORD=' .env | cut -d= -f2-)
TOKEN=$(curl -s -X POST localhost:8080/api/v1/auth/login -H 'Content-Type: application/json' \
  -d "{\"username\":\"analyst\",\"password\":\"$PW\"}" | jq -r .access_token)
curl -s localhost:8080/api/v1/stats -H "Authorization: Bearer $TOKEN" | jq
```

## Documentation

- [Architecture](docs/architecture.md)
- [Matching engine](docs/matching-engine.md)
- [Market simulator](docs/simulator.md)
- [Injected abuse and ground truth](docs/manipulation.md)
- [Ingest](docs/ingest.md)
- [Detectors and rules](docs/detectors.md)
- [Storage and the alert sink](docs/storage.md)
- [Dashboard](docs/dashboard.md)
- [API](docs/api.md)
- [Delivery semantics, end to end](docs/delivery-semantics.md)
- [Observability: metrics, dashboards, alerting rules, tracing, profiling](docs/observability.md)
- [Architecture decision records](docs/adr/README.md)
