# Kubernetes

`deploy/k8s` runs TradeWatch on Kubernetes with Kustomize. It deploys the
services of the compose stack, and the single-node Kafka, Postgres and
Redis they need, in the `tradewatch` namespace. It is a starting point for
a real cluster, not a production deployment: see [Limits](#limits).

## What is verified

- `make lint-k8s` renders both kustomizations and validates every object
  against the Kubernetes 1.33 API schemas with kubeconform, in strict mode.
  `make lint` and CI run it.
- `make k8s-smoke` deploys the manifests to a throwaway single-node k3s
  cluster in Docker and waits for every workload. Then it runs the
  [demo Job](demo): the scenarios of `make demo`, each of which must raise an
  alert that matches its ground truth. The latest run is in
  [docs/results/k8s](../../docs/results/k8s/README.md).

## Deploying

You need a cluster, `kubectl`, and the two TradeWatch images where the
cluster can pull them. `make up` builds them as `tradewatch/backend:local`
and `tradewatch/web:local`. Push them to a registry the cluster can reach,
and point the manifests at it:

```sh
cd deploy/k8s
kustomize edit set image tradewatch/backend=REGISTRY/tradewatch/backend:TAG \
  tradewatch/web=REGISTRY/tradewatch/web:TAG
```

With kind, load them into the cluster instead:
`kind load docker-image tradewatch/backend:local tradewatch/web:local`.

The services read their secrets from a Secret named `tradewatch`, with the
keys that `make env` writes to `.env`:

```sh
make env
kubectl apply -f deploy/k8s/namespace.yaml
kubectl -n tradewatch create secret generic tradewatch --from-env-file=.env
kubectl apply -k deploy/k8s
kubectl -n tradewatch wait --for=condition=Available deployment --all --timeout=10m
kubectl -n tradewatch port-forward svc/web 3000:8080
```

The dashboard is then on `http://localhost:3000`. To run the demo's
scenarios in the cluster:

```sh
kubectl apply -k deploy/k8s/demo
kubectl -n tradewatch logs -f job/demo
```

## What runs

| Workload | Kind | Notes |
| --- | --- | --- |
| `kafka` | StatefulSet | One KRaft broker with a 10 Gi volume. It advertises `kafka-0.kafka-headless`, which resolves before the broker is ready. |
| `postgres` | StatefulSet | A 10 Gi volume. |
| `redis` | Deployment | No persistence: Redis holds only derived state. |
| `marketsim` | Deployment | One replica, replaced with `Recreate`: a second would run a second market. Recordings go to an `emptyDir`. |
| `ingest` | Deployment | |
| `detector` | Deployment | Two replicas in one consumer group, and a PodDisruptionBudget that keeps one running during voluntary disruptions. |
| `alertsink`, `api` | Deployment | Each applies the database migrations in an init container. Goose holds a Postgres advisory lock, so both can run them at once. |
| `web` | Deployment | nginx proxies `/api` to `api.<namespace>.svc.cluster.local`: nginx does not use DNS search domains. |

- **Security.** The namespace enforces the `restricted` Pod Security
  Standard. Every container runs as a non-root user, with no capabilities,
  no privilege escalation and the `RuntimeDefault` seccomp profile. The
  TradeWatch containers and Redis also have read-only root filesystems.
- **Health.** The services' readiness and liveness probes are their admin
  servers' `/readyz` and `/healthz`.
- **Startup order.** Kubernetes has no `depends_on`. A service that starts
  before Kafka accepts connections exits, and Kubernetes starts it again.
  The smoke test's report shows these restarts.
- **Monitoring.** Each service's pod carries `prometheus.io/scrape` and
  `prometheus.io/port` annotations for the cluster's Prometheus. The
  dashboard's System page needs `TW_API_PROMETHEUS_URL` on the api. Without
  it, the page says that metrics are not configured. Tracing is off unless
  `TW_OTLP_ENDPOINT` names an OTLP/HTTP collector.
- **Rules.** The detectors run the rules baked into the image from `rules/`.
  Rebuild the image to change them.

## Limits

These manifests are not a production deployment:

- One Kafka broker with replication factor 1, and one Postgres without
  backups: losing the node loses the data. Production needs three or more
  brokers (with `TW_KAFKA_REPLICATION=3` on the services) and a replicated
  or managed Postgres.
- No TLS or authentication on Kafka, Postgres and Redis inside the cluster,
  and no NetworkPolicy.
- No Ingress: reach the dashboard with `kubectl port-forward`.
- The resource requests and limits are starting points, not measured
  sizing.
