import { useCallback, useEffect, useState } from "react";

import { ApiError } from "../api/client";
import type { Quantiles, SystemStatus } from "../api/types";
import { useAuth } from "../auth";
import { Sparkline } from "../components/Sparkline";
import { fmtDuration, fmtRate } from "../format";

/** How often the page asks for fresh metrics while it is visible. */
export const SYSTEM_REFRESH_MS = 10_000;

type Health = "ok" | "warn" | "down";

function health(up: number, instances: number): Health {
  if (instances === 0 || up === 0) return "down";
  return up < instances ? "warn" : "ok";
}

function QuantileRow({ name, q, description }: { name: string; q: Quantiles; description: string }) {
  return (
    <tr>
      <th scope="row">
        {name}
        <span className="muted row-note">{description}</span>
      </th>
      <td className="num">{fmtDuration(q.p50)}</td>
      <td className="num">{fmtDuration(q.p95)}</td>
      <td className="num">{fmtDuration(q.p99)}</td>
    </tr>
  );
}

/**
 * SystemPage shows the pipeline's health from Prometheus: throughput at each
 * stage, consumer lag, latency percentiles, dead letters and which service
 * instances are up.
 */
export function SystemPage() {
  const { client } = useAuth();
  const [status, setStatus] = useState<SystemStatus | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  const refresh = useCallback(async () => {
    try {
      setStatus(await client.system());
      setError(null);
    } catch (err) {
      setError(err instanceof ApiError ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  }, [client]);

  useEffect(() => {
    void refresh();
    const t = setInterval(() => {
      if (!document.hidden) void refresh();
    }, SYSTEM_REFRESH_MS);
    return () => clearInterval(t);
  }, [refresh]);

  const tp = status?.throughput;
  const lag = status?.consumer_lag;
  const h = status?.history;

  return (
    <div className="system">
      <div className="system-head">
        <h2>System</h2>
        {status && (
          <span className="muted" role="status">
            Updated {new Date(status.generated_at).toLocaleTimeString()}
          </span>
        )}
        <button type="button" onClick={() => void refresh()}>
          Refresh
        </button>
        {status?.links.grafana && (
          <a href={status.links.grafana} target="_blank" rel="noopener noreferrer">
            Pipeline dashboard in Grafana ↗
          </a>
        )}
      </div>
      {error && (
        <p className="error" role="alert">
          Metrics are unavailable: {error}
          {status && " The figures below are from the last successful refresh."}
        </p>
      )}
      {loading && !status && <p className="muted">Loading metrics…</p>}

      {status && tp && lag && h && (
        <div className="system-grid">
          <section className="card" aria-labelledby="sys-throughput">
            <h3 id="sys-throughput">Throughput</h3>
            <dl className="stat-list">
              <dt>Published by marketsim</dt>
              <dd>{fmtRate(tp.published)} <span className="unit">events/s</span></dd>
              <dt>Released by ingest</dt>
              <dd>{fmtRate(tp.released)} <span className="unit">events/s</span></dd>
              <dt>Processed by detectors</dt>
              <dd>{fmtRate(tp.processed)} <span className="unit">events/s</span></dd>
              <dt>Alerts stored</dt>
              <dd>{fmtRate(tp.alerts_per_minute)} <span className="unit">per minute</span></dd>
            </dl>
            <Sparkline
              label="Events per second"
              format={(v) => `${fmtRate(v)} events/s`}
              series={[
                { name: "published", values: h.published, tone: "a" },
                { name: "processed", values: h.processed, tone: "b" },
              ]}
            />
          </section>

          <section className="card" aria-labelledby="sys-lag">
            <h3 id="sys-lag">Consumer lag</h3>
            <dl className="stat-list">
              <dt>ingest</dt>
              <dd>{fmtRate(lag.ingest)} <span className="unit">records</span></dd>
              <dt>detector</dt>
              <dd>{fmtRate(lag.detector)} <span className="unit">records</span></dd>
              <dt>alertsink</dt>
              <dd>{fmtRate(lag.alertsink)} <span className="unit">records</span></dd>
            </dl>
            <Sparkline
              label="Detector lag"
              format={(v) => `${fmtRate(v)} records`}
              series={[{ name: "detector lag", values: h.detector_lag, tone: "a" }]}
            />
          </section>

          <section className="card" aria-labelledby="sys-services">
            <h3 id="sys-services">Services</h3>
            <ul className="services">
              {status.services.map((s) => {
                const state = health(s.up, s.instances);
                return (
                  <li key={s.name}>
                    <span className={`health ${state}`} aria-hidden="true" />
                    <span className="service-name">{s.name}</span>
                    <span className={state === "ok" ? "muted" : state === "warn" ? "warn" : "error"}>
                      {s.instances === 0 ? "not found" : `${s.up} of ${s.instances} up`}
                    </span>
                  </li>
                );
              })}
            </ul>
          </section>

          <section className="card wide" aria-labelledby="sys-latency">
            <h3 id="sys-latency">Latency</h3>
            <table className="latency">
              <thead>
                <tr>
                  <th scope="col">Measure</th>
                  <th scope="col" className="num">p50</th>
                  <th scope="col" className="num">p95</th>
                  <th scope="col" className="num">p99</th>
                </tr>
              </thead>
              <tbody>
                <QuantileRow name="Published → processed by a detector" description="last 5 min" q={status.latency.event_to_detector} />
                <QuantileRow name="Published → alert stored in Postgres" description="last 15 min" q={status.latency.event_to_stored_alert} />
                <QuantileRow name="Detector time per event" description="all detectors, last 5 min" q={status.latency.detector_processing} />
              </tbody>
            </table>
            <Sparkline
              label="Event to detector, p95"
              format={fmtDuration}
              series={[{ name: "event to detector, p95", values: h.event_to_detector_p95, tone: "b" }]}
            />
          </section>

          <section className="card" aria-labelledby="sys-dlq">
            <h3 id="sys-dlq">Dead letters, last hour</h3>
            <p className="big-number">{status.dead_letters.last_hour.toLocaleString("en-US")}</p>
            {status.dead_letters.by_reason.length === 0 ? (
              <p className="muted">None in the last hour.</p>
            ) : (
              <table aria-label="Dead letters by reason">
                <thead>
                  <tr>
                    <th scope="col">Stage</th>
                    <th scope="col">Reason</th>
                    <th scope="col" className="num">Records</th>
                  </tr>
                </thead>
                <tbody>
                  {status.dead_letters.by_reason.map((d) => (
                    <tr key={`${d.stage}/${d.reason}`}>
                      <td>{d.stage}</td>
                      <td>{d.reason.replaceAll("_", " ")}</td>
                      <td className="num">{d.count.toLocaleString("en-US")}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </section>
        </div>
      )}
      <p className="muted system-note">
        From Prometheus, refreshed every {SYSTEM_REFRESH_MS / 1000} seconds. Rates are per second over the last minute. Latency
        percentiles come from histograms whose bucket bounds are at most 1.5 times apart. All market data is simulated.
      </p>
    </div>
  );
}
