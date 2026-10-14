import { useCallback, useEffect, useState, type FormEvent } from "react";

import type { SimInjection, SimStatus } from "../api/types";
import { useAuth } from "../auth";
import { fmtNumber, fmtTime, typeLabel } from "../format";

const RATES: (keyof SimInjection)[] = ["wash_trading", "spoofing", "momentum_ignition", "quote_stuffing", "marking_the_close"];

/** SimulatorPage lets an admin control the simulated market. */
export function SimulatorPage() {
  const { session, client } = useAuth();
  const [status, setStatus] = useState<SimStatus | null>(null);
  const [scenarios, setScenarios] = useState<string[]>([]);
  const [rates, setRates] = useState<Record<string, string>>({});
  const [symbol, setSymbol] = useState("");
  const [message, setMessage] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const admin = session?.role === "admin";

  const refresh = useCallback(async () => {
    try {
      setStatus(await client.simStatus());
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  }, [client]);

  useEffect(() => {
    if (!admin) return;
    void refresh();
    client
      .scenarios()
      .then((s) => setScenarios(s.scenarios))
      .catch(() => {});
    const t = setInterval(() => void refresh(), 5000);
    return () => clearInterval(t);
  }, [admin, client, refresh]);

  useEffect(() => {
    if (status?.injection && Object.keys(rates).length === 0) {
      setRates(Object.fromEntries(RATES.map((k) => [k, String(status.injection?.[k] ?? 0)])));
    }
  }, [status, rates]);

  if (!admin) {
    return <p className="muted">Simulator control is for admins.</p>;
  }

  async function act(what: string, fn: () => Promise<unknown>) {
    setMessage(null);
    setError(null);
    try {
      await fn();
      setMessage(what);
      await refresh();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  }

  function saveRates(e: FormEvent) {
    e.preventDefault();
    const injection = Object.fromEntries(RATES.map((k) => [k, Number(rates[k] ?? 0)])) as SimInjection;
    void act("Injection rates saved.", () => client.setInjection(injection));
  }

  return (
    <div className="sim-layout">
      <section aria-labelledby="sim-title">
        <h2 id="sim-title">Simulated market</h2>
        <p className="notice">Everything here is simulated: the market, its participants and the abuse injected into it.</p>
        {error && (
          <p className="error" role="alert">
            {error}
          </p>
        )}
        {message && <p className="ok" role="status">{message}</p>}
        {status && (
          <dl className="facts">
            <dt>State</dt>
            <dd>{status.state}</dd>
            <dt>Session</dt>
            <dd>{status.session_id ?? "–"}</dd>
            <dt>Market time</dt>
            <dd>
              {fmtTime(status.sim_time)} (session {fmtTime(status.session_open)}–{fmtTime(status.session_close)})
            </dd>
            <dt>Events published</dt>
            <dd>{fmtNumber(status.events_published ?? 0)}</dd>
            <dt>Labelled episodes</dt>
            <dd>{fmtNumber(status.labels_published ?? 0)}</dd>
            <dt>Pace</dt>
            <dd>
              {status.pace} {status.pace === "realtime" && `×${status.speed}`}
            </dd>
          </dl>
        )}
        <div className="actions">
          <button type="button" onClick={() => void act("Sessions started.", client.simStart)}>
            Start
          </button>
          <button type="button" onClick={() => void act("Session stopped.", client.simStop)}>
            Stop
          </button>
        </div>
      </section>

      <section aria-labelledby="inject-title">
        <h2 id="inject-title">Inject an episode now</h2>
        <div className="field">
          <label htmlFor="sim-symbol">Symbol</label>
          <select id="sim-symbol" value={symbol} onChange={(e) => setSymbol(e.target.value)}>
            <option value="">chosen by the simulator</option>
            {(status?.symbols ?? []).map((s) => (
              <option key={s} value={s}>
                {s}
              </option>
            ))}
          </select>
        </div>
        <div className="scenario-buttons">
          {scenarios.map((sc) => (
            <button
              key={sc}
              type="button"
              onClick={() => void act(`Scenario ${sc} scheduled.`, () => client.trigger(sc, symbol || undefined))}
            >
              {sc}
            </button>
          ))}
        </div>

        <h2>Background injection rates</h2>
        <form onSubmit={saveRates} className="rates">
          {RATES.map((k) => (
            <div className="field" key={k}>
              <label htmlFor={`rate-${k}`}>{typeLabel(k)}</label>
              <input
                id={`rate-${k}`}
                type="number"
                min={0}
                step="0.1"
                value={rates[k] ?? ""}
                aria-describedby={`rate-${k}-unit`}
                onChange={(e) => setRates((r) => ({ ...r, [k]: e.target.value }))}
              />
              <span id={`rate-${k}-unit`} className="muted">
                {k === "marking_the_close" ? "probability per symbol and session" : "per symbol-hour"}
              </span>
            </div>
          ))}
          <button type="submit" className="primary">
            Save rates
          </button>
        </form>
      </section>
    </div>
  );
}
