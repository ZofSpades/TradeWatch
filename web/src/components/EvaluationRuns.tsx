import { useCallback, useEffect, useState } from "react";

import type { EvaluationRun } from "../api/types";
import { useClient } from "../auth";
import { fmtAgo } from "../format";
import { RunFigures } from "./RunFigures";

/**
 * EvaluationRuns lists the stored evaluation runs, newest first, with the
 * figures of the selected one. It follows runs that are still going.
 */
export function EvaluationRuns() {
  const client = useClient();
  const [runs, setRuns] = useState<EvaluationRun[] | null>(null);
  const [selected, setSelected] = useState<number | undefined>();
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    try {
      setRuns((await client.evaluations(20)).runs);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  }, [client]);

  useEffect(() => {
    void load();
  }, [load]);

  const running = runs?.some((r) => r.status === "running") ?? false;
  useEffect(() => {
    if (!running) return;
    const t = setInterval(() => void load(), 3000);
    return () => clearInterval(t);
  }, [running, load]);

  const run = runs?.find((r) => r.id === selected) ?? runs?.[0];
  return (
    <section aria-labelledby="runs-title">
      <h2 id="runs-title">Evaluation runs</h2>
      <p className="muted">
        <code>make eval</code> over simulated sessions, and replays of recordings, scored against the ground truth.
      </p>
      {error && (
        <p className="error" role="alert">
          {error}
        </p>
      )}
      {runs?.length === 0 && (
        <p className="muted">No runs yet. Run make eval, or replay a recording from the Simulator page.</p>
      )}
      {runs && runs.length > 0 && (
        <table aria-label="Evaluation runs">
          <thead>
            <tr>
              <th>Started</th>
              <th>Kind</th>
              <th>Source</th>
              <th>By</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody>
            {runs.map((r) => (
              <tr
                key={r.id}
                className={r === run ? "selected" : ""}
                aria-selected={r === run}
                tabIndex={0}
                onClick={() => setSelected(r.id)}
                onKeyDown={(e) => e.key === "Enter" && setSelected(r.id)}
              >
                <td className="nowrap">{fmtAgo(r.started_at)}</td>
                <td>{r.kind === "eval" ? "Evaluation" : "Replay"}</td>
                <td>{r.source}</td>
                <td>{r.requested_by}</td>
                <td>{r.status}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      {run && <RunFigures run={run} />}
    </section>
  );
}
