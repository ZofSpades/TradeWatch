import type { EvaluationRun } from "../api/types";
import { fmtNumber, fmtRatio, fmtTTD, typeLabel } from "../format";

/** RunFigures shows how an evaluation run went: its figures per abuse type. */
export function RunFigures({ run }: { run: EvaluationRun }) {
  if (run.status === "running") {
    return (
      <p className="muted" role="status">
        Run {run.id} is running…
      </p>
    );
  }
  if (run.status === "failed") {
    return (
      <p className="error" role="alert">
        Run {run.id} failed: {run.error}
      </p>
    );
  }
  return (
    <div className="run-figures">
      {run.summary && <p className="muted">{run.summary}</p>}
      <table aria-label={`Figures of run ${run.id}`}>
        <thead>
          <tr>
            <th>Abuse type</th>
            <th className="num">Episodes</th>
            <th className="num">Detected</th>
            <th className="num">Alerts</th>
            <th className="num">Recall</th>
            <th className="num">Precision</th>
            <th className="num">F1</th>
            <th className="num">Median time to detect</th>
          </tr>
        </thead>
        <tbody>
          {run.types.map((t) => (
            <tr key={t.abuse_type}>
              <td>{typeLabel(t.abuse_type)}</td>
              <td className="num">{fmtNumber(t.episodes)}</td>
              <td className="num">{fmtNumber(t.detected)}</td>
              <td className="num">{fmtNumber(t.alerts)}</td>
              <td className="num">{fmtRatio(t.recall)}</td>
              <td className="num">{fmtRatio(t.precision)}</td>
              <td className="num">{fmtRatio(t.f1)}</td>
              <td className="num">{fmtTTD(t.median_ttd_seconds)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
