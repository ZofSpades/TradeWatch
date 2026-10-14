import type { Severity, Status } from "../api/types";
import { statusLabel } from "../format";

export function SeverityBadge({ severity }: { severity: Severity }) {
  return <span className={`badge severity-${severity}`}>{severity}</span>;
}

export function StatusBadge({ status }: { status: Status }) {
  return <span className={`badge status-${status}`}>{statusLabel(status)}</span>;
}

/** Score is the triage order from 50 (at the thresholds) to 100. */
export function Score({ score }: { score: number }) {
  const level = score >= 90 ? "high" : score >= 70 ? "mid" : "low";
  return (
    <span className={`score score-${level}`} title="Triage score: 50 at the thresholds, 100 at twice past every one">
      {score.toFixed(0)}
    </span>
  );
}
