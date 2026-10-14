import { useCallback, useEffect, useState } from "react";

import type { AlertDetail as Detail, AlertSummary, AuditEntry, Status } from "../api/types";
import { useClient } from "../auth";
import { Score, SeverityBadge, StatusBadge } from "../components/Badges";
import { NoteDialog } from "../components/NoteDialog";
import { PriceChart } from "../components/PriceChart";
import { fmtAgo, fmtDateTime, fmtNumber, fmtPrice, fmtTime, needsNote, nextStatuses, typeLabel } from "../format";
import { useShortcuts } from "../keyboard";
import { useLiveEvents } from "../live";

type Pending = { kind: "transition"; to: Status } | { kind: "comment" };

const ACTION_LABELS: Record<string, string> = { triaged: "Triage", escalated: "Escalate", dismissed: "Dismiss" };
const ACTION_KEYS: Record<string, string> = { triaged: "t", escalated: "e", dismissed: "d" };

interface Props {
  alertKey: string;
  /** onChanged reports a status change made here, so lists can update. */
  onChanged: (a: AlertSummary) => void;
}

export function AlertDetail({ alertKey, onChanged }: Props) {
  const client = useClient();
  const [alert, setAlert] = useState<Detail | null>(null);
  const [audit, setAudit] = useState<AuditEntry[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [pending, setPending] = useState<Pending | null>(null);
  const [busy, setBusy] = useState(false);

  const load = useCallback(async () => {
    try {
      const [a, trail] = await Promise.all([client.alert(alertKey), client.audit(alertKey)]);
      setAlert(a);
      setAudit(trail.entries);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  }, [client, alertKey]);

  useEffect(() => {
    setAlert(null);
    void load();
  }, [load]);

  // Someone else changed this alert: show the new state.
  useLiveEvents((e) => {
    if (e.type === "alert.updated" && e.alert.key === alertKey) void load();
  });

  const transition = useCallback(
    async (to: Status, note?: string) => {
      const updated = await client.transition(alertKey, to, note);
      onChanged(updated);
      await load();
    },
    [client, alertKey, onChanged, load],
  );

  const start = useCallback(
    (to: Status) => {
      if (!alert || !nextStatuses(alert.status).includes(to)) return;
      if (needsNote(to)) {
        setPending({ kind: "transition", to });
        return;
      }
      setBusy(true);
      transition(to)
        .catch((err: unknown) => setError(err instanceof Error ? err.message : String(err)))
        .finally(() => setBusy(false));
    },
    [alert, transition],
  );

  useShortcuts(
    {
      t: () => start("triaged"),
      e: () => start("escalated"),
      d: () => start("dismissed"),
      c: () => alert && setPending({ kind: "comment" }),
    },
    pending === null,
  );

  if (error && !alert) {
    return (
      <p className="error" role="alert">
        Could not load the alert: {error}
      </p>
    );
  }
  if (!alert) return <p className="muted">Loading…</p>;

  const next = nextStatuses(alert.status);
  const ev = alert.evidence;
  return (
    <article className="alert-detail" aria-labelledby="alert-title">
      <header className="detail-header">
        <div>
          <h2 id="alert-title">
            {typeLabel(alert.abuse_type)} · {alert.symbol}
          </h2>
          <p className="muted">
            {alert.rule_id} v{alert.rule_version} · {alert.taxonomy_code} · session {alert.session_id}
          </p>
        </div>
        <div className="detail-badges">
          <Score score={alert.score} />
          <SeverityBadge severity={alert.severity} />
          <StatusBadge status={alert.status} />
        </div>
      </header>

      <p className="explanation">{alert.explanation}</p>

      <div className="actions" role="group" aria-label="Workflow">
        {(["triaged", "escalated", "dismissed"] as Status[]).map((to) => (
          <button
            key={to}
            type="button"
            className={to === "escalated" ? "danger" : to === "triaged" ? "primary" : undefined}
            disabled={busy || !next.includes(to)}
            onClick={() => start(to)}
            aria-keyshortcuts={ACTION_KEYS[to]}
          >
            {ACTION_LABELS[to]} <kbd>{ACTION_KEYS[to]}</kbd>
          </button>
        ))}
        <button type="button" onClick={() => setPending({ kind: "comment" })} aria-keyshortcuts="c">
          Comment <kbd>c</kbd>
        </button>
      </div>
      {error && (
        <p className="error" role="alert">
          {error}
        </p>
      )}

      <section aria-labelledby="why">
        <h3 id="why">Why it fired</h3>
        <table className="features">
          <thead>
            <tr>
              <th>Feature</th>
              <th className="num">Value</th>
              <th className="num">Threshold</th>
              <th>Result</th>
            </tr>
          </thead>
          <tbody>
            {ev.features.map((f) => (
              <tr key={f.name}>
                <td>{f.name.replace(/_/g, " ")}</td>
                <td className="num">
                  {fmtNumber(f.value)} {f.unit}
                </td>
                <td className="num">{f.op ? `${f.op} ${fmtNumber(f.threshold ?? 0)}` : "–"}</td>
                <td>{f.op ? f.breached ? <span className="ok">met</span> : <span className="error">not met</span> : <span className="muted">context</span>}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>

      <section aria-labelledby="market">
        <h3 id="market">Market around the alert</h3>
        <PriceChart points={ev.price_context} events={ev.timeline} windowStart={alert.window_start} windowEnd={alert.window_end} />
        <p className="muted legend">
          <span className="swatch last" /> last price <span className="swatch bid" /> best bid <span className="swatch ask" /> best
          offer · shaded: the alert's window · markers: its orders (triangles) and trades (dots)
        </p>
      </section>

      <section aria-labelledby="timeline">
        <h3 id="timeline">Evidence</h3>
        <dl className="facts">
          <dt>Accounts</dt>
          <dd>{alert.account_ids.join(", ")}</dd>
          <dt>Beneficial owners</dt>
          <dd>{alert.beneficial_owner_ids.join(", ") || "–"}</dd>
          <dt>Window</dt>
          <dd>
            {fmtTime(alert.window_start)} – {fmtTime(alert.window_end)} (simulated time)
          </dd>
          <dt>Detected</dt>
          <dd>
            {fmtDateTime(alert.detected_at)}, stored {fmtAgo(alert.first_seen_at)}
            {alert.deliveries > 1 && ` · delivered ${alert.deliveries} times, duplicates absorbed`}
          </dd>
          {alert.trace_id && (
            <>
              <dt>Trace</dt>
              <dd>
                {alert.trace_url ? (
                  <a href={alert.trace_url} target="_blank" rel="noopener noreferrer">
                    Open in Jaeger ↗
                  </a>
                ) : (
                  <code>{alert.trace_id}</code>
                )}
              </dd>
            </>
          )}
        </dl>
        <div className="table-scroll">
          <table className="timeline">
            <thead>
              <tr>
                <th>Time</th>
                <th>Event</th>
                <th>Role</th>
                <th>Side</th>
                <th className="num">Qty</th>
                <th className="num">Price</th>
                <th>Account</th>
                <th>Counterparty</th>
              </tr>
            </thead>
            <tbody>
              {ev.timeline.map((e, i) => (
                <tr key={i}>
                  <td>{fmtTime(e.time)}</td>
                  <td>
                    {e.kind} {e.trade_id ? `#${e.trade_id}` : e.order_id ? `#${e.order_id}` : ""}
                  </td>
                  <td>{e.role.replace(/_/g, " ")}</td>
                  <td>{e.side ?? "–"}</td>
                  <td className="num">{e.quantity ? fmtNumber(e.quantity) : "–"}</td>
                  <td className="num">{fmtPrice(e.price)}</td>
                  <td>{e.account_id ?? "–"}</td>
                  <td>{e.counterparty_account_id ?? "–"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section aria-labelledby="audit">
        <h3 id="audit">History</h3>
        <ol className="audit">
          {audit.map((a) => (
            <li key={a.id}>
              <span className="muted">{fmtAgo(a.at)}</span> <strong>{a.actor}</strong>{" "}
              {a.action === "transition" ? (
                <>
                  moved it from <StatusBadge status={a.from_status!} /> to <StatusBadge status={a.to_status!} />
                </>
              ) : a.action === "raised" ? (
                "raised it"
              ) : (
                "commented"
              )}
              {a.note && <blockquote>{a.note}</blockquote>}
            </li>
          ))}
        </ol>
        <p className="muted digest">
          Rule digest <code>{alert.rule_digest.slice(0, 12)}</code> · alert key <code>{alert.key}</code>
        </p>
      </section>

      {pending?.kind === "transition" && (
        <NoteDialog
          title={`${ACTION_LABELS[pending.to]} this alert`}
          action={ACTION_LABELS[pending.to]!}
          required
          onSubmit={(note) => transition(pending.to, note)}
          onClose={() => setPending(null)}
        />
      )}
      {pending?.kind === "comment" && (
        <NoteDialog
          title="Comment"
          action="Add comment"
          required
          onSubmit={async (note) => {
            await client.comment(alertKey, note);
            await load();
          }}
          onClose={() => setPending(null)}
        />
      )}
    </article>
  );
}
