import { useCallback, useEffect, useRef, useState } from "react";

import type { AlertQuery } from "../api/client";
import { ABUSE_TYPES, STATUSES, type AlertSummary, type Status } from "../api/types";
import { useClient } from "../auth";
import { Score, SeverityBadge, StatusBadge } from "../components/Badges";
import { fmtShort, statusLabel, typeLabel } from "../format";
import { useShortcuts } from "../keyboard";
import { useLiveEvents } from "../live";
import { AlertDetail } from "./AlertDetail";

interface Filters {
  status: Status | "";
  abuse_type: string;
  symbol: string;
  min_score: string;
}

const PAGE = 50;

function matches(f: Filters, a: AlertSummary): boolean {
  return (
    (f.status === "" || a.status === f.status) &&
    (f.abuse_type === "" || a.abuse_type === f.abuse_type) &&
    (f.symbol === "" || a.symbol === f.symbol.toUpperCase()) &&
    (f.min_score === "" || a.score >= Number(f.min_score))
  );
}

function query(f: Filters, cursor?: string): AlertQuery {
  return {
    status: f.status,
    abuse_type: f.abuse_type,
    symbol: f.symbol.toUpperCase(),
    min_score: f.min_score === "" ? undefined : Number(f.min_score),
    limit: PAGE,
    cursor,
  };
}

interface Props {
  selected?: string;
  onSelect: (key?: string) => void;
}

/** AlertsPage is the analyst's queue, with the selected alert beside it. */
export function AlertsPage({ selected, onSelect }: Props) {
  const client = useClient();
  const [filters, setFilters] = useState<Filters>({ status: "open", abuse_type: "", symbol: "", min_score: "" });
  const [alerts, setAlerts] = useState<AlertSummary[]>([]);
  const [next, setNext] = useState<string | undefined>();
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [fresh, setFresh] = useState<Set<string>>(new Set());
  const symbolRef = useRef<HTMLInputElement>(null);
  const rowRefs = useRef(new Map<string, HTMLTableRowElement>());

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const page = await client.alerts(query(filters));
      setAlerts(page.alerts);
      setNext(page.next_cursor);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  }, [client, filters]);

  useEffect(() => {
    void load();
  }, [load]);

  async function loadMore() {
    if (!next) return;
    try {
      const page = await client.alerts(query(filters, next));
      setAlerts((as) => [...as, ...page.alerts.filter((a) => !as.some((b) => b.key === a.key))]);
      setNext(page.next_cursor);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  }

  useLiveEvents((e) => {
    if (e.type === "alert.raised") {
      if (!matches(filters, e.alert)) return;
      setAlerts((as) => (as.some((a) => a.key === e.alert.key) ? as : [e.alert, ...as]));
      setFresh((s) => new Set(s).add(e.alert.key));
      setTimeout(() => setFresh((s) => {
        const n = new Set(s);
        n.delete(e.alert.key);
        return n;
      }), 6000);
    } else {
      setAlerts((as) => as.map((a) => (a.key === e.alert.key ? e.alert : a)));
    }
  });

  const onChanged = useCallback((a: AlertSummary) => {
    setAlerts((as) => as.map((b) => (b.key === a.key ? a : b)));
  }, []);

  const move = useCallback(
    (delta: number) => {
      if (alerts.length === 0) return;
      const i = alerts.findIndex((a) => a.key === selected);
      const j = i < 0 ? 0 : Math.min(alerts.length - 1, Math.max(0, i + delta));
      const key = alerts[j]!.key;
      onSelect(key);
      rowRefs.current.get(key)?.scrollIntoView?.({ block: "nearest" });
    },
    [alerts, selected, onSelect],
  );

  useShortcuts({
    j: () => move(1),
    k: () => move(-1),
    ArrowDown: () => move(1),
    ArrowUp: () => move(-1),
    r: () => void load(),
    "/": () => symbolRef.current?.focus(),
  });

  const set = (patch: Partial<Filters>) => setFilters((f) => ({ ...f, ...patch }));

  return (
    <div className="alerts-layout">
      <section className="queue" aria-label="Alert queue">
        <form className="filters" onSubmit={(e) => e.preventDefault()} aria-label="Filters">
          <div className="filter">
            <label htmlFor="filter-status">Status</label>
            <select id="filter-status" value={filters.status} onChange={(e) => set({ status: e.target.value as Status | "" })}>
              <option value="">All</option>
              {STATUSES.map((s) => (
                <option key={s} value={s}>
                  {statusLabel(s)}
                </option>
              ))}
            </select>
          </div>
          <div className="filter">
            <label htmlFor="filter-type">Type</label>
            <select id="filter-type" value={filters.abuse_type} onChange={(e) => set({ abuse_type: e.target.value })}>
              <option value="">All</option>
              {ABUSE_TYPES.map((t) => (
                <option key={t} value={t}>
                  {typeLabel(t)}
                </option>
              ))}
            </select>
          </div>
          <div className="filter">
            <label htmlFor="filter-symbol">Symbol</label>
            <input
              id="filter-symbol"
              ref={symbolRef}
              value={filters.symbol}
              placeholder="any"
              size={6}
              onChange={(e) => set({ symbol: e.target.value })}
              onKeyDown={(e) => e.key === "Escape" && (e.currentTarget as HTMLInputElement).blur()}
            />
          </div>
          <div className="filter">
            <label htmlFor="filter-score">Min score</label>
            <input
              id="filter-score"
              type="number"
              min={0}
              max={100}
              value={filters.min_score}
              placeholder="0"
              onChange={(e) => set({ min_score: e.target.value })}
            />
          </div>
        </form>

        {error && (
          <p className="error" role="alert">
            {error}
          </p>
        )}
        <div className="table-scroll">
          <table className="queue-table" aria-label="Alerts" aria-busy={loading}>
            <thead>
              <tr>
                <th className="num">Score</th>
                <th>Type</th>
                <th>Symbol</th>
                <th>Severity</th>
                <th>Status</th>
                <th>Detected (UTC)</th>
              </tr>
            </thead>
            <tbody>
              {alerts.map((a) => (
                <tr
                  key={a.key}
                  ref={(el) => {
                    if (el) rowRefs.current.set(a.key, el);
                    else rowRefs.current.delete(a.key);
                  }}
                  className={`${a.key === selected ? "selected" : ""} ${fresh.has(a.key) ? "fresh" : ""}`}
                  aria-selected={a.key === selected}
                  tabIndex={0}
                  onClick={() => onSelect(a.key)}
                  onKeyDown={(e) => e.key === "Enter" && onSelect(a.key)}
                >
                  <td className="num">
                    <Score score={a.score} />
                  </td>
                  <td>{typeLabel(a.abuse_type)}</td>
                  <td>{a.symbol}</td>
                  <td>
                    <SeverityBadge severity={a.severity} />
                  </td>
                  <td>
                    <StatusBadge status={a.status} />
                  </td>
                  <td className="nowrap">{fmtShort(a.detected_at)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        {!loading && alerts.length === 0 && <p className="muted empty">No alerts match these filters.</p>}
        {next && (
          <button type="button" className="load-more" onClick={() => void loadMore()}>
            Load more
          </button>
        )}
      </section>

      <section className="detail" aria-label="Selected alert">
        {selected ? (
          <AlertDetail key={selected} alertKey={selected} onChanged={onChanged} />
        ) : (
          <div className="placeholder">
            <p>Select an alert to see why it fired and work it.</p>
            <p className="muted">
              <kbd>j</kbd>/<kbd>k</kbd> to move, <kbd>?</kbd> for every shortcut.
            </p>
          </div>
        )}
      </section>
    </div>
  );
}
