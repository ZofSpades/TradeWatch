import { useEffect, useState } from "react";

import { ABUSE_TYPES } from "../api/types";
import { useClient } from "../auth";
import { typeLabel } from "../format";
import { useLiveEvents, useStreamState } from "../live";

/** LiveBar shows the live feed's state and the running alert totals. */
export function LiveBar() {
  const client = useClient();
  const state = useStreamState();
  const [counts, setCounts] = useState<Record<string, number>>({});
  const [total, setTotal] = useState<number | null>(null);

  useEffect(() => {
    let cancelled = false;
    const refresh = () =>
      client
        .live(1)
        .then((v) => {
          if (cancelled) return;
          setCounts(v.counts.by_abuse_type);
          setTotal(v.counts.total);
        })
        .catch(() => {});
    void refresh();
    const t = setInterval(refresh, 30_000);
    return () => {
      cancelled = true;
      clearInterval(t);
    };
  }, [client]);

  useLiveEvents((e) => {
    if (e.type !== "alert.raised") return;
    setCounts((c) => ({ ...c, [e.alert.abuse_type]: (c[e.alert.abuse_type] ?? 0) + 1 }));
    setTotal((t) => (t ?? 0) + 1);
  });

  const label = state === "live" ? "Live" : state === "connecting" ? "Connecting" : "Offline";
  return (
    <div className="live-bar">
      <span className={`live-dot ${state}`} role="status" aria-live="polite">
        {label}
      </span>
      {total !== null && (
        <span className="live-total" title="Alerts stored">
          {total} alerts
        </span>
      )}
      <ul className="live-counts" aria-label="Alerts by type">
        {ABUSE_TYPES.map((t) => (
          <li key={t} title={typeLabel(t)}>
            <span className={`type-dot type-${t}`} aria-hidden="true" />
            {typeLabel(t)} <strong>{counts[t] ?? 0}</strong>
          </li>
        ))}
      </ul>
    </div>
  );
}
