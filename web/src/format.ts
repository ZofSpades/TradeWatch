import type { AbuseType, Status } from "./api/types";

/** Prices are integers in units of 1/10,000 of the quote currency. */
export function fmtPrice(units: number | undefined): string {
  if (units === undefined || units === 0) return "–";
  return (units / 10_000).toFixed(units % 100 === 0 ? 2 : 4);
}

/** A simulated market time, as hh:mm:ss in UTC. */
export function fmtTime(iso: string | undefined): string {
  if (!iso) return "–";
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? "–" : d.toISOString().slice(11, 19);
}

/** A compact simulated date and time for lists, as MM-DD hh:mm:ss in UTC. */
export function fmtShort(iso: string | undefined): string {
  if (!iso) return "–";
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? "–" : `${d.toISOString().slice(5, 10)} ${d.toISOString().slice(11, 19)}`;
}

/** A simulated market date and time, in UTC. */
export function fmtDateTime(iso: string | undefined): string {
  if (!iso) return "–";
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? "–" : `${d.toISOString().slice(0, 10)} ${d.toISOString().slice(11, 19)} UTC`;
}

/** A wall-clock time relative to now, such as "3 min ago". */
export function fmtAgo(iso: string | undefined, now = Date.now()): string {
  if (!iso) return "–";
  const s = Math.max(0, Math.round((now - new Date(iso).getTime()) / 1000));
  if (s < 60) return `${s} s ago`;
  if (s < 3600) return `${Math.floor(s / 60)} min ago`;
  if (s < 86_400) return `${Math.floor(s / 3600)} h ago`;
  return `${Math.floor(s / 86_400)} d ago`;
}

export function fmtNumber(v: number, digits = 2): string {
  return Number.isInteger(v) ? v.toLocaleString("en-US") : v.toFixed(digits);
}

const TYPE_LABELS: Record<AbuseType, string> = {
  wash_trading: "Wash trading",
  spoofing: "Spoofing",
  momentum_ignition: "Momentum ignition",
  quote_stuffing: "Quote stuffing",
  marking_the_close: "Marking the close",
};

export function typeLabel(t: string): string {
  return TYPE_LABELS[t as AbuseType] ?? t;
}

export function statusLabel(s: Status): string {
  return s.charAt(0).toUpperCase() + s.slice(1);
}

/** The workflow: which statuses an alert may move to next. */
export function nextStatuses(s: Status): Status[] {
  switch (s) {
    case "open":
      return ["triaged"];
    case "triaged":
      return ["escalated", "dismissed"];
    default:
      return [];
  }
}

/** Escalating and dismissing must record why. */
export function needsNote(s: Status): boolean {
  return s === "escalated" || s === "dismissed";
}

/** A rate or count from the metrics, or "–" when there is no data. */
export function fmtRate(v: number | null | undefined): string {
  if (v === null || v === undefined) return "–";
  if (v !== 0 && Math.abs(v) < 10) return v.toFixed(1);
  return Math.round(v).toLocaleString("en-US");
}

/** A duration given in seconds, in the unit that suits it. */
export function fmtDuration(s: number | null | undefined): string {
  if (s === null || s === undefined) return "–";
  if (s < 0.001) return `${Math.round(s * 1_000_000)} µs`;
  if (s < 0.01) return `${(s * 1000).toFixed(1)} ms`;
  if (s < 1) return `${Math.round(s * 1000)} ms`;
  return `${s.toFixed(s < 10 ? 2 : 1)} s`;
}
