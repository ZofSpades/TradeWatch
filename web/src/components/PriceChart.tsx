import type { EvidenceEvent, PricePoint } from "../api/types";
import { fmtPrice, fmtTime } from "../format";

interface Props {
  points: PricePoint[];
  events: EvidenceEvent[];
  windowStart: string;
  windowEnd: string;
}

const W = 640;
const H = 220;
const PAD = { left: 64, right: 16, top: 12, bottom: 28 };

/**
 * PriceChart draws an alert's market context: the last trade price and the
 * best bid and offer over time, the alert's window shaded, and the orders
 * and trades of its evidence as markers (up for buys, down for sells).
 */
export function PriceChart({ points, events, windowStart, windowEnd }: Props) {
  const times = points.map((p) => new Date(p.time).getTime());
  const prices = points.flatMap((p) => [p.last_price, p.best_bid, p.best_ask]).filter((v): v is number => !!v);
  for (const e of events) if (e.price) prices.push(e.price);
  if (points.length < 2 || prices.length === 0) {
    return <p className="muted">Not enough market data around this alert to draw a chart.</p>;
  }
  const t0 = Math.min(...times);
  const t1 = Math.max(...times);
  let lo = Math.min(...prices);
  let hi = Math.max(...prices);
  if (hi === lo) {
    hi += 100;
    lo -= 100;
  }
  const margin = (hi - lo) * 0.08;
  lo -= margin;
  hi += margin;
  const x = (t: number) => PAD.left + ((t - t0) / Math.max(1, t1 - t0)) * (W - PAD.left - PAD.right);
  const y = (p: number) => PAD.top + (1 - (p - lo) / (hi - lo)) * (H - PAD.top - PAD.bottom);
  const line = (pick: (p: PricePoint) => number | undefined) =>
    points
      .filter((p) => pick(p))
      .map((p, i) => `${i === 0 ? "M" : "L"}${x(new Date(p.time).getTime()).toFixed(1)} ${y(pick(p)!).toFixed(1)}`)
      .join(" ");
  const ws = Math.max(t0, new Date(windowStart).getTime());
  const we = Math.min(t1, new Date(windowEnd).getTime());
  const ticks = [lo + (hi - lo) * 0.1, (lo + hi) / 2, hi - (hi - lo) * 0.1];
  const markers = events.filter((e) => e.price && new Date(e.time).getTime() >= t0 && new Date(e.time).getTime() <= t1);

  return (
    <svg className="price-chart" viewBox={`0 0 ${W} ${H}`} role="img" aria-label="Price around the alert">
      {we > ws && (
        <rect className="chart-window" x={x(ws)} y={PAD.top} width={Math.max(2, x(we) - x(ws))} height={H - PAD.top - PAD.bottom} />
      )}
      {ticks.map((p) => (
        <g key={p}>
          <line className="chart-grid" x1={PAD.left} x2={W - PAD.right} y1={y(p)} y2={y(p)} />
          <text className="chart-label" x={PAD.left - 6} y={y(p)} textAnchor="end" dominantBaseline="middle">
            {fmtPrice(Math.round(p))}
          </text>
        </g>
      ))}
      <text className="chart-label" x={PAD.left} y={H - 8}>
        {fmtTime(new Date(t0).toISOString())}
      </text>
      <text className="chart-label" x={W - PAD.right} y={H - 8} textAnchor="end">
        {fmtTime(new Date(t1).toISOString())}
      </text>
      <path className="chart-bid" d={line((p) => p.best_bid)} />
      <path className="chart-ask" d={line((p) => p.best_ask)} />
      <path className="chart-last" d={line((p) => p.last_price)} />
      {markers.map((e, i) => {
        const cx = x(new Date(e.time).getTime());
        const cy = y(e.price!);
        const up = e.side !== "sell";
        const label = `${e.kind} ${e.side ?? ""} ${e.quantity} @ ${fmtPrice(e.price)} (${e.role})`;
        return e.kind === "trade" ? (
          <circle key={i} className={`chart-marker ${up ? "buy" : "sell"}`} cx={cx} cy={cy} r={4}>
            <title>{label}</title>
          </circle>
        ) : (
          <path
            key={i}
            className={`chart-marker ${up ? "buy" : "sell"} ${e.kind}`}
            d={up ? `M${cx} ${cy - 5} l5 8 h-10 z` : `M${cx} ${cy + 5} l5 -8 h-10 z`}
          >
            <title>{label}</title>
          </path>
        );
      })}
    </svg>
  );
}
