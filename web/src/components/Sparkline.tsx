export interface SparkSeries {
  name: string;
  /** One value per step; null where there is no data. */
  values: (number | null)[];
  /** Picks the line colour: "a" or "b". */
  tone: "a" | "b";
}

interface Props {
  label: string;
  series: SparkSeries[];
  format: (v: number) => string;
}

const W = 300;
const H = 56;

/** path draws values as a line from 0 to max, broken where data is missing. */
function path(values: (number | null)[], n: number, max: number): string {
  const x = (i: number) => (i / Math.max(1, n - 1)) * W;
  const y = (v: number) => H - 2 - (v / max) * (H - 4);
  let d = "";
  let pen = false;
  values.forEach((v, i) => {
    if (v === null) {
      pen = false;
      return;
    }
    d += `${pen ? "L" : "M"}${x(i).toFixed(1)} ${y(v).toFixed(1)} `;
    pen = true;
  });
  return d.trim();
}

/**
 * Sparkline draws the recent history of one or two series, scaled from zero
 * to their peak, with a legend that names the series and the peak.
 */
export function Sparkline({ label, series, format }: Props) {
  const present = series.flatMap((s) => s.values).filter((v): v is number => v !== null);
  if (present.length === 0) {
    return <p className="muted spark-empty">No data in the last 30 minutes.</p>;
  }
  const peak = Math.max(...present);
  const max = peak > 0 ? peak : 1;
  const n = Math.max(...series.map((s) => s.values.length));
  return (
    <figure className="spark">
      <svg viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none" role="img" aria-label={`${label} over the last 30 minutes, peak ${format(peak)}`}>
        <line className="spark-base" x1={0} x2={W} y1={H - 2} y2={H - 2} />
        {series.map((s) => (
          <path key={s.name} className={`spark-line tone-${s.tone}`} d={path(s.values, n, max)} vectorEffect="non-scaling-stroke" />
        ))}
      </svg>
      <figcaption>
        {series.map((s) => (
          <span key={s.name}>
            <span className={`swatch tone-${s.tone}`} aria-hidden="true" /> {s.name}
          </span>
        ))}
        <span className="muted">
          30 min · peak {format(peak)}
        </span>
      </figcaption>
    </figure>
  );
}
