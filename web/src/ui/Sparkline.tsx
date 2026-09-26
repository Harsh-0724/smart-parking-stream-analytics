interface Props {
  values: number[];
  width?: number;
  height?: number;
  min?: number;
  max?: number;
  label?: string;
}

/** Thin line, no fill. The newest point is marked in the accent colour. */
export function Sparkline({ values, width = 96, height = 24, min, max, label }: Props) {
  if (values.length < 2) {
    return (
      <svg width={width} height={height} role="img" aria-label={label ?? "no trend data yet"}>
        <line x1="0" x2={width} y1={height / 2} y2={height / 2} className="spark__empty" />
      </svg>
    );
  }
  // Autoscale to the data so the shape of the last two hours is visible, but never zoom into noise:
  // the visible range is at least 10 percentage points.
  const lo = min ?? Math.min(...values);
  const hi = max ?? Math.max(...values);
  const mid = (lo + hi) / 2;
  const half = Math.max((hi - lo) / 2, 5);
  min = Math.max(0, mid - half);
  max = min + 2 * half;
  const pad = 3;
  const x = (i: number) => pad + (i / (values.length - 1)) * (width - 2 * pad);
  const lowest = min;
  const span = max - min;
  const y = (v: number) => height - pad - ((Math.min(max, Math.max(lowest, v)) - lowest) / span) * (height - 2 * pad);
  const points = values.map((v, i) => `${x(i).toFixed(1)},${y(v).toFixed(1)}`).join(" ");
  const last = values.length - 1;
  const lastValue = values[last] ?? 0;
  return (
    <svg width={width} height={height} role="img" aria-label={label ?? `trend, latest ${lastValue.toFixed(0)}%`}>
      <polyline points={points} className="spark__line" />
      <circle cx={x(last)} cy={y(lastValue)} r="2.25" className="spark__dot" />
    </svg>
  );
}
