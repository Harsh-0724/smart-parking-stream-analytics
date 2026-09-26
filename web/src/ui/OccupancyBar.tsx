interface Props {
  pct: number | null;
  full?: number;
}

/** Track + fill. At or above the full threshold the fill switches to the critical colour. */
export function OccupancyBar({ pct, full = 90 }: Props) {
  const value = Math.max(0, Math.min(100, pct ?? 0));
  const isFull = pct != null && pct >= full;
  return (
    <div
      className={`obar${isFull ? " obar--full" : ""}`}
      role="meter"
      aria-valuemin={0}
      aria-valuemax={100}
      aria-valuenow={Math.round(value)}
      aria-label="occupancy"
    >
      <div className="obar__fill" style={{ width: `${value}%` }} />
    </div>
  );
}
