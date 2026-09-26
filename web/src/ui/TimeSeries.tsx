import { useEffect, useRef, useState } from "react";
import { fmtClock } from "../format";

export interface SeriesPoint {
  t: number; // epoch ms
  v: number; // percent, 0-100
  closed: boolean; // closed windows are final; an open window may still change
  detail: string; // shown in the readout while hovering
}

interface Props {
  points: SeriesPoint[];
  label: string;
  threshold?: number;
  height?: number;
}

const M = { left: 32, right: 48, top: 8, bottom: 22 };

function useWidth() {
  const ref = useRef<HTMLDivElement>(null);
  const [width, setWidth] = useState(640);
  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    const ro = new ResizeObserver(([entry]) => entry && setWidth(Math.max(280, entry.contentRect.width)));
    ro.observe(el);
    return () => ro.disconnect();
  }, []);
  return [ref, width] as const;
}

/**
 * Hand-built line chart: thin line, muted axes, no legend, no fill. Closed windows are drawn
 * solid; the trailing open window is dashed with a hollow marker because it can still change.
 */
export function TimeSeries({ points, label, threshold = 90, height = 220 }: Props) {
  const [ref, width] = useWidth();
  const [hover, setHover] = useState<number | null>(null);
  const first = points[0];
  const last = points[points.length - 1];
  if (!first || !last || points.length < 2) return <div ref={ref} />;

  const t0 = first.t;
  const t1 = last.t;
  const w = width - M.left - M.right;
  const h = height - M.top - M.bottom;
  const x = (t: number) => M.left + ((t - t0) / (t1 - t0 || 1)) * w;
  const y = (v: number) => M.top + h - (Math.min(100, Math.max(0, v)) / 100) * h;

  let lastClosed = -1;
  points.forEach((p, i) => p.closed && (lastClosed = i));
  const solid = points.slice(0, lastClosed + 1);
  const tail = points.slice(Math.max(0, lastClosed));
  const path = (ps: SeriesPoint[]) => ps.map((p, i) => `${i ? "L" : "M"}${x(p.t).toFixed(1)} ${y(p.v).toFixed(1)}`).join("");

  const ticks = Array.from({ length: 6 }, (_, i) => t0 + ((t1 - t0) * i) / 5);
  const active = (hover != null ? points[hover] : undefined) ?? last;

  const onMove = (e: React.PointerEvent<SVGSVGElement>) => {
    const rect = e.currentTarget.getBoundingClientRect();
    const t = t0 + ((e.clientX - rect.left - M.left) / w) * (t1 - t0);
    let best = 0;
    for (let i = 1; i < points.length; i++) {
      if (Math.abs((points[i]?.t ?? 0) - t) < Math.abs((points[best]?.t ?? 0) - t)) best = i;
    }
    setHover(best);
  };

  return (
    <div ref={ref} className="ts">
      <div className="ts__readout num" aria-live="off">
        <span className="ts__readout-label">{label}</span>
        <span>{fmtClock(active.t)}</span>
        <span>{active.detail}</span>
        {!active.closed && <span className="ts__open">open window</span>}
      </div>
      <svg
        width={width}
        height={height}
        role="img"
        aria-label={`${label}, ${points.length} windows, latest ${last.v.toFixed(0)}%`}
        onPointerMove={onMove}
        onPointerLeave={() => setHover(null)}
      >
        {[0, 25, 50, 75, 100].map((g) => (
          <g key={g}>
            <line x1={M.left} x2={M.left + w} y1={y(g)} y2={y(g)} className="ts__grid" />
            <text x={M.left - 6} y={y(g) + 4} textAnchor="end" className="ts__axis">
              {g}
            </text>
          </g>
        ))}
        <line x1={M.left} x2={M.left + w} y1={y(threshold)} y2={y(threshold)} className="ts__threshold" />
        <text x={M.left + w + 6} y={y(threshold) + 4} className="ts__threshold-label">
          full {threshold}%
        </text>
        {ticks.map((t) => (
          <text key={t} x={x(t)} y={height - 6} textAnchor="middle" className="ts__axis">
            {fmtClock(t)}
          </text>
        ))}
        {solid.length > 1 && <path d={path(solid)} className="ts__line" />}
        {tail.length > 1 && <path d={path(tail)} className="ts__line ts__line--open" />}
        {!last.closed && <circle cx={x(last.t)} cy={y(last.v)} r="3.5" className="ts__marker ts__marker--open" />}
        {last.closed && <circle cx={x(last.t)} cy={y(last.v)} r="3" className="ts__marker" />}
        <text x={x(last.t) + 8} y={y(last.v) + 4} className="ts__endlabel">
          {last.v.toFixed(0)}%
        </text>
        {hover != null && (
          <g>
            <line x1={x(active.t)} x2={x(active.t)} y1={M.top} y2={M.top + h} className="ts__cross" />
            <circle cx={x(active.t)} cy={y(active.v)} r="3" className="ts__marker" />
          </g>
        )}
      </svg>
    </div>
  );
}
