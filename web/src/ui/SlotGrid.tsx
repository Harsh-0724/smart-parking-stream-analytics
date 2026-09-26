import { memo, useEffect, useRef, useState } from "react";

export type SlotStatus = boolean | null | undefined; // true occupied, false free, null/undefined unknown

interface Props {
  slotIds: string[];
  columns: number;
  states: ReadonlyMap<string, boolean>;
  size?: number;
  gap?: number;
}

const FLASH_MS = 700;

const Slot = memo(function Slot({
  x,
  y,
  size,
  id,
  status,
}: {
  x: number;
  y: number;
  size: number;
  id: string;
  status: SlotStatus;
}) {
  const [flash, setFlash] = useState(false);
  const previous = useRef(status);
  useEffect(() => {
    if (previous.current === status) return;
    const firstReport = previous.current === undefined || previous.current === null;
    previous.current = status;
    if (firstReport) return;
    setFlash(true);
    const t = setTimeout(() => setFlash(false), FLASH_MS);
    return () => clearTimeout(t);
  }, [status]);
  const kind = status === true ? "occupied" : status === false ? "free" : "unknown";
  return (
    <rect
      x={x}
      y={y}
      width={size}
      height={size}
      rx="2"
      className={`slot slot--${kind}${flash ? " is-flash" : ""}`}
      data-slot={id}
      data-status={kind}
    >
      <title>{`${id}: ${kind === "unknown" ? "no report yet" : kind}`}</title>
    </rect>
  );
});

/**
 * Live floor plan. Occupied = filled square, free = outlined square, unknown = dashed outline,
 * so state never depends on colour. Slots are memoised: an event re-renders one <rect>, not 500.
 */
export function SlotGrid({ slotIds, columns, states, size = 16, gap = 4 }: Props) {
  const rows = Math.ceil(slotIds.length / columns);
  const labelW = 20;
  const width = labelW + columns * (size + gap);
  const height = rows * (size + gap);
  let occupied = 0;
  for (const id of slotIds) if (states.get(id) === true) occupied++;
  return (
    <svg
      className="slotgrid"
      viewBox={`0 0 ${width} ${height}`}
      width={width}
      height={height}
      role="img"
      aria-label={`Floor plan: ${occupied} of ${slotIds.length} slots occupied`}
    >
      {Array.from({ length: rows }, (_, r) => (
        <text key={r} x="0" y={r * (size + gap) + size - 3} className="slotgrid__row">
          {slotIds[r * columns]?.split("-")[0] ?? ""}
        </text>
      ))}
      {slotIds.map((id, i) => (
        <Slot
          key={id}
          id={id}
          size={size}
          x={labelW + (i % columns) * (size + gap)}
          y={Math.floor(i / columns) * (size + gap)}
          status={states.get(id)}
        />
      ))}
    </svg>
  );
}
