import type { ReactNode } from "react";
import { Glyph, type Shape } from "./Glyph";

export type BadgeKind = "neutral" | "accent" | "free" | "occupied" | "warn" | "crit";

const SHAPES: Record<BadgeKind, Shape> = {
  neutral: "circle",
  accent: "circle",
  free: "square-outline",
  occupied: "square",
  warn: "triangle",
  crit: "diamond",
};

export function Badge({ kind = "neutral", children }: { kind?: BadgeKind; children: ReactNode }) {
  return (
    <span className={`badge badge--${kind}`}>
      <Glyph shape={SHAPES[kind]} size={9} />
      {children}
    </span>
  );
}
