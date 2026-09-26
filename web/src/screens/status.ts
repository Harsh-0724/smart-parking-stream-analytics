import type { BadgeKind } from "../ui";

/** Occupancy state used across screens; thresholds mirror the alerter (raise 90%). */
export function lotState(pct: number | null): { kind: BadgeKind; label: string } {
  if (pct == null) return { kind: "neutral", label: "No data" };
  if (pct >= 90) return { kind: "crit", label: "Full" };
  if (pct >= 75) return { kind: "warn", label: "Filling" };
  return { kind: "free", label: "Space" };
}
