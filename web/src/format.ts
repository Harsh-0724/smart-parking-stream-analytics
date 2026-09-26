const int = new Intl.NumberFormat("en-US");

export const fmtInt = (n: number | null | undefined): string => (n == null ? "-" : int.format(Math.round(n)));

export const fmtPct = (n: number | null | undefined, digits = 0): string =>
  n == null ? "-" : `${n.toFixed(digits)}%`;

export const fmtTime = (iso: string | number | Date | null | undefined): string => {
  if (iso == null) return "-";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "-";
  return d.toLocaleTimeString("en-GB", { hour: "2-digit", minute: "2-digit", second: "2-digit" });
};

export const fmtClock = (iso: string | number | Date): string =>
  new Date(iso).toLocaleTimeString("en-GB", { hour: "2-digit", minute: "2-digit" });

export const fmtDateTime = (iso: string | null | undefined): string => {
  if (!iso) return "-";
  const d = new Date(iso);
  return `${d.toLocaleDateString("en-GB", { day: "2-digit", month: "short" })} ${fmtClock(d)}`;
};

/** 75 -> "1 min", 4000 -> "1 h 7 min" */
export const fmtDuration = (seconds: number | null | undefined): string => {
  if (seconds == null) return "-";
  const s = Math.max(0, Math.round(seconds));
  if (s < 60) return `${s} s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m} min`;
  const h = Math.floor(m / 60);
  return `${h} h ${String(m % 60).padStart(2, "0")} min`;
};

export const fmtSeconds = (s: number | null | undefined): string =>
  s == null ? "-" : s < 1 ? `${Math.round(s * 1000)} ms` : `${s.toFixed(1)} s`;

export const fmtRate = (n: number | null | undefined): string =>
  n == null ? "-" : n >= 1000 ? `${(n / 1000).toFixed(1)}k` : String(Math.round(n));
