import type { ReactNode } from "react";

interface Props {
  label: string;
  value: ReactNode;
  unit?: string;
  sub?: ReactNode;
  size?: "xl" | "md" | "sm";
  title?: string;
}

/** A labelled number. Values are monospaced and tabular so live updates never shift the layout. */
export function Stat({ label, value, unit, sub, size = "md", title }: Props) {
  return (
    <div className={`stat stat--${size}`} title={title}>
      <div className="stat__label">{label}</div>
      <div className="stat__value num">
        {value}
        {unit && <span className="stat__unit">{unit}</span>}
      </div>
      {sub && <div className="stat__sub">{sub}</div>}
    </div>
  );
}
