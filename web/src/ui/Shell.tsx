import { Bell, LayoutList, Monitor, Moon, Sun, Workflow } from "lucide-react";
import type { ReactNode } from "react";
import { NavLink } from "react-router-dom";
import { LiveDot, type LiveStatus } from "./LiveDot";
import { useTheme } from "./theme";

const NAV = [
  { to: "/", label: "Overview", icon: LayoutList, end: true },
  { to: "/alerts", label: "Alerts", icon: Bell, end: false },
  { to: "/pipeline", label: "Pipeline", icon: Workflow, end: false },
] as const;

export function Shell({ status, alertCount, children }: { status: LiveStatus; alertCount?: number; children: ReactNode }) {
  const { mode, cycle } = useTheme();
  const ThemeIcon = mode === "light" ? Sun : mode === "dark" ? Moon : Monitor;
  return (
    <div className="shell">
      <nav className="rail" aria-label="Primary">
        <div className="rail__brand">
          <span className="rail__mark" aria-hidden="true" />
          <span className="rail__name">Parking Ops</span>
        </div>
        <ul className="rail__nav">
          {NAV.map(({ to, label, icon: Icon, end }) => (
            <li key={to}>
              <NavLink to={to} end={end} className="rail__link" title={label}>
                <Icon size={16} strokeWidth={1.5} aria-hidden="true" />
                <span className="rail__label">{label}</span>
                {to === "/alerts" && alertCount ? <span className="rail__count num">{alertCount}</span> : null}
              </NavLink>
            </li>
          ))}
        </ul>
        <div className="rail__foot">
          <LiveDot status={status} />
          <button type="button" className="rail__theme" onClick={cycle} aria-label={`Theme: ${mode}. Click to change`} title={`Theme: ${mode}`}>
            <ThemeIcon size={16} strokeWidth={1.5} aria-hidden="true" />
            <span className="rail__label">{mode}</span>
          </button>
        </div>
      </nav>
      <main className="main">{children}</main>
    </div>
  );
}
