import { useCallback, useEffect, useState } from "react";

export type ThemeMode = "system" | "light" | "dark";

const read = (): ThemeMode => {
  try {
    const v = localStorage.getItem("theme");
    return v === "light" || v === "dark" ? v : "system";
  } catch {
    return "system";
  }
};

/** Follows the OS by default; the toggle stores an explicit choice. Storage may be unavailable. */
export function useTheme() {
  const [mode, setMode] = useState<ThemeMode>(read);
  useEffect(() => {
    const root = document.documentElement;
    if (mode === "system") delete root.dataset.theme;
    else root.dataset.theme = mode;
    try {
      if (mode === "system") localStorage.removeItem("theme");
      else localStorage.setItem("theme", mode);
    } catch {
      /* private window or blocked storage: the theme still applies for this session */
    }
  }, [mode]);
  const cycle = useCallback(() => setMode((m) => (m === "system" ? "light" : m === "light" ? "dark" : "system")), []);
  return { mode, cycle };
}
