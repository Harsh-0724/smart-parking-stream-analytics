import { readFileSync } from "node:fs";
import { join } from "node:path";

const css = readFileSync(join(process.cwd(), "src/ui/tokens.css"), "utf8");

/** Token values inside the block that starts at `marker`. */
function tokens(marker: string): Record<string, string> {
  const start = css.indexOf(marker);
  const block = css.slice(start, css.indexOf("}", start));
  return Object.fromEntries([...block.matchAll(/--([a-z0-9-]+):\s*(#[0-9a-f]{6})/gi)].map((m) => [m[1]!, m[2]!]));
}

const lum = (hex: string) => {
  const [r, g, b] = [1, 3, 5].map((i) => {
    const v = parseInt(hex.slice(i, i + 2), 16) / 255;
    return v <= 0.03928 ? v / 12.92 : ((v + 0.055) / 1.055) ** 2.4;
  }) as [number, number, number];
  return 0.2126 * r + 0.7152 * g + 0.0722 * b;
};
const ratio = (a: string, b: string) => {
  const [hi, lo] = [lum(a), lum(b)].sort((x, y) => y - x) as [number, number];
  return (hi + 0.05) / (lo + 0.05);
};

const themes = {
  light: tokens(":root,\n:root[data-theme=\"light\"]"),
  dark: tokens(':root[data-theme="dark"]'),
};

// [foreground token, background token]: every text/background pairing the UI uses
const PAIRS: [string, string][] = [
  ["text", "bg"],
  ["text", "surface"],
  ["text-2", "bg"],
  ["text-2", "surface"],
  ["text-2", "sunken"],
  ["text-3", "bg"],
  ["text-3", "surface"],
  ["text-3", "sunken"],
  ["accent", "bg"],
  ["accent", "surface"],
  ["st-warn", "bg"],
  ["st-warn", "surface"],
  ["st-crit", "bg"],
  ["st-crit", "surface"],
  ["on-accent", "accent"],
];

describe.each(Object.entries(themes))("WCAG AA contrast, %s theme", (_name, t) => {
  it.each(PAIRS)("%s on %s is at least 4.5:1", (fg, bg) => {
    expect(t[fg], `missing token --${fg}`).toBeDefined();
    expect(t[bg], `missing token --${bg}`).toBeDefined();
    expect(ratio(t[fg]!, t[bg]!)).toBeGreaterThanOrEqual(4.5);
  });

  it("slot fills are distinguishable from the background (3:1 for graphics)", () => {
    expect(ratio(t["slot-occupied"]!, t["bg"]!)).toBeGreaterThanOrEqual(3);
    expect(ratio(t["slot-free"]!, t["bg"]!)).toBeGreaterThanOrEqual(1.5); // outlines also differ by shape
  });
});
