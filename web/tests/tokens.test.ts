import { readFileSync } from "node:fs";
import { join } from "node:path";
import { accent, fontSizes, radius, space } from "../src/ui/tokens";

const read = (rel: string) => readFileSync(join(process.cwd(), rel), "utf8");
const css = read("src/ui/tokens.css");

describe("design tokens", () => {
  it("keeps tokens.ts in sync with tokens.css", () => {
    expect(css).toContain(`--accent: ${accent.light}`);
    expect(css).toContain(`--accent: ${accent.dark}`);
    Object.values(fontSizes).forEach((px, i) => expect(css).toContain(`--fs-${i + 1}: ${px}px`));
    Object.entries(space).forEach(([k, px]) => expect(css).toContain(`--sp-${k}: ${px}px`));
    expect(css).toContain(`--radius: ${radius.sm}px`);
  });

  it("uses a 4px spacing grid", () => {
    Object.values(space).forEach((px) => expect(px % 4).toBe(0));
  });

  it("has exactly one accent hue per theme and no gradients or shadows anywhere in the UI CSS", () => {
    const ui = read("src/ui/ui.css");
    const screens = read("src/screens/screens.css");
    for (const sheet of [css, ui, screens]) {
      expect(sheet).not.toMatch(/gradient\(/);
      expect(sheet).not.toMatch(/box-shadow|backdrop-filter|text-shadow/);
    }
    expect(css.match(/--accent:/g)).toHaveLength(3); // light, dark (media query), dark (manual)
  });
});
