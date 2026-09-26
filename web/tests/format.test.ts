import { fmtDuration, fmtInt, fmtPct, fmtRate, fmtSeconds } from "../src/format";

describe("formatters", () => {
  it("formats numbers with a placeholder for missing values", () => {
    expect(fmtInt(1234.4)).toBe("1,234");
    expect(fmtInt(null)).toBe("-");
    expect(fmtPct(33.333, 1)).toBe("33.3%");
    expect(fmtRate(1200)).toBe("1.2k");
    expect(fmtRate(85)).toBe("85");
    expect(fmtSeconds(0.25)).toBe("250 ms");
    expect(fmtSeconds(3.14)).toBe("3.1 s");
  });
  it("formats durations for operators", () => {
    expect(fmtDuration(42)).toBe("42 s");
    expect(fmtDuration(6 * 60)).toBe("6 min");
    expect(fmtDuration(4020)).toBe("1 h 07 min");
    expect(fmtDuration(-5)).toBe("0 s");
  });
});
