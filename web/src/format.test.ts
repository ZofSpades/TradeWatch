import { describe, expect, it } from "vitest";

import { fmtDuration, fmtRate } from "./format";

describe("fmtDuration", () => {
  it("picks a unit that suits the duration", () => {
    expect(fmtDuration(0.000005)).toBe("5 µs");
    expect(fmtDuration(0.0042)).toBe("4.2 ms");
    expect(fmtDuration(0.071)).toBe("71 ms");
    expect(fmtDuration(1.234)).toBe("1.23 s");
    expect(fmtDuration(32.77)).toBe("32.8 s");
  });

  it("shows a dash without data", () => {
    expect(fmtDuration(null)).toBe("–");
    expect(fmtDuration(undefined)).toBe("–");
  });
});

describe("fmtRate", () => {
  it("keeps one decimal for small rates and rounds the rest", () => {
    expect(fmtRate(0)).toBe("0");
    expect(fmtRate(6.53)).toBe("6.5");
    expect(fmtRate(1500.5)).toBe("1,501");
    expect(fmtRate(null)).toBe("–");
  });
});
