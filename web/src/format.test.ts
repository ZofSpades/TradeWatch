import { describe, expect, it } from "vitest";

import { fmtBytes, fmtDuration, fmtRate, fmtRatio, fmtTTD } from "./format";

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

describe("fmtRatio", () => {
  it("shows two decimals, or a dash when undefined", () => {
    expect(fmtRatio(0.9312)).toBe("0.93");
    expect(fmtRatio(0)).toBe("0.00");
    expect(fmtRatio(null)).toBe("–");
  });
});

describe("fmtBytes", () => {
  it("scales the unit", () => {
    expect(fmtBytes(512)).toBe("512 B");
    expect(fmtBytes(3_500_000)).toBe("3.3 MB");
    expect(fmtBytes(250 * 1024 * 1024)).toBe("250 MB");
  });
});

describe("fmtTTD", () => {
  it("shows whole seconds, and milliseconds under a second", () => {
    expect(fmtTTD(0)).toBe("0 s");
    expect(fmtTTD(0.4)).toBe("400 ms");
    expect(fmtTTD(46.2)).toBe("46 s");
    expect(fmtTTD(null)).toBe("–");
  });
});
