import { describe, expect, it } from "vitest";

import { href, parseHash } from "./router";

describe("router", () => {
  it("round-trips every page", () => {
    for (const r of [
      { page: "alerts" as const },
      { page: "alerts" as const, key: "0123456789abcdef0123456789abcdef" },
      { page: "rules" as const },
      { page: "rules" as const, id: "TW-WASH-001" },
      { page: "simulator" as const },
    ]) {
      expect(parseHash(href(r))).toEqual(r);
    }
  });

  it("falls back to the alerts page", () => {
    expect(parseHash("")).toEqual({ page: "alerts" });
    expect(parseHash("#/nowhere")).toEqual({ page: "alerts" });
  });
});
