import { describe, expect, it } from "vitest";

import { matchesSearch } from "./search";
import { summary } from "./test/fakeApi";

describe("matchesSearch", () => {
  const a = summary(26, { account_ids: ["ACC-0001", "ACC-0042"], beneficial_owner_ids: ["BO-0007"] });

  it("matches what the API's q matches", () => {
    for (const q of ["", "  ", "acc-0042", "ACC-0001", "bo-0007", "qzap", "tw-spoof-001", "live-1", "0000000000", a.key.toUpperCase()]) {
      expect(matchesSearch(q, a), q).toBe(true);
    }
  });

  it("does not match parts of identifiers or another case of the session", () => {
    for (const q of ["ACC-00", "QZ", "LIVE-1", "spoofing", "0001"]) {
      expect(matchesSearch(q, a), q).toBe(false);
    }
  });
});
