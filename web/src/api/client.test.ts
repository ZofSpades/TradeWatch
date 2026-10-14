import { describe, expect, it, vi } from "vitest";

import { fakeApi } from "../test/fakeApi";
import { ApiError, makeClient, toQuery } from "./client";

describe("toQuery", () => {
  it("leaves out empty filters", () => {
    expect(toQuery({ status: "open", abuse_type: "", symbol: "QZAP", min_score: undefined, limit: 50 })).toBe(
      "status=open&symbol=QZAP&limit=50",
    );
  });
});

describe("makeClient", () => {
  it("sends the bearer token and decodes JSON", async () => {
    const api = fakeApi({ "GET /api/v1/stats": { total: 3 } });
    const client = makeClient(() => "tok");
    await expect(client.stats()).resolves.toEqual({ total: 3 });
    const init = api.fetchMock.mock.calls[0]![1] as RequestInit;
    expect((init.headers as Record<string, string>).Authorization).toBe("Bearer tok");
  });

  it("raises the problem detail as an ApiError", async () => {
    fakeApi({ "POST /api/v1/alerts/k/transitions": () => ({ status: 409, body: { detail: "an alert cannot move from open to escalated" } }) });
    const err = await makeClient(() => "tok")
      .transition("k", "escalated", "why")
      .catch((e: unknown) => e);
    expect(err).toBeInstanceOf(ApiError);
    expect((err as ApiError).status).toBe(409);
    expect((err as ApiError).message).toBe("an alert cannot move from open to escalated");
  });

  it("ends the session when the API rejects the token", async () => {
    fakeApi({ "GET /api/v1/stats": () => ({ status: 401, body: { detail: "expired" } }) });
    const onUnauthorized = vi.fn();
    await makeClient(() => "tok", onUnauthorized)
      .stats()
      .catch(() => {});
    expect(onUnauthorized).toHaveBeenCalledOnce();
  });
});
