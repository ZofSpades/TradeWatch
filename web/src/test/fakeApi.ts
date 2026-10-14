import { vi } from "vitest";

import type { Session } from "../auth";
import type { AlertDetail, AlertSummary } from "../api/types";

export interface Call {
  method: string;
  path: string;
  body: unknown;
}

type Handler = (req: { url: URL; body: unknown }) => { status?: number; body?: unknown };

/**
 * fakeApi replaces fetch with a router over the given handlers, keyed by
 * "METHOD /path", and records every call. Unknown routes answer 404.
 */
export function fakeApi(routes: Record<string, Handler | object>) {
  const calls: Call[] = [];
  const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = new URL(String(input), "http://localhost");
    const method = init?.method ?? "GET";
    const body = init?.body ? JSON.parse(String(init.body)) : undefined;
    calls.push({ method, path: url.pathname + url.search, body });
    const route = routes[`${method} ${url.pathname}`];
    if (!route) {
      return new Response(JSON.stringify({ detail: `no route ${method} ${url.pathname}` }), { status: 404 });
    }
    const out = typeof route === "function" ? (route as Handler)({ url, body }) : { body: route };
    return new Response(JSON.stringify(out.body ?? {}), {
      status: out.status ?? 200,
      headers: { "Content-Type": "application/json" },
    });
  });
  vi.stubGlobal("fetch", fetchMock);
  return { calls, fetchMock };
}

export const session: Session = { token: "t0ken", username: "ana", role: "analyst", expiresAt: Date.now() + 3_600_000 };

export function summary(i: number, patch: Partial<AlertSummary> = {}): AlertSummary {
  const key = i.toString(16).padStart(32, "0");
  return {
    key,
    session_id: "live-1",
    symbol: "QZAP",
    abuse_type: "spoofing",
    severity: "high",
    score: 60 + i,
    rule_id: "TW-SPOOF-001",
    rule_version: 1,
    taxonomy_code: "MAR-SPOOF",
    account_ids: ["ACC-0001"],
    beneficial_owner_ids: ["BO-0001"],
    window_start: "2026-10-14T10:00:00Z",
    window_end: "2026-10-14T10:00:05Z",
    detected_at: `2026-10-14T10:00:${String(10 + i).padStart(2, "0")}Z`,
    explanation: `Account ACC-0001 cancelled ${i} large buy orders.`,
    status: "open",
    deliveries: 1,
    first_seen_at: "2026-10-14T10:00:20Z",
    status_changed_at: "2026-10-14T10:00:20Z",
    ...patch,
  };
}

export function detail(i: number, patch: Partial<AlertDetail> = {}): AlertDetail {
  return {
    ...summary(i),
    detector: "spoofing",
    rule_digest: "abcdef0123456789",
    last_seen_at: "2026-10-14T10:00:20Z",
    evidence: {
      order_ids: [1, 2],
      trade_ids: [7],
      features: [
        { name: "layers", value: 3, threshold: 2, op: ">=", breached: true, unit: "orders" },
        { name: "opposite_fill_qty", value: 400, breached: false, unit: "shares" },
      ],
      timeline: [
        { time: "2026-10-14T10:00:01Z", kind: "order", order_id: 1, account_id: "ACC-0001", side: "buy", price: 1_000_000, quantity: 5000, role: "layer" },
        { time: "2026-10-14T10:00:04Z", kind: "trade", trade_id: 7, account_id: "ACC-0001", counterparty_account_id: "ACC-0002", side: "sell", price: 1_000_100, quantity: 400, role: "genuine_fill" },
      ],
      price_context: [
        { time: "2026-10-14T09:59:58Z", last_price: 1_000_000, best_bid: 999_900, best_ask: 1_000_100, bid_size: 800, ask_size: 700 },
        { time: "2026-10-14T10:00:03Z", last_price: 1_000_100, best_bid: 1_000_000, best_ask: 1_000_200, bid_size: 900, ask_size: 600 },
        { time: "2026-10-14T10:00:08Z", last_price: 1_000_000, best_bid: 999_900, best_ask: 1_000_100, bid_size: 700, ask_size: 800 },
      ],
    },
    ...patch,
  };
}
