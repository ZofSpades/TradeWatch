import type {
  AlertDetail,
  AlertPage,
  AlertSummary,
  AuditEntry,
  LiveView,
  LoginResponse,
  RuleHistory,
  RuleSet,
  SimInjection,
  SimStatus,
  Stats,
  Status,
} from "./types";

/** An HTTP error from the API, carrying the problem detail it returned. */
export class ApiError extends Error {
  readonly status: number;

  constructor(status: number, detail: string) {
    super(detail);
    this.name = "ApiError";
    this.status = status;
  }
}

export interface AlertQuery {
  status?: Status | "";
  abuse_type?: string;
  symbol?: string;
  account?: string;
  min_score?: number;
  limit?: number;
  cursor?: string;
}

/** Builds a query string from the set fields of q. */
export function toQuery(q: AlertQuery): string {
  const params = new URLSearchParams();
  for (const [k, v] of Object.entries(q)) {
    if (v !== undefined && v !== "" && v !== null) params.set(k, String(v));
  }
  return params.toString();
}

export type Client = ReturnType<typeof makeClient>;

/**
 * makeClient returns typed calls for every API endpoint. getToken supplies
 * the bearer token; onUnauthorized runs when the API rejects it, so the
 * session can end.
 */
export function makeClient(getToken: () => string | null, onUnauthorized: () => void = () => {}) {
  async function request<T>(method: string, path: string, body?: unknown): Promise<T> {
    const headers: Record<string, string> = { Accept: "application/json" };
    const token = getToken();
    if (token) headers.Authorization = `Bearer ${token}`;
    if (body !== undefined) headers["Content-Type"] = "application/json";
    const res = await fetch(path, {
      method,
      headers,
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    if (res.status === 401 && token) onUnauthorized();
    if (!res.ok) {
      let detail = res.statusText || `HTTP ${res.status}`;
      try {
        const problem = (await res.json()) as { detail?: string; error?: string };
        detail = problem.detail ?? problem.error ?? detail;
      } catch {
        // not a JSON body
      }
      throw new ApiError(res.status, detail);
    }
    return (await res.json()) as T;
  }

  const alertPath = (key: string) => `/api/v1/alerts/${encodeURIComponent(key)}`;

  return {
    login: (username: string, password: string) =>
      request<LoginResponse>("POST", "/api/v1/auth/login", { username, password }),
    alerts: (q: AlertQuery) => request<AlertPage>("GET", `/api/v1/alerts?${toQuery(q)}`),
    alert: (key: string) => request<AlertDetail>("GET", alertPath(key)),
    audit: (key: string) => request<{ entries: AuditEntry[] }>("GET", `${alertPath(key)}/audit`),
    transition: (key: string, to: Status, note?: string) =>
      request<AlertSummary>("POST", `${alertPath(key)}/transitions`, note ? { to, note } : { to }),
    comment: (key: string, note: string) => request<AuditEntry>("POST", `${alertPath(key)}/comments`, { note }),
    stats: () => request<Stats>("GET", "/api/v1/stats"),
    live: (limit = 20) => request<LiveView>("GET", `/api/v1/live?limit=${limit}`),
    rules: () => request<RuleSet>("GET", "/api/v1/rules"),
    ruleHistory: (id: string) => request<RuleHistory>("GET", `/api/v1/rules/${encodeURIComponent(id)}`),
    simStatus: () => request<SimStatus>("GET", "/api/v1/sim/status"),
    simStart: () => request<SimStatus>("POST", "/api/v1/sim/start"),
    simStop: () => request<SimStatus>("POST", "/api/v1/sim/stop"),
    setInjection: (injection: SimInjection) => request<SimStatus>("PUT", "/api/v1/sim/injection", injection),
    scenarios: () => request<{ scenarios: string[] }>("GET", "/api/v1/sim/scenarios"),
    trigger: (scenario: string, symbol?: string) =>
      request<{ scenario?: string; symbol?: string; status?: string }>(
        "POST",
        "/api/v1/sim/scenarios",
        symbol ? { scenario, symbol } : { scenario },
      ),
  };
}
