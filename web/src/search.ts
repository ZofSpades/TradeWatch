import type { AlertSummary } from "./api/types";

/**
 * matchesSearch mirrors the API's `q` filter, so that live arrivals follow
 * the search: an account, owner, symbol or rule ID ignoring case, a session
 * ID exactly, or the start of an alert key.
 */
export function matchesSearch(q: string, a: AlertSummary): boolean {
  const s = q.trim();
  if (s === "") return true;
  const up = s.toUpperCase();
  return (
    a.account_ids.includes(up) ||
    a.beneficial_owner_ids.includes(up) ||
    a.symbol === up ||
    a.rule_id.toUpperCase() === up ||
    a.session_id === s ||
    a.key.startsWith(s.toLowerCase())
  );
}
