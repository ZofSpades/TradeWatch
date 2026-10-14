// Convenient names for the schemas generated from the API's OpenAPI document
// (src/api/schema.d.ts, regenerated with `npm run gen:api`).
import type { components } from "./schema";

type S = components["schemas"];

export type AlertSummary = S["AlertSummary"];
export type AlertDetail = S["AlertDetail"];
export type AlertPage = S["AlertPage"];
export type AuditEntry = S["AuditEntry"];
export type Status = S["Status"];
export type AbuseType = S["AbuseType"];
export type Severity = S["Severity"];
export type Stats = S["Stats"];
export type LiveView = S["LiveView"];
export type RuleSet = S["RuleSet"];
export type RuleHistory = S["RuleHistory"];
export type RuleVersion = S["RuleVersion"];
export type SimStatus = S["SimStatus"];
export type SimInjection = S["SimInjection"];
export type Feature = S["Feature"];
export type EvidenceEvent = S["EvidenceEvent"];
export type PricePoint = S["PricePoint"];
export type LoginResponse = S["LoginResponse"];
export type Role = S["Role"];

export const ABUSE_TYPES: AbuseType[] = [
  "wash_trading",
  "spoofing",
  "momentum_ignition",
  "quote_stuffing",
  "marking_the_close",
];

export const STATUSES: Status[] = ["open", "triaged", "escalated", "dismissed"];
