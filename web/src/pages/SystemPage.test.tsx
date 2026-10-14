import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";

import type { SystemStatus } from "../api/types";
import { fakeApi } from "../test/fakeApi";
import { renderWithSession } from "../test/render";
import { SystemPage } from "./SystemPage";

const steps = (f: (i: number) => number | null) => Array.from({ length: 61 }, (_, i) => f(i));

function status(): SystemStatus {
  return {
    generated_at: "2026-10-14T12:00:00Z",
    throughput: { published: 1500.5, released: 1499.2, processed: 1498, alerts_per_minute: 6.5 },
    consumer_lag: { ingest: 0, detector: 1234, alertsink: null },
    latency: {
      event_to_detector: { p50: 0.0042, p95: 0.012, p99: 0.031 },
      detector_processing: { p50: 0.000005, p95: 0.00002, p99: 0.00004 },
      event_to_stored_alert: { p50: null, p95: null, p99: null },
    },
    dead_letters: {
      last_hour: 44,
      by_reason: [
        { stage: "ingest", reason: "invalid_field", count: 31 },
        { stage: "ingest", reason: "decode_error", count: 12 },
        { stage: "alertsink", reason: "store_rejected", count: 1 },
      ],
    },
    services: [
      { name: "marketsim", instances: 1, up: 1 },
      { name: "detector", instances: 2, up: 1 },
      { name: "api", instances: 0, up: 0 },
    ],
    history: {
      start: "2026-10-14T11:30:00Z",
      step_seconds: 30,
      published: steps((i) => 1000 + i),
      processed: steps((i) => (i < 5 ? null : 990 + i)),
      detector_lag: steps(() => null),
      event_to_detector_p95: steps(() => 0.01),
    },
    links: { grafana: "http://localhost:3001/d/tradewatch-pipeline" },
  };
}

const card = (name: string) => screen.getByRole("region", { name });

describe("SystemPage", () => {
  it("shows throughput, lag, latency, dead letters and services", async () => {
    fakeApi({ "GET /api/v1/system": status() });
    renderWithSession(<SystemPage />);

    const tp = await screen.findByRole("region", { name: "Throughput" });
    expect(tp).toHaveTextContent("Published by marketsim1,501 events/s");
    expect(tp).toHaveTextContent("Released by ingest1,499 events/s");
    expect(tp).toHaveTextContent("Alerts stored6.5 per minute");
    expect(within(tp).getByRole("img", { name: /Events per second over the last 30 minutes, peak 1,060 events\/s/ })).toBeInTheDocument();

    // A group without data shows a dash, and a series without data says so.
    const lag = card("Consumer lag");
    expect(lag).toHaveTextContent("ingest0 records");
    expect(lag).toHaveTextContent("detector1,234 records");
    expect(lag).toHaveTextContent("alertsink– records");
    expect(within(lag).getByText("No data in the last 30 minutes.")).toBeInTheDocument();

    const rows = within(card("Latency")).getAllByRole("row");
    expect(rows[1]).toHaveTextContent(/Published → processed by a detector.*4\.2 ms12 ms31 ms/);
    expect(rows[2]).toHaveTextContent(/Published → alert stored in Postgres.*–––/);
    expect(rows[3]).toHaveTextContent(/Detector time per event.*5 µs20 µs40 µs/);

    const dlq = card("Dead letters, last hour");
    expect(dlq).toHaveTextContent("44");
    expect(within(dlq).getAllByRole("row")[1]).toHaveTextContent("ingestinvalid field31");

    const services = within(card("Services")).getAllByRole("listitem");
    expect(services.map((li) => li.textContent)).toEqual(["marketsim1 of 1 up", "detector1 of 2 up", "apinot found"]);

    expect(screen.getByRole("link", { name: /Pipeline dashboard in Grafana/ })).toHaveAttribute(
      "href",
      "http://localhost:3001/d/tradewatch-pipeline",
    );
  });

  it("explains when metrics are unavailable", async () => {
    fakeApi({
      "GET /api/v1/system": () => ({ status: 503, body: { detail: "metrics are not configured (TW_API_PROMETHEUS_URL)" } }),
    });
    renderWithSession(<SystemPage />);
    expect(await screen.findByRole("alert")).toHaveTextContent(
      "Metrics are unavailable: metrics are not configured (TW_API_PROMETHEUS_URL)",
    );
    expect(screen.queryByRole("region", { name: "Throughput" })).not.toBeInTheDocument();
  });

  it("keeps the last figures when a refresh fails", async () => {
    let calls = 0;
    const { calls: log } = fakeApi({
      "GET /api/v1/system": () =>
        ++calls === 1 ? { body: status() } : { status: 503, body: { detail: "Prometheus could not be queried" } },
    });
    const user = userEvent.setup();
    renderWithSession(<SystemPage />);
    await screen.findByRole("region", { name: "Throughput" });

    await user.click(screen.getByRole("button", { name: "Refresh" }));
    expect(await screen.findByRole("alert")).toHaveTextContent(
      "Metrics are unavailable: Prometheus could not be queried The figures below are from the last successful refresh.",
    );
    expect(card("Throughput")).toHaveTextContent("1,501 events/s");
    await waitFor(() => expect(log.filter((c) => c.path === "/api/v1/system")).toHaveLength(2));
  });
});
