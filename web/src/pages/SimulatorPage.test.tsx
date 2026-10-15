import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";

import type { EvaluationRun } from "../api/types";
import { fakeApi, session } from "../test/fakeApi";
import { renderWithSession } from "../test/render";
import { SimulatorPage } from "./SimulatorPage";

const admin = { ...session, username: "admin", role: "admin" as const };

const running: EvaluationRun = {
  id: 7,
  kind: "replay",
  status: "running",
  source: "live-2.twrec.gz",
  requested_by: "admin",
  rules_digest: "e6297d9918c3",
  started_at: "2026-10-15T12:00:00Z",
  types: [],
};

const done: EvaluationRun = {
  ...running,
  status: "done",
  summary: "70212 events on 8 symbols; 9 labelled episodes; 12 alerts",
  finished_at: "2026-10-15T12:00:04Z",
  types: [{ abuse_type: "quote_stuffing", episodes: 3, detected: 3, alerts: 3, matched: 3, recall: 1, precision: 1, f1: 1, median_ttd_seconds: 0.4 }],
};

function api(recordings: { name: string; size_bytes: number; modified_at: string }[]) {
  return fakeApi({
    "GET /api/v1/sim/status": { state: "running", session_id: "live-3", events_published: 10, pace: "realtime", speed: 1, symbols: ["QZAP"] },
    "GET /api/v1/sim/scenarios": { scenarios: ["wash.ring"] },
    "GET /api/v1/recordings": { recordings },
    "POST /api/v1/replays": () => ({ status: 202, body: running }),
    "GET /api/v1/evaluations/7": done,
  });
}

describe("SimulatorPage", () => {
  it("replays a recording and shows its figures when it is done", async () => {
    const { calls } = api([
      { name: "live-2.twrec.gz", size_bytes: 3_500_000, modified_at: "2026-10-15T11:30:00Z" },
      { name: "live-1.twrec.gz", size_bytes: 3_400_000, modified_at: "2026-10-15T11:00:00Z" },
    ]);
    const user = userEvent.setup();
    renderWithSession(<SimulatorPage />, admin);

    const select = await screen.findByLabelText("Recording");
    expect(select).toHaveValue("live-2.twrec.gz");
    expect(screen.getByRole("option", { name: "live-1.twrec.gz (3.2 MB)" })).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Replay" }));

    expect(await screen.findByText("Run 7 is running…")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Replay" })).toBeDisabled();
    expect(calls.find((c) => c.method === "POST" && c.path === "/api/v1/replays")?.body).toEqual({ recording: "live-2.twrec.gz" });

    expect(await screen.findByRole("table", { name: "Figures of run 7" }, { timeout: 10_000 })).toHaveTextContent(
      /Quote stuffing333\s*1\.001\.001\.00400 ms/,
    );
    expect(screen.getByText(done.summary!)).toBeInTheDocument();
    await waitFor(() => expect(screen.getByRole("button", { name: "Replay" })).toBeEnabled());
  });

  it("says when there is nothing to replay yet", async () => {
    api([]);
    renderWithSession(<SimulatorPage />, admin);
    expect(await screen.findByText(/No complete recordings yet/)).toBeInTheDocument();
  });
});
