import { screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";

import type { EvaluationRun } from "../api/types";
import { fakeApi } from "../test/fakeApi";
import { renderWithSession } from "../test/render";
import { EvaluationRuns } from "./EvaluationRuns";

const evalRun: EvaluationRun = {
  id: 2,
  kind: "eval",
  status: "done",
  source: "20 sessions with injected abuse and 5 without, seeds 1000 to 1024, 2h10m0s each",
  summary: "1123 labelled episodes; 949 alerts on the sessions with abuse and 8 on those without",
  requested_by: "evaluator",
  rules_digest: "e6297d9918c3",
  started_at: "2026-10-15T10:00:00Z",
  finished_at: "2026-10-15T10:02:00Z",
  types: [
    { abuse_type: "wash_trading", episodes: 252, detected: 234, alerts: 254, matched: 233, recall: 0.9286, precision: 0.9173, f1: 0.923, median_ttd_seconds: 0 },
    { abuse_type: "spoofing", episodes: 3, detected: 0, alerts: 0, matched: 0, recall: 0, precision: null, f1: null, median_ttd_seconds: null },
  ],
};

const failedReplay: EvaluationRun = {
  id: 3,
  kind: "replay",
  status: "failed",
  source: "live-1.twrec.gz",
  requested_by: "admin",
  rules_digest: "e6297d9918c3",
  started_at: "2026-10-15T11:00:00Z",
  error: "the API stopped before the replay finished",
  types: [],
};

describe("EvaluationRuns", () => {
  it("lists the runs and shows the figures of the one selected", async () => {
    fakeApi({ "GET /api/v1/evaluations": { runs: [failedReplay, evalRun] } });
    const user = userEvent.setup();
    renderWithSession(<EvaluationRuns />);

    const runs = await screen.findByRole("table", { name: "Evaluation runs" });
    const rows = within(runs).getAllByRole("row").slice(1);
    expect(rows.map((r) => r.textContent)).toEqual([
      expect.stringContaining("Replaylive-1.twrec.gzadminfailed"),
      expect.stringContaining("Evaluation20 sessions"),
    ]);
    expect(screen.getByRole("alert")).toHaveTextContent("Run 3 failed: the API stopped before the replay finished");

    await user.click(rows[1]!);
    const figures = screen.getByRole("table", { name: "Figures of run 2" });
    const [wash, spoofing] = within(figures).getAllByRole("row").slice(1);
    expect(wash).toHaveTextContent(/Wash trading252234254\s*0\.930\.920\.920 s/);
    expect(spoofing).toHaveTextContent(/Spoofing3000\.00––/);
    expect(screen.getByText(evalRun.summary!)).toBeInTheDocument();
  });

  it("explains when there are no runs", async () => {
    fakeApi({ "GET /api/v1/evaluations": { runs: [] } });
    renderWithSession(<EvaluationRuns />);
    expect(await screen.findByText(/No runs yet/)).toBeInTheDocument();
  });
});
