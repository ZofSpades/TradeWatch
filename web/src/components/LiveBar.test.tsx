import { screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { fakeApi, summary } from "../test/fakeApi";
import { renderWithSession } from "../test/render";
import { LiveBar } from "./LiveBar";

describe("LiveBar", () => {
  it("shows the totals and the last hour, and counts live arrivals", async () => {
    fakeApi({
      "GET /api/v1/live": {
        counts: { total: 22, by_abuse_type: { wash_trading: 8 }, by_symbol: {} },
        recent: [],
        last_hour: { total: 6, by_abuse_type: { wash_trading: 2 }, per_minute: Array<number>(60).fill(0) },
      },
    });
    const { publish } = renderWithSession(<LiveBar />);
    expect(await screen.findByText("22 alerts")).toBeInTheDocument();
    expect(screen.getByText("6 in the last hour")).toBeInTheDocument();

    publish({ type: "alert.raised", alert: summary(1, { abuse_type: "wash_trading" }) });
    expect(await screen.findByText("7 in the last hour")).toBeInTheDocument();
    expect(screen.getByText("23 alerts")).toBeInTheDocument();
  });
});
