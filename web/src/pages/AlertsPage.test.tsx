import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useState } from "react";
import { describe, expect, it } from "vitest";

import { detail, fakeApi, summary } from "../test/fakeApi";
import { renderWithSession } from "../test/render";
import { AlertsPage } from "./AlertsPage";

function Harness() {
  const [selected, setSelected] = useState<string | undefined>();
  return <AlertsPage selected={selected} onSelect={setSelected} />;
}

function api() {
  return fakeApi({
    "GET /api/v1/alerts": { alerts: [summary(2), summary(1)] },
    [`GET /api/v1/alerts/${summary(2).key}`]: detail(2),
    [`GET /api/v1/alerts/${summary(1).key}`]: detail(1),
    [`GET /api/v1/alerts/${summary(2).key}/audit`]: { entries: [] },
    [`GET /api/v1/alerts/${summary(1).key}/audit`]: { entries: [] },
  });
}

const dataRows = () => within(screen.getByRole("table", { name: "Alerts" })).getAllByRole("row").slice(1);

describe("AlertsPage", () => {
  it("moves through the queue with j and k", async () => {
    api();
    const user = userEvent.setup();
    renderWithSession(<Harness />);
    await waitFor(() => expect(dataRows()).toHaveLength(2));

    await user.keyboard("j");
    expect(dataRows()[0]).toHaveAttribute("aria-selected", "true");
    await screen.findByText("Account ACC-0001 cancelled 2 large buy orders.");
    await user.keyboard("j");
    expect(dataRows()[1]).toHaveAttribute("aria-selected", "true");
    await user.keyboard("k");
    expect(dataRows()[0]).toHaveAttribute("aria-selected", "true");
  });

  it("adds live alerts that match the filters, at the top", async () => {
    api();
    const { publish } = renderWithSession(<Harness />);
    await waitFor(() => expect(dataRows()).toHaveLength(2));

    publish({ type: "alert.raised", alert: summary(9, { abuse_type: "wash_trading" }) });
    await waitFor(() => expect(dataRows()).toHaveLength(3));
    expect(dataRows()[0]).toHaveClass("fresh");
    expect(dataRows()[0]).toHaveTextContent("Wash trading");

    // The queue shows open alerts; a triaged one does not belong there.
    publish({ type: "alert.raised", alert: summary(10, { status: "triaged" }) });
    publish({ type: "alert.updated", alert: summary(1, { status: "escalated" }) });
    await waitFor(() => expect(dataRows()[2]).toHaveTextContent("Escalated"));
    expect(dataRows()).toHaveLength(3);
  });

  it("queries the API with the chosen filters", async () => {
    const { calls } = api();
    const user = userEvent.setup();
    renderWithSession(<Harness />);
    await waitFor(() => expect(dataRows()).toHaveLength(2));

    await user.selectOptions(screen.getByLabelText("Type"), "marking_the_close");
    await waitFor(() =>
      expect(calls.some((c) => c.path === "/api/v1/alerts?status=open&abuse_type=marking_the_close&limit=50")).toBe(true),
    );
  });
});
