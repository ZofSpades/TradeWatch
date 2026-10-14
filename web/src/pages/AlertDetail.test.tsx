import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import type { Status } from "../api/types";
import { detail, fakeApi, summary } from "../test/fakeApi";
import { renderWithSession } from "../test/render";
import { AlertDetail } from "./AlertDetail";

const KEY = summary(1).key;

function workflowApi() {
  let status: Status = "open";
  return fakeApi({
    [`GET /api/v1/alerts/${KEY}`]: () => ({ body: detail(1, { status }) }),
    [`GET /api/v1/alerts/${KEY}/audit`]: { entries: [{ id: 1, at: "2026-10-14T10:00:20Z", actor: "detector/det-a", action: "raised", to_status: "open", note: "" }] },
    [`POST /api/v1/alerts/${KEY}/transitions`]: ({ body }) => {
      status = (body as { to: Status }).to;
      return { body: summary(1, { status }) };
    },
  });
}

describe("AlertDetail", () => {
  it("explains why the alert fired", async () => {
    workflowApi();
    renderWithSession(<AlertDetail alertKey={KEY} onChanged={vi.fn()} />);
    expect(await screen.findByText("Account ACC-0001 cancelled 1 large buy orders.")).toBeInTheDocument();
    expect(screen.getByText("layers")).toBeInTheDocument();
    expect(screen.getByText(">= 2")).toBeInTheDocument();
    expect(screen.getByText("met")).toBeInTheDocument();
    expect(screen.getByRole("img", { name: "Price around the alert" })).toBeInTheDocument();
    expect(screen.getByText("genuine fill")).toBeInTheDocument();
  });

  it("walks the alert through the workflow, requiring a reason to dismiss", async () => {
    const { calls } = workflowApi();
    const onChanged = vi.fn();
    const user = userEvent.setup();
    renderWithSession(<AlertDetail alertKey={KEY} onChanged={onChanged} />);
    await screen.findByText("Account ACC-0001 cancelled 1 large buy orders.");
    expect(screen.getByRole("button", { name: /Escalate/ })).toBeDisabled();
    expect(screen.getByRole("button", { name: /Dismiss/ })).toBeDisabled();

    await user.keyboard("t");
    await waitFor(() => expect(screen.getByRole("button", { name: /Dismiss/ })).toBeEnabled());
    expect(calls.find((c) => c.method === "POST")?.body).toEqual({ to: "triaged" });
    expect(onChanged).toHaveBeenCalledWith(expect.objectContaining({ status: "triaged" }));

    await user.keyboard("d");
    const dialog = await screen.findByRole("dialog", { name: "Dismiss this alert" });
    await user.click(screen.getByRole("button", { name: "Dismiss" }));
    expect(await screen.findByText("A reason is required.")).toBeInTheDocument();
    expect(calls.filter((c) => c.method === "POST")).toHaveLength(1);

    await user.type(screen.getByLabelText("Reason (required)"), "legitimate hedge");
    await user.click(screen.getByRole("button", { name: "Dismiss" }));
    await waitFor(() => expect(dialog).not.toBeInTheDocument());
    expect(calls.filter((c) => c.method === "POST").at(-1)?.body).toEqual({ to: "dismissed", note: "legitimate hedge" });
  });

  it("closes the dialog with Escape without changing anything", async () => {
    const { calls } = workflowApi();
    const user = userEvent.setup();
    renderWithSession(<AlertDetail alertKey={KEY} onChanged={vi.fn()} />);
    await screen.findByText("Account ACC-0001 cancelled 1 large buy orders.");
    await user.keyboard("c");
    expect(await screen.findByRole("dialog", { name: "Comment" })).toBeInTheDocument();
    await user.keyboard("{Escape}");
    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
    expect(calls.filter((c) => c.method === "POST")).toHaveLength(0);
  });
});
