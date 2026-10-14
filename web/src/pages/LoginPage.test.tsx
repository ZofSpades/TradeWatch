import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";

import { AuthProvider, useAuth } from "../auth";
import { fakeApi } from "../test/fakeApi";
import { LoginPage } from "./LoginPage";

function Who() {
  const { session } = useAuth();
  return <p>{session ? `signed in as ${session.username}` : "signed out"}</p>;
}

describe("LoginPage", () => {
  it("signs in and keeps the session for this tab", async () => {
    fakeApi({
      "POST /api/v1/auth/login": { access_token: "tok", token_type: "Bearer", expires_in: 3600, username: "analyst", role: "analyst" },
    });
    const user = userEvent.setup();
    render(
      <AuthProvider initial={null}>
        <LoginPage />
        <Who />
      </AuthProvider>,
    );
    await user.type(screen.getByLabelText("Password"), "secret password");
    await user.click(screen.getByRole("button", { name: "Sign in" }));
    expect(await screen.findByText("signed in as analyst")).toBeInTheDocument();
    expect(JSON.parse(sessionStorage.getItem("tradewatch.session")!)).toMatchObject({ token: "tok", role: "analyst" });
  });

  it("explains a refused login", async () => {
    fakeApi({ "POST /api/v1/auth/login": () => ({ status: 401, body: { detail: "invalid username or password" } }) });
    const user = userEvent.setup();
    render(
      <AuthProvider initial={null}>
        <LoginPage />
      </AuthProvider>,
    );
    await user.type(screen.getByLabelText("Password"), "wrong");
    await user.click(screen.getByRole("button", { name: "Sign in" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Invalid username or password.");
    await waitFor(() => expect(screen.getByRole("button", { name: "Sign in" })).toBeEnabled());
  });
});
