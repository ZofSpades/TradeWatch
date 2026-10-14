import { useState, type FormEvent } from "react";

import { ApiError } from "../api/client";
import { useAuth } from "../auth";

export function LoginPage() {
  const { login } = useAuth();
  const [username, setUsername] = useState("analyst");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  async function submit(e: FormEvent) {
    e.preventDefault();
    setBusy(true);
    setError(null);
    try {
      await login(username.trim(), password);
    } catch (err) {
      if (err instanceof ApiError && err.status === 401) setError("Invalid username or password.");
      else setError(err instanceof Error ? err.message : String(err));
      setBusy(false);
    }
  }

  return (
    <main className="login">
      <form className="login-card" onSubmit={submit} aria-labelledby="login-title">
        <div className="brand">
          <img src="/favicon.svg" alt="" width={32} height={32} />
          <h1 id="login-title">TradeWatch</h1>
        </div>
        <p className="muted">Market surveillance on simulated data</p>
        <label htmlFor="username">Username</label>
        <input id="username" autoComplete="username" value={username} onChange={(e) => setUsername(e.target.value)} />
        <label htmlFor="password">Password</label>
        <input
          id="password"
          type="password"
          autoComplete="current-password"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          autoFocus
        />
        {error && (
          <p className="error" role="alert">
            {error}
          </p>
        )}
        <button type="submit" className="primary" disabled={busy || password === ""}>
          {busy ? "Signing in…" : "Sign in"}
        </button>
        <p className="hint">
          Accounts are <code>analyst</code> and <code>admin</code>; <code>make env</code> wrote their passwords to{" "}
          <code>.env</code>.
        </p>
      </form>
    </main>
  );
}
