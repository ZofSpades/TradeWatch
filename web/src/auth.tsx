import { createContext, useCallback, useContext, useEffect, useMemo, useState, type ReactNode } from "react";

import { makeClient, type Client } from "./api/client";
import type { Role } from "./api/types";

export interface Session {
  token: string;
  username: string;
  role: Role;
  expiresAt: number; // epoch milliseconds
}

interface AuthValue {
  session: Session | null;
  client: Client;
  login: (username: string, password: string) => Promise<void>;
  logout: () => void;
}

const AuthContext = createContext<AuthValue | null>(null);

// The token lives in sessionStorage so that a reload keeps the session but
// closing the tab ends it. It is readable by scripts on this origin, which
// is why the page loads no third-party scripts and sends a strict CSP.
const STORAGE_KEY = "tradewatch.session";

function loadSession(): Session | null {
  try {
    const raw = sessionStorage.getItem(STORAGE_KEY);
    if (!raw) return null;
    const s = JSON.parse(raw) as Session;
    return s.expiresAt > Date.now() ? s : null;
  } catch {
    return null;
  }
}

function saveSession(s: Session | null) {
  try {
    if (s) sessionStorage.setItem(STORAGE_KEY, JSON.stringify(s));
    else sessionStorage.removeItem(STORAGE_KEY);
  } catch {
    // storage unavailable: the session lasts until reload
  }
}

export function AuthProvider({ children, initial }: { children: ReactNode; initial?: Session | null }) {
  const [session, setSession] = useState<Session | null>(() => (initial !== undefined ? initial : loadSession()));

  const logout = useCallback(() => {
    saveSession(null);
    setSession(null);
  }, []);

  const client = useMemo(() => makeClient(() => session?.token ?? null, logout), [session, logout]);

  const login = useCallback(async (username: string, password: string) => {
    const res = await makeClient(() => null).login(username, password);
    const s: Session = {
      token: res.access_token,
      username: res.username,
      role: res.role,
      expiresAt: Date.now() + res.expires_in * 1000,
    };
    saveSession(s);
    setSession(s);
  }, []);

  // End the session when the token expires.
  useEffect(() => {
    if (!session) return;
    const t = setTimeout(logout, Math.max(0, session.expiresAt - Date.now()));
    return () => clearTimeout(t);
  }, [session, logout]);

  const value = useMemo(() => ({ session, client, login, logout }), [session, client, login, logout]);
  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth(): AuthValue {
  const v = useContext(AuthContext);
  if (!v) throw new Error("useAuth outside AuthProvider");
  return v;
}

/** The API client of the current session. */
export function useClient(): Client {
  return useAuth().client;
}
