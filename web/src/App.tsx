import { useState } from "react";

import { useAuth } from "./auth";
import { LiveBar } from "./components/LiveBar";
import { ShortcutsHelp } from "./components/ShortcutsHelp";
import { useShortcuts } from "./keyboard";
import { LiveProvider } from "./live";
import { AlertsPage } from "./pages/AlertsPage";
import { LoginPage } from "./pages/LoginPage";
import { RulesPage } from "./pages/RulesPage";
import { SimulatorPage } from "./pages/SimulatorPage";
import { SystemPage } from "./pages/SystemPage";
import { href, useRoute, type Route } from "./router";
import { useTheme } from "./theme";

export function App() {
  const { session } = useAuth();
  if (!session) return <LoginPage />;
  return (
    <LiveProvider>
      <Shell />
    </LiveProvider>
  );
}

function Shell() {
  const { session, logout } = useAuth();
  const [route, navigate] = useRoute();
  const [theme, toggleTheme] = useTheme();
  const [help, setHelp] = useState(false);
  const admin = session?.role === "admin";

  useShortcuts(
    {
      "1": () => navigate({ page: "alerts" }),
      "2": () => navigate({ page: "rules" }),
      "3": () => navigate({ page: "system" }),
      "4": () => admin && navigate({ page: "simulator" }),
      "?": () => setHelp(true),
    },
    !help,
  );

  const tab = (r: Route, label: string, key: string) => (
    <a href={href(r)} className={route.page === r.page ? "active" : undefined} aria-current={route.page === r.page ? "page" : undefined}>
      {label} <kbd aria-hidden="true">{key}</kbd>
    </a>
  );

  return (
    <div className="app">
      <a className="skip" href="#main">
        Skip to content
      </a>
      <header className="top">
        <div className="brand">
          <img src="/favicon.svg" alt="" width={24} height={24} />
          <span>TradeWatch</span>
        </div>
        <nav aria-label="Main">
          {tab({ page: "alerts" }, "Alerts", "1")}
          {tab({ page: "rules" }, "Rules", "2")}
          {tab({ page: "system" }, "System", "3")}
          {admin && tab({ page: "simulator" }, "Simulator", "4")}
        </nav>
        <div className="top-right">
          <button type="button" className="icon" onClick={() => setHelp(true)} aria-label="Keyboard shortcuts">
            ?
          </button>
          <button type="button" className="icon" onClick={toggleTheme} aria-label={`Switch to ${theme === "dark" ? "light" : "dark"} mode`}>
            {theme === "dark" ? "☀" : "☾"}
          </button>
          <span className="user">
            {session?.username} <span className="muted">({session?.role})</span>
          </span>
          <button type="button" onClick={logout}>
            Sign out
          </button>
        </div>
      </header>
      <LiveBar />
      <main id="main" tabIndex={-1}>
        {route.page === "alerts" && (
          <AlertsPage selected={route.key} onSelect={(key) => navigate(key ? { page: "alerts", key } : { page: "alerts" })} />
        )}
        {route.page === "rules" && (
          <RulesPage selected={route.id} onSelect={(id) => navigate(id ? { page: "rules", id } : { page: "rules" })} />
        )}
        {route.page === "system" && <SystemPage />}
        {route.page === "simulator" && <SimulatorPage />}
      </main>
      <footer className="foot">All market data is simulated. Not compliance advice.</footer>
      {help && <ShortcutsHelp onClose={() => setHelp(false)} />}
    </div>
  );
}
