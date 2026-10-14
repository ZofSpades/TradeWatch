# Dashboard

The dashboard is the analyst's view of TradeWatch. It has four parts:

- a live queue of alerts;
- each alert's evidence and workflow;
- the rules in force and their history;
- for admins, control of the simulated market.

In the compose stack it is served on `http://localhost:3000`. Sign in as
`analyst` or `admin`; `make env` generated their passwords into `.env`
(`TW_API_ANALYST_PASSWORD`, `TW_API_ADMIN_PASSWORD`).

## Screens

**Alerts.** The queue lists alerts newest first.
- **Filters:** status (open by default), abuse type, symbol and minimum
  score. "Load more" pages through older alerts.
- **Live arrivals:** alerts raised while the page is open appear at the top,
  highlighted, if they match the filters. Status changes by other analysts
  update in place.

Selecting an alert opens its detail beside the queue:

- **Explanation:** the paragraph the rule rendered for this alert.
- **Why it fired:** every feature with its value, threshold and whether it
  was met. Informational features are marked as context.
- **Market around the alert:** the last trade price and the best bid and
  offer, with the alert's window shaded and its orders (triangles) and
  trades (dots) marked.
- **Evidence:**
  - the accounts and beneficial owners;
  - the window, in simulated market time;
  - when the alert was stored, and how many times it was delivered, if
    duplicates were absorbed;
  - the timeline of orders and trades, with their roles.
- **History:** who raised, moved and commented on the alert, and when,
  including the reasons given.
- **Workflow:** triage, escalate, dismiss and comment. The buttons follow
  the workflow (open → triaged → escalated or dismissed), and escalating,
  dismissing and commenting ask for a note.

**Rules.** The rule set most recently loaded by a detector, with each rule's
version. A rule whose content changed without a version bump is flagged.
Selecting a rule shows every version a detector has loaded, with its YAML
source.

**Simulator** (admins only). The session's state and market time. From here
an admin can inject a labelled abuse episode now (optionally on a chosen
symbol) and change the background injection rates. Everything on this page
is simulated.

## Keyboard

| Keys | Action |
| --- | --- |
| `j` / `k` (or ↓ / ↑) | Next / previous alert |
| `t` | Triage the selected alert |
| `e` | Escalate it (asks for a reason) |
| `d` | Dismiss it (asks for a reason) |
| `c` | Comment on it |
| `r` | Reload the queue |
| `/` | Filter by symbol |
| `1` / `2` / `3` | Alerts / Rules / Simulator |
| `?` | Show the shortcuts |
| `Esc` | Close a dialog |

Shortcuts never fire while typing in a field, and are suspended while a
dialog is open.

## Accessibility and theming

- **Structure:**
  - semantic landmarks and a skip link;
  - every control has a label;
  - the selected row is marked with `aria-selected`;
  - actions announce their shortcuts with `aria-keyshortcuts`;
  - the live feed's state is a polite live region.
- **Dialogs** take focus, keep Tab inside themselves, close on Escape, and
  give focus back to whatever had it.
- **Focus outlines** are always visible, and the new-alert highlight
  respects reduced-motion preferences.
- **Theme:** light and dark follow the system setting. The toggle in the
  header overrides it, and the choice is remembered in the browser.

## How it is built

- **React 19, TypeScript and Vite.** TypeScript runs in strict mode. The only
  runtime dependencies are `react` and `react-dom`. Hash routing, the price
  chart (SVG) and the event-stream client are small modules in `src/`
  rather than libraries.
- **API types are generated** from the API's OpenAPI document
  (`npm run gen:api` writes `src/api/schema.d.ts`). `make web-test` fails if
  the committed types are out of date, so the dashboard cannot drift from the
  contract unnoticed.
- **The live feed** reads the API's server-sent events with `fetch`. That
  lets it send the bearer token in a header, which `EventSource` cannot do.
  It reconnects with exponential backoff.
- **The session token** lives in `sessionStorage`. A reload keeps the
  session, but closing the tab ends it. The session also ends when the token
  expires or the API rejects it.
- **nginx serves the build**, from the unprivileged image, with these
  headers:
  - a `Content-Security-Policy` that allows only the page's own origin;
  - `X-Frame-Options: DENY`;
  - `nosniff` and `no-referrer`.

  Content-hashed assets are cached as immutable. nginx also proxies `/api`
  to the API, with buffering off so that events reach the browser at once.
  The dashboard and the API therefore share one origin, and no CORS is
  needed.

## Tests

| Command | What it checks |
| --- | --- |
| `make web-test` (in Docker; also part of `make test`) | The generated API types match `openapi.yaml`, the app type-checks, Vitest component tests pass, and the production build succeeds. The tests cover parsing the event stream (including events split across chunks), client errors and the session ending on 401, the router, keyboard navigation of the queue, live arrivals and filters, and the full workflow (required reasons, Escape, button states). They also cover the chart and sign-in. |
| `make web-e2e` (needs `make up`) | Playwright drives the running stack in Chromium. An analyst signs in and the live feed connects through nginx. The analyst triages and then dismisses an alert with the keyboard; the dismissal asks for a reason. An admin injects a quote-stuffing burst through the API, and its alert arrives live in the open queue. The simulator page schedules a scenario, and no page logs an error. This changes data. |
| `make screenshots` (needs `make up`) | Captures the screens into `docs/images` |

## Developing

```sh
make up                   # the API on localhost:8080
cd web && npm ci && npm run dev
```

The development server proxies `/api` to `localhost:8080`, so the
dashboard runs against the compose stack with hot reloading.
