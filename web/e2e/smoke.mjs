// Drives the running dashboard in a real browser, end to end through nginx,
// the API, Redis and the live pipeline: sign in, follow the live feed, work
// an alert with the keyboard, and watch an injected episode arrive live.
// It changes data (one alert is triaged and dismissed), so it is meant for
// the local compose stack: `make web-e2e`.
import assert from "node:assert/strict";
import { chromium } from "playwright";

const base = process.env.TW_WEB_URL ?? "http://web:8080";
const passwords = {
  analyst: process.env.TW_API_ANALYST_PASSWORD,
  admin: process.env.TW_API_ADMIN_PASSWORD,
};

function step(name) {
  console.log(`- ${name}`);
}

async function signIn(page, user) {
  await page.goto(base);
  await page.fill("#username", user);
  await page.fill("#password", passwords[user]);
  await page.click("button[type=submit]");
  await page.waitForSelector(".live-bar");
}

const browser = await chromium.launch();
const errors = [];
try {
  const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
  const page = await context.newPage();
  page.on("pageerror", (e) => errors.push(String(e)));
  page.on("console", (m) => m.type() === "error" && errors.push(m.text()));

  step("an analyst signs in and the live feed connects");
  await signIn(page, "analyst");
  await page.waitForSelector(".live-dot.live", { timeout: 15_000 });
  assert.equal(await page.locator("nav a", { hasText: "Simulator" }).count(), 0, "analysts get no simulator tab");

  step("the queue lists open alerts and j selects the first");
  await page.waitForSelector("table[aria-label=Alerts] tbody tr", { timeout: 15_000 });
  await page.keyboard.press("j");
  await page.waitForSelector(".alert-detail .explanation");
  const detail = page.locator(".alert-detail");
  assert.match(await detail.locator(".detail-badges").innerText(), /Open/);

  step("t triages it, and the history records the analyst");
  await page.keyboard.press("t");
  await detail.locator(".detail-badges", { hasText: "Triaged" }).waitFor();
  await detail.locator(".audit li", { hasText: "analyst" }).first().waitFor();

  step("d asks for a reason before dismissing");
  await page.keyboard.press("d");
  const dialog = page.getByRole("dialog", { name: "Dismiss this alert" });
  await dialog.waitFor();
  await dialog.getByRole("button", { name: "Dismiss" }).click();
  await dialog.getByText("A reason is required.").waitFor();
  await dialog.getByLabel("Reason (required)").fill("end-to-end check: legitimate activity");
  await dialog.getByRole("button", { name: "Dismiss" }).click();
  await dialog.waitFor({ state: "detached" });
  await detail.locator(".detail-badges", { hasText: "Dismissed" }).waitFor();
  await detail.getByText("end-to-end check: legitimate activity").waitFor();

  step("3 opens the system page, with live rates and every service up from Prometheus");
  await page.keyboard.press("3");
  const throughput = page.getByRole("region", { name: "Throughput" });
  await throughput.waitFor({ timeout: 15_000 });
  assert.match(await throughput.innerText(), /Published by marketsim\s*[\d,.]+ events\/s/);
  const services = page.getByRole("region", { name: "Services" });
  await services.getByText("2 of 2 up").waitFor();
  for (const name of ["marketsim", "ingest", "alertsink", "api"]) {
    assert.match(await services.innerText(), new RegExp(`${name}\\s*1 of 1 up`), `${name} is up`);
  }

  step("an admin injects a quote-stuffing burst and its alert arrives live");
  const admin = await browser.newContext({ viewport: { width: 1440, height: 900 } });
  const adminPage = await admin.newPage();
  adminPage.on("pageerror", (e) => errors.push(String(e)));
  await signIn(adminPage, "admin");
  await adminPage.waitForSelector(".live-dot.live", { timeout: 15_000 });
  await adminPage.getByLabel("Type", { exact: true }).selectOption("quote_stuffing");
  // Trigger through the API while the queue stays open: only the live feed
  // inserts rows marked fresh, so the new row proves live delivery.
  await adminPage.evaluate(async () => {
    const { token } = JSON.parse(sessionStorage.getItem("tradewatch.session"));
    const res = await fetch("/api/v1/sim/scenarios", {
      method: "POST",
      headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
      body: JSON.stringify({ scenario: "quote_stuffing.burst" }),
    });
    if (res.status !== 202) throw new Error(`trigger: HTTP ${res.status} ${await res.text()}`);
  });
  await adminPage.waitForSelector("table[aria-label=Alerts] tbody tr.fresh", { timeout: 120_000 });

  step("the simulator page schedules a scenario");
  await adminPage.keyboard.press("4");
  await adminPage.getByRole("button", { name: "spoofing.layering" }).click();
  await adminPage.getByText("Scenario spoofing.layering scheduled.").waitFor();

  assert.deepEqual(errors, [], "the pages logged no errors");
  console.log("dashboard smoke test passed");
} finally {
  await browser.close();
}
