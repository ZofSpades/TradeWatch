// Captures the running dashboard for the documentation:
// `make screenshots` writes docs/images/*.png from the compose stack.
import { mkdir } from "node:fs/promises";
import { chromium } from "playwright";

const base = process.env.TW_WEB_URL ?? "http://web:8080";
const out = process.argv[2] ?? "/src/docs/images";
await mkdir(out, { recursive: true });

const browser = await chromium.launch();
try {
  for (const theme of ["light", "dark"]) {
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 }, colorScheme: theme });
    const page = await context.newPage();
    await page.goto(base);
    await page.fill("#username", "admin");
    await page.fill("#password", process.env.TW_API_ADMIN_PASSWORD);
    await page.click("button[type=submit]");
    await page.waitForSelector(".live-dot.live", { timeout: 15_000 });
    await page.getByLabel("Status", { exact: true }).selectOption("");
    await page.getByLabel("Type", { exact: true }).selectOption("spoofing");
    await page.waitForSelector("table[aria-label=Alerts] tbody tr");
    await page.keyboard.press("j");
    await page.waitForSelector(".alert-detail .price-chart");
    await page.waitForTimeout(300);
    await page.screenshot({ path: `${out}/dashboard-alerts-${theme}.png` });
    if (theme === "light") {
      await page.keyboard.press("2");
      await page.click(".rules-table tbody tr >> nth=1");
      await page.waitForSelector(".source");
      await page.screenshot({ path: `${out}/dashboard-rules.png` });
      await page.keyboard.press("3");
      await page.waitForSelector("text=Simulated market");
      await page.waitForTimeout(500);
      await page.screenshot({ path: `${out}/dashboard-simulator.png` });
    }
    await context.close();
  }
} finally {
  await browser.close();
}
console.log(`screenshots written to ${out}`);
