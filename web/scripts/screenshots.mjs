// Usage: node scripts/screenshots.mjs [baseUrl]   (server must be running)
// Phone (390x844 @2x) shots -> screenshots/mobile/, desktop sanity shots -> screenshots/desktop/
import { chromium } from "playwright";
import { mkdirSync } from "node:fs";

const base = process.argv[2] ?? "http://localhost:3000";
const root = new URL("../screenshots/", import.meta.url).pathname;
const pages = [
  ["01-landing", "/"],
  ["02-login", "/login"],
  ["03-signup", "/signup"],
  ["04-cbb-slate", "/cbb/slate"],
  ["05-cbb-results", "/cbb/results"],
  ["06-my-models", "/models"],
  ["07-pricing", "/pricing"],
  ["08-nba-coming-soon", "/nba/slate"],
];

const browser = await chromium.launch();
async function shoot(dir, viewport, scale, extras) {
  const out = `${root}${dir}/`;
  mkdirSync(out, { recursive: true });
  const page = await browser.newPage({ viewport, deviceScaleFactor: scale, isMobile: dir === "mobile", hasTouch: dir === "mobile" });
  const snap = async (name, full = true) => {
    if (full) {
      // grow the viewport to the page height so fixed UI (bottom tab bar) sits at the true bottom
      const h = await page.evaluate(() => document.documentElement.scrollHeight);
      await page.setViewportSize({ width: viewport.width, height: Math.max(h, viewport.height) });
      await page.waitForTimeout(250); // let fixed UI repaint after the resize
    }
    await page.screenshot({ path: `${out}${name}.png` });
    await page.setViewportSize(viewport);
    console.log(`${out}${name}.png`);
  };
  for (const [name, path] of pages) {
    await page.goto(base + path, { waitUntil: "networkidle" });
    const sw = await page.evaluate(() => document.documentElement.scrollWidth);
    if (sw > viewport.width) console.warn(`WARN ${path}: horizontal overflow ${sw}px > ${viewport.width}px`);
    await snap(name);
  }
  if (extras) {
    // value-bet sheet open
    await page.goto(base + "/cbb/slate", { waitUntil: "networkidle" });
    await page.getByRole("button", { name: /Granite Falls \+2\.5/ }).click();
    await page.waitForSelector('[role="dialog"]');
    await snap("09-cbb-slate-value-sheet", false);
    // value-only filter
    await page.keyboard.press("Escape");
    await page.getByRole("button", { name: /^Value/ }).click();
    await snap("10-cbb-slate-value-filter");
    // models: free tier at its 1-model limit
    await page.goto(base + "/models", { waitUntil: "networkidle" });
    await page.getByRole("button", { name: /New model/ }).click();
    await snap("11-my-models-free-limit");
    // models: pro tier
    await page.getByRole("button", { name: "Pro", exact: true }).click();
    await snap("12-my-models-pro");
  }
  await page.close();
}
await shoot("mobile", { width: 390, height: 844 }, 2, true);
await shoot("desktop", { width: 1440, height: 900 }, 1, false);
await browser.close();
