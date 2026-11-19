// Back/Forward check for the matchup maker (needs a running server and Supabase env).
// Usage: node scripts/matchup-back.mjs [baseUrl]    e.g. after `npm run build && npx next start -p 3102`
// Set CHROME_PATH to use a system Chrome when Playwright's own browsers aren't installed.
import { chromium } from "playwright";

const base = process.argv[2] ?? "http://localhost:3000";
const b = await chromium.launch(process.env.CHROME_PATH ? { executablePath: process.env.CHROME_PATH, args: ["--no-sandbox"] } : {});
const p = await b.newPage({ viewport: { width: 390, height: 900 } });
let failed = 0;
const check = (label, ok, got) => { if (!ok) failed++; console.log(`${ok ? "PASS" : "FAIL"}  ${label}${ok ? "" : "  got " + JSON.stringify(got)}`); };
const pick = async (i, q, name) => {
  const c = p.getByRole("combobox").nth(i);
  await c.click(); await c.fill(q);
  await p.getByRole("option", { name, exact: true }).click();
};
const state = async () => ({
  url: p.url().replace(base, ""),
  teams: await p.getByRole("combobox").evaluateAll((e) => e.map((x) => x.value)),
  court: await p.getByRole("radio").evaluateAll((e) => e.map((x) => x.getAttribute("aria-checked"))),
});
const same = (a, e) => JSON.stringify(a) === JSON.stringify(e);

// 1. pick both teams, choose "B home", go to Slate through the tab bar, press Back
await p.goto(`${base}/cbb/matchup`, { waitUntil: "networkidle" });
await pick(0, "texas st", "Texas St."); await pick(1, "duke", "Duke");
await p.getByRole("radio").nth(2).click(); await p.waitForTimeout(800);
const s1 = await state();
await p.locator("nav.fixed a", { hasText: "Slate" }).click(); await p.waitForURL(/slate/);
await p.goBack(); await p.waitForTimeout(1200);
const s1b = await state();
check("1. Back keeps picks and court", same(s1b.teams, ["Texas St.", "Duke"]) && same(s1b.court, ["false", "false", "true"]) && s1b.url === s1.url, s1b);
await p.goForward(); await p.waitForURL(/slate/); await p.goBack(); await p.waitForTimeout(800);
check("1b. Forward then Back again keeps them", same((await state()).teams, ["Texas St.", "Duke"]), await state());

// 2. deep link, change B, leave, Back
await p.goto(`${base}/cbb/matchup?a=520&b=78&venue=neutral`, { waitUntil: "networkidle" });
await pick(1, "kansas", "Kansas"); await p.waitForTimeout(800);
await p.locator("nav.fixed a", { hasText: "Portfolio" }).click(); await p.waitForURL(/results/);
await p.goBack(); await p.waitForTimeout(1200);
const s2 = await state();
check("2. Back after changing B shows the new B, not the deep-link one", same(s2.teams, ["Texas St.", "Kansas"]), s2);

// 3. reload keeps the URL state
await p.reload({ waitUntil: "networkidle" }); await p.waitForTimeout(800);
check("3. Reload keeps picks", same((await state()).teams, ["Texas St.", "Kansas"]), await state());

// 4. swap, leave, Back
await p.getByRole("button", { name: "Swap teams" }).click(); await p.waitForTimeout(600);
await p.locator("nav.fixed a", { hasText: "Plans" }).click(); await p.waitForURL(/pricing/);
await p.goBack(); await p.waitForTimeout(1200);
check("4. Back after Swap keeps the swapped order", same((await state()).teams, ["Kansas", "Texas St."]), await state());

await b.close();
process.exit(failed ? 1 : 0);
