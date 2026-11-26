import { test } from "node:test";
import assert from "node:assert/strict";
import { existsSync, readFileSync, readdirSync, statSync } from "node:fs";
import { join, resolve } from "node:path";
import { bottomNavLinks, headerNavLinks, homeCtas } from "../src/lib/accounts";
import { MIN_STAKE_UNITS } from "../src/lib/labels";
import { OFFICIAL_CATCHUP_TIME, OFFICIAL_POST_HOME_NOTE, OFFICIAL_POST_MINUTES_CT, OFFICIAL_POST_TIME } from "../src/lib/officialPost";
import { HOLD_CODES } from "../src/lib/verdicts";
import { HOME_DESCRIPTION, HOME_TITLE, TITLE_SUFFIX, pageMetadata } from "../src/lib/seo";
import { CONTACT_EMAIL, CONTACT_MAILTO } from "../src/lib/contact";
import { marketFromPick, rollupMarketUnits, type MarketUnits } from "../src/lib/data";
import type { LedgerRow } from "../src/lib/types";

const root = [resolve(__dirname, ".."), resolve(__dirname, "../..")].find((d) => {
  try { return statSync(join(d, "src/app/globals.css")).isFile(); } catch { return false; }
})!;
const read = (rel: string) => readFileSync(join(root, rel), "utf8");

test("the unit floor is 0.1u (pipeline MIN_STAKE $1 on a $1,000 bankroll) and public copy uses units", () => {
  assert.equal(MIN_STAKE_UNITS, 0.1);
  assert.equal(HOLD_CODES.kelly_too_small, "stake under 0.1u");
  assert.doesNotMatch(HOLD_CODES.kelly_too_small, /\$/);
  assert.match(read("src/app/methodology/page.tsx"), /stake of at least 0\.1u/);
});

test("methodology intro and hold copy: the 3+ pt move skip is totals-only", () => {
  const intro = read("src/app/methodology/page.tsx");
  assert.match(
    intro,
    /4% edge on sides, 3% on totals, a stake of at least 0\.1u, and on totals, no 3-point-or-more move against us since the open/,
  );
  assert.doesNotMatch(intro, /0\.1u, and no 3-point-or-more move/);
  assert.equal(HOLD_CODES.line_moved_against, "Total moved 3+ pts away from CZarp since open");
  assert.match(
    read("src/lib/verdicts.ts"),
    /new-coach pause, a 3\+ point move against our total, or stake under/,
  );
});

test("official post is 6:30 AM CT with a 7:00 AM CT catch‑up, both in one module, and retired 7:30 / 7:15 times are gone from public copy", () => {
  assert.equal(OFFICIAL_POST_TIME, "6:30 AM CT");
  assert.equal(OFFICIAL_POST_MINUTES_CT, 6 * 60 + 30);
  assert.equal(OFFICIAL_CATCHUP_TIME, "7:00 AM CT");
  assert.equal(
    OFFICIAL_POST_HOME_NOTE,
    "Official plays post every morning by about 6:30 AM CT, with a single 7:00 AM CT catch‑up for markets that had no line yet. Follow @CZarpAnalytics on X.",
  );
  const skip = new Set(["node_modules", ".next", ".test-build", "coverage"]);
  const retired: string[] = [];
  const hardcoded: string[] = [];
  const stale: string[] = [];
  const walk = (dir: string) => {
    for (const f of readdirSync(dir)) {
      if (skip.has(f)) continue;
      const p = join(dir, f);
      if (statSync(p).isDirectory()) walk(p);
      else if (/\.(ts|tsx|mjs)$/.test(f) && !p.endsWith("officialPost.ts")) {
        const text = readFileSync(p, "utf8");
        if (/7:15|7:30\s*AM|7 \* 60 \+ 30/.test(text)) retired.push(p);
        if (/7:00\s*AM/.test(text)) hardcoded.push(p);
        if (/set once|never added later/i.test(text)) stale.push(p);
      }
    }
  };
  walk(join(root, "src"));
  assert.deepEqual(retired, []);
  assert.deepEqual(hardcoded, [], "hardcode 7:00 AM CT via OFFICIAL_CATCHUP_TIME");
  assert.deepEqual(stale, []);
});

test("header and bottom nav say Record, not Portfolio, and home CTAs go to the slate and the record", () => {
  assert.deepEqual(
    headerNavLinks("cbb").filter((l) => l.match === "/results").map((l) => l.label),
    ["Record"],
  );
  assert.deepEqual(
    bottomNavLinks("cbb").filter((l) => l.match === "/results").map((l) => l.label),
    ["Record"],
  );
  assert.deepEqual(
    homeCtas().map((l) => ({ href: l.href, label: l.label, tone: l.tone })),
    [
      { href: "/cbb/slate", label: "Today's slate", tone: "primary" },
      { href: "/cbb/results", label: "See the record", tone: "secondary" },
    ],
  );
});

test("hero, about, results, and footer use the approved marketing copy", () => {
  const home = read("src/app/page.tsx");
  assert.match(home, /The market&apos;s wrong more often than you think\./);
  assert.match(home, /CZarp prices every D1 men&apos;s game and only plays when the edge is real\. Every pick is time-stamped\. Every loss is on the record\./);
  assert.match(home, /Edge % is the headline\. We only play when the edge is real\./);
  assert.doesNotMatch(home, /No alerts yet/);
  assert.doesNotMatch(home, /Buy bets like|stocks — at a price|Full portfolio|Season portfolio/i);
  assert.match(home, /OFFICIAL_POST_TIME/);
  assert.match(home, /OFFICIAL_CATCHUP_TIME/);
  assert.match(home, /https:\/\/x\.com\/CZarpAnalytics/);

  const about = read("src/app/about/page.tsx");
  assert.match(about, /CZarp prices every D1 men&apos;s game and only plays when the edge is real\. Every official pick is time-stamped before tip\. Every loss stays on the record\./);
  assert.doesNotMatch(about, /buying stocks|treat bets like/i);

  const results = read("src/app/[sport]/results/page.tsx");
  assert.match(results, /title="The public record"/);
  assert.match(results, /Every official pick is time-stamped and graded in public\. Early sample: \$\{res\.data\.summary\.graded\} graded picks — evidence, not proof\./);
  assert.match(results, /units\?\.roi/);
  assert.doesNotMatch(results, /The book, treated like a portfolio|Buy at value/);

  const footer = read("src/components/SiteFooter.tsx");
  assert.match(footer, /Daily plays on X:/);
  assert.match(footer, /https:\/\/x\.com\/CZarpAnalytics/);
  assert.match(footer, /https:\/\/www\.instagram\.com\/czarp_analytics_club/);
  assert.match(footer, /rel="noopener noreferrer"/);
  assert.match(footer, /target="_blank"/);
  assert.doesNotMatch(footer, /waitlist|type="email"|<input/i);

  const slate = read("src/components/SlateBoard.tsx");
  assert.match(slate, /Price = market consensus\./);
  assert.doesNotMatch(slate, /Vegas consensus/);
});

test("by-market ROI is the stake-sized figure from the same ledger the headline uses, with flat 1u as the secondary line", () => {
  const row = (over: Partial<LedgerRow>): LedgerRow => ({
    sport: "cbb",
    game_date: "2026-11-22",
    game_id: 1,
    pick: "Home -3.5",
    line: -3.5,
    odds: -110,
    stake_units: 1,
    result: "W",
    profit_units: 0.91,
    is_paper: false,
    pick_tier: "official",
    flat_units: 0.91,
    ...over,
  });
  const ledger: LedgerRow[] = [
    row({ game_id: 1, pick: "Home -3.5", stake_units: 2, profit_units: 1.82, flat_units: 0.91, result: "W" }),
    row({ game_id: 2, pick: "Away +4.5", stake_units: 1, profit_units: -1, flat_units: -1, result: "L" }),
    row({ game_id: 3, pick: "Under 140.5", stake_units: 0.8, profit_units: 0.72, flat_units: 0.91, result: "W" }),
    row({ game_id: 4, pick: "Over 150.5", stake_units: 1.2, profit_units: null, flat_units: null, result: null }), // pending
  ];
  assert.equal(marketFromPick("Under 140.5"), "total");
  assert.equal(marketFromPick("Home -3.5"), "spread");
  const by = rollupMarketUnits(ledger, "official");
  const spread: MarketUnits = by.spread;
  const total: MarketUnits = by.total;
  assert.equal(spread.kelly, 0.82);
  assert.equal(spread.flat, -0.09);
  assert.equal(spread.staked, 3);
  assert.equal(spread.roi, 0.82 / 3);
  assert.equal(total.kelly, 0.72);
  assert.equal(total.staked, 0.8);
  assert.equal(total.roi, 0.72 / 0.8);
  assert.equal(by.moneyline, undefined);
  // Same formula as the headline record: kelly profit / staked, not a flat 1u ratio and not hardcoded.
  const staked = 2 + 1 + 0.8;
  const units = 1.82 + -1 + 0.72;
  assert.equal(units / staked, (spread.kelly + total.kelly) / (spread.staked + total.staked));
});

test("SEO titles, unique descriptions, canonicals, and the title template do not double the brand", () => {
  const layout = read("src/app/layout.tsx");
  assert.match(layout, /template:\s*`%s\$\{TITLE_SUFFIX\}`/);
  assert.match(layout, /HOME_TITLE/);
  assert.equal(TITLE_SUFFIX, " | CZarp Analytics");
  assert.equal(HOME_TITLE, "CZarp Analytics — Daily D1 Men's Hoops Picks & Public Record");
  assert.match(HOME_DESCRIPTION, /units only/);

  const slate = read("src/app/[sport]/slate/page.tsx");
  assert.match(slate, /Today's CBB Slate — Official Plays & Edge %/);
  assert.doesNotMatch(slate, /Today's CBB Slate — Official Plays & Edge % \| CZarp Analytics/);

  const results = read("src/app/[sport]/results/page.tsx");
  assert.match(results, /CBB Public Record — Units, ROI & Every Graded Pick/);
  assert.doesNotMatch(results, /CBB Public Record — Units, ROI & Every Graded Pick \| CZarp Analytics/);

  const about = read("src/app/about/page.tsx");
  assert.match(about, /CZarp prices every D1 men's game and only plays when the edge is real\. A public, time-stamped record in units\./);

  const home = pageMetadata({ title: HOME_TITLE, description: HOME_DESCRIPTION, path: "/", absolute: true });
  assert.deepEqual(home.title, { absolute: HOME_TITLE });
  assert.equal((home.alternates as { canonical: string }).canonical, "https://www.czarpanalytics.com/");
  assert.equal((home.openGraph as { images: { url: string }[] }).images[0].url, "/og.png");

  const inner = pageMetadata({ title: "Methodology", description: "How CZarp grades a game.", path: "/methodology" });
  assert.equal(inner.title, "Methodology");
  assert.equal((inner.openGraph as { title: string }).title, "Methodology | CZarp Analytics");
  assert.notEqual(inner.description, HOME_DESCRIPTION);

  const files = [
    "src/app/page.tsx",
    "src/app/about/page.tsx",
    "src/app/methodology/page.tsx",
    "src/app/terms/page.tsx",
    "src/app/privacy/page.tsx",
    "src/app/[sport]/slate/page.tsx",
    "src/app/[sport]/results/page.tsx",
    "src/app/[sport]/rankings/page.tsx",
    "src/app/[sport]/matchup/page.tsx",
    "src/app/[sport]/team/[slug]/page.tsx",
  ];
  const descriptions: string[] = [];
  for (const f of files) {
    const src = read(f);
    assert.match(src, /pageMetadata\(/, f);
    assert.match(src, /canonical|\bpath\b/, f);
  }
  assert.ok(existsSync(join(root, "src/lib/seo.ts")));
});

test("terms and privacy drop the template banner and share one contact-email constant", () => {
  assert.equal(CONTACT_EMAIL, "CZarp210@gmail.com");
  assert.equal(CONTACT_MAILTO, "mailto:CZarp210@gmail.com");
  for (const rel of ["src/app/terms/page.tsx", "src/app/privacy/page.tsx"]) {
    const src = read(rel);
    assert.doesNotMatch(src, /TEMPLATE|pending owner review|banner=/);
    assert.doesNotMatch(src, /will be reviewed|This draft|draft pending|pending review/i);
    assert.match(src, /Last updated: November 9, 2026\./);
    assert.match(src, /<ContactEmail/);
    assert.doesNotMatch(src, /CZarp210@gmail\.com/);
  }
  assert.match(read("src/components/LegalPage.tsx"), /CONTACT_MAILTO/);
  assert.match(read("src/app/privacy/page.tsx"), /Contact us at/);
});

test("by-market secondary lines wrap between nowrap value chunks, not mid-token", () => {
  const src = read("src/app/[sport]/results/page.tsx");
  assert.match(src, /className="whitespace-nowrap">\{fmtUnits\(units\?\.kelly \?\? 0, 1\)\} ¼-K</);
  assert.match(src, /className="whitespace-nowrap">Flat 1u: \{fmtUnits\(units\?\.flat \?\? r\.units, 1\)\}</);
  assert.match(src, /className="whitespace-nowrap">CLV \{fmtClv\(r\.avg_clv\)\}</);
  assert.match(src, /className="whitespace-nowrap">beat close \{r\.beat_close\}</);
});
