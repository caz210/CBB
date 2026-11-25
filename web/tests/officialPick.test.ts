// Official-play GameSheet destination: deep links, ledger stake/result merge, banner states.
// Run: npm test
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, resolve } from "node:path";
import {
  applyOfficialRecord,
  gameSheetHref,
  officialPickViews,
  OFFICIAL_PENDING_NOTE,
  stakeUnitsLabel,
} from "../src/lib/officialPick";
import type { SlateGame, ValueBet } from "../src/lib/types";

const root = [resolve(__dirname, ".."), resolve(__dirname, "../..")].find((d) => {
  try { return statSync(join(d, "src/components/SlateBoard.tsx")).isFile(); } catch { return false; }
})!;
const read = (rel: string) => readFileSync(join(root, rel), "utf8");

test("gameSheetHref is /{sport}/slate?date=YYYY-MM-DD&game={id}", () => {
  assert.equal(gameSheetHref("cbb", "2026-11-24", 1956), "/cbb/slate?date=2026-11-24&game=1956");
  assert.equal(gameSheetHref("cbb", "2026-11-23", 1), "/cbb/slate?date=2026-11-23&game=1");
});

const game = (over: Partial<SlateGame> = {}): SlateGame => ({
  sport: "cbb",
  game_date: "2026-11-24",
  game_id: 1956,
  home: "BYU",
  away: "Mississippi",
  tipoff: "2026-11-24T19:30:00+00:00",
  game: "Mississippi vs BYU",
  neutral: true,
  proj_home: 75.75,
  proj_away: 78.25,
  proj_spread: 2.5,
  proj_total: 154,
  czarp_total: 154,
  home_win_prob: 0.41,
  home_cover_prob: 0.4,
  archetypes: { home: [], away: [], home_tentative: false, away_tentative: false, tentative: false, notes: [] },
  market_spread: -1.5,
  market_total: 160.5,
  market_source: "vegas",
  verdict_spread: "low_value",
  verdict_total: "official",
  edge_now_pct_total: 0.0405,
  edge_vs_open_pct_total: 0.0405,
  ...over,
} as SlateGame);

const bet = (over: Partial<ValueBet> = {}): ValueBet => ({
  sport: "cbb",
  game_id: 1956,
  market: "total",
  side: "Under 160.5",
  model_prob: null,
  market_prob: 0,
  prob_edge: 0,
  line_edge_pct: 0.0405,
  kelly_quarter_units: null,
  pick_tier: "official",
  fair_at_post: 154,
  line_at_post: 160.5,
  posted_line: 160.5,
  open_line: 160.5,
  edge_at_post_pct: 0.0405,
  edge_vs_open_pct: 0.0405,
  posted_at: "2026-11-24T13:16:18+00:00",
  logged_at: "2026-11-24T13:16:18+00:00",
  pick_hash: "abc",
  ...over,
});

test("applyOfficialRecord attaches ledger stake, result, P/L and CLV; ignores tracked", () => {
  const official = bet();
  const tracked = bet({ pick_tier: "track", side: "Under 160.5", pick_hash: "track-1" });
  const [a, b] = applyOfficialRecord([official, tracked], [{
    game_id: 1956,
    pick: "Under 160.5",
    stake_units: 0.35,
    result: "W",
    profit_units: 0.32,
    clv: 2.0,
    logged_at: "2026-11-24T13:16:18+00:00",
    pick_hash: "abc",
  }]);
  assert.equal(a.stake_units, 0.35);
  assert.equal(a.result, "W");
  assert.equal(a.profit_units, 0.32);
  assert.equal(a.clv, 2.0);
  assert.equal(b.stake_units, undefined);
  assert.equal(b.result, undefined);
});

test("applyOfficialRecord matches on pick text when hash is missing, and does not invent a stake", () => {
  const [a] = applyOfficialRecord([bet({ pick_hash: null })], [{
    game_id: 1956, pick: "Under 160.5", stake_units: 0.59, result: null, profit_units: null, clv: null,
  }]);
  assert.equal(a.stake_units, 0.59);
  assert.equal(a.result, null);
  const [b] = applyOfficialRecord([bet({ side: "Over 145.5", pick_hash: null, game_id: 1999 })], [{
    game_id: 1956, pick: "Under 160.5", stake_units: 0.35, result: "W", profit_units: 0.32, clv: 2,
  }]);
  assert.equal(b.stake_units, undefined);
  assert.equal(b.result, undefined);
});

test("pre-grade Official Pick banner: stake from ledger, FV not Posted, pending note, no result", () => {
  const b = applyOfficialRecord([bet({
    game_id: 1977, side: "Under 157.5", fair_at_post: 151.5, line_at_post: 157.5, posted_line: 157.5,
    open_line: 157.5, pick_hash: "vandy",
  })], [{
    game_id: 1977, pick: "Under 157.5", stake_units: 0.59, result: null, profit_units: null, clv: null,
  }])[0];
  const g = game({
    game_id: 1977, home: "Ohio St.", away: "Vanderbilt", game: "Vanderbilt @ Ohio St.",
    market_total: 157.5, edge_now_pct_total: 0.038, edge_vs_open_pct_total: 0.038,
    insight: { open_total: 157.5, cur_total: 157.5 },
  } as Partial<SlateGame>);
  const views = officialPickViews(g, [b]);
  assert.equal(views.length, 1);
  const v = views[0];
  assert.equal(v.side, "Under 157.5");
  assert.equal(v.marketLabel, "Total");
  assert.equal(v.stakeLabel, "0.59u");
  assert.equal(v.fairLabel, "151.5");
  assert.equal(v.priceLabel, "157.5");
  assert.equal(v.postedLineLabel, "157.5");
  assert.equal(v.postedAtLabel, "7:16 AM CT"); // 13:16 UTC
  assert.equal(v.result, null);
  assert.equal(v.resultLabel, null);
  assert.equal(v.profitLabel, null);
  assert.equal(v.pendingNote, OFFICIAL_PENDING_NOTE);
  assert.doesNotMatch(JSON.stringify(v), /Posted 151|Posted 15[0-9]\.5(?! AM)/);
  assert.doesNotMatch(v.pendingNote ?? "", /\$|dollar/i);
  assert.match(v.edgeLabel, /^\d+\.\d%$/);
});

test("graded Official Pick banner: W, P/L in units, CLV, no pending note", () => {
  const b = applyOfficialRecord([bet()], [{
    game_id: 1956, pick: "Under 160.5", stake_units: 0.35, result: "W", profit_units: 0.32, clv: 2.0,
  }])[0];
  const g = game({ insight: { open_total: 160.5, cur_total: 160.5 } } as Partial<SlateGame>);
  const v = officialPickViews(g, [b])[0];
  assert.equal(v.stakeLabel, "0.35u");
  assert.equal(v.fairLabel, "154.0");
  assert.equal(v.priceLabel, "160.5");
  assert.equal(v.result, "W");
  assert.equal(v.resultLabel, "W");
  assert.equal(v.profitLabel, "+0.32u");
  assert.equal(v.clvLabel, "+2.0 pts");
  assert.equal(v.pendingNote, null);
  assert.doesNotMatch(`${v.fairLabel} ${v.priceLabel}`, /Posted/);
});

test("multi official bets produce one banner block each, official only", () => {
  const total = applyOfficialRecord([bet()], [{
    game_id: 1956, pick: "Under 160.5", stake_units: 0.35, result: "W", profit_units: 0.32, clv: 2,
  }])[0];
  const spread = applyOfficialRecord([bet({
    market: "spread", side: "BYU +1.5", line_edge_pct: 0.045, fair_at_post: 2.5, line_at_post: 1.5,
    posted_line: 1.5, pick_hash: "spr",
  })], [{
    game_id: 1956, pick: "BYU +1.5", stake_units: 0.80, result: "L", profit_units: -0.80, clv: -0.5, pick_hash: "spr",
  }])[0];
  const tracked = bet({ pick_tier: "track", side: "Over 160.5", pick_hash: "trk" });
  const views = officialPickViews(game({ verdict_spread: "official", edge_now_pct_spread: 0.045 }), [total, spread, tracked]);
  assert.equal(views.length, 2);
  assert.deepEqual(views.map((x) => x.side).sort(), ["BYU +1.5", "Under 160.5"]);
  const w = views.find((x) => x.side === "Under 160.5")!;
  const l = views.find((x) => x.side === "BYU +1.5")!;
  assert.equal(w.result, "W");
  assert.equal(w.stakeLabel, "0.35u");
  assert.equal(l.result, "L");
  assert.equal(l.profitLabel, "−0.80u");
  assert.equal(l.stakeLabel, "0.80u");
});

test("below-bar official edge uses the #83 open/now context (floored now %)", () => {
  const b = bet({
    edge_at_post_pct: 0.0399,
    line_edge_pct: 0.0399,
    edge_vs_open_pct: 0.0399,
    fair_at_post: 144.5,
    line_at_post: 150.5,
    open_line: 150.5,
  });
  const g = game({
    market_total: 148.5,
    edge_now_pct_total: 0.0269,
    edge_vs_open_pct_total: 0.0399,
    insight: { open_total: 150.5, cur_total: 148.5 },
  } as Partial<SlateGame>);
  const v = officialPickViews(g, [b])[0];
  assert.equal(v.belowBar, true);
  assert.equal(v.edgeLabel, "2.6%"); // floored, not 2.7
  assert.equal(v.lineMoveNote, false);
  assert.equal(v.openNowContext, "4.0% at open 150.5 · 2.6% now");
});

test("stakeUnitsLabel is 2-decimal units, no leading plus", () => {
  assert.equal(stakeUnitsLabel(0.35), "0.35u");
  assert.equal(stakeUnitsLabel(0.59), "0.59u");
  assert.equal(stakeUnitsLabel(null), null);
  assert.equal(stakeUnitsLabel(undefined), null);
});

// ---- routing / copy source scans ----

test("official-play taps open GameSheet; ValueSheet and dollar EV are gone", () => {
  const slate = read("src/components/SlateBoard.tsx");
  const tabs = read("src/components/GameSheetTabs.tsx");
  const home = read("src/app/page.tsx");
  const results = read("src/app/[sport]/results/page.tsx");
  const team = read("src/app/[sport]/team/[slug]/page.tsx");
  assert.doesNotMatch(slate, /function ValueSheet|setSel\(/);
  assert.match(slate, /officialPickViews\(g, bets\)/);
  assert.match(slate, /<OfficialPickSection views=\{officialViews\} \/>/);
  assert.match(slate, /onClick=\{\(\) => g && openGame\(g\)\}/);
  assert.match(slate, /onGame=\{\(\) => openGame\(g\)\}/);
  assert.doesNotMatch(slate + tabs, /EV \/ \$100|fmtEv100|evPer100/);
  assert.match(home, /gameSheetHref\("cbb", g\.game_date, g\.game_id\)/);
  assert.match(home, /gameSheetHref\("cbb", r\.game_date, r\.game_id\)/);
  assert.match(results, /gameSheetHref\(r\.sport, r\.game_date, r\.game_id\)/);
  assert.match(team, /gameSheetHref\(sportKey, next\.game_date, next\.game_id\)/);
});

test("clickable official rows use a non-interactive Neutral chip so the tip cannot steal the tap", () => {
  const slate = read("src/components/SlateBoard.tsx");
  const home = read("src/app/page.tsx");
  const results = read("src/app/[sport]/results/page.tsx");
  // CZarp Plays list and home official cards and results ledger.
  assert.match(slate, /interactive=\{false\}/);
  assert.match(home, /interactive=\{false\}/);
  assert.match(results, /interactive=\{false\}/);
});

test("no EV / $100 or EV +$ in customer UI source", () => {
  const hits: string[] = [];
  const walk = (dir: string) => {
    for (const f of readdirSync(dir)) {
      const p = join(dir, f);
      if (statSync(p).isDirectory()) {
        if (f === "node_modules" || f === ".next") continue;
        walk(p);
      } else if (/\.(ts|tsx)$/.test(f)) {
        const src = readFileSync(p, "utf8");
        if (/EV \/ \$100|EV \+\$/.test(src)) hits.push(p);
      }
    }
  };
  walk(join(root, "src"));
  assert.deepEqual(hits, []);
});
