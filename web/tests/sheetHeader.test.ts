// Game sheet uses the card's one-row headline; no "edge now below" copy anywhere; PK in spread footers. Run: npm test
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, resolve } from "node:path";
import { cardHeaderRow, cardNoteLine } from "../src/lib/cardHeader";
import { fmtLine, fmtOne, fmtSpread } from "../src/lib/labels";
import { notAPlayAlso, notAPlayLine, notAPlayLines } from "../src/lib/verdict";
import type { SlateGame } from "../src/lib/types";

const root = [resolve(__dirname, ".."), resolve(__dirname, "../..")].find((d) => { try { return statSync(join(d, "src/components/SlateBoard.tsx")).isFile(); } catch { return false; } })!;
const read = (rel: string) => readFileSync(join(root, rel), "utf8");
const slate = () => read("src/components/SlateBoard.tsx");
const between = (src: string, a: string, b: string) => src.slice(src.indexOf(a), src.indexOf(b, src.indexOf(a) + a.length));

test("the game sheet renders the same one-row headline as the card", () => {
  const src = slate();
  const card = between(src, "function GameCard(", "\nfunction ");
  const sheet = between(src, "function GameSheet(", "\nfunction ");
  assert.ok(card.length > 1000 && sheet.length > 1000);
  // Card: row = cardHeaderRow(g, bets) → <HeaderRow row={row} />. Sheet: the same component and data.
  assert.match(card, /const row = cardHeaderRow\(g, bets\)/);
  assert.match(card, /<HeaderRow row=\{row\} \/>/);
  assert.match(sheet, /<HeaderRow row=\{cardHeaderRow\(g, bets\)\} \/>/);
  // Same note source (hold reasons only) in both.
  assert.match(card, /cardNoteLine\(g, bets\)/);
  assert.match(sheet, /cardNoteLine\(g, bets\)/);
  // No old headline copy or debug lines left in the sheet.
  assert.doesNotMatch(sheet, /fair vs price|Fair value vs price|edge\.subtitle|postedFairVsProjections|notAPlayLine\(/);
});

test("no customer-visible 'edge now below' string remains in the app source", () => {
  const hits: string[] = [];
  const walk = (dir: string) => {
    for (const f of readdirSync(dir)) {
      const p = join(dir, f);
      if (statSync(p).isDirectory()) walk(p);
      else if (/\.(ts|tsx)$/.test(f) && /edge now below/i.test(readFileSync(p, "utf8"))) hits.push(p);
    }
  };
  walk(join(root, "src"));
  assert.deepEqual(hits, []);
});

test("no 'edge now below' text from the card or sheet note helpers, for any tier", () => {
  const base = {
    sport: "cbb", game_date: "2026-11-22", game_id: 1, home: "Texas St.", away: "McNeese", tipoff: null, game: "McNeese @ Texas St.",
    proj_home: 74.05, proj_away: 67.95, proj_spread: -6.1, proj_total: 144.3, czarp_total: 142, home_win_prob: 0.7, home_cover_prob: 0.6,
    archetypes: { home: [], away: [], home_tentative: false, away_tentative: false, tentative: false, notes: [] },
    market_spread: -2.5, market_total: 142.5, market_source: null, verdict_spread: "low_value", verdict_total: "low_value",
  } as unknown as SlateGame;
  const codes = [[], ["below_open_threshold"], ["below_current_threshold"], ["kelly_too_small"], ["line_moved_against"], ["new_coach_lock"], ["brand_new_rule"]];
  let n = 0;
  for (const es of [null, 0, 0.0199, 0.02, 0.03, 0.04, 0.06]) for (const et of [null, 0, 0.0149, 0.015, 0.0225, 0.03, 0.05]) for (const cs of codes) for (const ct of codes) {
    const g = { ...base, edge_now_pct_spread: es, edge_now_pct_total: et, skip_reasons_spread: cs, skip_reasons_total: ct } as SlateGame;
    const texts = [cardNoteLine(g, []), notAPlayLine(g, []), notAPlayLine(g, [], "spread"), notAPlayLine(g, [], "total"), ...notAPlayAlso(g, []), ...notAPlayLines(g, []),
      cardHeaderRow(g, []).pick, cardHeaderRow(g, []).fallback];
    for (const t of texts) assert.doesNotMatch(t ?? "", /edge now below|below the bar|needs \d/i);
    n++;
  }
  assert.ok(n > 2000);
});

test("PK in the spread footer when the value is 0; totals never PK", () => {
  assert.equal(fmtSpread(0), "PK");
  assert.equal(fmtSpread(-0), "PK");
  assert.equal(fmtSpread(0.04), "PK");
  assert.equal(fmtSpread(-0.04), "PK");
  assert.equal(fmtSpread(-0.1), "-0.1");
  assert.equal(fmtSpread(-6.1), "-6.1");
  assert.equal(fmtSpread(3), "+3.0");
  assert.equal(fmtLine(0), "0.0"); // the generic line formatter stays one decimal
  assert.equal(fmtOne(0), "0.0"); // totals: never PK
  assert.equal(fmtOne(142), "142.0");
  const src = slate();
  // Card footer: Spread FV and Price via fmtSpread; Total via fmtOne.
  const footer = between(src, 'label="Spread"', 'label="Total"');
  assert.match(footer, /fair=\{g\.proj_spread == null \? "—" : fmtSpread\(g\.proj_spread\)\}/);
  assert.match(footer, /price=\{g\.market_spread == null \? "—" : fmtSpread\(g\.market_spread\)\}/);
  assert.match(between(src, 'label="Total"', "/>"), /fmtOne\(g\.market_total\)/);
  // Game sheet Bet tab: spread FV and Price go through spreadRef → fmtSpread; totals use fmtOne / totalFair, never fmtSpread.
  const tabs = read("src/components/GameSheetTabs.tsx");
  assert.match(between(tabs, "function spreadRef(", "\nfunction "), /fmtSpread\(n\)/);
  const bet = between(tabs, "export function QuickBetBlock(", "\nfunction ");
  assert.match(bet, /spreadRef\(czSpread, spreadTeam\)/);
  assert.match(bet, /spreadRef\(curS, spreadTeam\)/);
  assert.match(bet, /const czTotal = totalFair\(g, bets\)/);
  assert.match(bet, /fmtOne\(curT\)/);
  assert.doesNotMatch(bet, /fmtSpread\(/);
  // Team page next game: spread FV and Price via fmtSpread.
  const team = read("src/app/[sport]/team/[slug]/page.tsx");
  assert.match(team, /\$\{next\.home\} \$\{fmtSpread\(next\.proj_spread\)\}/);
  assert.match(team, /\$\{next\.home\} \$\{fmtSpread\(next\.market_spread\)\}/);
});

test("no customer-visible 'needs X%' / 'vs now' headline copy remains in the app source", () => {
  const hits: string[] = [];
  const walk = (dir: string) => {
    for (const f of readdirSync(dir)) {
      const p = join(dir, f);
      if (statSync(p).isDirectory()) walk(p);
      else if (/\.(ts|tsx)$/.test(f)) {
        const src = readFileSync(p, "utf8");
        if (/\bvs now\b/i.test(src) || /\(needs\b|needs \$\{|needs \d/i.test(src)) hits.push(p);
      }
    }
  };
  walk(join(root, "src"));
  assert.deepEqual(hits, []);
});

test("the team page's next game uses the same one-row headline and note as the card and sheet", () => {
  const team = read("src/app/[sport]/team/[slug]/page.tsx");
  assert.match(team, /<HeaderRow row=\{cardHeaderRow\(next, next\.officialBets \?\? \[\]\)\} \/>/);
  assert.match(team, /cardNoteLine\(next, next\.officialBets \?\? \[\]\)/);
  assert.doesNotMatch(team, /subtitle|fair vs price|cardHeadline\(|notAPlayLine\(/);
  // One shared component file; the card and sheet import it too.
  assert.match(slate(), /import \{ HeaderRow, LineMoveNote \} from "\.\/HeaderRow"/);
  assert.doesNotMatch(slate(), /function HeaderRow\(|function HeadlineBody\(|function edgeNumberClass\(/);
});
