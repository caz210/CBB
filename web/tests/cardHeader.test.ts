// Slate card header row (variant C): "SPREAD  Texas St. -2.5  2.5%". Run: npm test
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { NO_EDGE_TEXT, cardHeaderRow, cardNoteLine, edgeTone, fmtPickLine, nowLine, sideAtPrice } from "../src/lib/cardHeader";
import { LINE_MOVE_NOTE } from "../src/lib/verdicts";
import type { SlateGame, ValueBet } from "../src/lib/types";

const game = (o: Partial<SlateGame> = {}): SlateGame => ({
  sport: "cbb", game_date: "2026-11-22", game_id: 1770, home: "Texas St.", away: "McNeese",
  tipoff: null, game: "McNeese @ Texas St.", proj_home: 74.05, proj_away: 67.95, proj_spread: -6.1,
  proj_total: 144.3, czarp_total: 142, home_win_prob: 0.7, home_cover_prob: 0.62,
  archetypes: { home: [], away: [], home_tentative: false, away_tentative: false, tentative: false, notes: [] },
  market_spread: -2.5, market_total: 142.5, market_source: null,
  edge_now_pct_spread: 0.0252, skip_reasons_spread: ["below_open_threshold"],
  edge_now_pct_total: 0.0035, skip_reasons_total: ["below_open_threshold"],
  verdict_spread: "low_value", verdict_total: "low_value",
  ...o,
} as SlateGame);

const posted = (o: Partial<ValueBet> = {}): ValueBet => ({
  sport: "cbb", game_id: 1768, market: "total", side: "Over 136.5", model_prob: null, market_prob: 0, prob_edge: 0,
  line_edge_pct: 0.022, edge_at_post_pct: 0.022, kelly_quarter_units: null, pick_tier: "official",
  posted_line: 136.5, line_at_post: 136.5, fair_at_post: 139.5, ...o,
} as ValueBet);

// Real rows, 11-22 unless noted (anon read).
const troy = () => game({
  game_id: 1768, home: "Nicholls", away: "Troy", game: "Troy vs Nicholls", proj_home: 65.9, proj_away: 73.6, proj_spread: 7.7,
  proj_total: 140.3, czarp_total: 139.5, market_spread: 6.5, market_total: 136.5,
  edge_now_pct_spread: 0.009, edge_now_pct_total: 0.022, skip_reasons_total: [], verdict_total: "official",
});

test("side: home at the price when FV is more negative (McNeese @ Texas St.)", () => {
  assert.deepEqual(sideAtPrice(game(), "spread"), { pick: "Texas St. -2.5", status: "pick" });
  const r = cardHeaderRow(game(), []);
  assert.equal(r.market, "spread");
  assert.equal(r.marketLabel, "Spread");
  assert.equal(r.pick, "Texas St. -2.5");
  assert.equal(r.edgeLabel, "2.5%");
  assert.equal(r.verdict, "lean");
});

test("side: away at the flipped price when FV is less negative than the price", () => {
  // Fairfield @ LIU 11-22: FV LIU -3.4 vs price LIU -2.5 → LIU -2.5 (home).
  assert.equal(sideAtPrice(game({ home: "LIU", away: "Fairfield", proj_spread: -3.4, market_spread: -2.5 }), "spread").pick, "LIU -2.5");
  // FV home -1.0 vs price home -4.5 → the away side +4.5.
  assert.equal(sideAtPrice(game({ proj_spread: -1, market_spread: -4.5 }), "spread").pick, "McNeese +4.5");
  // Home dog: FV home +2.0 vs price home +6.5 → home +6.5.
  assert.equal(sideAtPrice(game({ proj_spread: 2, market_spread: 6.5 }), "spread").pick, "Texas St. +6.5");
  // Rice @ Lamar 11-22: FV Lamar -0.3 vs price Lamar -2.5 → Rice +2.5.
  assert.equal(sideAtPrice(game({ home: "Lamar", away: "Rice", proj_spread: -0.3, market_spread: -2.5 }), "spread").pick, "Rice +2.5");
});

test("side: over / under from the official total", () => {
  assert.equal(sideAtPrice(game({ czarp_total: 145, market_total: 140.5 }), "total").pick, "Over 140.5");
  assert.equal(sideAtPrice(game({ czarp_total: 142, market_total: 142.5 }), "total").pick, "Under 142.5");
});

test("totals always use the official total (czarp_total), never slate_games.proj_total", () => {
  // Fairfield @ LIU 11-22: official 146.0 vs price 145.5 → Over, though proj_total 144.7 would say Under.
  const g = game({ czarp_total: 146, proj_total: 144.7, market_total: 145.5 });
  assert.equal(sideAtPrice(g, "total").pick, "Over 145.5");
  // No official total: no pick, even with a proj_total present.
  assert.deepEqual(sideAtPrice(game({ czarp_total: null, proj_total: 150, market_total: 145.5 }), "total"), { pick: null, status: "missing" });
  // IU Indy vs UMES 11-22: Late Value total, official 145.0 vs 140.5 (proj_total 142.6).
  const iu = game({ home: "Maryland Eastern Shore", away: "IU Indy", proj_spread: 8.9, market_spread: 10.5, czarp_total: 145, proj_total: 142.6, market_total: 140.5,
    edge_now_pct_spread: 0.0114, edge_now_pct_total: 0.032, skip_reasons_total: [] });
  const r = cardHeaderRow(iu, []);
  assert.equal(r.market, "total");
  assert.equal(r.pick, "Over 140.5");
  assert.equal(r.verdict, "late_value");
  assert.equal(r.tone, "green");
});

test("a spread of 0 shows PK", () => {
  assert.equal(fmtPickLine(0), "PK");
  assert.equal(fmtPickLine(-0.04), "PK");
  assert.equal(fmtPickLine(-2.5), "-2.5");
  assert.equal(sideAtPrice(game({ proj_spread: -1.5, market_spread: 0 }), "spread").pick, "Texas St. PK");
  assert.equal(sideAtPrice(game({ proj_spread: 1.5, market_spread: 0 }), "spread").pick, "McNeese PK");
});

test("FV equal to Price: no pick, 'No edge at this price', Toss Up, dim", () => {
  assert.deepEqual(sideAtPrice(game({ proj_spread: -2.5, market_spread: -2.5 }), "spread"), { pick: null, status: "even" });
  assert.deepEqual(sideAtPrice(game({ czarp_total: 142.5, market_total: 142.5 }), "total"), { pick: null, status: "even" });
  const r = cardHeaderRow(game({ proj_spread: -2.5, market_spread: -2.5, edge_now_pct_spread: 0, edge_now_pct_total: 0 }), []);
  assert.equal(r.pick, null);
  assert.equal(r.fallback, NO_EDGE_TEXT);
  assert.equal(r.verdict, "toss_up");
  assert.equal(r.tone, "dim");
});

test("CZarp Play: posted pick and price; 'Now' only when the current line differs; % is the current edge", () => {
  const r = cardHeaderRow(troy(), [posted()]);
  assert.equal(r.verdict, "official");
  assert.equal(r.pick, "Over 136.5");
  assert.equal(r.now, null);
  assert.equal(r.edgeLabel, "2.2%");
  // Army @ New Haven 11-10: posted Over 131.5, line now 132.5.
  const army = game({ game_id: 1, home: "New Haven", away: "Army", market_total: 132.5, czarp_total: 135, edge_now_pct_total: 0.0189, verdict_total: "official" });
  const b = posted({ game_id: 1, side: "Over 131.5", posted_line: 131.5, line_at_post: 131.5, edge_at_post_pct: 0.031 });
  const ra = cardHeaderRow(army, [b]);
  assert.equal(ra.pick, "Over 131.5");
  assert.equal(ra.now, "Now 132.5");
  assert.equal(ra.edgeLabel, "1.8%"); // the current edge (floored below the bar), not the 3.1% at post
  // Spread pick on the away side: "Now" is that side's line.
  const sb = posted({ market: "spread", side: "McNeese +3.5", posted_line: 3.5, line_at_post: 3.5 });
  assert.equal(nowLine(sb, game({ market_spread: -2.5 })), "Now +2.5");
  assert.equal(nowLine(sb, game({ market_spread: -3.5 })), null);
  assert.equal(nowLine(posted({ market: "spread", side: "Texas St. -1.5", posted_line: -1.5, line_at_post: -1.5 }), game({ market_spread: 0 })), "Now PK");
  // A bare side gets the posted price appended.
  assert.equal(cardHeaderRow(troy(), [posted({ side: "Over" })]).pick, "Over 136.5");
});

test("% color by tier; a CZarp Play that lost value is neutral", () => {
  assert.equal(edgeTone("official", false), "green");
  assert.equal(edgeTone("official", true), "neutral");
  assert.equal(edgeTone("late_value", false), "green");
  assert.equal(edgeTone("value", false), "neutral");
  assert.equal(edgeTone("lean", false), "neutral");
  assert.equal(edgeTone("toss_up", false), "dim");
  assert.equal(edgeTone("official", false, false), "dim");
  // Troy vs Nicholls: posted 2.2%, edge now 2.2% < 3% → line-move note, neutral.
  const r = cardHeaderRow(troy(), [posted()]);
  assert.equal(r.lineMoveNote, true);
  assert.equal(r.tone, "neutral");
  assert.equal(LINE_MOVE_NOTE, "No longer a value play due to line movement.");
  // Same pick with the edge back at the bar: green.
  assert.equal(cardHeaderRow({ ...troy(), edge_now_pct_total: 0.03 }, [posted()]).tone, "green");
  assert.equal(cardHeaderRow(game(), []).tone, "neutral"); // Lean
  assert.equal(cardHeaderRow(game({ edge_now_pct_spread: 0.006 }), []).tone, "dim"); // Toss Up
});

test("one market in the header: the stronger by rank, ties to the bigger edge", () => {
  // Spread Lean 2.5% vs total Late Value 3.1%: total.
  assert.equal(cardHeaderRow(game({ edge_now_pct_total: 0.031, skip_reasons_total: [] }), []).market, "total");
  // Both Lean: bigger edge (spread 2.5% vs total 1.6%).
  assert.equal(cardHeaderRow(game({ edge_now_pct_total: 0.016 }), []).market, "spread");
  // CZarp Play beats a Late Value spread.
  assert.equal(cardHeaderRow({ ...troy(), edge_now_pct_spread: 0.09, skip_reasons_spread: [] }, [posted()]).market, "total");
});

test("the 'Not a play: edge now below' line is gone; only hold reasons remain", () => {
  assert.equal(cardNoteLine(game(), []), null); // Lean
  assert.equal(cardNoteLine(game({ edge_now_pct_spread: 0.006 }), []), null); // Toss Up
  assert.equal(cardNoteLine(game({ edge_now_pct_spread: 0.035 }), []), null); // Value below the bar
  assert.equal(cardNoteLine(game({ edge_now_pct_total: 0.031, skip_reasons_total: [] }), []), null); // Late Value
  assert.equal(cardNoteLine(troy(), [posted()]), null); // CZarp Play
  // Rice @ Lamar 11-22: held Value total (4.83%, market moved 3+ pts) keeps its hold reason.
  const rice = game({ home: "Lamar", away: "Rice", proj_spread: -0.3, market_spread: -2.5, czarp_total: 157.5, market_total: 165.5,
    edge_now_pct_spread: 0.013, edge_now_pct_total: 0.0483, skip_reasons_total: ["line_moved_against"] });
  assert.equal(cardNoteLine(rice, []), "Not a play: Total moved 3+ pts away from CZarp since open");
  assert.equal(cardHeaderRow(rice, []).pick, "Under 165.5");
  // New-coach hold.
  const coach = game({ edge_now_pct_total: 0.05, skip_reasons_total: ["new_coach_lock"], skip_reason_total: "paused until game 4 (new coach, 2 played)" });
  assert.match(cardNoteLine(coach, []) ?? "", /^Not a play: paused until game 4/);
  for (const g of [game(), game({ edge_now_pct_spread: 0.035 }), game({ edge_now_pct_spread: 0.006 })]) {
    assert.doesNotMatch(cardNoteLine(g, []) ?? "", /edge now below/);
  }
});

test("the slate card no longer renders the below-bar note or the posted-FV mono line", () => {
  const f = [resolve(__dirname, "../src/components/SlateBoard.tsx"), resolve(__dirname, "../../src/components/SlateBoard.tsx")].find((x) => { try { readFileSync(x); return true; } catch { return false; } });
  const src = readFileSync(f!, "utf8");
  const card = src.slice(src.indexOf("function GameCard("), src.indexOf("function MarketStat("));
  assert.ok(card.length > 1000);
  assert.doesNotMatch(card, /notAPlayLine\(/);
  assert.doesNotMatch(card, /postedFairVsProjections\(/);
  assert.doesNotMatch(card, /fair vs price/);
  assert.match(card, /cardNoteLine\(g, bets\)/);
});
