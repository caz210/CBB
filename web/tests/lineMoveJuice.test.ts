// Line-move note (away from CZarp since post) and juice skip wording. Run: npm test
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { cardHeaderRow } from "../src/lib/cardHeader";
import { favoredSidePrice, kellyHoldLabel, kellyStakeUnits, KELLY_HIT_RATE, parseMarketPrices, priceKillsMinStake } from "../src/lib/kelly";
import { fmtOdds, MIN_STAKE_UNITS } from "../src/lib/labels";
import { pickStatusTags } from "../src/lib/statusTags";
import type { SlateGame, ValueBet } from "../src/lib/types";
import { cardHeadline, formatSkip, notAPlayLine } from "../src/lib/verdict";
import { HOLD_CODES, LINE_MOVE_NOTE, lineMovedAwayFromCzarp, officialOpenNowContext, showsLineMoveNote } from "../src/lib/verdicts";

const game = (o: Partial<SlateGame> = {}): SlateGame => ({
  sport: "cbb", game_date: "2026-11-23", game_id: 1856, home: "Clemson", away: "Mississippi",
  tipoff: null, game: "Mississippi @ Clemson", proj_home: 70, proj_away: 77, proj_spread: 7.5,
  proj_total: 147.1, czarp_total: 144.5, home_win_prob: 0.3, home_cover_prob: 0.4,
  archetypes: { home: [], away: [], home_tentative: false, away_tentative: false, tentative: false, notes: [] },
  market_spread: 5.5, market_total: 148.5, market_source: "vegas",
  edge_now_pct_spread: 0.0132, edge_vs_open_pct_spread: 0.013,
  edge_now_pct_total: 0.0269, edge_vs_open_pct_total: 0.0399,
  skip_reasons_spread: ["below_open_threshold"], skip_reasons_total: [],
  verdict_spread: "low_value", verdict_total: "official",
  ...o,
} as SlateGame);

const bet = (o: Partial<ValueBet> = {}): ValueBet => ({
  sport: "cbb", game_id: 1856, market: "total", side: "Under 148.5",
  model_prob: 0.535, market_prob: 0.5238, prob_edge: 0.0112, line_edge_pct: 0.0269,
  kelly_quarter_units: 0.59, pick_tier: "official",
  posted_line: 148.5, line_at_post: 148.5, fair_at_post: 144.5, open_line: 150.5,
  edge_at_post_pct: 0.0269, edge_vs_open_pct: 0.0399,
  ...o,
} as ValueBet);

test("a move toward CZarp is not against the posted pick; a move away is", () => {
  // Under: fair 144.5, posted 148.5. Dropping to 147.5 is toward CZarp; rising to 151.5 is away.
  assert.equal(lineMovedAwayFromCzarp(148.5, 147.5, 144.5), false);
  assert.equal(lineMovedAwayFromCzarp(148.5, 148.5, 144.5), false);
  assert.equal(lineMovedAwayFromCzarp(148.5, 151.5, 144.5), true);
  // Over: fair 139.5, posted 136.5. Rising to 137.5 is toward; dropping to 134.5 is away.
  assert.equal(lineMovedAwayFromCzarp(136.5, 137.5, 139.5), false);
  assert.equal(lineMovedAwayFromCzarp(136.5, 134.5, 139.5), true);
});

test("Mississippi @ Clemson 11-23: U148.5, open 150.5 toward CZarp 144.5, no move since post — no line-move tag", () => {
  const g = game();
  const b = bet();
  const r = cardHeaderRow(g, [b]);
  assert.equal(r.verdict, "official");
  assert.equal(r.pick, "Under 148.5");
  assert.equal(r.now, null); // posted 148.5, current 148.5 — keep nowLine()'s "no change" silence
  assert.equal(r.lineMoveNote, false);
  assert.equal(r.belowBar, true);
  assert.equal(r.tone, "neutral");
  assert.equal(r.openNowContext, "4.0% at open 150.5 · 2.6% now"); // 2.69% floored vs the 3% bar
  assert.equal(r.edgeLabel, "2.6%");
  assert.equal(officialOpenNowContext("total", 0.0399, 150.5, 0.0269), "4.0% at open 150.5 · 2.6% now");
  const tags = pickStatusTags(g, [b]);
  assert.equal(tags.length <= 2, true);
  assert.equal(tags[0]?.kind, "verdict");
  // If the same pick's line then moved away from CZarp, the tag comes back.
  const away = cardHeaderRow({ ...g, market_total: 151.5, edge_now_pct_total: 0.02 }, [b]);
  assert.equal(away.now, "Now 151.5");
  assert.equal(away.lineMoveNote, true);
  assert.equal(away.openNowContext, null);
  assert.equal(LINE_MOVE_NOTE, "No longer a value play due to line movement.");
});

test("Sunday 11-22: Troy @ Nicholls and Southern Miss @ Valparaiso — toward CZarp before post, no move since, no tag", () => {
  const troyG = game({
    game_id: 1768, game_date: "2026-11-22", home: "Nicholls", away: "Troy", game: "Troy @ Nicholls",
    czarp_total: 139.5, market_total: 136.5, edge_now_pct_total: 0.022, edge_vs_open_pct_total: 0.0372,
    verdict_total: "official",
  });
  const troyB = bet({
    game_id: 1768, side: "Over 136.5", posted_line: 136.5, line_at_post: 136.5, fair_at_post: 139.5,
    open_line: 134.5, edge_at_post_pct: 0.022, edge_vs_open_pct: 0.0372, line_edge_pct: 0.022,
  });
  const troy = cardHeaderRow(troyG, [troyB]);
  assert.equal(troy.lineMoveNote, false);
  assert.equal(troy.openNowContext, "3.7% at open 134.5 · 2.2% now");
  assert.equal(troy.now, null);

  const valpoG = game({
    game_id: 1781, game_date: "2026-11-22", home: "Valparaiso", away: "Southern Miss", game: "Southern Miss @ Valparaiso",
    czarp_total: 149, market_total: 145.5, edge_now_pct_total: 0.0241, edge_vs_open_pct_total: 0.0311,
    verdict_total: "official",
  });
  const valpoB = bet({
    game_id: 1781, side: "Over 145.5", posted_line: 145.5, line_at_post: 145.5, fair_at_post: 149,
    open_line: 144.5, edge_at_post_pct: 0.0241, edge_vs_open_pct: 0.0311, line_edge_pct: 0.0241,
  });
  const valpo = cardHeaderRow(valpoG, [valpoB]);
  assert.equal(valpo.lineMoveNote, false);
  assert.equal(valpo.openNowContext, "3.1% at open 144.5 · 2.4% now");
  assert.equal(valpo.now, null);
  assert.equal(pickStatusTags(valpoG, [valpoB]).length <= 2, true);
});

test("totals Kelly at a flat 53.5% hit rate: -115 or worse kills a 0.1u stake; -110 does not", () => {
  assert.equal(KELLY_HIT_RATE.total, 0.535);
  assert.equal(KELLY_HIT_RATE.spread, 0.545);
  assert.ok(kellyStakeUnits(0.535, -110) >= MIN_STAKE_UNITS);
  assert.ok(kellyStakeUnits(0.535, -114) >= MIN_STAKE_UNITS);
  assert.equal(priceKillsMinStake("total", -115), true);
  assert.equal(priceKillsMinStake("total", -110), false);
  assert.equal(priceKillsMinStake("spread", -115), false); // sides use 54.5%; -115 still clears
});

test("Northern Iowa @ Davidson 11-23: O141.5 -115 is a juice skip, not the generic 0.1u floor", () => {
  const g = game({
    game_id: 1850, home: "Davidson", away: "Northern Iowa", game: "Northern Iowa @ Davidson",
    czarp_total: 147, market_total: 141.5, proj_spread: -6, market_spread: -5.5,
    edge_now_pct_total: 0.0389, edge_vs_open_pct_total: 0.0316,
    skip_reasons_total: ["kelly_too_small"],
    skip_reason_total: "price too juiced for the assumed hit rate (Kelly stake < $1)",
    verdict_total: "low_value", verdict_spread: "low_value",
    market_over_price: -115, market_under_price: -105,
  });
  assert.equal(favoredSidePrice(g, "total"), -115); // FV 147 > 141.5 → Over
  assert.equal(notAPlayLine(g, []), "Not a play: price too juiced (-115) for a stake of 0.1u+");
  assert.doesNotMatch(notAPlayLine(g, []) ?? "", /\$/);
  assert.equal(HOLD_CODES.kelly_too_small, "stake under 0.1u"); // generic default stays
  // Same hold, but the price would still fund 0.1u: keep the generic wording.
  const even = { ...g, market_over_price: -110, market_under_price: -110 };
  assert.equal(notAPlayLine(even, []), "Not a play: stake under 0.1u");
  // No price on the game: generic, and never surface the pipeline's "$1" text.
  const { market_over_price: _o, market_under_price: _u, ...noPrice } = g;
  assert.equal(notAPlayLine(noPrice as SlateGame, []), "Not a play: stake under 0.1u");
  assert.doesNotMatch(formatSkip(g.skip_reason_total, ["kelly_too_small"], "total", undefined, noPrice as SlateGame).primary ?? "", /\$|Kelly stake/i);
  assert.equal(kellyHoldLabel(HOLD_CODES.kelly_too_small, "total", g), `price too juiced (${fmtOdds(-115)}) for a stake of ${MIN_STAKE_UNITS}u+`);
});

test("parseMarketPrices reads DraftKings current odds and ignores a Kalshi board blob", () => {
  assert.deepEqual(parseMarketPrices({ over_price: -115, under_price: -105, home_spread_price: -105, away_spread_price: -115 }), {
    over: -115, under: -105, homeSpread: -105, awaySpread: -115,
  });
  assert.deepEqual(parseMarketPrices({ total: [{ strike: 141.5, yes_ask: 0.53 }] }).over, null);
  assert.deepEqual(parseMarketPrices(null).over, null);
});

test("open/now context and juice copy do not wrap mid-token: nowrap chunks in the source", () => {
  const root = [resolve(__dirname, "../src"), resolve(__dirname, "../../src")].find((d) => {
    try { readFileSync(resolve(d, "components/HeaderRow.tsx")); return true; } catch { return false; }
  });
  assert.ok(root, "src/components/HeaderRow.tsx");
  const header = readFileSync(resolve(root, "components/HeaderRow.tsx"), "utf8");
  assert.match(header, /whitespace-nowrap/);
  assert.match(header, /OpenNowNote/);
  const board = readFileSync(resolve(root, "components/SlateBoard.tsx"), "utf8");
  assert.match(board, /price too juiced/);
  assert.match(board, /whitespace-nowrap/);
  for (const src of [header, board, readFileSync(resolve(root, "lib/verdicts.ts"), "utf8"), readFileSync(resolve(root, "lib/kelly.ts"), "utf8")]) {
    assert.doesNotMatch(src, /KenPom|kenpom|\bpaper\b|v1\/v2/i);
    assert.doesNotMatch(src, /Kelly stake < \$|stake < \$1\b/);
  }
});

test("showsLineMoveNote still requires an official play below the bar", () => {
  const away = { official: true, market: "total" as const, edgeNow: 0.02, postedLine: 148.5, currentLine: 152.5, fair: 144.5 };
  assert.equal(showsLineMoveNote(away), true);
  assert.equal(showsLineMoveNote({ ...away, currentLine: 147.5 }), false); // toward CZarp
  assert.equal(cardHeadline(game(), [bet()]).lineMoveNote, false);
});
