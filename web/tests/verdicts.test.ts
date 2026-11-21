// Verdict ladder: Toss Up → Lean → Favor → Official Play. Run: npm test
import { test } from "node:test";
import assert from "node:assert/strict";
import { HOLD_CODES, NO_LINE_TIP, VERDICT_LADDER, VERDICT_RANK, holdCodes, ladderLevel, verdictLabel, type LadderInput } from "../src/lib/verdicts";
import { cardHeadline, marketVerdict, notAPlayLine } from "../src/lib/verdict";
import { EXPLAINERS } from "../src/lib/explainers";
import { POSTED_TOTAL_NOTE, totalFair } from "../src/lib/posted";
import { fmtEdgeVsBar, fmtPlayPct, fmtPostedVsNow } from "../src/lib/labels";
import { knownSkipCodeLabel } from "../src/lib/verdict";
import { pickStatusTags } from "../src/lib/statusTags";
import type { SlateGame, ValueBet } from "../src/lib/types";

const L = (o: Partial<LadderInput>): LadderInput => ({ market: "total", official: false, edgeNow: null, edgeVsOpen: null, skipCodes: [], ...o });

test("ladder order and labels (weakest to strongest)", () => {
  assert.deepEqual(VERDICT_LADDER.map((d) => d.label), ["Toss Up", "Lean", "Favor", "Official Play"]);
  assert.deepEqual(VERDICT_LADDER.map((d) => VERDICT_RANK[d.level]), [0, 1, 2, 3]);
  assert.equal(EXPLAINERS.officialPlay.title, "Official Play");
  assert.equal(EXPLAINERS.officialPlay.body, VERDICT_LADDER[3].short);
  assert.equal(verdictLabel("official"), "Official Play");
  for (const d of VERDICT_LADDER) assert.ok(d.short.length > 20, d.level);
});

test("Toss Up (sides): edge now below the bar, whatever the open or codes say", () => {
  assert.equal(ladderLevel(L({ market: "spread", edgeNow: 0.0399, edgeVsOpen: 0.05 })).level, "toss_up");
  assert.equal(ladderLevel(L({ market: "spread", edgeNow: 0.0399, edgeVsOpen: 0.05, skipCodes: ["new_coach_lock"] })).level, "toss_up");
  assert.equal(ladderLevel(L({ market: "moneyline", edgeNow: 0.035 })).level, "toss_up");
  // Real row: Alcorn St. @ Missouri spread 11-18 (below_current_threshold, now 3.59%, open 4.24%).
  assert.equal(ladderLevel(L({ market: "spread", edgeNow: 0.0359, edgeVsOpen: 0.0424, skipCodes: ["below_current_threshold"] })).level, "toss_up");
});

test("Toss Up (totals): neither the open nor the current edge clears 3%", () => {
  assert.equal(ladderLevel(L({ edgeNow: 0.0299, edgeVsOpen: 0.0299 })).level, "toss_up");
  assert.equal(ladderLevel(L({ edgeNow: 0.0, edgeVsOpen: 0.0, skipCodes: ["below_open_threshold", "line_moved_against"] })).level, "toss_up");
  assert.equal(ladderLevel(L({ edgeNow: 0.019, edgeVsOpen: 0.0065, skipCodes: ["below_open_threshold", "new_coach_lock"] })).level, "toss_up");
});

test("Favor (totals) is judged on the open bar only: held totals with edge now under 3%", () => {
  // The five prod totals Scantron flagged on PR #62 (open >= 3%, now < 3%, hold code, not posted).
  const rows: [string, number, number, string[], string][] = [
    ["g1052 Arkansas Pine Bluff @ Cal Baptist 11-13", 0.0234, 0.0303, ["new_coach_lock"], "new_coach_lock"],
    ["g1076 Drexel @ Maine 11-14", 0.0268, 0.0347, ["kelly_too_small"], "kelly_too_small"],
    ["g1193 Alabama St. @ San Jose St. 11-15", 0.0096, 0.0345, ["new_coach_lock"], "new_coach_lock"],
    ["g1314 Lindenwood @ Northern Iowa 11-17", 0.0295, 0.0363, ["kelly_too_small"], "kelly_too_small"],
    ["g1488 Oregon St. @ Oregon 11-19", 0.0093, 0.0334, ["new_coach_lock"], "new_coach_lock"],
  ];
  for (const [name, en, eo, codes, hold] of rows) {
    assert.deepEqual(ladderLevel(L({ edgeNow: en, edgeVsOpen: eo, skipCodes: codes })), { level: "favor", holds: [hold] }, name);
  }
  // Each hold code on a total with edge now under 3% and open at/above 3%.
  for (const c of ["new_coach_lock", "line_moved_against", "kelly_too_small"]) {
    assert.equal(ladderLevel(L({ edgeNow: 0.01, edgeVsOpen: 0.03, skipCodes: [c] })).level, "favor", c);
  }
  // Spreads still need both bars.
  assert.equal(ladderLevel(L({ market: "spread", edgeNow: 0.0234, edgeVsOpen: 0.0592, skipCodes: ["new_coach_lock"] })).level, "toss_up");
});

test("totals: open clears with no recorded hold reads Lean (pre-skip-code rows only)", () => {
  // Real row: Georgia St. @ Charlotte total 11-05 (now 0.34%, open 3.61%, no codes yet).
  assert.equal(ladderLevel(L({ edgeNow: 0.0034, edgeVsOpen: 0.0361, skipCodes: [] })).level, "lean");
});

test("Toss Up when there is no stored edge", () => {
  assert.equal(ladderLevel(L({})).level, "toss_up");
  assert.equal(ladderLevel(L({ edgeNow: Number.NaN })).level, "toss_up");
});

test("Lean: clears the bar now but not vs the open", () => {
  // Real row: Cal Baptist @ Loyola Marymount total 11-21 (now 4.76%, open 2.51%).
  assert.equal(ladderLevel(L({ edgeNow: 0.0476, edgeVsOpen: 0.0251, skipCodes: ["below_open_threshold", "line_moved_against"] })).level, "lean");
  assert.equal(ladderLevel(L({ market: "spread", edgeNow: 0.0409, edgeVsOpen: 0.0276, skipCodes: ["below_open_threshold"] })).level, "lean");
  assert.equal(ladderLevel(L({ edgeNow: 0.05, edgeVsOpen: null })).level, "lean");
});

test("bars are inclusive: exactly 4% / 3% clears", () => {
  assert.equal(ladderLevel(L({ market: "spread", edgeNow: 0.04, edgeVsOpen: 0.04, skipCodes: ["new_coach_lock"] })).level, "favor");
  assert.equal(ladderLevel(L({ edgeNow: 0.03, edgeVsOpen: 0.029 })).level, "lean");
});

test("Favor for each hold code: new_coach_lock, line_moved_against, kelly_too_small", () => {
  // Real rows: Arkansas Pine Bluff @ Cal Baptist spread 11-13; UC Irvine @ San Diego total 11-21; Charleston @ Drake total 11-19.
  const a = ladderLevel(L({ market: "spread", edgeNow: 0.0588, edgeVsOpen: 0.0592, skipCodes: ["new_coach_lock"] }));
  assert.deepEqual(a, { level: "favor", holds: ["new_coach_lock"] });
  const b = ladderLevel(L({ edgeNow: 0.0645, edgeVsOpen: 0.0348, skipCodes: ["line_moved_against"] }));
  assert.deepEqual(b, { level: "favor", holds: ["line_moved_against"] });
  const c = ladderLevel(L({ edgeNow: 0.0439, edgeVsOpen: 0.0379, skipCodes: ["kelly_too_small"] }));
  assert.deepEqual(c, { level: "favor", holds: ["kelly_too_small"] });
  const d = ladderLevel(L({ edgeNow: 0.0564, edgeVsOpen: 0.0322, skipCodes: ["line_moved_against", "new_coach_lock"] }));
  assert.deepEqual(d, { level: "favor", holds: ["new_coach_lock", "line_moved_against"] });
  assert.deepEqual(Object.keys(HOLD_CODES), ["new_coach_lock", "line_moved_against", "kelly_too_small"]);
});

test("both bars cleared with no hold code is not Favor (pre-skip-code rows read Lean)", () => {
  assert.equal(ladderLevel(L({ edgeNow: 0.0315, edgeVsOpen: 0.0315, skipCodes: [] })).level, "lean");
  assert.equal(ladderLevel(L({ edgeNow: 0.05, edgeVsOpen: 0.05, skipCodes: ["game_started", "mystery_code"] })).level, "lean");
  assert.equal(ladderLevel(L({ edgeNow: 0.05, edgeVsOpen: 0.05, skipCodes: null })).level, "lean");
});

test("Official Play beats everything", () => {
  assert.equal(ladderLevel(L({ official: true, edgeNow: 0.0217, edgeVsOpen: 0.041 })).level, "official");
  assert.equal(ladderLevel(L({ official: true, skipCodes: ["new_coach_lock"] })).level, "official");
});

test("holdCodes keeps only hold codes, in a stable order", () => {
  assert.deepEqual(holdCodes(["kelly_too_small", "below_open_threshold", "new_coach_lock"]), ["new_coach_lock", "kelly_too_small"]);
  assert.deepEqual(holdCodes(undefined), []);
});

const game = (o: Partial<SlateGame> = {}): SlateGame => ({
  sport: "cbb", game_date: "2026-11-21", game_id: 9, home: "San Diego", away: "UC Irvine",
  tipoff: null, game: "UC Irvine @ San Diego", proj_home: 70, proj_away: 75, proj_spread: 5,
  proj_total: 145, home_win_prob: 0.3, home_cover_prob: 0.5,
  archetypes: { home: [], away: [], home_tentative: false, away_tentative: false, tentative: false, notes: [] },
  market_spread: 6, market_total: 154, market_source: null,
  edge_now_pct_spread: 0.002, edge_vs_open_pct_spread: 0.002, skip_reasons_spread: ["below_open_threshold"],
  edge_now_pct_total: 0.0645, edge_vs_open_pct_total: 0.0348, skip_reasons_total: ["line_moved_against"],
  skip_reason_total: "Not a play: market moved 3+ pts away from CZarp since open",
  verdict_spread: "low_value", verdict_total: "low_value",
  ...o,
} as SlateGame);

const bet = (o: Partial<ValueBet> = {}): ValueBet => ({
  sport: "cbb", game_id: 9, market: "total", side: "Under 154", model_prob: 0.55, market_prob: 0.5,
  prob_edge: 0.05, line_edge_pct: 0.06, kelly_quarter_units: 1, pick_tier: "official", ...o,
});

test("cardHeadline: Favor headline with its hold and a not-a-play reason", () => {
  const h = cardHeadline(game(), []);
  assert.equal(h.market, "total");
  assert.equal(h.verdict, "favor");
  assert.deepEqual(h.holds, ["line_moved_against"]);
  assert.equal(notAPlayLine(game(), []), "Not a play: market moved 3+ pts away from CZarp since open");
});

test("cardHeadline: ladder rank beats a bigger raw edge when choosing the headline market", () => {
  // Spread Toss Up at 3.9% vs total Lean at 3.1%: the Lean total headlines.
  const g = game({ edge_now_pct_spread: 0.039, edge_now_pct_total: 0.031, edge_vs_open_pct_total: 0.01, skip_reasons_total: ["below_open_threshold"] });
  const h = cardHeadline(g, []);
  assert.equal(h.market, "total");
  assert.equal(h.verdict, "lean");
});

test("cardHeadline: posted pick is Official Play; tracked picks never count", () => {
  assert.equal(cardHeadline(game(), [bet()]).verdict, "official");
  assert.equal(cardHeadline(game(), [bet({ pick_tier: "track" })]).verdict, "favor");
  assert.equal(marketVerdict(game({ verdict_total: "official" }), [], "total").level, "official");
});

test("verdict InfoTips come from the shared ladder", () => {
  assert.equal(EXPLAINERS.tossUp.title, "Toss Up");
  assert.equal(EXPLAINERS.favor.title, "Favor");
  assert.equal(EXPLAINERS.officialPlay.title, "Official Play");
  assert.equal(EXPLAINERS.lean.body, VERDICT_LADDER[1].short);
  for (const e of Object.values(EXPLAINERS)) {
    assert.doesNotMatch(`${e.title} ${e.body}`, /\b(HOT|VALUE|EDGE)\b|low value|Official play|\bv[12]\b/);
  }
});

test("posted-total sentence applies only to an official total with a posted fair", () => {
  assert.equal(POSTED_TOTAL_NOTE, "Total shown is CZarp's number when this play was posted.");
  assert.equal(totalFair(game({ czarp_total: 150 }), [bet({ fair_at_post: 147.5 })]).tag, "posted");
  assert.equal(totalFair(game({ czarp_total: 150 }), [bet({ fair_at_post: 147.5, pick_tier: "track" })]).tag, undefined);
  assert.equal(totalFair(game({ czarp_total: 150 }), [bet({ market: "spread", fair_at_post: 3 })]).tag, undefined);
  assert.equal(totalFair(game({ czarp_total: 150 }), []).tag, undefined);
});

test("Drexel @ Maine 11-14 card: Favor chip, subtitle and footer agree", () => {
  const g = game({
    game_id: 1076, game_date: "2026-11-14", home: "Maine", away: "Drexel", game: "Drexel @ Maine",
    edge_now_pct_spread: 0.0059, edge_vs_open_pct_spread: 0.0137, skip_reasons_spread: ["below_open_threshold"],
    edge_now_pct_total: 0.0268, edge_vs_open_pct_total: 0.0347, skip_reasons_total: ["kelly_too_small"],
    skip_reason_total: "price too juiced for the assumed hit rate (Kelly stake < $1)",
  });
  const h = cardHeadline(g, []);
  assert.equal(h.market, "total");
  assert.equal(h.verdict, "favor");
  assert.deepEqual(h.holds, ["kelly_too_small"]);
  assert.equal(h.subtitle, "vs open 3.5% (needs 3%) · vs now 2.6%");
  assert.equal(notAPlayLine(g, []), "Not a play: stake under $1");
});

test("near-bar edges never display as meeting the bar", () => {
  assert.equal(fmtEdgeVsBar(0.0297, "total"), "2.9%");
  assert.equal(fmtEdgeVsBar(0.0299, "total"), "2.9%");
  assert.equal(fmtEdgeVsBar(0.0399, "spread"), "3.9%");
  assert.equal(fmtEdgeVsBar(0.0295, "total"), "2.9%");
  assert.equal(fmtEdgeVsBar(0, "spread"), "0.0%");
  assert.equal(fmtEdgeVsBar(0.03, "total"), "3.0%");
  assert.equal(fmtEdgeVsBar(0.04, "spread"), "4.0%");
  assert.equal(fmtEdgeVsBar(0.0476, "total"), "4.8%");
  // Toss Up spread at 3.99%: headline and subtitle both read 3.9%.
  const g = game({ edge_now_pct_spread: 0.0399, edge_vs_open_pct_spread: 0.0399, edge_now_pct_total: 0.001, edge_vs_open_pct_total: 0.001, skip_reasons_total: [] });
  const h = cardHeadline(g, []);
  assert.equal(h.verdict, "toss_up");
  assert.equal(h.edgeLabel, "3.9%");
  assert.equal(h.subtitle, "vs open 3.9% (needs 4%) · vs now 3.9%");
});

test("no-line card: Toss Up with a neutral tip, not 'below the bar'", () => {
  const g = game({ edge_now_pct_spread: null, edge_vs_open_pct_spread: null, edge_now_pct_total: null, edge_vs_open_pct_total: null, skip_reasons_total: null, skip_reasons_spread: null });
  const h = cardHeadline(g, []);
  assert.equal(h.noLine, true);
  assert.equal(h.market, null);
  assert.equal(h.edgeLabel, null);
  const t = pickStatusTags(g, [])[0];
  assert.ok(t.kind === "verdict" && t.verdict === "toss_up" && t.noLine);
  assert.equal(EXPLAINERS.noLine, NO_LINE_TIP);
  assert.doesNotMatch(NO_LINE_TIP.body, /bar/i);
  assert.equal(cardHeadline(game(), []).noLine, false);
});

test("line-move wording is one string everywhere", () => {
  assert.equal(HOLD_CODES.line_moved_against, "market moved 3+ pts away from CZarp since open");
  assert.equal(knownSkipCodeLabel("line_moved_against", "3%"), HOLD_CODES.line_moved_against);
  assert.equal(knownSkipCodeLabel("kelly_too_small", "3%"), HOLD_CODES.kelly_too_small);
  for (const d of VERDICT_LADDER) assert.doesNotMatch(d.short, /total moved/);
});

test("one decimal style for every edge: posted, vs now, vs open, headline", () => {
  assert.equal(fmtPlayPct(0.02), "2.0%");
  assert.equal(fmtPlayPct(0.045), "4.5%");
  // Posted 2% · vs now 2.0% used to mix styles.
  const g = game({ edge_now_pct_total: 0.02, edge_vs_open_pct_total: 0.035, skip_reasons_total: [] });
  const h = cardHeadline(g, [bet({ edge_at_post_pct: 0.02, line_edge_pct: 0.02 })]);
  assert.equal(h.subtitle, "Posted 2.0% · vs now 2.0%");
  assert.equal(h.edgeLabel, "2.0%");
  assert.equal(
    fmtPostedVsNow({ fair: 142, edge: 0.0173, line: 144.5, now: 144.5, vsOpen: 0.0373, market: "total" }),
    "Posted 142.0 · 1.7% (line 144.5) → Now 144.5 · 3.7% vs open",
  );
  assert.equal(fmtPostedVsNow({ vsOpen: 0.0299, market: "total" }), "· 2.9% vs open");
  for (const s of [h.subtitle!, h.edgeLabel!]) assert.doesNotMatch(s, /(?<![\d.])\d+%/);
});
