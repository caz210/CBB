// Five-tier verdict ladder, per market, judged on the edge vs the current line:
// Toss Up → Lean → Value → Late Value → CZarp Play. Run: npm test
import { test } from "node:test";
import assert from "node:assert/strict";
import { HOLD_CODES, LATE_VALUE_NOTE, LINE_MOVE_NOTE, NO_LINE_TIP, TIER_BOUNDS, VERDICT_LADDER, VERDICT_RANK, clearsBar, holdCodes, ladderLevel, showsLineMoveNote, verdictLabel, type LadderInput, type VerdictLevel } from "../src/lib/verdicts";
import { OFFICIAL_POST_TIME } from "../src/lib/officialPost";
import { MIN_STAKE_UNITS } from "../src/lib/labels";
import { cardHeaderRow } from "../src/lib/cardHeader";
import { cardHeadline, marketRatings, marketVerdict, notAPlayLine, notAPlayLines } from "../src/lib/verdict";
import { EXPLAINERS } from "../src/lib/explainers";
import { POSTED_TOTAL_NOTE, totalFair } from "../src/lib/posted";
import { fmtEdgeVsBar, fmtPlayPct, fmtPostedVsNow } from "../src/lib/labels";
import { knownSkipCodeLabel } from "../src/lib/verdict";
import { pickStatusTags } from "../src/lib/statusTags";
import type { SlateGame, ValueBet } from "../src/lib/types";

const L = (o: Partial<LadderInput>): LadderInput => ({ market: "total", official: false, edgeNow: null, skipCodes: [], ...o });
const lvl = (market: LadderInput["market"], edgeNow: number | null, skipCodes: string[] = [], official = false): VerdictLevel =>
  ladderLevel(L({ market, edgeNow, skipCodes, official })).level;

test("ladder order, labels and ranking (weakest to strongest)", () => {
  assert.deepEqual(VERDICT_LADDER.map((d) => d.label), ["Toss Up", "Lean", "Value", "Late Value", "CZarp Play"]);
  assert.deepEqual(VERDICT_LADDER.map((d) => VERDICT_RANK[d.level]), [0, 1, 2, 3, 4]);
  assert.equal(verdictLabel("official"), "CZarp Play");
  assert.equal(EXPLAINERS.czarpPlay.title, "CZarp Play");
  assert.equal(EXPLAINERS.czarpPlay.body, VERDICT_LADDER[4].short);
  for (const d of VERDICT_LADDER) assert.ok(d.short.length > 20, d.level);
});

test("boundaries live in one table: sides 2 / 3 / 4%, totals 1.5 / 2.25 / 3%", () => {
  assert.deepEqual(TIER_BOUNDS, { spread: { lean: 0.02, value: 0.03, bar: 0.04 }, total: { lean: 0.015, value: 0.0225, bar: 0.03 } });
});

test("sides: every boundary, exact and at full precision", () => {
  const cases: [number, VerdictLevel][] = [
    [0, "toss_up"], [0.0199, "toss_up"], [0.019999, "toss_up"],
    [0.02, "lean"], [0.0299, "lean"], [0.029999, "lean"],
    [0.03, "value"], [0.0399, "value"], [0.039999, "value"],
    [0.04, "late_value"], [0.0515, "late_value"], [0.2, "late_value"],
  ];
  for (const [e, want] of cases) {
    assert.equal(lvl("spread", e), want, `spread ${e}`);
    assert.equal(lvl("moneyline", e), want, `moneyline ${e}`);
  }
});

test("totals: every boundary, exact and at full precision", () => {
  const cases: [number, VerdictLevel][] = [
    [0, "toss_up"], [0.0149, "toss_up"], [0.014999, "toss_up"],
    [0.015, "lean"], [0.0224, "lean"], [0.022499, "lean"],
    [0.0225, "value"], [0.0299, "value"], [0.029999, "value"],
    [0.03, "late_value"], [0.0301, "late_value"], [0.0671, "late_value"],
  ];
  for (const [e, want] of cases) assert.equal(lvl("total", e), want, `total ${e}`);
});

test("display rounding never decides a tier: 1.9999% is Toss Up, 2.0% is Lean", () => {
  assert.equal(lvl("spread", 0.019999), "toss_up");
  assert.equal(fmtPlayPct(0.019999), "2.0%"); // rounds for display only
  assert.equal(lvl("spread", 0.02), "lean");
  assert.equal(lvl("total", 0.029999), "value");
  assert.equal(lvl("total", 0.03), "late_value");
});

test("past the bar with a hold is Value (with the holds); without one it's Late Value", () => {
  for (const c of ["new_coach_lock", "line_moved_against", "kelly_too_small"]) {
    assert.deepEqual(ladderLevel(L({ market: "spread", edgeNow: 0.04, skipCodes: [c] })), { level: "value", holds: [c] }, c);
    assert.deepEqual(ladderLevel(L({ edgeNow: 0.03, skipCodes: [c] })), { level: "value", holds: [c] }, c);
  }
  // Real rows: Lamar @ Nebraska Omaha total 11-17 (6.71%, new_coach_lock); Illinois @ Texas Tech total 11-10 (7.21%, line_moved_against).
  assert.deepEqual(ladderLevel(L({ edgeNow: 0.0671, skipCodes: ["new_coach_lock"] })), { level: "value", holds: ["new_coach_lock"] });
  assert.deepEqual(ladderLevel(L({ edgeNow: 0.0564, skipCodes: ["line_moved_against", "new_coach_lock"] })).holds, ["new_coach_lock", "line_moved_against"]);
  // Non-hold codes don't hold it back.
  assert.equal(lvl("total", 0.05, ["below_open_threshold", "game_started", "mystery_code"]), "late_value");
  assert.equal(ladderLevel(L({ edgeNow: 0.05, skipCodes: null })).level, "late_value");
  assert.deepEqual(Object.keys(HOLD_CODES), ["new_coach_lock", "line_moved_against", "kelly_too_small"]);
});

test("holds only matter past the bar: a held edge below it keeps its edge tier", () => {
  assert.deepEqual(ladderLevel(L({ edgeNow: 0.0268, skipCodes: ["kelly_too_small"] })), { level: "value", holds: [] }); // Drexel @ Maine 11-14
  assert.deepEqual(ladderLevel(L({ market: "spread", edgeNow: 0.0322, skipCodes: ["new_coach_lock"] })), { level: "value", holds: [] });
  assert.equal(lvl("spread", 0.025, ["new_coach_lock"]), "lean");
  assert.equal(lvl("total", 0.01, ["line_moved_against"]), "toss_up");
});

test("Toss Up when there is no stored edge", () => {
  assert.equal(lvl("total", null), "toss_up");
  assert.equal(ladderLevel(L({ edgeNow: Number.NaN })).level, "toss_up");
  assert.equal(ladderLevel(L({ edgeNow: undefined })).level, "toss_up");
});

test("CZarp Play takes precedence over every edge and hold, and is never derived from the edge", () => {
  for (const e of [null, 0, 0.0099, 0.025, 0.035, 0.09]) assert.equal(lvl("total", e, ["new_coach_lock"], true), "official", String(e));
  assert.equal(lvl("spread", 0.2), "late_value"); // never promoted
});

test("clearsBar is inclusive at full precision", () => {
  assert.equal(clearsBar("spread", 0.04), true);
  assert.equal(clearsBar("spread", 0.039999), false);
  assert.equal(clearsBar("total", 0.03), true);
  assert.equal(clearsBar("total", 0.029999), false);
  assert.equal(clearsBar("total", null), false);
});

test("line-move note: CZarp Play whose edge now is below the bar", () => {
  assert.equal(LINE_MOVE_NOTE, "No longer a value play due to line movement.");
  assert.equal(showsLineMoveNote("total", true, 0.0099), true);
  assert.equal(showsLineMoveNote("total", true, 0.029999), true);
  assert.equal(showsLineMoveNote("total", true, 0.03), false);
  assert.equal(showsLineMoveNote("spread", true, 0.039999), true);
  assert.equal(showsLineMoveNote("spread", true, 0.04), false);
  assert.equal(showsLineMoveNote("total", false, 0.01), false);
  assert.equal(showsLineMoveNote("total", true, null), false);
});

test("ladder copy: Late Value says it's off the record; no paper, model-version or KenPom wording", () => {
  assert.equal(LATE_VALUE_NOTE, `Value on the current line. Not part of the official record; official plays are set at ${OFFICIAL_POST_TIME}.`);
  assert.ok(VERDICT_LADDER[3].short.endsWith(`official plays are set at ${OFFICIAL_POST_TIME}.`));
  for (const d of VERDICT_LADDER) {
    assert.doesNotMatch(`${d.label} ${d.short}`, /paper|\bv[12]\b|kenpom|\bfavor\b/i, d.level);
    assert.doesNotMatch(`${d.label} ${d.short}`, /Official Play/, d.level);
  }
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
  skip_reason_total: "Not a play: Line moved 3+ pts away from CZarp since open",
  verdict_spread: "low_value", verdict_total: "low_value",
  ...o,
} as SlateGame);

const bet = (o: Partial<ValueBet> = {}): ValueBet => ({
  sport: "cbb", game_id: 9, market: "total", side: "Under 154", model_prob: 0.55, market_prob: 0.5,
  prob_edge: 0.05, line_edge_pct: 0.06, kelly_quarter_units: 1, pick_tier: "official", ...o,
});

test("cardHeadline: held Value headline with its hold as the note", () => {
  const h = cardHeadline(game(), []);
  assert.equal(h.market, "total");
  assert.equal(h.verdict, "value");
  assert.deepEqual(h.holds, ["line_moved_against"]);
  assert.equal(notAPlayLine(game(), []), "Not a play: Line moved 3+ pts away from CZarp since open");
});

test("cardHeadline: ladder rank beats a bigger raw edge when choosing the headline market", () => {
  // Spread Value at 3.9% vs total Late Value at 3.1%: the Late Value total headlines.
  const g = game({ edge_now_pct_spread: 0.039, edge_now_pct_total: 0.031, edge_vs_open_pct_total: 0.01, skip_reasons_total: ["below_open_threshold"] });
  const h = cardHeadline(g, []);
  assert.equal(h.market, "total");
  assert.equal(h.verdict, "late_value");
  assert.equal(notAPlayLine(g, []), LATE_VALUE_NOTE);
  // Spread held Value at 6% vs total Late Value at 3.1%: Late Value outranks Value despite the smaller edge.
  const g2 = game({ edge_now_pct_spread: 0.06, skip_reasons_spread: ["new_coach_lock"], edge_now_pct_total: 0.031, skip_reasons_total: [] });
  assert.equal(cardHeadline(g2, []).market, "total");
  // Same tier on both markets: the bigger edge headlines.
  const g3 = game({ edge_now_pct_spread: 0.025, skip_reasons_spread: [], edge_now_pct_total: 0.016, skip_reasons_total: [] });
  assert.equal(cardHeadline(g3, []).market, "spread");
  assert.equal(cardHeadline(g3, []).verdict, "lean");
});

test("cardHeadline: posted pick is CZarp Play; tracked picks never count", () => {
  assert.equal(cardHeadline(game(), [bet()]).verdict, "official");
  assert.equal(cardHeadline(game(), [bet({ pick_tier: "track" })]).verdict, "value");
  assert.equal(marketVerdict(game({ verdict_total: "official" }), [], "total").level, "official");
});

test("verdict InfoTips come from the shared ladder", () => {
  assert.equal(EXPLAINERS.tossUp.title, "Toss Up");
  assert.equal(EXPLAINERS.lean.body, VERDICT_LADDER[1].short);
  assert.equal(EXPLAINERS.value.body, VERDICT_LADDER[2].short);
  assert.equal(EXPLAINERS.lateValue.title, "Late Value");
  assert.equal(EXPLAINERS.lateValue.body, VERDICT_LADDER[3].short);
  assert.equal(EXPLAINERS.czarpPlay.title, "CZarp Play");
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

test("Drexel @ Maine 11-14 card: Value on the current line (2.68% total; stake hold ignored below the bar)", () => {
  const g = game({
    game_id: 1076, game_date: "2026-11-14", home: "Maine", away: "Drexel", game: "Drexel @ Maine",
    edge_now_pct_spread: 0.0059, edge_vs_open_pct_spread: 0.0137, skip_reasons_spread: ["below_open_threshold"],
    edge_now_pct_total: 0.0268, edge_vs_open_pct_total: 0.0347, skip_reasons_total: ["kelly_too_small"],
    skip_reason_total: "price too juiced for the assumed hit rate (Kelly stake < $1)",
  });
  const h = cardHeadline(g, []);
  assert.equal(h.market, "total");
  assert.equal(h.verdict, "value");
  assert.deepEqual(h.holds, []);
  assert.equal(h.edgeLabel, "2.6%");
  assert.equal(notAPlayLine(g, []), `Not a play: stake under ${MIN_STAKE_UNITS}u`);
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
  // Value spread at 3.99% (short of the 4% bar): headline and subtitle both read 3.9%.
  const g = game({ edge_now_pct_spread: 0.0399, edge_vs_open_pct_spread: 0.0399, edge_now_pct_total: 0.001, edge_vs_open_pct_total: 0.001, skip_reasons_total: [] });
  const h = cardHeadline(g, []);
  assert.equal(h.verdict, "value");
  assert.equal(h.edgeLabel, "3.9%");
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
  assert.equal(HOLD_CODES.line_moved_against, "Line moved 3+ pts away from CZarp since open");
  assert.equal(knownSkipCodeLabel("line_moved_against", "3%"), HOLD_CODES.line_moved_against);
  assert.equal(knownSkipCodeLabel("kelly_too_small", "3%"), HOLD_CODES.kelly_too_small);
  for (const d of VERDICT_LADDER) assert.doesNotMatch(d.short, /total moved/);
});

test("one decimal style for every edge: posted, current, vs open, headline", () => {
  assert.equal(fmtPlayPct(0.02), "2.0%");
  assert.equal(fmtPlayPct(0.045), "4.5%");
  // Posted and current edges both read one decimal.
  const g = game({ edge_now_pct_total: 0.02, edge_vs_open_pct_total: 0.035, skip_reasons_total: [] });
  const h = cardHeadline(g, [bet({ edge_at_post_pct: 0.02, line_edge_pct: 0.02 })]);
  assert.equal(cardHeaderRow(g, [bet({ edge_at_post_pct: 0.02, line_edge_pct: 0.02 })]).edgeLabel, "2.0%"); // current edge
  assert.equal(h.edgeLabel, "2.0%");
  assert.equal(
    fmtPostedVsNow({ fair: 142, edge: 0.0173, line: 144.5, now: 144.5, vsOpen: 0.0373, market: "total" }),
    "Posted 142.0 · 1.7% (line 144.5) → Now 144.5 · 3.7% vs open",
  );
  assert.equal(fmtPostedVsNow({ vsOpen: 0.0299, market: "total" }), "· 2.9% vs open");
  for (const s of [h.edgeLabel!]) assert.doesNotMatch(s, /(?<![\d.])\d+%/);
});

// ---- Real games under rule A (stored values from slate_games / official_picks_locked) ----

test("South Alabama @ UCF 11-10: Spread Late Value, Total Lean; header names the market", () => {
  const g = game({
    game_id: 300, game_date: "2026-11-10", home: "UCF", away: "South Alabama", game: "South Alabama @ UCF",
    market_spread: -13.5, market_total: 150.5,
    edge_now_pct_spread: 0.0515, edge_vs_open_pct_spread: 0.0249, skip_reasons_spread: ["below_open_threshold"],
    skip_reason_spread: "edge vs open below 4%",
    edge_now_pct_total: 0.0199, edge_vs_open_pct_total: 0.0199, skip_reasons_total: ["below_open_threshold"],
    skip_reason_total: "edge vs open below 3%",
  });
  const r = marketRatings(g, []);
  assert.equal(r.spread.level, "late_value");
  assert.equal(r.total.level, "lean"); // 1.99% on a total: past 1.5%, short of 2.25%
  assert.equal(r.spread.lineMoveNote, false);
  const h = cardHeadline(g, []);
  assert.equal(h.market, "spread");
  assert.equal(h.edgeLabel, "5.1%");
  const t = pickStatusTags(g, [])[0];
  assert.ok(t.kind === "verdict" && t.verdict === "late_value" && t.market === "spread");
  // No opening-line copy anywhere on the card.
  assert.equal(notAPlayLine(g, []), LATE_VALUE_NOTE);
  assert.equal(notAPlayLine(g, [], "total"), null); // below the bar: no reason line, the chip and edge say it
  // Team page lines use the same copy.
  assert.deepEqual(notAPlayLines(g, []), [LATE_VALUE_NOTE]);
});

test("Lamar @ Nebraska Omaha 11-17: Total Value (new-coach pause), Spread Value (edge 3.2%)", () => {
  const g = game({
    game_id: 1316, game_date: "2026-11-17", home: "Nebraska Omaha", away: "Lamar", game: "Lamar @ Nebraska Omaha",
    market_spread: 2.5, market_total: 156.5,
    edge_now_pct_spread: 0.0322, edge_vs_open_pct_spread: 0.0261, skip_reasons_spread: ["below_open_threshold", "new_coach_lock"],
    edge_now_pct_total: 0.0671, edge_vs_open_pct_total: 0.055, skip_reasons_total: ["new_coach_lock"],
    skip_reason_total: "paused until game 4 (new coach, 2 played)",
  });
  const r = marketRatings(g, []);
  assert.equal(r.total.level, "value");
  assert.deepEqual(r.total.holds, ["new_coach_lock"]);
  assert.equal(r.spread.level, "value");
  assert.deepEqual(r.spread.holds, []);
  const t = pickStatusTags(g, [])[0];
  // Same tier: the bigger edge (total 6.7%) takes the header slot.
  assert.ok(t.kind === "verdict" && t.verdict === "value" && t.market === "total");
  assert.equal(cardHeadline(g, []).edgeLabel, "6.7%");
  assert.match(notAPlayLine(g, []) ?? "", /^Not a play: paused until game 4/);
});

test("Norfolk St. @ Georgia Tech 11-17: posted total stays a CZarp Play and shows the line-move note", () => {
  const g = game({
    game_id: 1294, game_date: "2026-11-17", home: "Georgia Tech", away: "Norfolk St.", game: "Norfolk St. @ Georgia Tech",
    market_spread: -21.5, market_total: 151.5,
    edge_now_pct_spread: 0.0001, edge_vs_open_pct_spread: 0.0128, skip_reasons_spread: ["below_open_threshold", "new_coach_lock"],
    edge_now_pct_total: 0.0099, edge_vs_open_pct_total: 0.0476, skip_reasons_total: [],
    verdict_total: "official",
  });
  const b = bet({ game_id: 1294, side: "Under 151.5", line_edge_pct: 0.0099, edge_at_post_pct: 0.0099, edge_vs_open_pct: 0.0476, fair_at_post: 150 });
  const r = marketRatings(g, [b]);
  assert.equal(r.total.level, "official");
  assert.equal(r.total.lineMoveNote, true);
  assert.equal(r.spread.level, "toss_up");
  assert.equal(r.spread.lineMoveNote, false);
  const h = cardHeadline(g, [b]);
  assert.equal(h.verdict, "official");
  assert.equal(h.lineMoveNote, true);
  const t = pickStatusTags(g, [b])[0];
  assert.ok(t.kind === "verdict" && t.verdict === "official" && t.market === "total");
  // Same pick with an edge now at the bar: no note.
  assert.equal(cardHeadline(game({ ...g, edge_now_pct_total: 0.03 }), [b]).lineMoveNote, false);
});

test("CZarp Play outranks Late Value for the header slot, whatever the edges", () => {
  const g = game({ edge_now_pct_spread: 0.09, skip_reasons_spread: [], edge_now_pct_total: 0.01, verdict_total: "official" });
  const h = cardHeadline(g, [bet()]);
  assert.equal(h.verdict, "official");
  assert.equal(h.market, "total");
  assert.equal(marketRatings(g, [bet()]).spread.level, "late_value");
});
