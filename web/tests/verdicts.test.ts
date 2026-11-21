// Verdict ladder: Toss Up → Lean → Favor → Official Play, per market, judged on the current line
// (rule A). Run: npm test
import { test } from "node:test";
import assert from "node:assert/strict";
import { HOLD_CODES, LINE_MOVE_NOTE, NO_LINE_TIP, VERDICT_LADDER, VERDICT_RANK, clearsBar, holdCodes, ladderLevel, showsLineMoveNote, verdictLabel, type LadderInput } from "../src/lib/verdicts";
import { FAVOR_NOT_POSTED, cardHeadline, marketRatings, marketVerdict, notAPlayLine } from "../src/lib/verdict";
import { EXPLAINERS } from "../src/lib/explainers";
import { POSTED_TOTAL_NOTE, totalFair } from "../src/lib/posted";
import { fmtEdgeVsBar, fmtPlayPct, fmtPostedVsNow } from "../src/lib/labels";
import { knownSkipCodeLabel } from "../src/lib/verdict";
import { pickStatusTags } from "../src/lib/statusTags";
import type { SlateGame, ValueBet } from "../src/lib/types";

const L = (o: Partial<LadderInput>): LadderInput => ({ market: "total", official: false, edgeNow: null, skipCodes: [], ...o });

test("ladder order and labels (weakest to strongest)", () => {
  assert.deepEqual(VERDICT_LADDER.map((d) => d.label), ["Toss Up", "Lean", "Favor", "Official Play"]);
  assert.deepEqual(VERDICT_LADDER.map((d) => VERDICT_RANK[d.level]), [0, 1, 2, 3]);
  assert.equal(EXPLAINERS.officialPlay.title, "Official Play");
  assert.equal(EXPLAINERS.officialPlay.body, VERDICT_LADDER[3].short);
  assert.equal(verdictLabel("official"), "Official Play");
  for (const d of VERDICT_LADDER) assert.ok(d.short.length > 20, d.level);
});

test("ladder definitions describe the current-line rule", () => {
  assert.equal(VERDICT_LADDER[1].short, "Clears our bar on the current line, but a hold rule (new-coach pause, big market move, or stake under $1) keeps it from a play.");
  assert.equal(VERDICT_LADDER[2].short, "Clears our bar on the current line with nothing holding it back.");
  for (const d of VERDICT_LADDER) assert.doesNotMatch(d.short, /opening line|vs open/i, d.level);
});

test("Toss Up: edge now below the bar (4% sides, 3% totals), whatever the codes say", () => {
  assert.equal(ladderLevel(L({ market: "spread", edgeNow: 0.0399 })).level, "toss_up");
  assert.equal(ladderLevel(L({ market: "spread", edgeNow: 0.0399, skipCodes: ["new_coach_lock"] })).level, "toss_up");
  assert.equal(ladderLevel(L({ market: "moneyline", edgeNow: 0.035 })).level, "toss_up");
  assert.equal(ladderLevel(L({ edgeNow: 0.0299 })).level, "toss_up");
  // Real row: Drexel @ Maine total 11-14 (now 2.68%, open 3.47%, kelly_too_small) was Favor under #62.
  assert.equal(ladderLevel(L({ edgeNow: 0.0268, skipCodes: ["kelly_too_small"] })).level, "toss_up");
});

test("Toss Up when there is no stored edge", () => {
  assert.equal(ladderLevel(L({})).level, "toss_up");
  assert.equal(ladderLevel(L({ edgeNow: Number.NaN })).level, "toss_up");
});

test("bars are inclusive: exactly 4% / 3% clears", () => {
  assert.equal(clearsBar("spread", 0.04), true);
  assert.equal(clearsBar("total", 0.03), true);
  assert.equal(clearsBar("total", 0.0299), false);
  assert.equal(ladderLevel(L({ market: "spread", edgeNow: 0.04 })).level, "favor");
  assert.equal(ladderLevel(L({ edgeNow: 0.03, skipCodes: ["new_coach_lock"] })).level, "lean");
});

test("Favor: edge now clears the bar with no hold code; the opening line plays no part", () => {
  // Real row: South Alabama @ UCF spread 11-10 (now 5.15%, open 2.49%, only below_open_threshold).
  assert.deepEqual(ladderLevel(L({ market: "spread", edgeNow: 0.0515, skipCodes: ["below_open_threshold"] })), { level: "favor", holds: [] });
  // Real row: Eastern Illinois @ Illinois total 11-20 (now 3.63%, open 2.45%).
  assert.equal(ladderLevel(L({ edgeNow: 0.0363, skipCodes: ["below_open_threshold"] })).level, "favor");
  // Non-hold codes (and no codes at all) don't hold it back.
  assert.equal(ladderLevel(L({ edgeNow: 0.05, skipCodes: [] })).level, "favor");
  assert.equal(ladderLevel(L({ edgeNow: 0.05, skipCodes: null })).level, "favor");
  assert.equal(ladderLevel(L({ edgeNow: 0.05, skipCodes: ["game_started", "mystery_code"] })).level, "favor");
});

test("Lean: edge now clears the bar but a hold code applies", () => {
  // Real rows: Lamar @ Nebraska Omaha total 11-17 (6.71%, new_coach_lock) was Favor under #62;
  // Illinois @ Texas Tech total 11-10 (7.21%, line_moved_against); Santa Clara @ North Texas 11-11 (3.65%, kelly_too_small).
  assert.deepEqual(ladderLevel(L({ edgeNow: 0.0671, skipCodes: ["new_coach_lock"] })), { level: "lean", holds: ["new_coach_lock"] });
  assert.deepEqual(ladderLevel(L({ edgeNow: 0.0721, skipCodes: ["line_moved_against"] })), { level: "lean", holds: ["line_moved_against"] });
  assert.deepEqual(ladderLevel(L({ edgeNow: 0.0365, skipCodes: ["kelly_too_small"] })), { level: "lean", holds: ["kelly_too_small"] });
  // Real row: Cal Baptist @ Loyola Marymount total 11-21 (4.76%, below_open + line_moved_against).
  assert.deepEqual(ladderLevel(L({ edgeNow: 0.0476, skipCodes: ["below_open_threshold", "line_moved_against"] })), { level: "lean", holds: ["line_moved_against"] });
  // Real row: Hofstra @ Arizona St. spread 11-10 (4.09%, below_open + new_coach_lock).
  assert.equal(ladderLevel(L({ market: "spread", edgeNow: 0.0409, skipCodes: ["below_open_threshold", "new_coach_lock"] })).level, "lean");
  assert.deepEqual(ladderLevel(L({ edgeNow: 0.0564, skipCodes: ["line_moved_against", "new_coach_lock"] })).holds, ["new_coach_lock", "line_moved_against"]);
  assert.deepEqual(Object.keys(HOLD_CODES), ["new_coach_lock", "line_moved_against", "kelly_too_small"]);
});

test("Official Play beats everything and is never derived from the edge", () => {
  assert.equal(ladderLevel(L({ official: true, edgeNow: 0.0099 })).level, "official");
  assert.equal(ladderLevel(L({ official: true, skipCodes: ["new_coach_lock"] })).level, "official");
  // An edge that clears the bar with no hold is Favor, never promoted to Official Play.
  assert.equal(ladderLevel(L({ edgeNow: 0.09 })).level, "favor");
});

test("line-move note: official pick whose edge now is below the bar", () => {
  assert.equal(LINE_MOVE_NOTE, "No longer a value play due to line movement.");
  assert.equal(showsLineMoveNote("total", true, 0.0099), true);
  assert.equal(showsLineMoveNote("total", true, 0.0299), true);
  assert.equal(showsLineMoveNote("total", true, 0.03), false);
  assert.equal(showsLineMoveNote("spread", true, 0.0399), true);
  assert.equal(showsLineMoveNote("spread", true, 0.045), false);
  assert.equal(showsLineMoveNote("total", false, 0.01), false);
  assert.equal(showsLineMoveNote("total", true, null), false);
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

test("cardHeadline: Lean headline with its hold and a not-a-play reason", () => {
  const h = cardHeadline(game(), []);
  assert.equal(h.market, "total");
  assert.equal(h.verdict, "lean");
  assert.deepEqual(h.holds, ["line_moved_against"]);
  assert.equal(notAPlayLine(game(), []), "Not a play: market moved 3+ pts away from CZarp since open");
});

test("cardHeadline: ladder rank beats a bigger raw edge when choosing the headline market", () => {
  // Spread Toss Up at 3.9% vs total Favor at 3.1%: the Favor total headlines.
  const g = game({ edge_now_pct_spread: 0.039, edge_now_pct_total: 0.031, edge_vs_open_pct_total: 0.01, skip_reasons_total: ["below_open_threshold"] });
  const h = cardHeadline(g, []);
  assert.equal(h.market, "total");
  assert.equal(h.verdict, "favor");
  assert.equal(notAPlayLine(g, []), `Not a play: ${FAVOR_NOT_POSTED}`);
  // Spread Lean at 6% vs total Favor at 3.1%: Favor outranks Lean despite the smaller edge.
  const g2 = game({ edge_now_pct_spread: 0.06, skip_reasons_spread: ["new_coach_lock"], edge_now_pct_total: 0.031, skip_reasons_total: [] });
  assert.equal(cardHeadline(g2, []).market, "total");
});

test("cardHeadline: posted pick is Official Play; tracked picks never count", () => {
  assert.equal(cardHeadline(game(), [bet()]).verdict, "official");
  assert.equal(cardHeadline(game(), [bet({ pick_tier: "track" })]).verdict, "lean");
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

test("Drexel @ Maine 11-14 card: Toss Up on the current line (was Favor on the open bar)", () => {
  const g = game({
    game_id: 1076, game_date: "2026-11-14", home: "Maine", away: "Drexel", game: "Drexel @ Maine",
    edge_now_pct_spread: 0.0059, edge_vs_open_pct_spread: 0.0137, skip_reasons_spread: ["below_open_threshold"],
    edge_now_pct_total: 0.0268, edge_vs_open_pct_total: 0.0347, skip_reasons_total: ["kelly_too_small"],
    skip_reason_total: "price too juiced for the assumed hit rate (Kelly stake < $1)",
  });
  const h = cardHeadline(g, []);
  assert.equal(h.market, "total");
  assert.equal(h.verdict, "toss_up");
  assert.deepEqual(h.holds, []);
  assert.equal(h.edgeLabel, "2.6%");
  assert.equal(h.subtitle, "vs now 2.6% (needs 3%)");
  assert.equal(notAPlayLine(g, []), "Not a play: edge now below 3%; stake under $1");
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
  assert.equal(h.subtitle, "vs now 3.9% (needs 4%)");
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

// ---- Real games under rule A (stored values from slate_games / official_picks_locked) ----

test("South Alabama @ UCF 11-10: Spread Favor, Total Toss Up; header names the market", () => {
  const g = game({
    game_id: 300, game_date: "2026-11-10", home: "UCF", away: "South Alabama", game: "South Alabama @ UCF",
    market_spread: -13.5, market_total: 150.5,
    edge_now_pct_spread: 0.0515, edge_vs_open_pct_spread: 0.0249, skip_reasons_spread: ["below_open_threshold"],
    skip_reason_spread: "edge vs open below 4%",
    edge_now_pct_total: 0.0199, edge_vs_open_pct_total: 0.0199, skip_reasons_total: ["below_open_threshold"],
    skip_reason_total: "edge vs open below 3%",
  });
  const r = marketRatings(g, []);
  assert.equal(r.spread.level, "favor");
  assert.equal(r.total.level, "toss_up");
  assert.equal(r.spread.lineMoveNote, false);
  const h = cardHeadline(g, []);
  assert.equal(h.market, "spread");
  assert.equal(h.edgeLabel, "5.1%");
  const t = pickStatusTags(g, [])[0];
  assert.ok(t.kind === "verdict" && t.verdict === "favor" && t.market === "spread");
  // No opening-line copy anywhere on the card.
  assert.equal(notAPlayLine(g, []), `Not a play: ${FAVOR_NOT_POSTED}`);
  assert.equal(notAPlayLine(g, [], "total"), "Not a play: edge now below 3%");
});

test("Lamar @ Nebraska Omaha 11-17: Total Lean (new-coach pause), Spread Toss Up", () => {
  const g = game({
    game_id: 1316, game_date: "2026-11-17", home: "Nebraska Omaha", away: "Lamar", game: "Lamar @ Nebraska Omaha",
    market_spread: 2.5, market_total: 156.5,
    edge_now_pct_spread: 0.0322, edge_vs_open_pct_spread: 0.0261, skip_reasons_spread: ["below_open_threshold", "new_coach_lock"],
    edge_now_pct_total: 0.0671, edge_vs_open_pct_total: 0.055, skip_reasons_total: ["new_coach_lock"],
    skip_reason_total: "paused until game 4 (new coach, 2 played)",
  });
  const r = marketRatings(g, []);
  assert.equal(r.total.level, "lean");
  assert.deepEqual(r.total.holds, ["new_coach_lock"]);
  assert.equal(r.spread.level, "toss_up");
  const t = pickStatusTags(g, [])[0];
  assert.ok(t.kind === "verdict" && t.verdict === "lean" && t.market === "total");
  assert.equal(cardHeadline(g, []).edgeLabel, "6.7%");
  assert.match(notAPlayLine(g, []) ?? "", /^Not a play: paused until game 4/);
});

test("Norfolk St. @ Georgia Tech 11-17: official total keeps Official Play and shows the line-move note", () => {
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

test("Official wins ties for the header slot; a held official pick still reads Official Play", () => {
  const g = game({ edge_now_pct_spread: 0.09, skip_reasons_spread: [], edge_now_pct_total: 0.01, verdict_total: "official" });
  const h = cardHeadline(g, [bet()]);
  assert.equal(h.verdict, "official");
  assert.equal(h.market, "total");
  assert.equal(marketRatings(g, [bet()]).spread.level, "favor");
});
