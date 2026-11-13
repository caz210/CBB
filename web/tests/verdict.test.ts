// Plain node:test + tsc, no extra deps. Run: npm test
import { test } from "node:test";
import assert from "node:assert/strict";
import { formatSkip, notAPlayLine, notAPlayAlso } from "../src/lib/verdict";
import { pausedChipLabel, type NewCoachLock } from "../src/lib/coach";
import type { SlateGame } from "../src/lib/types";

const lock = (o: Partial<NewCoachLock> = {}): NewCoachLock => ({
  team: "Indiana", gamesPlayed: 3, lockSides: 8, lockTotals: 4,
  sidesRemaining: 5, totalsRemaining: 1, sidesLocked: true, totalsLocked: true, ...o,
});

const game = (o: Partial<SlateGame> = {}): SlateGame => ({
  sport: "cbb", game_date: "2026-11-12", game_id: 1, home: "Indiana", away: "Tennessee Tech",
  tipoff: null, game: "Tennessee Tech @ Indiana", proj_home: 93.3, proj_away: 70, proj_spread: -23.3,
  proj_total: 163.3, home_win_prob: 0.98, home_cover_prob: 0.42,
  archetypes: { home: [], away: [], home_tentative: false, away_tentative: false, tentative: false, notes: [] },
  market_spread: -25.5, market_total: 169.5, market_source: null,
  edge_now_pct_spread: 0.0129, edge_now_pct_total: 0.0501, verdict_spread: "low_value", verdict_total: "low_value",
  ...o,
} as SlateGame);

test("known code labels win over stored text", () => {
  const d = formatSkip("total moved 3 pts against our side since open (market disagrees)", ["below_open_threshold", "line_moved_against"], "total");
  assert.deepEqual(d, { primary: "edge vs open below 3%; market moved 3+ pts away from CZarp since open", also: [] });
});

test("unknown codes fall back to the stored text, with old line-move text mapped", () => {
  const d = formatSkip("model disagreement too high; total moved 4 pts against our side since open", ["model_disagreement", "some_new_code"], "total");
  assert.deepEqual(d, { primary: "model disagreement too high; market moved 3+ pts away from CZarp since open", also: [] });
});

test("unknown code + lock-word text is rewritten; coach copy becomes the pause", () => {
  assert.equal(formatSkip("Sides locked for review", ["mystery"], "spread").primary, "Sides paused for review");
  assert.equal(formatSkip("locked: new coach", ["mystery"], "spread").primary, "paused, new coach"); // coach → pause leads
});

test("unknown code with no text shows no line (as before)", () => {
  assert.deepEqual(formatSkip(null, ["mystery"], "total"), { primary: null, also: [] });
  assert.deepEqual(formatSkip("", [], "total"), { primary: null, also: [] });
});

test("coach pause leads; known codes go to Also", () => {
  const d = formatSkip("x; Paused until game 4 (new coach, 3 played)", ["line_moved_against", "new_coach_lock"], "total", [lock()]);
  assert.deepEqual(d, { primary: "paused until game 4 (new coach, 3 played)", also: ["market moved 3+ pts away from CZarp since open"] });
});

test("coach pause with only unknown codes: Also falls back to the stored text", () => {
  const d = formatSkip("edge vs open below 3%; total moved 5 pts against our side; Paused until game 4 (new coach, 2 played)", ["new_coach_lock", "mystery"], "total");
  assert.deepEqual(d, {
    primary: "paused until game 4 (new coach, 2 played)",
    also: ["edge vs open below 3%", "market moved 3+ pts away from CZarp since open"],
  });
});

test("notAPlayLine / notAPlayAlso on a total-headline coach card", () => {
  const g = game({ skip_reasons_total: ["line_moved_against", "new_coach_lock"], skip_reason_total: "…", coachLocks: [lock()] });
  assert.equal(notAPlayLine(g, []), "Not a play: paused until game 4 (new coach, 3 played)");
  assert.deepEqual(notAPlayAlso(g, []), ["Also: market moved 3+ pts away from CZarp since open"]);
});

test("notAPlayLine falls back to stored text for unknown codes", () => {
  const g = game({ skip_reasons_total: ["brand_new_rule"], skip_reason_total: "injury news pending" });
  assert.equal(notAPlayLine(g, []), "Not a play: injury news pending");
  assert.equal(notAPlayLine(game({ skip_reasons_total: ["brand_new_rule"], skip_reason_total: null }), []), null);
});

test("pausedChipLabel by market", () => {
  assert.equal(pausedChipLabel(lock(), false, "total"), "Paused until game 4 (new coach, 3 played)");
  assert.equal(pausedChipLabel(lock(), false, "spread"), "Paused until game 8 (new coach, 3 played)");
  assert.equal(pausedChipLabel(lock({ totalsLocked: false, totalsRemaining: 0 }), false, "total"), "Sides paused until game 8 (new coach, 3 played)");
  assert.equal(pausedChipLabel(lock({ sidesLocked: false, sidesRemaining: 0 }), false, "moneyline"), "Totals paused until game 4 (new coach, 3 played)");
  assert.equal(pausedChipLabel(lock()), "Paused until game 8 (new coach, 3 played)");
  assert.equal(pausedChipLabel(lock(), true, "total"), "Indiana · paused until game 4 (new coach, 3 played)");
});
