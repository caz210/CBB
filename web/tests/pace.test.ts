import { test } from "node:test";
import assert from "node:assert/strict";
import { cardReason, paceAndReason } from "../src/lib/pace";
import type { GameInsight, SlateGame } from "../src/lib/types";

const insight = (o: Partial<GameInsight> = {}): GameInsight => ({
  proj_home: 80, proj_away: 70, home_win_prob: 0.8, home_cover_prob: 0.5, over_prob: 0.5, neutral: false,
  open_spread: null, cur_spread: null, open_total: null, cur_total: null,
  reason: "v2 reason. Projected pace about 71 possessions.", proj_possessions: 70.6, ...o,
});
const game = (i?: GameInsight): SlateGame => ({
  game_id: 1, reason: "slate reason (v2)", proj_possessions: 70.6, insight: i,
} as unknown as SlateGame);

test("total: uses the v1 row's own pace and reason", () => {
  const g = game(insight({ v1_proj_possessions: 68.1, v1_reason: "Ohio at Purdue. Projected total 147.0. Projected pace about 68 possessions." }));
  assert.deepEqual(paceAndReason(g, "total"), { pace: 68.1, reason: "Ohio at Purdue. Projected total 147.0. Projected pace about 68 possessions." });
  assert.equal(cardReason(g, "total"), "Ohio at Purdue. Projected total 147.0. Projected pace about 68 possessions.");
});

test("total: v1 null hides the lines, never falls back to v2", () => {
  const g = game(insight({ v1_proj_possessions: null, v1_reason: null }));
  assert.deepEqual(paceAndReason(g, "total"), { pace: null, reason: null });
  assert.equal(cardReason(g, "total"), null);
  assert.deepEqual(paceAndReason(game(undefined), "total"), { pace: null, reason: null }); // older rows / no insight
});

test("spread and moneyline keep the default-model (v2) sources", () => {
  const g = game(insight({ v1_proj_possessions: 68.1, v1_reason: "v1 reason" }));
  assert.deepEqual(paceAndReason(g, "spread"), { pace: 70.6, reason: "v2 reason. Projected pace about 71 possessions." });
  assert.deepEqual(paceAndReason(g, "moneyline"), { pace: 70.6, reason: "v2 reason. Projected pace about 71 possessions." });
  assert.equal(cardReason(g, "spread"), "slate reason (v2)");
  assert.equal(cardReason(g, null), "slate reason (v2)");
});
