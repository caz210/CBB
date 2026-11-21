import { test } from "node:test";
import assert from "node:assert/strict";
import type { NewCoachLock } from "../src/lib/coach";
import { pickStatusTags, statusTagLabel, STATUS_TAG_MAX, STATUS_TAG_PRIORITY } from "../src/lib/statusTags";
import type { SlateGame, ValueBet } from "../src/lib/types";

const lock = (o: Partial<NewCoachLock> = {}): NewCoachLock => ({
  team: "Cal Baptist", gamesPlayed: 5, lockSides: 8, lockTotals: 4,
  sidesRemaining: 3, totalsRemaining: 0, sidesLocked: true, totalsLocked: false, ...o,
});

const game = (o: Partial<SlateGame> = {}): SlateGame => ({
  sport: "cbb", game_date: "2026-11-21", game_id: 1687, home: "Loyola Marymount", away: "Cal Baptist",
  tipoff: null, game: "Cal Baptist vs Loyola Marymount", proj_home: 78.8, proj_away: 64.2, proj_spread: -14.6,
  proj_total: 143, home_win_prob: 0.9, home_cover_prob: 0.5,
  archetypes: { home: [], away: [], home_tentative: false, away_tentative: false, tentative: false, notes: [] },
  market_spread: -13.5, market_total: 146, market_source: null,
  edge_now_pct_spread: 0.012, edge_now_pct_total: 0.048, verdict_spread: "low_value", verdict_total: "low_value",
  lean_total: true,
  ...o,
} as SlateGame);

const officialBet = (o: Partial<ValueBet> = {}): ValueBet => ({
  sport: "cbb",
  game_id: 1687,
  market: "spread",
  side: "Loyola Marymount -13.5",
  model_prob: 0.55,
  market_prob: 0.5,
  prob_edge: 0.05,
  line_edge_pct: 0.04,
  kelly_quarter_units: null,
  pick_tier: "official",
  ...o,
});

const labels = (g: SlateGame, bets: ValueBet[] = []) => pickStatusTags(g, bets).map(statusTagLabel);

test("STATUS_TAG_PRIORITY is verdict first, then pause, then value; cap is 2", () => {
  assert.deepEqual(STATUS_TAG_PRIORITY, ["verdict", "pause", "value"]);
  assert.equal(STATUS_TAG_MAX, 2);
});

test("both teams paused plus an official total gives Official Play and one merged pause chip", () => {
  const g = game({
    lean_total: false,
    coachLocks: [
      lock({ team: "Bradley", sidesRemaining: 3, totalsRemaining: 0, sidesLocked: true, totalsLocked: false }),
      lock({ team: "Pacific", sidesRemaining: 3, totalsRemaining: 0, sidesLocked: true, totalsLocked: false }),
    ],
  });
  const bets = [officialBet({ market: "total", side: "Over 126.5" })];
  const tags = pickStatusTags(g, bets);
  assert.deepEqual(tags.map(statusTagLabel), ["official", "pause"]);
  assert.equal(tags[1].kind, "pause");
  if (tags[1].kind === "pause") assert.equal(tags[1].locks.length, 2);
});

test("a Lean card with a pause gives Lean + pause", () => {
  const g = game({ coachLocks: [lock()] });
  assert.deepEqual(labels(g), ["lean", "pause"]);
});

test("Cal Baptist vs LMU (pause + Lean) keeps 2 tags: Lean first, then pause", () => {
  const g = game({ coachLocks: [lock()] });
  assert.deepEqual(labels(g), ["lean", "pause"]);
  assert.equal(pickStatusTags(g, []).length, 2);
});

test("one pause plus an Official Play keeps Official Play + pause", () => {
  const g = game({ coachLocks: [lock()] });
  assert.deepEqual(labels(g, [officialBet()]), ["official", "pause"]);
});

test("no value badge ever: an Official Play with no pause is Official Play alone, at any edge", () => {
  for (const e of [0.02, 0.04, 0.055, 0.09]) {
    assert.deepEqual(labels(game({ coachLocks: [] }), [officialBet({ line_edge_pct: e, edge_at_post_pct: e })]), ["official"]);
  }
});

test("no pause: Lean alone, Toss Up alone, Favor alone", () => {
  assert.deepEqual(labels(game({ coachLocks: [] })), ["lean"]);
  assert.deepEqual(labels(game({ edge_now_pct_total: 0.01, coachLocks: [] })), ["toss_up"]);
  assert.deepEqual(labels(game({ edge_vs_open_pct_total: 0.035, skip_reasons_total: ["kelly_too_small"], coachLocks: [] })), ["favor"]);
});

test("old VALUE / EDGE / HOT / Low value labels never come out of the picker", () => {
  const cases: [Partial<SlateGame>, ValueBet[]][] = [
    [{}, []],
    [{ edge_now_pct_total: 0.0 }, []],
    [{ edge_now_pct_total: 0.09, edge_vs_open_pct_total: 0.08, skip_reasons_total: ["line_moved_against"] }, []],
    [{}, [officialBet({ line_edge_pct: 0.08 })]],
    [{ coachLocks: [lock(), lock({ team: "Pacific" })] }, [officialBet({ line_edge_pct: 0.06 })]],
  ];
  for (const [o, bets] of cases) {
    for (const l of labels(game(o), bets)) {
      assert.ok(["official", "favor", "lean", "toss_up", "pause"].includes(l), l);
    }
  }
});

test("a cleared coach lock does not take a slot", () => {
  const g = game({
    coachLocks: [lock({ sidesLocked: false, totalsLocked: false, sidesRemaining: 0, totalsRemaining: 0 })],
  });
  assert.deepEqual(labels(g), ["lean"]);
});

test("Toss Up with a pause is verdict + pause", () => {
  const g = game({ edge_now_pct_total: 0.01, coachLocks: [lock()] });
  assert.deepEqual(labels(g), ["toss_up", "pause"]);
});

test("Favor (new-coach hold) with both teams paused: Favor + one merged pause chip", () => {
  const g = game({
    edge_now_pct_total: 0.05, edge_vs_open_pct_total: 0.045, skip_reasons_total: ["new_coach_lock"],
    coachLocks: [lock(), lock({ team: "Pacific" })],
  });
  const tags = pickStatusTags(g, []);
  assert.deepEqual(tags.map(statusTagLabel), ["favor", "pause"]);
  if (tags[1].kind === "pause") assert.equal(tags[1].locks.length, 2);
});

test("never returns more than STATUS_TAG_MAX status tags, and never drops the verdict", () => {
  const two = game({ coachLocks: [lock(), lock({ team: "Pacific" })] });
  const tags = pickStatusTags(two, [officialBet({ market: "total", side: "Over 126.5" })]);
  assert.ok(tags.length <= STATUS_TAG_MAX);
  assert.equal(tags[0] && statusTagLabel(tags[0]), "official");
  for (const o of [{}, { edge_now_pct_total: 0 }, { edge_vs_open_pct_total: 0.04, skip_reasons_total: ["new_coach_lock"] }]) {
    const t = pickStatusTags(game({ ...o, coachLocks: [lock(), lock({ team: "Pacific" })] }), []);
    assert.ok(t.length <= STATUS_TAG_MAX);
    assert.equal(t[0].kind, "verdict");
  }
});
