// Game-sheet helpers: rest guard, Form copy, official chips only, Bet tab default.
// Run: npm test
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import {
  COMPARE_ROWS,
  DEFAULT_SHEET_TAB,
  SHEET_TABS,
  TAPE_ROWS,
  formCountLabel,
  formatTeamValue,
  logIncomplete,
  officialPicksFromLedger,
  parseOfficialResult,
  teamValue,
  type SheetRecentGame,
  type SheetTeamExtras,
} from "../src/lib/sheetExtras";

const slate = JSON.parse(readFileSync(resolve("tests/fixtures/slate-2026-11-22.json"), "utf8")) as {
  games: { game_id: number; away: string; home: string }[];
  posted: { game_id: number; market: string; side: string; pick_tier: string }[];
};

const mcneese = slate.games.find((g) => g.game_id === 1770);
assert.ok(mcneese, "fixture includes McNeese @ Texas St.");

const team = (o: Partial<SheetTeamExtras> = {}): SheetTeamExtras => ({
  team_id: 1,
  name: mcneese!.away,
  games_played: 3,
  log_games: 3,
  tentative: false,
  adj_oe: null, adj_de: null, tempo: null, tempo_pctile: null,
  efg_pctile: null, d_efg_pctile: null, to_pct_pctile: null, d_to_pct_pctile: null,
  or_pct_pctile: null, opp_or_pct_pctile: null, ftr_pctile: null, d_ftr_pctile: null,
  tags: [],
  rank: null, prev_rank: null, rating: null,
  di_wins: null, di_losses: null, rank_week_of: null,
  log_wins: 2, log_losses: 1,
  official_wins: 0, official_losses: 0, official_pushes: 0,
  rest_days: 1,
  recent: [],
  ...o,
});

const recent = (n: number, w: number, l: number): SheetRecentGame[] => {
  const out: SheetRecentGame[] = [];
  for (let i = 0; i < n; i++) {
    const win = i < w;
    out.push({
      game_id: 1000 + i, date: "2026-11-15", opponent: "Opp", location: "home",
      team_score: win ? 70 : 60, opp_score: win ? 60 : 70, result: win ? "W" : "L", official: [],
    });
  }
  // pad losses if w+l > n? just use n games already
  if (w + l !== n) throw new Error("recent count");
  return out;
};

const restRow = COMPARE_ROWS.find((r) => r.key === "rest")!;

test("rest is — when the log is short of games_played, even if rest_days was derived", () => {
  const incomplete = team({ games_played: 3, log_games: 2, rest_days: 1 });
  assert.equal(logIncomplete(incomplete), true);
  assert.equal(teamValue(incomplete, "rest"), null);
  assert.equal(formatTeamValue(incomplete, restRow), "—");
});

test("rest shows nights off only when every game is in the log", () => {
  assert.equal(formatTeamValue(team({ games_played: 3, log_games: 3, rest_days: 2 }), restRow), "2 days");
  assert.equal(formatTeamValue(team({ games_played: 3, log_games: 3, rest_days: 1 }), restRow), "1 day");
  assert.equal(formatTeamValue(team({ games_played: 3, log_games: 3, rest_days: 0 }), restRow), "0 (played yesterday)");
  // No profile count: don't pretend the log is short.
  const noGp = team({ games_played: null, log_games: 2, rest_days: 1 });
  assert.equal(logIncomplete(noGp), false);
  assert.equal(formatTeamValue(noGp, restRow), "1 day");
  // Complete log but no prior final.
  assert.equal(formatTeamValue(team({ games_played: 3, log_games: 3, rest_days: null }), restRow), "—");
});

test("Form header says 'X of Y games logged' when the log is incomplete", () => {
  const t = team({ games_played: 3, log_games: 2, recent: recent(2, 1, 1) });
  assert.equal(formCountLabel(t, 2), "2 of 3 games logged");
  // Must not look like we chose a "Last 2" window.
  assert.equal(/Last 2/.test(formCountLabel(t, 2)), false);
});

test("Form header uses Last N / N games once the log is complete", () => {
  const full = team({
    games_played: 6, log_games: 6, log_wins: 3, log_losses: 3,
    recent: recent(5, 2, 3),
  });
  assert.equal(formCountLabel(full, 5), "Last 5: 2–3");
  const short = team({
    games_played: 2, log_games: 2, log_wins: 1, log_losses: 1,
    recent: recent(2, 1, 1),
  });
  assert.equal(formCountLabel(short, 2), "2 games: 1–1");
  const empty = team({ games_played: 0, log_games: 0, log_wins: 0, log_losses: 0, recent: [] });
  assert.equal(formCountLabel(empty, 0), "No finals in our log");
});

test("chips come only from tier=official; fixture official sides are kept, tracked and lower tiers are not", () => {
  const official = slate.posted.filter((p) => p.pick_tier === "official");
  assert.ok(official.length >= 1, "fixture has official posted plays");
  const officialRows = official.map((p) => ({
    tier: "official" as const,
    pick: `${p.side} (${p.game_id}-${p.market})`,
    result: "W" as const,
  }));
  const chips = officialPicksFromLedger([
    ...officialRows,
    { tier: "track", pick: "TRACK-ONLY Under 199.5", result: "W" },
    { tier: "paper", pick: "PAPER-ONLY McNeese +2.5", result: "L" },
    { tier: "lean", pick: "LEAN-ONLY Texas St. -2.5", result: "W" },
    { tier: "official", pick: "   ", result: "W" },
    { tier: "official", pick: "Over 136.5", result: "loss" },
  ]);
  assert.deepEqual(chips.map((c) => c.pick), [...officialRows.map((r) => r.pick), "Over 136.5"]);
  assert.equal(chips.at(-1)?.result, "L");
  assert.deepEqual(officialPicksFromLedger([]), []);
  assert.equal(parseOfficialResult("Push"), "P");
  assert.equal(parseOfficialResult("win"), "W");
  assert.equal(parseOfficialResult(null), null);
});

test("the game sheet tabs default to Bet", () => {
  assert.deepEqual(SHEET_TABS.map((t) => t.k), ["bet", "matchup", "form"]);
  assert.equal(DEFAULT_SHEET_TAB, "bet");
  assert.equal(SHEET_TABS[0].l, "Bet");
  assert.ok(TAPE_ROWS.some((r) => r.key === "rest"));
});
