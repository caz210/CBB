// Game-sheet helpers: rest guard, Form copy, official chips only, Bet tab default.
// Run: npm test
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync, statSync } from "node:fs";
import { join, resolve } from "node:path";
import {
  COMPARE_ROWS,
  DEFAULT_SHEET_TAB,
  SHEET_TABS,
  TAPE_ROWS,
  formCountLabel,
  formatTeamValue,
  logIncomplete,
  namedScore,
  namedSpread,
  nextSheetTab,
  officialPicksFromLedger,
  parseOfficialResult,
  teamValue,
  type SheetRecentGame,
  type SheetTeamExtras,
} from "../src/lib/sheetExtras";
import { fmtScore, fmtSpread } from "../src/lib/labels";

const slate = JSON.parse(readFileSync(resolve("tests/fixtures/slate-2026-11-22.json"), "utf8")) as {
  games: { game_id: number; away: string; home: string; proj_away: number | null; proj_home: number | null; proj_spread: number | null }[];
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
      team_score: win ? 70 : 60, opp_score: win ? 60 : 70, result: win ? "W" : "L", vs_di: true, official: [],
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


test("rest still shows when the log is ahead of games_played (snapshot lag)", () => {
  const ahead = team({ games_played: 5, log_games: 6, rest_days: 1 });
  assert.equal(logIncomplete(ahead), false);
  assert.equal(teamValue(ahead, "rest"), 1);
  assert.equal(formatTeamValue(ahead, restRow), "1 day");
  // Only a short log is missing data.
  assert.equal(logIncomplete(team({ games_played: 6, log_games: 5 })), true);
});

test("Form header stays sensible when the log is a game ahead of games_played", () => {
  const ahead = team({
    games_played: 5, log_games: 6, log_wins: 2, log_losses: 4,
    recent: recent(5, 2, 3),
  });
  assert.equal(formCountLabel(ahead, 5), "Last 5: 2–3");
  assert.equal(/of 5 games logged/.test(formCountLabel(ahead, 5)), false);
});

test("Matchup D-I record uses D-I finals, not all-log W-L or a lagging snapshot", () => {
  const recordRow = COMPARE_ROWS.find((r) => r.key === "record")!;
  // Cal Baptist-style: 1-5 in D-I finals vs a weekly snapshot that still says 1-4.
  const t = team({
    log_games: 6, log_wins: 1, log_losses: 5,
    di_wins: 1, di_losses: 5,
  });
  assert.equal(formatTeamValue(t, recordRow), "1–5");
  // A non-D-I win is in the log but must not change the D-I record.
  const withNonDi = team({
    log_games: 7, log_wins: 2, log_losses: 5,
    di_wins: 1, di_losses: 5,
  });
  assert.equal(formatTeamValue(withNonDi, recordRow), "1–5");
  assert.equal(formatTeamValue(team({ di_wins: 0, di_losses: 0, log_wins: 2, log_losses: 0 }), recordRow), "—");
  assert.match(recordRow.hint ?? "", /D-I finals in our log/);
});

test("sheet tabs move with arrows, Home, and End", () => {
  assert.equal(nextSheetTab("bet", "ArrowRight"), "matchup");
  assert.equal(nextSheetTab("matchup", "ArrowRight"), "form");
  assert.equal(nextSheetTab("form", "ArrowRight"), "bet");
  assert.equal(nextSheetTab("bet", "ArrowLeft"), "form");
  assert.equal(nextSheetTab("form", "Home"), "bet");
  assert.equal(nextSheetTab("bet", "End"), "form");
  assert.equal(nextSheetTab("matchup", "ArrowDown"), "form");
  assert.equal(nextSheetTab("matchup", "ArrowUp"), "bet");
  assert.equal(nextSheetTab("bet", "Enter"), null);
  const root = [resolve("."), resolve("..")].find((d) => { try { return statSync(join(d, "src/components/GameSheetTabs.tsx")).isFile(); } catch { return false; } })!;
  const src = readFileSync(join(root, "src/components/GameSheetTabs.tsx"), "utf8");
  assert.match(src, /role="tablist"/);
  assert.match(src, /role="tab"/);
  assert.match(src, /role="tabpanel"/);
  assert.match(src, /aria-selected/);
  assert.match(src, /aria-controls/);
  const route = readFileSync(join(root, "src/app/api/sheet-extras/route.ts"), "utf8");
  assert.match(route, /team_game_log/);
  assert.doesNotMatch(route, /from\("games"\)/);
  assert.doesNotMatch(route, /\.limit\(80\)/);
});

test("Bet projection and FV/Price keep the number apart from a long name so it cannot be hidden", () => {
  const mdes = slate.games.find((g) => g.home === "Maryland Eastern Shore");
  const uci = slate.games.find((g) => g.home === "Loyola Marymount");
  assert.ok(mdes && uci, "fixture includes the long-name games");
  const fv = namedSpread(mdes.home, mdes.proj_spread);
  assert.equal(fv.name, "Maryland Eastern Shore");
  assert.equal(fv.value, fmtSpread(mdes.proj_spread));
  assert.equal(fv.value, "+8.9");
  assert.ok(!fv.name!.includes(fv.value));
  assert.ok(uci.proj_home != null);
  const lmu = namedScore("Loyola Marymount", uci.proj_home);
  assert.equal(lmu.name, "Loyola Marymount");
  assert.equal(lmu.value, fmtScore(uci.proj_home));
  assert.ok(!lmu.value.includes("…") && lmu.value !== "—");
  const pres = namedScore("Presbyterian", 76.9);
  assert.deepEqual(pres, { name: "Presbyterian", value: "76.9" });
  assert.deepEqual(namedSpread("Presbyterian", 0), { name: "Presbyterian", value: "PK" });
  assert.deepEqual(namedSpread("Presbyterian", null), { name: null, value: "—" });
  assert.deepEqual(namedScore("Maryland Eastern Shore", null), { name: "Maryland Eastern Shore", value: "—" });
  // Layout: the Bet strip never truncates a name together with its number; the number is nowrap / shrink-0.
  const root = [resolve("."), resolve("..")].find((d) => { try { return statSync(join(d, "src/components/GameSheetTabs.tsx")).isFile(); } catch { return false; } })!;
  const src = readFileSync(join(root, "src/components/GameSheetTabs.tsx"), "utf8");
  const line = src.slice(src.indexOf("function NamedNumLine("), src.indexOf("function MarketBetRow("));
  assert.match(line, /break-words/);
  assert.match(line, /whitespace-nowrap/);
  assert.match(line, /shrink-0/);
  assert.doesNotMatch(line, /truncate/);
  const proj = src.slice(src.indexOf("function ProjStrip("), src.indexOf("function CompareTable("));
  assert.match(proj, /namedScore\(g\.away, pa\)/);
  assert.match(proj, /namedScore\(g\.home, ph\)/);
  assert.doesNotMatch(proj, /truncate/);
  const bet = src.slice(src.indexOf("export function QuickBetBlock("), src.indexOf("function ProjStrip("));
  assert.match(bet, /namedSpread\(spreadTeam, czSpread\)/);
  assert.match(bet, /namedSpread\(spreadTeam, curS\)/);
});
