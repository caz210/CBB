/** Local-only slate/results fixture for CZarp Lean screenshots.
 *  Enabled with CZARP_LEAN_FIXTURE=1. Never on in production unless that env is set.
 *  Not used in unit tests. */
import { leanRecordLine } from "./lean";
import type { LeanPick, LeanRecord, SlateGame } from "./types";

export const LEAN_FIXTURE_DATE = "2026-11-24";
const PITT_ID = 9100001;
const FLA_ID = 9100002;

export function leanFixtureEnabled(): boolean {
  return process.env.CZARP_LEAN_FIXTURE === "1";
}

const emptyArch = { home: [] as const, away: [] as const, home_tentative: false, away_tentative: false, tentative: false, notes: [] as string[] };

function baseGame(o: Partial<SlateGame> & Pick<SlateGame, "game_id" | "home" | "away" | "game" | "proj_spread" | "market_spread">): SlateGame {
  return {
    sport: "cbb",
    game_date: LEAN_FIXTURE_DATE,
    tipoff: `${LEAN_FIXTURE_DATE}T19:00:00-06:00`,
    proj_home: 74,
    proj_away: 67,
    proj_total: 141,
    czarp_total: 141,
    home_win_prob: 0.7,
    home_cover_prob: 0.6,
    archetypes: { ...emptyArch, home: [...emptyArch.home], away: [...emptyArch.away], notes: [] },
    market_total: 142.5,
    market_source: "vegas",
    edge_now_pct_spread: 0.029,
    edge_vs_open_pct_spread: 0.029,
    edge_now_pct_total: 0.004,
    skip_reasons_spread: ["below_open_threshold"],
    skip_reasons_total: ["below_open_threshold"],
    line_moved_away: false,
    line_moved_away_pts: 0,
    leans: [],
    ...o,
  };
}

export function fixtureLeanRecord(): LeanRecord {
  return {
    league: "ncaab",
    season: 2026,
    wins: 28,
    losses: 25,
    pushes: 0,
    pending: 0,
    units: 0.45,
    roi: 0.45 / 53,
    avg_clv: 0.12,
    new_coach_picks: 0,
    new_coach_units: 0,
  };
}

export function fixtureLeanPicks(): LeanPick[] {
  return [
    {
      sport: "cbb",
      game_date: LEAN_FIXTURE_DATE,
      game_id: PITT_ID,
      tipoff: `${LEAN_FIXTURE_DATE}T19:00:00-06:00`,
      home: "Virginia",
      away: "Pittsburgh",
      pick_id: "fixture-pitt",
      pick: "Pittsburgh -2.5",
      line: -2.5,
      odds: -110,
      edge_pct: 0.029,
      edge_vs_open_pct: 0.029,
      czarp_line: -6.8,
      open_line: -2.5,
      new_coach: false,
      result: "W",
      units: 0.91,
      clv: 0.5,
      created_at: `${LEAN_FIXTURE_DATE}T07:30:00-06:00`,
      pick_hash: "fixturepitt",
    },
    {
      sport: "cbb",
      game_date: LEAN_FIXTURE_DATE,
      game_id: FLA_ID,
      tipoff: `${LEAN_FIXTURE_DATE}T18:00:00-06:00`,
      home: "Florida",
      away: "Milwaukee",
      pick_id: "fixture-milw",
      pick: "Milwaukee +14.5",
      line: 14.5,
      odds: -110,
      edge_pct: 0.031,
      edge_vs_open_pct: 0.018,
      czarp_line: 11.5,
      open_line: 17.5,
      new_coach: false,
      result: null,
      units: null,
      clv: null,
      created_at: `${LEAN_FIXTURE_DATE}T07:30:00-06:00`,
      pick_hash: "fixturemilw",
    },
  ];
}

export function fixtureSlateGames(): SlateGame[] {
  const [pitt, milw] = fixtureLeanPicks();
  const pittGame = baseGame({
    game_id: PITT_ID,
    home: "Virginia",
    away: "Pittsburgh",
    game: "Pittsburgh @ Virginia",
    // CZarp likes Pitt by 6.8 on a -2.5 road line → home-line FV is +6.8
    proj_spread: 6.8,
    market_spread: 2.5,
    edge_now_pct_spread: 0.029,
    line_moved_away: false,
    line_moved_away_pts: 0,
    leans: [pitt],
  });
  const flaGame = baseGame({
    game_id: FLA_ID,
    home: "Florida",
    away: "Milwaukee",
    game: "Milwaukee @ Florida",
    proj_spread: -11.5,
    market_spread: -14.5,
    edge_now_pct_spread: 0.031,
    line_moved_away: true,
    line_moved_away_pts: 3,
    leans: [milw],
  });
  return [pittGame, flaGame];
}

/** Prepend fixture games for the screenshot date. Mutates `games`. */
export function mergeLeanFixtureGames(games: SlateGame[], date: string): void {
  if (date !== LEAN_FIXTURE_DATE) return;
  const incoming = fixtureSlateGames();
  const ids = new Set(incoming.map((g) => g.game_id));
  for (let i = games.length - 1; i >= 0; i--) {
    if (ids.has(games[i].game_id)) games.splice(i, 1);
  }
  games.unshift(...incoming);
}

export function fixtureLeanResults(): { record: LeanRecord; picks: LeanPick[]; summaryLine: string; season: number } {
  const record = fixtureLeanRecord();
  return { record, picks: fixtureLeanPicks(), summaryLine: leanRecordLine(record), season: record.season };
}
