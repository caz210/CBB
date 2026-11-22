/** Types and pure helpers for the game sheet (compare + recent results).
 *  The UI never computes projections. Last-N W/L and rest days are simple counts
 *  from stored game dates and scores. Official picks only — never tracked. */

import { visibleTags, type ArchetypeTag } from "./archetypes";
import { fmtRating } from "./rankings";

export const SHEET_TABS = [
  { k: "bet", l: "Bet" },
  { k: "matchup", l: "Matchup" },
  { k: "form", l: "Form" },
] as const;
export type SheetTab = (typeof SHEET_TABS)[number]["k"];
export const DEFAULT_SHEET_TAB: SheetTab = "bet";

export type OfficialPickResult = {
  pick: string;
  result: "W" | "L" | "P" | null;
};


export type SheetRecentGame = {
  game_id: number;
  date: string;
  opponent: string;
  location: "home" | "away" | "neutral";
  team_score: number | null;
  opp_score: number | null;
  result: "W" | "L" | "T" | null;
  official: OfficialPickResult[];
};

export type SheetTeamExtras = {
  team_id: number;
  name: string;
  games_played: number | null;
  /** Games we actually have in the log (may be less than games_played). */
  log_games: number;
  tentative: boolean;
  adj_oe: number | null;
  adj_de: number | null;
  tempo: number | null;
  tempo_pctile: number | null;
  efg_pctile: number | null;
  d_efg_pctile: number | null;
  to_pct_pctile: number | null;
  d_to_pct_pctile: number | null;
  or_pct_pctile: number | null;
  opp_or_pct_pctile: number | null;
  ftr_pctile: number | null;
  d_ftr_pctile: number | null;
  tags: ArchetypeTag[];
  rank: number | null;
  prev_rank: number | null;
  rating: number | null;
  /** D-I W-L from the latest weekly snapshot, when present. */
  di_wins: number | null;
  di_losses: number | null;
  rank_week_of: string | null;
  /** Season W-L from the games in our log (not invented for missing games). */
  log_wins: number;
  log_losses: number;
  official_wins: number;
  official_losses: number;
  official_pushes: number;
  /** Nights between the last final in our log and this game date. Null if we have no prior final. */
  rest_days: number | null;
  recent: SheetRecentGame[];
};

export type SheetExtras = {
  away: SheetTeamExtras | null;
  home: SheetTeamExtras | null;
};

export type CompareKind = "high" | "low" | "style";

export type CompareRow = {
  key: string;
  label: string;
  hint?: string;
  kind: CompareKind;
  /** When set, this is a percentile (0–100). */
  pctile?: boolean;
};

/** Side-by-side rows we can actually fill from stored team_profile_latest + ranking_snapshots. */
export const COMPARE_ROWS: CompareRow[] = [
  { key: "rating", label: "Rating", hint: "CZarp rating vs an average D-I team", kind: "high" },
  { key: "rank", label: "Rank", kind: "low" },
  { key: "record", label: "D-I record", hint: "From the games in our log", kind: "style" },
  { key: "adj_oe", label: "Offense", hint: "Points per 100 possessions", kind: "high" },
  { key: "adj_de", label: "Defense", hint: "Points allowed per 100 possessions (lower is better)", kind: "low" },
  { key: "tempo", label: "Tempo", hint: "Possessions per 40 minutes — style, not better or worse", kind: "style" },
  { key: "efg_pctile", label: "Shooting", hint: "Effective FG% percentile vs D-I", kind: "high", pctile: true },
  { key: "d_efg_pctile", label: "Opp. shooting", hint: "Opponent effective FG% percentile (higher = tighter D)", kind: "high", pctile: true },
  { key: "to_pct_pctile", label: "Ball security", hint: "Turnover-rate percentile", kind: "high", pctile: true },
  { key: "d_to_pct_pctile", label: "Force TOs", hint: "Forced-turnover percentile", kind: "high", pctile: true },
  { key: "or_pct_pctile", label: "Off. glass", hint: "Offensive-rebound percentile", kind: "high", pctile: true },
  { key: "opp_or_pct_pctile", label: "Def. glass", hint: "Defensive-rebound percentile", kind: "high", pctile: true },
  { key: "rest", label: "Rest", hint: "Nights off since the last final in our log. Shown only when every game is in the log.", kind: "style" },
];

/** A tighter set for the tale-of-the-tape layout. */
export const TAPE_ROWS: CompareRow[] = COMPARE_ROWS.filter((r) =>
  ["rating", "rank", "record", "adj_oe", "adj_de", "tempo", "rest"].includes(r.key),
);

/** The six four-factor percentile bars, shown behind "More stats" on the Matchup tab. */
export const MORE_STATS_ROWS: CompareRow[] = COMPARE_ROWS.filter((r) => r.pctile);

export function teamValue(t: SheetTeamExtras | null, key: string): number | null {
  if (!t) return null;
  switch (key) {
    case "rating": return t.rating;
    case "rank": return t.rank;
    case "adj_oe": return t.adj_oe;
    case "adj_de": return t.adj_de;
    case "tempo": return t.tempo;
    case "tempo_pctile": return t.tempo_pctile;
    case "efg_pctile": return t.efg_pctile;
    case "d_efg_pctile": return t.d_efg_pctile;
    case "to_pct_pctile": return t.to_pct_pctile;
    case "d_to_pct_pctile": return t.d_to_pct_pctile;
    case "or_pct_pctile": return t.or_pct_pctile;
    case "opp_or_pct_pctile": return t.opp_or_pct_pctile;
    case "ftr_pctile": return t.ftr_pctile;
    case "d_ftr_pctile": return t.d_ftr_pctile;
    case "rest": return logIncomplete(t) ? null : t.rest_days;
    default: return null;
  }
}

export function formatTeamValue(t: SheetTeamExtras | null, row: CompareRow): string {
  if (!t) return "—";
  if (row.key === "record") {
    if (t.log_games === 0) return "—";
    return `${t.log_wins}–${t.log_losses}`;
  }
  if (row.key === "rank") return t.rank != null ? `#${t.rank}` : "—";
  if (row.key === "rating") return fmtRating(t.rating);
  if (row.key === "rest") {
    if (logIncomplete(t) || t.rest_days == null) return "—";
    if (t.rest_days === 0) return "0 (played yesterday)";
    return t.rest_days === 1 ? "1 day" : `${t.rest_days} days`;
  }
  if (row.key === "tempo") {
    if (t.tempo == null) return "—";
    const p = t.tempo_pctile != null ? ` · ${Math.round(t.tempo_pctile)}%ile` : "";
    return `${t.tempo.toFixed(1)}${p}`;
  }
  const n = teamValue(t, row.key);
  if (n == null) return "—";
  if (row.pctile) return `${Math.round(n)}`;
  return n.toFixed(1);
}

/** Which side is better on this row. Null when it's style, a tie, or a value is missing. */
export function betterSide(
  away: SheetTeamExtras | null,
  home: SheetTeamExtras | null,
  row: CompareRow,
): "away" | "home" | null {
  if (row.kind === "style") return null;
  const a = teamValue(away, row.key);
  const h = teamValue(home, row.key);
  if (a == null || h == null) return null;
  if (a === h) return null;
  if (row.kind === "high") return a > h ? "away" : "home";
  return a < h ? "away" : "home";
}

export function locLabel(loc: SheetRecentGame["location"]): string {
  return loc === "away" ? "@" : "vs";
}

export function scoreLine(g: SheetRecentGame): string | null {
  if (g.team_score == null || g.opp_score == null) return null;
  return `${g.team_score}–${g.opp_score}`;
}

export function recentRecord(games: SheetRecentGame[]): { w: number; l: number; n: number } {
  let w = 0, l = 0;
  for (const g of games) {
    if (g.result === "W") w += 1;
    else if (g.result === "L") l += 1;
  }
  return { w, l, n: w + l };
}

export function officialRecordLine(t: SheetTeamExtras): string | null {
  const n = t.official_wins + t.official_losses + t.official_pushes;
  if (n === 0) return null;
  return `${t.official_wins}–${t.official_losses}–${t.official_pushes}`;
}

/** True when the profile's games_played is higher than the finals in our log. */
export function logIncomplete(t: SheetTeamExtras | null | undefined): boolean {
  if (!t || t.games_played == null) return false;
  return t.log_games < t.games_played;
}

/** Form/Recent header: never looks like we chose a short "Last 2" window when the log is short. */
export function formCountLabel(t: SheetTeamExtras, shown: number): string {
  if (logIncomplete(t)) {
    return `${t.log_games} of ${t.games_played} games logged`;
  }
  if (shown <= 0) return "No finals in our log";
  const rec = recentRecord(t.recent.slice(0, shown));
  if (t.log_games > shown) return `Last ${shown}: ${rec.w}–${rec.l}`;
  return `${rec.n} games: ${rec.w}–${rec.l}`;
}

export function parseOfficialResult(r: string | null | undefined): "W" | "L" | "P" | null {
  if (!r) return null;
  const s = r.trim().toUpperCase();
  if (s === "W" || s.startsWith("W")) return "W";
  if (s === "L" || s.startsWith("L")) return "L";
  if (s === "P" || s.startsWith("P")) return "P";
  return null;
}

export type LedgerPickRow = {
  tier?: string | null;
  pick?: string | null;
  result?: string | null;
};

/** Keep official-play results only. Tracked, paper, Toss Up, and Lean never appear as chips. */
export function officialPicksFromLedger(rows: LedgerPickRow[]): OfficialPickResult[] {
  const out: OfficialPickResult[] = [];
  for (const row of rows) {
    if (row.tier !== "official") continue;
    const pick = (row.pick ?? "").trim();
    if (!pick) continue;
    out.push({ pick, result: parseOfficialResult(row.result) });
  }
  return out;
}

export { visibleTags };
