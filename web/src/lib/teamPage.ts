/** Pure helpers for the team page: CZarp pick labels and the header subtitle. */
import { fmtRating, fmtRecord, movementMark, rankMovement } from "./rankings";

/** Columns for the team-page ranking snapshot. Never request unused W-L fields. */
export const TEAM_RANKING_COLUMNS =
  "rank,prev_rank,rating,di_wins,di_losses,week_of,as_of";

export const TEAM_RANKING_TIMEOUT_MS = 2000;

export type TeamRankingSnapshot = {
  rank: number;
  prev_rank: number | null;
  rating: number;
  di_wins: number | null;
  di_losses: number | null;
  week_of: string | null;
  as_of: string | null;
};

const isFiniteNum = (n: unknown): n is number => typeof n === "number" && Number.isFinite(n);

function textOrNull(v: unknown): string | null {
  return typeof v === "string" && v.trim() ? v.trim() : null;
}

function numOrNull(v: unknown): number | null {
  if (v == null || v === "") return null;
  const n = typeof v === "number" ? v : Number(v);
  return Number.isFinite(n) ? n : null;
}

/** Parse a ranking_snapshots row. Missing rank or rating → hide the ranking. */
export function parseTeamRanking(row: Record<string, unknown> | null | undefined): TeamRankingSnapshot | null {
  if (!row) return null;
  const rank = numOrNull(row.rank);
  const rating = numOrNull(row.rating);
  if (!isFiniteNum(rank) || rank < 1 || !isFiniteNum(rating)) return null;
  const prev = numOrNull(row.prev_rank);
  return {
    rank,
    prev_rank: isFiniteNum(prev) && prev >= 1 ? prev : null,
    rating,
    di_wins: numOrNull(row.di_wins),
    di_losses: numOrNull(row.di_losses),
    week_of: textOrNull(row.week_of),
    as_of: textOrNull(row.as_of),
  };
}

/** Ending year 2027 → "2026–27". */
export function fmtSeasonSpan(season: number | null | undefined): string | null {
  if (!isFiniteNum(season) || season < 1) return null;
  return `${season - 1}–${String(season).slice(-2)}`;
}

/** Official ATS / O/U records are pick counts (wins + losses). */
export function officialPickCount(wins: number | null | undefined, losses: number | null | undefined): number {
  const w = isFiniteNum(wins) && wins >= 0 ? wins : 0;
  const l = isFiniteNum(losses) && losses >= 0 ? losses : 0;
  return w + l;
}

export function officialPicksCue(wins: number | null | undefined, losses: number | null | undefined): string {
  const n = officialPickCount(wins, losses);
  return n === 1 ? "1 pick" : `${n} picks`;
}

export const NO_OFFICIAL_PICKS_YET = "No official picks yet";

export type OfficialMarketDisplay = {
  value: string | null;
  hint: string;
  quiet: boolean;
};

/** ATS / O/U official rows. Zero picks is a quiet note, not "0-0 / 0 picks". */
export function officialMarketDisplay(
  wins: number | null | undefined,
  losses: number | null | undefined,
): OfficialMarketDisplay {
  const n = officialPickCount(wins, losses);
  if (n === 0) return { value: null, hint: NO_OFFICIAL_PICKS_YET, quiet: true };
  return { value: `${wins}-${losses}`, hint: officialPicksCue(wins, losses), quiet: false };
}

/** W / L / P (or win / loss / push) → Win / Loss / Push. */
export function fmtPickResult(result: string | null | undefined): string | null {
  if (!result) return null;
  const s = result.trim().toLowerCase();
  if (!s) return null;
  if (s.startsWith("w") && s !== "withdrawn") return "Win";
  if (s.startsWith("l") || s === "lose") return "Loss";
  if (s.startsWith("p")) return "Push";
  return result.trim();
}

const SIDE_ALIASES = new Set(["home", "away", "push", "over", "under"]);

/** Name the team CZarp picked, when the side is home/away (or already a team name). */
export function atsPickedTeam(
  side: string | null | undefined,
  away: string,
  home: string,
): string | null {
  if (!side) return null;
  const s = side.trim();
  if (!s) return null;
  const low = s.toLowerCase();
  if (low === "away") return away || null;
  if (low === "home") return home || null;
  if (low === "push") return null;
  if (SIDE_ALIASES.has(low)) return null;
  return s;
}

function formatOuSide(side: string | null | undefined): string | null {
  if (!side) return null;
  const s = side.trim();
  if (!s) return null;
  const low = s.toLowerCase();
  if (low === "over") return "Over";
  if (low === "under") return "Under";
  if (low === "push") return null;
  return s;
}

function joinPick(prefix: string, side: string | null, result: string | null): string | null {
  if (!side && !result) return null;
  if (side && result) {
    if (side.toLowerCase() === result.toLowerCase()) return `${prefix}${result}`;
    return `${prefix}${side} · ${result}`;
  }
  return `${prefix}${side ?? result}`;
}

/** e.g. "CZarp ATS: Grambling St. · Win". Null when there is no pick and no result. */
export function czarpAtsLine(opts: {
  side: string | null | undefined;
  result: string | null | undefined;
  away: string;
  home: string;
}): string | null {
  const team = atsPickedTeam(opts.side, opts.away, opts.home);
  const result = fmtPickResult(opts.result);
  return joinPick("CZarp ATS: ", team, result);
}

/** e.g. "CZarp: Under · Win". Null when there is no pick and no result. */
export function czarpTotalLine(opts: {
  side: string | null | undefined;
  result: string | null | undefined;
}): string | null {
  const side = formatOuSide(opts.side);
  const result = fmtPickResult(opts.result);
  return joinPick("CZarp: ", side, result);
}

export type TeamSubtitlePart = {
  key: "season" | "games" | "tentative" | "rank" | "rating" | "record";
  text: string;
};

export type TeamSubtitleRanking = {
  rank: number;
  prev_rank: number | null;
  rating: number;
  di_wins: number | null;
  di_losses: number | null;
};

/**
 * Header subtitle parts, e.g. 2026–27 · 6 GP · #59 ▲2 · +12.8 · 4–2 D-I.
 * Omit ranking parts when no snapshot row is provided.
 */
export function teamSubtitleParts(input: {
  season?: number | null;
  gamesPlayed?: number | null;
  tentative?: boolean;
  ranking?: TeamSubtitleRanking | null;
}): TeamSubtitlePart[] {
  const parts: TeamSubtitlePart[] = [];
  const season = fmtSeasonSpan(input.season);
  if (season) parts.push({ key: "season", text: season });
  if (input.gamesPlayed !== undefined) {
    const gp = isFiniteNum(input.gamesPlayed) ? String(input.gamesPlayed) : "—";
    parts.push({ key: "games", text: `${gp} GP` });
  }
  if (input.tentative) parts.push({ key: "tentative", text: "tentative" });

  const r = input.ranking;
  if (r && isFiniteNum(r.rank) && r.rank >= 1 && isFiniteNum(r.rating)) {
    const mark = movementMark(rankMovement(r.rank, r.prev_rank));
    parts.push({ key: "rank", text: `#${r.rank} ${mark}` });
    parts.push({ key: "rating", text: fmtRating(r.rating) });
    const rec = fmtRecord(r.di_wins, r.di_losses);
    parts.push({ key: "record", text: rec === "—" ? "— D-I" : `${rec} D-I` });
  }
  return parts;
}

export function formatTeamSubtitle(parts: TeamSubtitlePart[]): string {
  return parts.map((p) => p.text).join(" · ");
}
