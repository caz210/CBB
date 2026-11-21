/** The CZarp verdict ladder: one shared source for labels, short copy, and the mapping rule.
 *  Weakest to strongest: Toss Up → Lean → Favor → Official Play.
 *
 *  Every game gets TWO ratings, one per market (spread and total), each judged on the current
 *  line. Inputs are stored pipeline values only (slate_games / projections): edge_now_pct_*,
 *  skip_reasons_*, plus posted picks. The opening-line edge plays no part in the ladder. The UI
 *  never computes a projection or an edge; it only compares the stored edge to the play bar.
 *
 *  A glossary module may import VERDICT_LADDER for its definitions. */
import { PLAY_THRESHOLD, type Market } from "./labels";

export type VerdictLevel = "toss_up" | "lean" | "favor" | "official";

export type VerdictDef = {
  level: VerdictLevel;
  /** Chip text, exactly as customers see it. */
  label: string;
  /** One-line definition for tooltips and the glossary. */
  short: string;
};

/** Hold rules that keep an edge that clears the bar from being a play. Keys are the pipeline's skip codes. */
export const HOLD_CODES = {
  new_coach_lock: "new-coach pause",
  line_moved_against: "market moved 3+ pts away from CZarp since open",
  kelly_too_small: "stake under $1",
} as const;
export type HoldCode = keyof typeof HOLD_CODES;
const HOLD_ORDER = Object.keys(HOLD_CODES) as HoldCode[];

export const VERDICT_LADDER: readonly VerdictDef[] = [
  {
    level: "toss_up",
    label: "Toss Up",
    short: "The edge on the current line is below our play bar (4% sides, 3% totals), so it's not a play.",
  },
  {
    level: "lean",
    label: "Lean",
    short: "Clears our bar on the current line, but a hold rule (new-coach pause, big market move, or stake under $1) keeps it from a play.",
  },
  {
    level: "favor",
    label: "Favor",
    short: "Clears our bar on the current line with nothing holding it back.",
  },
  {
    level: "official",
    label: "Official Play",
    short: "A posted pick, set at the 7:15 AM CT run and never added later. Official Plays are time-stamped and graded in the public record.",
  },
] as const;

/** Tip for a card with no line yet: no edge to grade, so nothing about the bar. */
export const NO_LINE_TIP = { title: "Toss Up", body: "No line yet, so there's no edge to grade. Not a play." } as const;

/** Shown under the headline of an Official Play whose edge on the current line is now below the bar.
 *  The chip stays Official Play. */
export const LINE_MOVE_NOTE = "No longer a value play due to line movement.";

export const VERDICT_RANK: Record<VerdictLevel, number> = { toss_up: 0, lean: 1, favor: 2, official: 3 };

export function verdictDef(level: VerdictLevel): VerdictDef {
  return VERDICT_LADDER.find((d) => d.level === level)!;
}
export const verdictLabel = (level: VerdictLevel) => verdictDef(level).label;

/** Customer label for a market: sides read "Spread", totals "Total". */
export const ladderMarketLabel = (m: Market) => (m === "total" ? "Total" : "Spread");

export type LadderInput = {
  market: Market;
  /** A posted official pick exists for this market (official_picks_locked tier official). */
  official: boolean;
  /** Stored edge at the current line (fraction, 0.04 = 4%). */
  edgeNow: number | null | undefined;
  /** Stored skip codes for the market (skip_reasons_spread / skip_reasons_total). */
  skipCodes: readonly string[] | null | undefined;
};

export type LadderResult = { level: VerdictLevel; holds: HoldCode[] };

const finite = (n: unknown): n is number => typeof n === "number" && Number.isFinite(n);

/** Hold codes present in a skip list, in a stable order. */
export function holdCodes(codes: readonly string[] | null | undefined): HoldCode[] {
  const set = new Set(codes ?? []);
  return HOLD_ORDER.filter((c) => set.has(c));
}

/** True when a market's stored edge now clears the bar (landing exactly on it counts). */
export function clearsBar(market: Market, edgeNow: number | null | undefined): boolean {
  return finite(edgeNow) && edgeNow >= PLAY_THRESHOLD[market];
}

/** Map one market to its ladder level, judged on the current line (same rule for sides and totals):
 *  - Official Play: posted at the 7:15 AM CT run. Never promoted later.
 *  - Toss Up: edge now below the bar (4% sides, 3% totals), or no stored edge.
 *  - Favor: edge now clears the bar and no hold code.
 *  - Lean: edge now clears the bar but a hold code applies (new_coach_lock, line_moved_against,
 *    kelly_too_small).
 *  The opening-line edge is ignored. */
export function ladderLevel(x: LadderInput): LadderResult {
  if (x.official) return { level: "official", holds: [] };
  if (!clearsBar(x.market, x.edgeNow)) return { level: "toss_up", holds: [] };
  const holds = holdCodes(x.skipCodes);
  return holds.length ? { level: "lean", holds } : { level: "favor", holds: [] };
}

/** An Official Play shows LINE_MOVE_NOTE whenever that market's edge now is below the bar.
 *  No stored edge → no note (we can't say the line moved). */
export function showsLineMoveNote(market: Market, official: boolean, edgeNow: number | null | undefined): boolean {
  return official && finite(edgeNow) && !clearsBar(market, edgeNow);
}
