/** The CZarp verdict ladder: one shared source for labels, short copy, and the mapping rule.
 *  Weakest to strongest: Toss Up → Lean → Favor → Official Play.
 *
 *  Inputs are stored pipeline values only (slate_games / projections): edge_now_pct_*,
 *  edge_vs_open_pct_*, skip_reasons_*, verdict_*, plus posted picks. The UI never computes a
 *  projection or an edge; it only compares the stored edges to the play bar.
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

/** Hold rules that keep a both-bars edge from posting. Keys are the pipeline's skip codes. */
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
    short: "The edge is below our play bar (4% sides, 3% totals), so it's not a play.",
  },
  {
    level: "lean",
    label: "Lean",
    short: "The edge clears our bar at today's line, but not vs the opening line, so it's not a play.",
  },
  {
    level: "favor",
    label: "Favor",
    short: "The edge clears our bar vs the opening line (sides need today's line too), but a hold rule kept it from posting: a new-coach pause, a market that moved 3+ pts away from CZarp since open, or a stake under $1.",
  },
  {
    level: "official",
    label: "Official Play",
    short: "A posted pick. Official Plays are time-stamped and graded in the public record.",
  },
] as const;

/** Tip for a card with no line yet: no edge to grade, so nothing about the bar. */
export const NO_LINE_TIP = { title: "Toss Up", body: "No line yet, so there's no edge to grade. Not a play." } as const;

export const VERDICT_RANK: Record<VerdictLevel, number> = { toss_up: 0, lean: 1, favor: 2, official: 3 };

export function verdictDef(level: VerdictLevel): VerdictDef {
  return VERDICT_LADDER.find((d) => d.level === level)!;
}
export const verdictLabel = (level: VerdictLevel) => verdictDef(level).label;

export type LadderInput = {
  market: Market;
  /** A posted official pick exists for this market (or stored verdict_* = 'official'). */
  official: boolean;
  /** Stored edge at today's line (fraction, 0.04 = 4%). */
  edgeNow: number | null | undefined;
  /** Stored edge vs the opening line (fraction). */
  edgeVsOpen: number | null | undefined;
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

/** Map one market to its ladder level.
 *  - Official Play: posted pick.
 *  - Sides (spread/moneyline): the pipeline needs both bars.
 *      Toss Up: edge now below the bar (or no stored edge).
 *      Favor: edge now AND edge vs open clear the bar, and a hold code blocked it.
 *      Lean: edge now clears, otherwise (open doesn't clear; or a pre-skip-code row).
 *  - Totals: the pipeline judges the open bar only (no current-line rule; posted totals can
 *    have an edge now under 3%), so the open edge decides:
 *      Favor: edge vs open clears the bar and a hold code blocked it (edge now doesn't matter).
 *      Lean: edge now clears but the open doesn't; or the open clears with no recorded hold
 *        (only rows from before skip codes were stored, 11-05..11-09).
 *      Toss Up: neither edge clears the bar (or no stored edge). */
export function ladderLevel(x: LadderInput): LadderResult {
  if (x.official) return { level: "official", holds: [] };
  const bar = PLAY_THRESHOLD[x.market];
  const nowClears = finite(x.edgeNow) && x.edgeNow >= bar;
  const openClears = finite(x.edgeVsOpen) && x.edgeVsOpen >= bar;
  const holds = holdCodes(x.skipCodes);
  if (x.market === "total") {
    if (openClears && holds.length) return { level: "favor", holds };
    if (nowClears || openClears) return { level: "lean", holds: [] };
    return { level: "toss_up", holds: [] };
  }
  if (!nowClears) return { level: "toss_up", holds: [] };
  if (openClears && holds.length) return { level: "favor", holds };
  return { level: "lean", holds: [] };
}
