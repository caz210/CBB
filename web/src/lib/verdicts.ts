/** The CZarp verdict ladder: the single source for tier labels, copy, boundaries, ranking and the
 *  mapping rule. Every game gets TWO ratings, one per market (spread and total), each judged on the
 *  stored edge vs the CURRENT line (edge_now_pct_*), plus the stored hold codes (skip_reasons_*) and
 *  posted picks (official_picks_locked). The UI computes no projections or edges; it only compares
 *  stored edges to the boundaries below, at full precision (display rounds to one decimal).
 *
 *  Glossary entries (lib/glossary.ts) and tap-to-learn tips (lib/explainers.ts) read VERDICT_LADDER. */
import { fmtEdgeVsBar, fmtOne, fmtPlayPct, fmtSpread, MIN_STAKE_UNITS, PLAY_THRESHOLD, type Market } from "./labels";
import { OFFICIAL_POST_TIME } from "./officialPost";

/** Internal keys. "official" is the CZarp Play (a posted pick); customers never see the key. */
export type VerdictLevel = "toss_up" | "lean" | "value" | "late_value" | "official";

export type VerdictDef = {
  level: VerdictLevel;
  /** Chip text, exactly as customers see it. */
  label: string;
  /** One-line definition for tooltips and the glossary. */
  short: string;
};

/** Hold rules that keep an edge at or past the bar off the record. Keys are the pipeline's skip codes. */
export const HOLD_CODES = {
  new_coach_lock: "new-coach pause",
  line_moved_against: "Total moved 3+ pts away from CZarp since open",
  kelly_too_small: `stake under ${MIN_STAKE_UNITS}u`,
} as const;
export type HoldCode = keyof typeof HOLD_CODES;
const HOLD_ORDER = Object.keys(HOLD_CODES) as HoldCode[];

/** Lower edge bounds (fractions, inclusive) for each tier, per market. Sides use the spread row.
 *  bar = the play bar (PLAY_THRESHOLD): at or past it, a hold makes Value and no hold makes Late Value. */
export const TIER_BOUNDS = {
  spread: { lean: 0.02, value: 0.03, bar: PLAY_THRESHOLD.spread },
  total: { lean: 0.015, value: 0.0225, bar: PLAY_THRESHOLD.total },
} as const;

const boundsFor = (m: Market) => (m === "total" ? TIER_BOUNDS.total : TIER_BOUNDS.spread);

export const LATE_VALUE_NOTE = `Value on the current line. Not part of the official record; official plays are set at ${OFFICIAL_POST_TIME}.`;

/** Weakest to strongest; also the glossary order. */
export const VERDICT_LADDER: readonly VerdictDef[] = [
  {
    level: "toss_up",
    label: "Toss Up",
    short: "Little or no edge on the current line: under 2% on sides, under 1.5% on totals.",
  },
  {
    level: "lean",
    label: "Lean",
    short: "A small edge on the current line: 2% to 3% on sides, 1.5% to 2.25% on totals. Not a play.",
  },
  {
    level: "value",
    label: "Value",
    short: `A real edge on the current line: 3% to 4% on sides, 2.25% to 3% on totals. Also an edge past our play bar (4% sides, 3% totals) that a hold rule (new-coach pause, a 3+ point move against our total, or stake under ${MIN_STAKE_UNITS}u) keeps off the record; the note says which.`,
  },
  {
    level: "late_value",
    label: "Late Value",
    short: `Clears our play bar (4% sides, 3% totals) on the current line with nothing holding it back. ${LATE_VALUE_NOTE}`,
  },
  {
    level: "official",
    label: "CZarp Play",
    short: `Our official play, posted at the ${OFFICIAL_POST_TIME} run. It's time-stamped, graded in the public record, and stays a CZarp Play even if the line moves later.`,
  },
] as const;

/** Header ranking: the stronger market's verdict takes slot 1. One place; higher wins. */
export const VERDICT_RANK: Record<VerdictLevel, number> = { toss_up: 0, lean: 1, value: 2, late_value: 3, official: 4 };

/** Tip for a card with no line yet: no edge to grade, so nothing about the bar. */
export const NO_LINE_TIP = { title: "Toss Up", body: "No line yet, so there's no edge to grade. Not a play." } as const;

/** Shown under the headline of a CZarp Play when the current line has moved away from CZarp
 *  since post and the edge now is below the bar. The chip stays CZarp Play. */
export const LINE_MOVE_NOTE = "No longer a value play due to line movement.";

export function verdictDef(level: VerdictLevel): VerdictDef {
  return VERDICT_LADDER.find((d) => d.level === level)!;
}
export const verdictLabel = (level: VerdictLevel) => verdictDef(level).label;

/** Customer label for a market: sides read "Spread", totals "Total". */
export const ladderMarketLabel = (m: Market) => (m === "total" ? "Total" : "Spread");

export type LadderInput = {
  market: Market;
  /** A posted pick exists for this market (official_picks_locked tier official). */
  official: boolean;
  /** Stored edge at the current line (fraction, 0.04 = 4%). */
  edgeNow: number | null | undefined;
  /** Stored skip codes for the market (skip_reasons_spread / skip_reasons_total). */
  skipCodes: readonly string[] | null | undefined;
};

/** holds: the hold codes behind a Value that is past the bar; empty otherwise. */
export type LadderResult = { level: VerdictLevel; holds: HoldCode[] };

const finite = (n: unknown): n is number => typeof n === "number" && Number.isFinite(n);

/** Hold codes present in a skip list, in a stable order. */
export function holdCodes(codes: readonly string[] | null | undefined): HoldCode[] {
  const set = new Set(codes ?? []);
  return HOLD_ORDER.filter((c) => set.has(c));
}

/** True when a market's stored edge now is at or past the play bar (landing exactly on it counts). */
export function clearsBar(market: Market, edgeNow: number | null | undefined): boolean {
  return finite(edgeNow) && edgeNow >= boundsFor(market).bar;
}

/** One market's tier. Precedence, first match wins:
 *  1. CZarp Play: posted at the morning run, whatever the edge does later.
 *  2. Toss Up: no stored edge.
 *  3. Past the bar (≥ 4% sides / 3% totals): a hold code → Value (with the holds); none → Late Value.
 *  4. Value: ≥ 3% / 2.25%.   5. Lean: ≥ 2% / 1.5%.   6. Toss Up otherwise.
 *  Lower bounds are inclusive; holds only matter past the bar. */
export function ladderLevel(x: LadderInput): LadderResult {
  if (x.official) return { level: "official", holds: [] };
  if (!finite(x.edgeNow)) return { level: "toss_up", holds: [] };
  const b = boundsFor(x.market);
  const e = x.edgeNow;
  if (e >= b.bar) {
    const holds = holdCodes(x.skipCodes);
    return holds.length ? { level: "value", holds } : { level: "late_value", holds: [] };
  }
  if (e >= b.value) return { level: "value", holds: [] };
  if (e >= b.lean) return { level: "lean", holds: [] };
  return { level: "toss_up", holds: [] };
}

export type LineMoveInput = {
  official: boolean;
  market: Market;
  edgeNow: number | null | undefined;
  /** Market line at post (same convention as currentLine). */
  postedLine?: number | null;
  /** Current market line (same convention as postedLine). */
  currentLine?: number | null;
  /** CZarp's number at post (fair_at_post), or the current official number if that's all we have. */
  fair?: number | null;
};

const FINITE_N = (n: unknown): n is number => typeof n === "number" && Number.isFinite(n);

/** True when the line has moved farther from CZarp's number than it was at post (away from CZarp).
 *  A move toward CZarp, or no move since post, is not against the posted pick. */
export function lineMovedAwayFromCzarp(posted: number, current: number, fair: number): boolean {
  if (![posted, current, fair].every(FINITE_N)) return false;
  if (current === posted) return false;
  return Math.abs(current - fair) > Math.abs(posted - fair) + 1e-9;
}

/** A CZarp Play shows LINE_MOVE_NOTE only when the edge now is below the bar AND the current
 *  line has moved away from CZarp since post. Missing lines → no note (we can't prove a move). */
export function showsLineMoveNote(x: LineMoveInput): boolean {
  return x.official && finite(x.edgeNow) && !clearsBar(x.market, x.edgeNow)
    && FINITE_N(x.postedLine) && FINITE_N(x.currentLine) && FINITE_N(x.fair)
    && lineMovedAwayFromCzarp(x.postedLine, x.currentLine, x.fair);
}

/** Neutral context when a CZarp Play is below the bar but the line did not move away since post.
 *  e.g. "4.0% at open 150.5 · 2.7% now". Requires open line, edge at open, and edge now. */
export function officialOpenNowContext(
  market: Market,
  edgeVsOpen: number | null | undefined,
  openLine: number | null | undefined,
  edgeNow: number | null | undefined,
): string | null {
  if (!FINITE_N(edgeVsOpen) || !FINITE_N(openLine) || !FINITE_N(edgeNow)) return null;
  const line = market === "total" ? fmtOne(openLine) : fmtSpread(openLine);
  return `${fmtPlayPct(edgeVsOpen)} at open ${line} · ${fmtEdgeVsBar(edgeNow, market)} now`;
}

/** Official play whose stored edge now is below the play bar (used for % color, not the move tag). */
export function officialBelowBar(market: Market, official: boolean, edgeNow: number | null | undefined): boolean {
  return official && finite(edgeNow) && !clearsBar(market, edgeNow);
}
