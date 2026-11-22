/** The slate card's header row: ONE market (the stronger by VERDICT_RANK, via cardHeadline), the side or
 *  total the edge favors at the current price, and the edge now. e.g. "SPREAD  Texas St. -2.5  2.5%".
 *  Reads stored fields only; the UI computes no projections. The side comes from comparing two stored
 *  numbers, FV vs Price, the same comparison the stored edge is built from:
 *    spreads: proj_spread vs market_spread (home-line notation);
 *    totals:  czarp_total (the official total, the one edge_now_pct_total uses) vs market_total.
 *             Never slate_games.proj_total. */
import { MARKET_LABEL, fmtEdgeVsBar, fmtLine, fmtOne, type Market } from "./labels";
import { currentLineForBet } from "./posted";
import type { SlateGame, ValueBet } from "./types";
import { cardHeadline, notAPlayLine, type CardHeadline } from "./verdict";
import type { VerdictLevel } from "./verdicts";

export const NO_EDGE_TEXT = "No edge at this price";
export const NO_LINE_TEXT = "No line yet";

/** Edge-number color: CZarp Play and Late Value green; Value and Lean neutral; Toss Up dim.
 *  A CZarp Play that lost value (line-move note) is neutral, not green. */
export type EdgeTone = "green" | "neutral" | "dim";
export function edgeTone(verdict: VerdictLevel, lineMoveNote: boolean, hasEdge = true): EdgeTone {
  if (!hasEdge) return "dim";
  if (verdict === "official") return lineMoveNote ? "neutral" : "green";
  if (verdict === "late_value") return "green";
  if (verdict === "value" || verdict === "lean") return "neutral";
  return "dim";
}

/** Signed spread for a pick: one decimal, or "PK" at zero. */
export const fmtPickLine = (n: number) => (fmtLine(n) === "0.0" ? "PK" : fmtLine(n));

/** The side the edge favors at the current price. null pick + "even" when FV equals Price (as displayed);
 *  null pick + "missing" when either number is missing. */
export function sideAtPrice(g: SlateGame, market: Market): { pick: string | null; status: "pick" | "even" | "missing" } {
  if (market === "total") {
    const fv = g.czarp_total ?? null, px = g.market_total;
    if (fv == null || px == null) return { pick: null, status: "missing" };
    if (fmtOne(fv) === fmtOne(px)) return { pick: null, status: "even" };
    return { pick: `${fv > px ? "Over" : "Under"} ${fmtOne(px)}`, status: "pick" };
  }
  const fv = g.proj_spread, px = g.market_spread;
  if (fv == null || px == null) return { pick: null, status: "missing" };
  if (fmtLine(fv) === fmtLine(px)) return { pick: null, status: "even" };
  // FV more negative than the price: the home side covers it. Otherwise the away side, at the flipped price.
  return { pick: fv < px ? `${g.home} ${fmtPickLine(px)}` : `${g.away} ${fmtPickLine(-px)}`, status: "pick" };
}

/** Posted pick text: the stored side, with the posted price appended if the side lacks a number. */
function postedPick(b: ValueBet): string {
  const s = b.side.trim();
  const line = b.line_at_post ?? b.posted_line ?? null;
  if (/\d/.test(s) || /\bPK\b/i.test(s) || line == null) return s;
  return `${s} ${b.market === "total" ? fmtOne(line) : fmtPickLine(line)}`;
}

/** "Now 138.5" (or "Now -3.5" for the picked side of a spread) when the current line differs from the posted one. */
export function nowLine(b: ValueBet, g: SlateGame): string | null {
  const posted = b.line_at_post ?? b.posted_line ?? null;
  const cur = currentLineForBet(g, b.market, b.side);
  if (posted == null || cur == null) return null;
  const f = b.market === "total" ? fmtOne : fmtPickLine;
  return f(cur) === f(posted) ? null : `Now ${f(cur)}`;
}

export type CardHeaderRow = {
  market: Market | null;
  /** "Spread" / "Total" (shown upper-case). */
  marketLabel: string | null;
  /** "Texas St. -2.5", "Over 140.5", "Over 136.5" (posted). Null when there's nothing to pick. */
  pick: string | null;
  /** Shown instead of a pick: NO_EDGE_TEXT or NO_LINE_TEXT. */
  fallback: string | null;
  /** CZarp Play only: "Now 138.5" when the current line differs from the posted one. */
  now: string | null;
  /** Edge on the current line, one decimal (below the bar floored so it never reads as meeting it). */
  edgeLabel: string | null;
  tone: EdgeTone;
  verdict: VerdictLevel;
  lineMoveNote: boolean;
  headline: CardHeadline;
};

export function cardHeaderRow(g: SlateGame, bets: ValueBet[]): CardHeaderRow {
  const h = cardHeadline(g, bets);
  const m = h.market;
  const edgeLabel = m && h.edgeNow != null ? fmtEdgeVsBar(h.edgeNow, m) : null;
  const base = { market: m, marketLabel: m ? MARKET_LABEL[m] : null, edgeLabel, verdict: h.verdict, lineMoveNote: h.lineMoveNote, headline: h };
  const tone = edgeTone(h.verdict, h.lineMoveNote, edgeLabel != null);
  if (h.officialBet) {
    return { ...base, pick: postedPick(h.officialBet), fallback: null, now: nowLine(h.officialBet, g), tone };
  }
  if (!m) return { ...base, pick: null, fallback: NO_LINE_TEXT, now: null, tone };
  const s = sideAtPrice(g, m);
  if (s.status === "pick") return { ...base, pick: s.pick, fallback: null, now: null, tone };
  if (s.status === "even") return { ...base, pick: null, fallback: NO_EDGE_TEXT, now: null, tone: "dim" };
  return { ...base, pick: null, fallback: NO_LINE_TEXT, now: null, tone };
}

/** The card's note line under the projection: only a hold reason (new-coach pause, market move, stake under
 *  $1) on a held Value. "Not a play: edge now below X%" is gone; the chip and the edge say that. The
 *  line-move note sits under the header row instead. */
export function cardNoteLine(g: SlateGame, bets: ValueBet[]): string | null {
  const h = cardHeadline(g, bets);
  if (h.verdict !== "value" || !h.holds.length) return null;
  return notAPlayLine(g, bets);
}
