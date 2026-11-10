/** Locked post-time fair value for an official card.
 *
 * TODO(CZarp): `picks` has no `fair_at_post` (and no `edge_at_post` alias —
 * posted edge already lives in `picks.edge` / value_bets.line_edge_pct).
 * Preferred source: the projection row for the pick's `model_version` at or
 * before `logged_at` (official totals use czarp-v1-totals). Fallback:
 * reconstruct fair from `line_or_price` + `edge` + side. Spreads need a Vegas
 * total as the edge denominator; that post-time total is not on the pick row,
 * so the fallback uses the current market total (approximate).
 */
import type { Market } from "./labels";
import type { SlateGame, ValueBet } from "./types";

export type PostedFairSource = "projection" | "reconstructed";

/** Reconstruct fair from the posted market line and posted edge.
 *  Totals: denom = posted total. Spreads: denom = Vegas total (caller supplies;
 *  should be post-time, else current). Result is a total, or a home-line spread.
 */
export function reconstructPostedFair(
  market: Market,
  side: string,
  postedLine: number | null | undefined,
  edge: number | null | undefined,
  vegasTotal: number | null | undefined,
  home: string,
  away: string,
): number | null {
  if (postedLine == null || edge == null || !Number.isFinite(postedLine) || !Number.isFinite(edge)) return null;
  const s = side.trim().toLowerCase();
  if (market === "total") {
    const denom = Math.abs(postedLine);
    if (!(denom > 0)) return null;
    if (s.startsWith("over")) return postedLine + edge * denom;
    if (s.startsWith("under")) return postedLine - edge * denom;
    return null;
  }
  if (market === "spread") {
    const denom = vegasTotal;
    if (denom == null || !(Math.abs(denom) > 0)) return null;
    const pts = edge * Math.abs(denom);
    // picks.line_or_price is the side's line (daily_run: sgn * home_spread).
    // Model likes this side, so fair_side = posted_line − edge×total.
    const fairSide = postedLine - pts;
    if (mentionsTeam(s, home)) return fairSide;
    if (mentionsTeam(s, away)) return -fairSide;
    return fairSide;
  }
  return null;
}

function mentionsTeam(sideLower: string, team: string) {
  const t = team.trim().toLowerCase();
  if (!t) return false;
  return sideLower === t || sideLower.startsWith(t + " ") || sideLower.includes(t);
}

export function currentLineForBet(g: SlateGame, market: Market): number | null {
  if (market === "total") return g.market_total;
  if (market === "spread") return g.market_spread;
  return null;
}

export function betCoverProb(g: SlateGame, b: ValueBet | null, market: Market | null): number | null {
  if (b?.model_prob != null) return b.model_prob;
  if (market === "spread") {
    if (g.home_cover_prob == null || g.proj_spread == null || g.market_spread == null) return null;
    // More negative home-line fair than the market → home is the value side.
    return g.proj_spread < g.market_spread ? g.home_cover_prob : 1 - g.home_cover_prob;
  }
  if (market === "total") {
    const fair = g.proj_total_v1 ?? g.proj_total;
    const over = g.insight?.over_prob ?? null;
    if (over == null || fair == null || g.market_total == null) return null;
    return fair > g.market_total ? over : 1 - over;
  }
  return null;
}
