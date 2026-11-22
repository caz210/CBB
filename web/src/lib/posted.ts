/** Locked post-time fields from official_picks_locked / value_bets.
 *  Do not reconstruct missing fair/open — hide those pieces instead.
 *  edge_at_post_pct / edge_vs_open_pct are fractions (0.0173 = 1.73%).
 */
import { fmtLine, fmtOne, fmtPostedVsNow, fmtScore, type Market } from "./labels";
import type { SlateGame, ValueBet } from "./types";

function mentionsTeam(side: string, team: string) {
  const s = side.trim().toLowerCase();
  const t = team.trim().toLowerCase();
  if (!t || !s) return false;
  return s === t || s.startsWith(t + " ") || s.startsWith(t + "+") || s.startsWith(t + "-");
}

/** Current market line in the same convention as line_at_post / fair_at_post
 *  (totals: the total; spreads: the picked-side line, not always home-line). */
export function currentLineForBet(g: SlateGame, market: Market, side?: string | null): number | null {
  if (market === "total") return g.market_total;
  if (market === "spread") {
    if (g.market_spread == null) return null;
    if (side && mentionsTeam(side, g.away) && !mentionsTeam(side, g.home)) return -g.market_spread;
    return g.market_spread;
  }
  return null;
}

/** "Posted 142.0 · 1.7% (line 144.5) → Now 144.5 · 3.7% vs open". Hides nulls. */
export function postedVsNow(b: ValueBet, g: SlateGame | null | undefined): string | null {
  if (!g) return null;
  return fmtPostedVsNow({
    fair: b.fair_at_post,
    edge: b.edge_at_post_pct ?? b.line_edge_pct,
    line: b.line_at_post ?? b.posted_line,
    now: currentLineForBet(g, b.market, b.side),
    vsOpen: b.edge_vs_open_pct,
    market: b.market,
  });
}

export function betCoverProb(g: SlateGame, b: ValueBet | null, market: Market | null): number | null {
  if (b?.model_prob != null) return b.model_prob;
  if (market === "spread") {
    if (g.home_cover_prob == null || g.proj_spread == null || g.market_spread == null) return null;
    return g.proj_spread < g.market_spread ? g.home_cover_prob : 1 - g.home_cover_prob;
  }
  if (market === "total") {
    // Totals: both the fair total and the over probability come from czarp-v1-totals, never v2.
    const fair = g.czarp_total ?? null;
    const over = g.czarp_over_prob ?? null;
    if (over == null || fair == null || g.market_total == null) return null;
    return fair > g.market_total ? over : 1 - over;
  }
  return null;
}

/** Compact card line: "Posted 142.0 → Now 144.5". Hides any missing piece. */
export function postedNowCompact(b: ValueBet, g: SlateGame | null | undefined): string | null {
  if (!g) return null;
  const fmt = (n: number) => (b.market === "total" ? fmtOne(n) : fmtLine(n));
  const fair = b.fair_at_post;
  const now = currentLineForBet(g, b.market, b.side);
  const parts: string[] = [];
  if (typeof fair === "number" && Number.isFinite(fair)) parts.push(`Posted ${fmt(fair)}`);
  if (typeof now === "number" && Number.isFinite(now)) parts.push(`Now ${fmt(now)}`);
  if (!parts.length) return null;
  return parts.join(" → ");
}

/** Posted fair value (time-stamped, never edited) next to what CZarp projects now.
 *  Totals: compared with the current official CZarp total (czarp_total), never the sum of the team scores.
 *  Spreads: compared with the current team-score line (sides model).
 *  e.g. "Posted FV 140.5 · CZarp total now: 140.5" / "Posted FV −3.5 · CZarp projection now: 64.1–70.0, −5.9" */
export function postedFairVsProjections(b: ValueBet | null | undefined, g: SlateGame): string | null {
  if (!b || typeof b.fair_at_post !== "number" || !Number.isFinite(b.fair_at_post)) return null;
  // Model names are internal; customers only ever see "CZarp".
  const pa = g.proj_away, ph = g.proj_home;
  if (b.market === "total") {
    const posted = `Posted FV ${fmtOne(b.fair_at_post)}`;
    // Customers only ever see the one official CZarp total. Never the team-score sum (sides model).
    if (g.czarp_total == null) return posted;
    return `${posted} · CZarp total now: ${fmtOne(g.czarp_total)}`;
  }
  if (b.market === "spread") {
    const posted = `Posted FV ${fmtLine(b.fair_at_post)}`;
    if (pa == null || ph == null) return posted;
    const homeLine = -(ph - pa);
    const pickedAway = mentionsTeam(b.side, g.away) && !mentionsTeam(b.side, g.home);
    const now = pickedAway ? -homeLine : homeLine;
    return `${posted} · CZarp projection now: ${fmtScore(pa)}–${fmtScore(ph)}, ${fmtLine(now)}`;
  }
  return null;
}

/** Game-sheet note shown instead of a "posted" tag when the total FV is the posted pick's number. */
export const POSTED_TOTAL_NOTE = "Total shown is CZarp's number when this play was posted.";

/** Fair total for a card, sheet, or team page. Always czarp-v1-totals (the official totals model):
 *  the posted fair for an official total, else the current v1 projection, else "—".
 *  Never the v2 team-score sum. */
export function totalFair(g: SlateGame, bets: ValueBet[]): { value: string; num: number | null; tag?: "posted" } {
  const posted = bets.find((b) => b.market === "total" && b.pick_tier !== "track" && typeof b.fair_at_post === "number" && Number.isFinite(b.fair_at_post));
  if (posted?.fair_at_post != null) return { value: fmtOne(posted.fair_at_post), num: posted.fair_at_post, tag: "posted" };
  if (g.czarp_total != null) return { value: fmtOne(g.czarp_total), num: g.czarp_total };
  return { value: "—", num: null };
}
