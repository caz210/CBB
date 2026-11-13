/** Pace and reason text by market.
 *  Total cards / total sheets use the czarp-v1-totals row's own proj_possessions and reason, and hide
 *  the line when v1's value is null (never fall back to v2). Spread and moneyline keep the default
 *  model's (v2) pace and reason, as before. */
import type { Market } from "./labels";
import type { SlateGame } from "./types";

export type PaceReason = { pace: number | null; reason: string | null };

export function paceAndReason(g: SlateGame, market: Market | null | undefined): PaceReason {
  const i = g.insight;
  if (market === "total") {
    return { pace: i?.v1_proj_possessions ?? null, reason: i?.v1_reason ?? null };
  }
  return {
    pace: i?.proj_possessions ?? g.proj_possessions ?? null,
    reason: i?.reason || g.reason || null,
  };
}

/** Card "Why:" text: total cards use v1's reason only; others keep slate_games.reason. */
export function cardReason(g: SlateGame, market: Market | null | undefined): string | null {
  if (market === "total") return g.insight?.v1_reason ?? null;
  return g.reason ?? null;
}
