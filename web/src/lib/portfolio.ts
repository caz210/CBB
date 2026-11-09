import type { LedgerRow } from "./types";

export type PortfolioRisk = {
  maxDrawdown: number; // units, peak-to-trough (always >= 0)
  longestLosingStreak: number;
  currentStreak: { n: number; result: "W" | "L" | null };
};

const unitsOf = (r: LedgerRow) => (r.pick_tier === "track" ? r.flat_units ?? r.profit_units : r.profit_units);

/** Chronological risk stats from graded ledger rows. Pushes don't break or extend W/L streaks. */
export function portfolioRisk(ledger: LedgerRow[]): PortfolioRisk {
  const chrono = [...ledger].reverse(); // loadResults returns newest-first
  let cum = 0, peak = 0, maxDrawdown = 0;
  let runL = 0, longestLosingStreak = 0;
  let currentStreak: PortfolioRisk["currentStreak"] = { n: 0, result: null };

  for (const r of chrono) {
    if (r.result == null) continue;
    const u = unitsOf(r) ?? 0;
    cum = Math.round((cum + u) * 100) / 100;
    peak = Math.max(peak, cum);
    maxDrawdown = Math.max(maxDrawdown, Math.round((peak - cum) * 100) / 100);

    if (r.result === "P") continue;
    if (r.result === "L") {
      runL++;
      longestLosingStreak = Math.max(longestLosingStreak, runL);
    } else {
      runL = 0;
    }
  }

  // Current streak from most recent graded (non-push) pick
  for (const r of ledger) {
    if (r.result == null || r.result === "P") continue;
    if (currentStreak.result == null) {
      currentStreak = { n: 1, result: r.result };
    } else if (r.result === currentStreak.result) {
      currentStreak = { n: currentStreak.n + 1, result: r.result };
    } else {
      break;
    }
  }

  return { maxDrawdown, longestLosingStreak, currentStreak };
}

/**
 * CZarp line edge: |fair − price| ÷ Vegas total (game total), for sides AND totals.
 * Denominator is always the market total — never the spread itself (that blows up near pick'em).
 * Returns null when inputs are missing or the result is not a sane fraction (< 0 or > 50%).
 */
export function computedLineEdge(
  fair: number | null | undefined,
  price: number | null | undefined,
  vegasTotal: number | null | undefined,
): number | null {
  if (fair == null || price == null || vegasTotal == null) return null;
  const t = Math.abs(vegasTotal);
  if (!(t > 0)) return null;
  const edge = Math.abs(fair - price) / t;
  // Sanity: real posted edges top out ~8%; anything >50% is bad data / wrong denom
  if (!Number.isFinite(edge) || edge < 0 || edge > 0.5) return null;
  return edge;
}
