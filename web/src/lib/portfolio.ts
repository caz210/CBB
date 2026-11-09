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

/** Line edge fraction from fair value vs price. Null when either side is missing or price is 0. */
export function computedLineEdge(fair: number | null | undefined, price: number | null | undefined): number | null {
  if (fair == null || price == null || price === 0) return null;
  return Math.abs(fair - price) / Math.abs(price);
}
