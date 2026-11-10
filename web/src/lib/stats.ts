/** Stats helpers: Wilson interval, American-odds EV. No promises of winning. */

/** 95% Wilson score interval for a binomial win rate. `n` is decided bets (wins+losses, no pushes). */
export function wilsonCi(wins: number, n: number, z = 1.959964): { lo: number; hi: number } | null {
  if (!(n > 0) || wins < 0 || wins > n) return null;
  const p = wins / n;
  const z2 = z * z;
  const denom = 1 + z2 / n;
  const center = (p + z2 / (2 * n)) / denom;
  const margin = (z * Math.sqrt((p * (1 - p) + z2 / (4 * n)) / n)) / denom;
  return { lo: Math.max(0, center - margin), hi: Math.min(1, center + margin) };
}

export const SMALL_SAMPLE_N = 50;

export function americanToDecimal(odds: number): number {
  return odds < 0 ? 1 + 100 / Math.abs(odds) : 1 + odds / 100;
}

/** Expected profit on a $100 stake at American odds, given a model win probability. */
export function evPer100(modelProb: number | null | undefined, americanOdds: number | null | undefined): number | null {
  if (modelProb == null || !(modelProb >= 0 && modelProb <= 1)) return null;
  const odds = americanOdds == null || !Number.isFinite(americanOdds) || americanOdds === 0 ? -110 : americanOdds;
  const dec = americanToDecimal(odds);
  const ev = 100 * (modelProb * (dec - 1) - (1 - modelProb));
  return Number.isFinite(ev) ? Math.round(ev * 100) / 100 : null;
}

export const fmtEv100 = (ev: number) => `${ev >= 0 ? "+" : "−"}$${Math.abs(ev).toFixed(2)}`;
