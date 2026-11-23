/** Quarter-Kelly stake used for official-play sizing, matching the pipeline's formula
 *  (0.25 Kelly, 2.5% cap, $1,000 notional bankroll). Customers see units only. */
import { fmtOdds, MIN_STAKE_UNITS, type Market } from "./labels";
import type { SlateGame } from "./types";

/** Conservative hit-rate the official-play Kelly uses: 54.5% sides, 53.5% totals. */
export const KELLY_HIT_RATE: Record<"spread" | "total", number> = { spread: 0.545, total: 0.535 };
const KELLY_MULT = 0.25;
const MAX_STAKE_FRAC = 0.025;
const BANKROLL = 1000;

export function americanToDecimal(price: number): number {
  const a = !Number.isFinite(price) || price === 0 ? -110 : price;
  return 1 + (a > 0 ? a / 100 : 100 / -a);
}

/** Quarter-Kelly units at the official-play hit rate. 1u = 1% of the $1,000 bankroll. */
export function kellyStakeUnits(p: number, price: number): number {
  const b = americanToDecimal(price) - 1;
  if (!(b > 0) || !Number.isFinite(p)) return 0;
  const f = Math.max(0, (b * p - (1 - p)) / b) * KELLY_MULT;
  const dollars = Math.round(Math.min(f, MAX_STAKE_FRAC) * BANKROLL * 100) / 100;
  return dollars / (BANKROLL / 100);
}

/** True when this American price, at the market's official-play hit rate, leaves a stake under 0.1u. */
export function priceKillsMinStake(market: Market, price: number): boolean {
  const m = market === "total" ? "total" : "spread";
  return Number.isFinite(price) && kellyStakeUnits(KELLY_HIT_RATE[m], price) < MIN_STAKE_UNITS;
}

/** American odds on the side the current edge favors. Null if we don't have a line or a price. */
export function favoredSidePrice(g: SlateGame, market: Market): number | null {
  const price = (n: number | null | undefined) => (typeof n === "number" && Number.isFinite(n) ? n : null);
  if (market === "total") {
    const fv = g.czarp_total ?? null, px = g.market_total;
    if (fv == null || px == null || fv === px) return null;
    return fv > px ? price(g.market_over_price) : price(g.market_under_price);
  }
  const fv = g.proj_spread, px = g.market_spread;
  if (fv == null || px == null || fv === px) return null;
  // Same comparison as sideAtPrice: FV more negative than the price → home, else away.
  return fv < px ? price(g.market_home_spread_price) : price(g.market_away_spread_price);
}

/** Customer hold copy when a too-small stake is caused by the price; otherwise the generic floor wording. */
export function kellyHoldLabel(generic: string, market: Market, g?: SlateGame | null): string {
  if (!g) return generic;
  const price = favoredSidePrice(g, market === "total" ? "total" : "spread");
  if (price == null || !priceKillsMinStake(market, price)) return generic;
  return `price too juiced (${fmtOdds(price)}) for a stake of ${MIN_STAKE_UNITS}u+`;
}

export function parseMarketPrices(prices: unknown): {
  over: number | null;
  under: number | null;
  homeSpread: number | null;
  awaySpread: number | null;
} {
  const blank = { over: null, under: null, homeSpread: null, awaySpread: null };
  if (!prices || typeof prices !== "object" || Array.isArray(prices)) return blank;
  const rec = prices as Record<string, unknown>;
  const n = (k: string) => {
    const v = rec[k];
    return typeof v === "number" && Number.isFinite(v) ? v : null;
  };
  return {
    over: n("over_price"),
    under: n("under_price"),
    homeSpread: n("home_spread_price"),
    awaySpread: n("away_spread_price"),
  };
}
