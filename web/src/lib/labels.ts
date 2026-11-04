// DB enums are lowercase (see /workspace/CBB/db/schema.sql); map to display labels here only.
export const MARKET_LABEL = { spread: "Spread", total: "Total", moneyline: "Moneyline" } as const;
export const SOURCE_LABEL = { vegas: "Vegas", kalshi: "Kalshi", robinhood: "Robinhood" } as const;
export const TIER_LABEL = { free: "Free", pro: "Pro", syndicate: "Syndicate" } as const;
export const RESULT_LABEL = { W: "Win", L: "Loss", P: "Push" } as const;
export const GAME_STATUS_LABEL = {
  scheduled: "Scheduled", in_progress: "Live", final: "Final", postponed: "Postponed", canceled: "Canceled",
} as const;

export type Market = keyof typeof MARKET_LABEL;
export type MarketSource = keyof typeof SOURCE_LABEL;
export type Tier = keyof typeof TIER_LABEL;

/** Tip-off (timestamptz) shown in Central time, e.g. "6:00 PM CT". */
export function fmtTipoff(iso: string) {
  return (
    new Intl.DateTimeFormat("en-US", { timeZone: "America/Chicago", hour: "numeric", minute: "2-digit" }).format(new Date(iso)) +
    " CT"
  );
}
export function fmtGameDate(d: string) {
  return new Intl.DateTimeFormat("en-US", { timeZone: "UTC", weekday: "short", month: "short", day: "numeric" }).format(
    new Date(d + "T00:00:00Z"),
  );
}
export const pct = (x: number, dp = 0) => `${(x * 100).toFixed(dp)}%`;
export const signedPct = (x: number, dp = 1) => `${x >= 0 ? "+" : "−"}${Math.abs(x * 100).toFixed(dp)}%`;
/** Lines/spreads to at most 1 decimal (model spreads arrive as e.g. -31.54). */
export const fmtLine = (n: number) => { const r = Math.round(n * 10) / 10; return r > 0 ? `+${r}` : `${r}`; };
/** Win probability for display: never shows a flat 0% / 100%. */
export const fmtWinProb = (p: number) => (p < 0.01 ? "<1%" : p > 0.99 ? ">99%" : pct(p));
export const fmtOdds = (o: number) => (o > 0 ? `+${o}` : `${o}`);
export const fmtUnits = (u: number, dp = 2) => `${u > 0 ? "+" : u < 0 ? "−" : ""}${Math.abs(u).toFixed(dp)}u`;

/** CZarp edge tiers on prob_edge (BettingConfig.tiers = 0.03 / 0.05 / 0.08). */
export function edgeTier(probEdge: number): "STRONG" | "VALUE" | "LEAN" | null {
  if (probEdge >= 0.08) return "STRONG";
  if (probEdge >= 0.05) return "VALUE";
  if (probEdge >= 0.03) return "LEAN";
  return null;
}
