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

/** CZarp bet tiers on line edge % (value_bets.line_edge_pct, 0.05 = 5%).
 *  Play threshold: sides (spread/moneyline) 4%, totals 3%. VALUE at threshold, EDGE >= 5%, HOT >= 7%. */
export type EdgeTier = "HOT" | "EDGE" | "VALUE";
export const PLAY_THRESHOLD: Record<Market, number> = { spread: 0.04, moneyline: 0.04, total: 0.03 };
export function edgeTier(lineEdgePct: number, market: Market): EdgeTier | null {
  if (lineEdgePct >= 0.07) return "HOT";
  if (lineEdgePct >= 0.05) return "EDGE";
  if (lineEdgePct >= PLAY_THRESHOLD[market]) return "VALUE";
  return null;
}
/** Badge classes per tier; HOT gets the strongest color. */
export const TIER_BADGE: Record<EdgeTier, string> = {
  HOT: "bg-orange-500 text-ink-950",
  EDGE: "bg-edge-400/25 text-edge-400 ring-1 ring-edge-400/40",
  VALUE: "bg-edge-400/10 text-edge-400",
};
