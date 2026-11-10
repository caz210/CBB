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
/** picks.tier: 'official' = staked plays, 'track' = paper-only 1u tracking, never bet. */
export const PICK_TIER_LABEL = { official: "Official", track: "Tracked" } as const;
export type PickTier = keyof typeof PICK_TIER_LABEL;
export const isPickTier = (v: unknown): v is PickTier => v === "official" || v === "track";

/** Tip-off (timestamptz) shown in Central time, e.g. "6:00 PM CT". */
export function fmtTipoff(iso: string) {
  return (
    new Intl.DateTimeFormat("en-US", { timeZone: "America/Chicago", hour: "numeric", minute: "2-digit" }).format(new Date(iso)) +
    " CT"
  );
}

/** "3:15 PM CT" or "Sat 3:15 PM CT" when the stamp is not today in Central time. */
export function fmtUpdated(iso: string, now = new Date()) {
  const t = new Date(iso);
  if (Number.isNaN(t.getTime())) return null;
  const dayFmt = new Intl.DateTimeFormat("en-CA", { timeZone: "America/Chicago", year: "numeric", month: "2-digit", day: "2-digit" });
  const sameDay = dayFmt.format(t) === dayFmt.format(now);
  const time = new Intl.DateTimeFormat("en-US", { timeZone: "America/Chicago", hour: "numeric", minute: "2-digit", ...(sameDay ? {} : { weekday: "short" }) }).format(t);
  return `${time} CT`;
}

export const STALE_MS = 2 * 60 * 60 * 1000;
export function isStale(iso: string | null | undefined, now = new Date()) {
  if (iso == null || iso === "") return false; // no stamp measured — don't hide Live
  const t = Date.parse(iso);
  return !Number.isFinite(t) || now.getTime() - t > STALE_MS;
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

/** "Posted 3:12 PM CT · 4h 18m before tip" from logged_at vs tipoff. */
export function fmtPostedProof(loggedAt: string | null | undefined, tipoff: string | null | undefined) {
  if (!loggedAt) return null;
  const t = new Date(loggedAt);
  if (Number.isNaN(t.getTime())) return null;
  let s = `Posted ${fmtTipoff(loggedAt)}`;
  if (!tipoff) {
    s += " · Tip TBD";
  } else {
    const mins = Math.round((new Date(tipoff).getTime() - t.getTime()) / 60000);
    if (Number.isFinite(mins)) {
      const a = Math.abs(mins), h = Math.floor(a / 60), m = a % 60;
      const span = h ? `${h}h ${m}m` : `${m}m`;
      s += mins >= 0 ? ` · ${span} before tip` : ` · ${span} after tip`;
    }
  }
  return s;
}
export const shortHash = (h: string | null | undefined) => (h ? h.slice(0, 8) : null);
export const fmtClv = (c: number) => `${c > 0 ? "+" : c < 0 ? "−" : ""}${Math.abs(c).toFixed(1)} pts`;
export const LOCKED_NOTE = "Picks are locked once posted: they can't be edited or deleted.";

/** "Posted 142.0 · 4.1% → Now 146.2" (totals as 1 decimal; spreads as signed lines). */
export function fmtPostedVsNow(
  fair: number | null | undefined,
  edge: number | null | undefined,
  now: number | null | undefined,
  market: Market,
): string | null {
  if (fair == null || edge == null) return null;
  const f = market === "total" ? fair.toFixed(1) : fmtLine(fair);
  const n = now == null ? "—" : market === "total" ? now.toFixed(1) : fmtLine(now);
  return `Posted ${f} · ${pct(edge, 1)} → Now ${n}`;
}
