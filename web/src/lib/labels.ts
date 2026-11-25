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
export const PICK_TIER_LABEL = { official: "Official", track: "Tracked", lean: "CZarp Leans" } as const;
export type PickTier = keyof typeof PICK_TIER_LABEL;
export const isPickTier = (v: unknown): v is PickTier => v === "official" || v === "track" || v === "lean";
/** Map a stored tier. Unknown values are null — never official. */
export function parsePickTier(v: unknown): PickTier | null {
  return isPickTier(v) ? v : null;
}

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

/** "12:58 PM CT" when final_at is the same America/Chicago date as `relativeTo`
 *  (a YYYY-MM-DD game/slate date, or today if omitted); otherwise "Nov 9, 12:30 AM CT".
 *  Prefix "~" when the stamp is estimated. Invalid/missing → null. */
export function fmtFinalAt(
  iso: string | null | undefined,
  opts?: { estimated?: boolean | null; relativeTo?: string | null },
): string | null {
  if (!iso) return null;
  const t = new Date(iso);
  if (Number.isNaN(t.getTime())) return null;
  const dayFmt = new Intl.DateTimeFormat("en-CA", { timeZone: "America/Chicago", year: "numeric", month: "2-digit", day: "2-digit" });
  const stampDay = dayFmt.format(t);
  const rel = opts?.relativeTo;
  const sameDay = rel && /^\d{4}-\d{2}-\d{2}$/.test(rel) ? stampDay === rel : stampDay === dayFmt.format(new Date());
  const time = new Intl.DateTimeFormat("en-US", { timeZone: "America/Chicago", hour: "numeric", minute: "2-digit" }).format(t);
  const date = new Intl.DateTimeFormat("en-US", { timeZone: "America/Chicago", month: "short", day: "numeric" }).format(t);
  const label = sameDay ? `${time} CT` : `${date}, ${time} CT`;
  return opts?.estimated ? `~${label}` : label;
}

/** "Final · 12:58 PM CT" or just "Final" when final_at is missing. */
export function finalLabel(
  iso: string | null | undefined,
  estimated?: boolean | null,
  relativeTo?: string | null,
): string {
  const when = fmtFinalAt(iso, { estimated, relativeTo });
  return when ? `Final · ${when}` : "Final";
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
/** Integer count of 10^-dp units, rounded half away from zero, with a tolerance for float artifacts
 *  (74.05 stored as 74.0499999… still counts as 7405 hundredths). */
function units(x: number, dp: number): number {
  const f = 10 ** dp;
  return Math.sign(x) * Math.round(Math.abs(x) * f + 1e-6);
}
/** Fixed-point string from an integer count of 10^-dp units: 7405, 2 → "74.05". */
function fromUnits(u: number, dp: number): string {
  const f = 10 ** dp, a = Math.abs(u);
  return `${u < 0 ? "-" : ""}${Math.floor(a / f)}.${String(a % f).padStart(dp, "0")}`;
}
/** Exactly one decimal, keeping a trailing .0 (142.0, 142.5). FV and Price totals. Never a half-point round. */
export const fmtOne = (n: number) => { const u = units(n, 1); return fromUnits(u === 0 ? 0 : u, 1); };
/** Signed line at exactly one decimal, keeping a trailing .0: "-6.1", "+8.9", "-6.0", "0.0". FV and Price spreads. */
export const fmtLine = (n: number) => { const u = units(n, 1); return u > 0 ? `+${fromUnits(u, 1)}` : fromUnits(u === 0 ? 0 : u, 1); };
/** Spread for display: one decimal, or "PK" when it is 0 (the only exception to the one-decimal rule).
 *  Spreads only; totals are never PK. */
export const fmtSpread = (n: number | null | undefined) => {
  // Missing or non-numeric is "—", never "PK" (Number(null) === 0).
  if (typeof n !== "number" || !Number.isFinite(n)) return "—";
  return fmtLine(n) === "0.0" ? "PK" : fmtLine(n);
};

/** Graded-hero pick label: totals stay unsigned, spreads use PK at 0, and we do not
 *  append a number the pick string already carries ("Over 135.5", "Team +26.5"). */
export function fmtLedgerPick(pick: string, line: number | null | undefined): string {
  if (line == null || !Number.isFinite(line)) return pick;
  const isTotal = /^(over|under)\b/i.test(pick.trim());
  const shown = isTotal ? fmtOne(line) : fmtSpread(line);
  const already = [shown, fmtLine(line), fmtOne(line)];
  if (already.some((s) => s && pick.includes(s))) {
    if (!isTotal && shown === "PK") return pick.replace(/(?:\+)?0\.0\b/, "PK");
    return pick;
  }
  return `${pick} ${shown}`;
}
/** Projected team score at its stored precision, never re-rounded to one decimal. The pipeline stores
 *  (total ± margin) / 2 at two decimals with total and margin at 0.1, so a score ends in .x0 or .x5:
 *  two decimals only when the second digit isn't zero (74.05, 67.95), else one (74.0, 74.5). Never more
 *  than two decimals; float artifacts (74.0499999) still read 74.05. Then away + home equals the FV total
 *  and home − away equals the FV margin, exactly as displayed. */
export const fmtScore = (x: number) => { const u = units(x, 2); return u % 10 === 0 ? fromUnits(u / 10, 1) : fromUnits(u, 2); };
/** Win probability for display: never shows a flat 0% / 100%. */
export const fmtWinProb = (p: number) => (p < 0.01 ? "<1%" : p > 0.99 ? ">99%" : pct(p));
export const fmtOdds = (o: number) => (o > 0 ? `+${o}` : `${o}`);
export const fmtUnits = (u: number, dp = 2) => `${u > 0 ? "+" : u < 0 ? "−" : ""}${Math.abs(u).toFixed(dp)}u`;

/** Play bar on line edge % (fractions, 0.04 = 4%): sides (spread/moneyline) 4%, totals 3%.
 *  The verdict ladder that uses it lives in lib/verdicts.ts. */
export const PLAY_THRESHOLD: Record<Market, number> = { spread: 0.04, moneyline: 0.04, total: 0.03 };
/** Pipeline MIN_STAKE is $1 on a $1,000 notional bankroll. 1u = 1% of bankroll, so the floor is 0.1u. */
export const MIN_STAKE_UNITS = 0.1;


/** "Posted 3:12 PM CT · 4h 18m before tip" from logged_at vs tipoff. Kept for callers that still want the tip gap. */
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

/** "Posted 7:12 AM CT · time-stamped, can't be edited". Central Time via fmtTipoff. */
export function fmtPostedStamp(iso: string | null | undefined) {
  if (!iso) return null;
  const t = new Date(iso);
  if (Number.isNaN(t.getTime())) return null;
  return `Posted ${fmtTipoff(iso)} · time-stamped, can't be edited`;
}
export const shortHash = (h: string | null | undefined) => (h ? h.slice(0, 8) : null);
export const fmtClv = (c: number) => `${c > 0 ? "+" : c < 0 ? "−" : ""}${Math.abs(c).toFixed(1)} pts`;
export const LOCKED_NOTE = "Picks are time-stamped once posted: they can't be edited or deleted.";

const isFiniteNum = (n: unknown): n is number => typeof n === "number" && Number.isFinite(n);

/** Fraction → edge percent, always one decimal (0.045 → "4.5%", 0.04 → "4.0%"), so every edge on
 *  cards, sheets and the team page reads in the same style. */
export function fmtPlayPct(x: number) {
  return `${(x * 100).toFixed(1)}%`;
}

/** Edge % next to the play bar. At or above the bar: same as fmtPlayPct. Below it: floored to one
 *  decimal, so a near-miss never reads as meeting the bar (0.0297 → "2.9%", never "3%"). */
export function fmtEdgeVsBar(x: number, market: Market) {
  if (x >= PLAY_THRESHOLD[market]) return fmtPlayPct(x);
  const floored = Math.floor(x * 1000 + 1e-9) / 10;
  return `${Math.max(0, floored).toFixed(1)}%`;
}

/** Threshold as a whole percent for copy: 0.04 → "4%". */
export function fmtThresholdPct(market: Market) {
  return `${Math.round(PLAY_THRESHOLD[market] * 100)}%`;
}


/** "Posted 142.0 · 1.7% (line 144.5) → Now 144.5 · 3.7% vs open".
 *  Hides any missing piece (never "NaN" or a placeholder dash in the chain).
 *  edge_at_post_pct / edge_vs_open_pct are fractions (0.0173 = 1.73%). */
export function fmtPostedVsNow(opts: {
  fair?: number | null;
  edge?: number | null;
  line?: number | null;
  now?: number | null;
  vsOpen?: number | null;
  market: Market;
}): string | null {
  const fmt = (n: number) => (opts.market === "total" ? fmtOne(n) : fmtLine(n));
  const parts: string[] = [];
  if (isFiniteNum(opts.fair)) parts.push(`FV ${fmt(opts.fair)}`);
  if (isFiniteNum(opts.edge)) {
    const e = fmtPlayPct(opts.edge);
    parts.push(parts.length ? `· ${e}` : e);
  }
  if (isFiniteNum(opts.line)) parts.push(`(line ${fmt(opts.line)})`);
  if (isFiniteNum(opts.now)) parts.push(`→ Now ${fmt(opts.now)}`);
  if (isFiniteNum(opts.vsOpen)) parts.push(`· ${fmtEdgeVsBar(opts.vsOpen, opts.market)} vs open`);
  return parts.length ? parts.join(" ") : null;
}
