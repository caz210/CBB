/** Card verdict, headline numbers, and "not a play" copy.
 *  Reads stored slate_games columns (edge_now_*, edge_vs_open_*, verdict_*, lean_*).
 *  Never computes an edge or the Lean flag in the UI. If those columns are
 *  missing/null, the vs-open / vs-now line and Lean chip are hidden, and
 *  verdict falls back to official picks from value_bets / official_picks_locked.
 */
import { fmtPlayPct, fmtThresholdPct, type Market } from "./labels";
import type { SlateGame, ValueBet } from "./types";

export type Verdict = "official" | "low_value";

const FINITE = (n: unknown): n is number => typeof n === "number" && Number.isFinite(n);

export function parseVerdict(v: unknown): Verdict | null {
  return v === "official" || v === "low_value" ? v : null;
}

/** Sides (spread/moneyline) share the spread columns; totals have their own. */
export function marketEdgeNow(g: SlateGame, market: Market): number | null {
  const n = market === "total" ? g.edge_now_pct_total : g.edge_now_pct_spread;
  return FINITE(n) ? n : null;
}

export function marketEdgeVsOpen(g: SlateGame, market: Market): number | null {
  const n = market === "total" ? g.edge_vs_open_pct_total : g.edge_vs_open_pct_spread;
  return FINITE(n) ? n : null;
}

export function storedVerdict(g: SlateGame, market: Market): Verdict | null {
  return market === "total" ? parseVerdict(g.verdict_total) : parseVerdict(g.verdict_spread);
}

/** Read the stored lean flag. Only `true` shows the chip; null/missing is not computed. */
export function marketLean(g: SlateGame, market: Market): boolean {
  const v = market === "total" ? g.lean_total : g.lean_spread;
  return v === true;
}

function officialBetFor(bets: ValueBet[], market: Market): ValueBet | undefined {
  if (market === "total") return bets.find((b) => b.market === "total");
  return bets.find((b) => b.market === "spread" || b.market === "moneyline");
}

function isOfficialMarket(g: SlateGame, officialBets: ValueBet[], market: Market): boolean {
  if (officialBetFor(officialBets, market)) return true;
  return storedVerdict(g, market) === "official";
}

export type CardHeadline = {
  market: Market | null;
  verdict: Verdict;
  /** Posted edge for official plays; stored edge_now for low value. Null if we don't have it. */
  edgePct: number | null;
  officialBet: ValueBet | null;
  edgeNow: number | null;
  edgeVsOpen: number | null;
  /** Stored lean_* for the headline market; only on low-value cards. */
  lean: boolean;
  /** "vs open 2.5% (needs 4%) · vs now 4.5%" or "Posted 4.2% · vs now 3.1%". Hidden when stored edges are missing. */
  subtitle: string | null;
};

function officialBetsOnly(bets: ValueBet[]): ValueBet[] {
  return bets.filter((b) => b.pick_tier !== "track");
}

/** Headline market: an official market if any, else the market with the higher stored edge_now. */
export function cardHeadline(g: SlateGame, officialBets: ValueBet[]): CardHeadline {
  const official = officialBetsOnly(officialBets);

  if (official.length) {
    const officialBet = official.reduce((a, b) => {
      const ea = a.edge_at_post_pct ?? a.line_edge_pct;
      const eb = b.edge_at_post_pct ?? b.line_edge_pct;
      return eb > ea ? b : a;
    });
    const market = officialBet.market;
    const posted = officialBet.edge_at_post_pct ?? officialBet.line_edge_pct;
    const edgeNow = marketEdgeNow(g, market);
    const edgeVsOpen = marketEdgeVsOpen(g, market);
    const postedLabel = FINITE(posted) ? fmtPlayPct(posted) : null;
    const subtitle = postedLabel && edgeNow != null
      ? `Posted ${postedLabel} · vs now ${fmtPlayPct(edgeNow)}`
      : null;
    return {
      market,
      verdict: "official",
      edgePct: FINITE(posted) ? posted : null,
      officialBet,
      edgeNow,
      edgeVsOpen,
      lean: false,
      subtitle,
    };
  }

  // No official play. Pick the market with the higher stored edge_now.
  const spreadNow = marketEdgeNow(g, "spread");
  const totalNow = marketEdgeNow(g, "total");
  let market: Market | null = null;
  if (spreadNow != null || totalNow != null) {
    market = (spreadNow ?? -1) >= (totalNow ?? -1) ? "spread" : "total";
  }

  const edgeNow = market ? marketEdgeNow(g, market) : null;
  const edgeVsOpen = market ? marketEdgeVsOpen(g, market) : null;
  return {
    market,
    verdict: "low_value",
    edgePct: edgeNow,
    officialBet: null,
    edgeNow,
    edgeVsOpen,
    lean: market ? marketLean(g, market) : false,
    subtitle: lowValueSubtitle(market, edgeNow, edgeVsOpen),
  };
}

function lowValueSubtitle(market: Market | null, edgeNow: number | null, edgeVsOpen: number | null): string | null {
  if (!market) return null;
  const parts: string[] = [];
  if (edgeVsOpen != null) parts.push(`vs open ${fmtPlayPct(edgeVsOpen)} (needs ${fmtThresholdPct(market)})`);
  if (edgeNow != null) parts.push(`vs now ${fmtPlayPct(edgeNow)}`);
  return parts.length ? parts.join(" · ") : null;
}

const COACH_SKIP = "new_coach_lock";

/** Machine-readable skip codes → short labels. Y is the play threshold for that market. */
export function skipCodeLabel(code: string, thresholdPct: string): string | null {
  if (code === COACH_SKIP) return null; // covered by the coach chip
  switch (code) {
    case "below_open_threshold": return `edge vs open below ${thresholdPct}`;
    case "below_current_threshold": return `edge now below ${thresholdPct}`;
    case "line_moved_against": return "line moved against us";
    case "no_opening_line": return "no opening line yet";
    case "no_current_line": return "no current line";
    case "game_started": return "game already started";
    case "kelly_too_small": return "stake too small to post";
    default: return code.replace(/_/g, " ");
  }
}

/** True if user-facing text still talks about lock/unlock (or a new-coach hold). */
function isCoachOrLockCopy(s: string) {
  if (/\bnew\s*coach\b/i.test(s)) return true;
  return /\b(?:un)?lock(?:ed|s|ing)?\b/i.test(s);
}

function rewriteLockWords(s: string): string {
  return s
    .replace(/\bunlock(?:ed|s|ing)?\b/gi, "are back on")
    .replace(/\blocked\b/gi, "paused")
    .replace(/\block(?:s|ing)?\b/gi, "pause")
    .replace(/\s{2,}/g, " ")
    .replace(/^[:\-\s]+|[:\-\s]+$/g, "")
    .trim();
}

/** "paused until game 8 (new coach, 3 played)" from the stored reason text, if present. */
export function coachPauseText(human: string | null | undefined): string | null {
  const m = (human ?? "").match(/paused until game \d+(?:\s*\([^)]*\))?/i);
  if (!m) return null;
  return m[0].charAt(0).toLowerCase() + m[0].slice(1);
}

function formatSkip(human: string | null | undefined, codes: string[] | null | undefined, market: Market): string | null {
  const threshold = fmtThresholdPct(market);
  const all = codes ?? [];
  const filtered = all.filter((c) => c !== COACH_SKIP);
  const text = human?.trim() ?? "";
  if (text && !isCoachOrLockCopy(text)) return text;
  if (filtered.length) {
    const labels = filtered.map((c) => skipCodeLabel(c, threshold)).filter((x): x is string => !!x);
    if (labels.length) return labels.join("; ");
  }
  // Skipped only because of the new-coach pause: say so ("Not a play: paused until game 8 (new coach, 3 played)").
  if (all.includes(COACH_SKIP) || /\bnew\s*coach\b/i.test(text)) {
    return coachPauseText(text) ?? "paused, new coach";
  }
  if (text) {
    const rewritten = rewriteLockWords(text);
    if (rewritten && !isCoachOrLockCopy(rewritten)) return rewritten;
  }
  return null;
}

function skipForMarket(g: Pick<SlateGame, "skip_reason_spread" | "skip_reason_total" | "skip_reasons_spread" | "skip_reasons_total">, market: Market): string | null {
  if (market === "total") return formatSkip(g.skip_reason_total, g.skip_reasons_total, "total");
  return formatSkip(g.skip_reason_spread, g.skip_reasons_spread, market === "moneyline" ? "moneyline" : "spread");
}

/** One-line reason for a non-play headline market, e.g. "Not a play: edge vs open below 4%". */
export function notAPlayLine(g: SlateGame, officialBets: ValueBet[], market?: Market | null): string | null {
  const h = cardHeadline(g, officialBets);
  const m = market ?? h.market;
  if (!m || h.verdict === "official") return null;
  if (market && (isOfficialMarket(g, officialBets, market) || officialBetFor(officialBets, market))) return null;
  const reason = skipForMarket(g, m);
  return reason ? `Not a play: ${reason}` : null;
}

/** Reasons for skipped markets (team page). A coach-only skip reads "Not a play: paused until game N (…)". */
export function notAPlayLines(g: SlateGame, officialMarkets: Iterable<string>): string[] {
  const mk = new Set(officialMarkets);
  const out: string[] = [];
  if (!mk.has("spread") && !mk.has("moneyline")) {
    const s = skipForMarket(g, "spread");
    if (s) out.push(`Not a play: ${s}`);
  }
  if (!mk.has("total")) {
    const s = skipForMarket(g, "total");
    if (s) out.push(`Not a play: ${s}`);
  }
  return [...new Set(out)];
}
