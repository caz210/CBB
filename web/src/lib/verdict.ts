/** Card verdicts (one per market), headline numbers, and "not a play" copy.
 *  Reads stored slate_games columns (edge_now_*, verdict_*, skip_reasons_*). Never computes an
 *  edge in the UI. The ladder rule (Toss Up / Lean / Value / Late Value / CZarp Play) lives in
 *  lib/verdicts.ts and is judged on the current line only. If the edge columns are missing/null,
 *  the vs-now line is hidden and the market reads Toss Up unless it has a posted official pick.
 */
import { fmtEdgeVsBar, fmtPlayPct, fmtThresholdPct, type Market } from "./labels";
import type { SlateGame, ValueBet } from "./types";
import { marketPauseText } from "./coach";
import { HOLD_CODES, LATE_VALUE_NOTE, ladderLevel, showsLineMoveNote, VERDICT_RANK, type HoldCode, type VerdictLevel } from "./verdicts";

/** The pipeline's stored verdict_* column (not the customer ladder). */
export type StoredVerdict = "official" | "low_value";

const FINITE = (n: unknown): n is number => typeof n === "number" && Number.isFinite(n);

export function parseVerdict(v: unknown): StoredVerdict | null {
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

export function storedVerdict(g: SlateGame, market: Market): StoredVerdict | null {
  return market === "total" ? parseVerdict(g.verdict_total) : parseVerdict(g.verdict_spread);
}

function marketSkipCodes(g: SlateGame, market: Market): string[] | null | undefined {
  return market === "total" ? g.skip_reasons_total : g.skip_reasons_spread;
}

function officialBetFor(bets: ValueBet[], market: Market): ValueBet | undefined {
  if (market === "total") return bets.find((b) => b.market === "total");
  return bets.find((b) => b.market === "spread" || b.market === "moneyline");
}

function isOfficialMarket(g: SlateGame, officialBets: ValueBet[], market: Market): boolean {
  if (officialBetFor(officialBetsOnly(officialBets), market)) return true;
  return storedVerdict(g, market) === "official";
}

/** Ladder level for one market of a game, from stored values only. */
export function marketVerdict(g: SlateGame, officialBets: ValueBet[], market: Market): { level: VerdictLevel; holds: HoldCode[] } {
  return ladderLevel({
    market,
    official: isOfficialMarket(g, officialBets, market),
    edgeNow: marketEdgeNow(g, market),
    skipCodes: marketSkipCodes(g, market),
  });
}

/** One market's rating for the card footer, sheet Rating column, and team page. */
export type MarketRating = {
  market: "spread" | "total";
  level: VerdictLevel;
  holds: HoldCode[];
  edgeNow: number | null;
  /** No stored edge for this market (no line yet). */
  noLine: boolean;
  /** CZarp Play whose edge now is below the bar: show LINE_MOVE_NOTE. */
  lineMoveNote: boolean;
};

/** Both ladder ratings for a game: spread (sides) and total. */
export function marketRatings(g: SlateGame, officialBets: ValueBet[]): { spread: MarketRating; total: MarketRating } {
  const one = (market: "spread" | "total"): MarketRating => {
    const v = marketVerdict(g, officialBets, market);
    const edgeNow = marketEdgeNow(g, market);
    return {
      market,
      level: v.level,
      holds: v.holds,
      edgeNow,
      noLine: edgeNow == null && v.level !== "official",
      lineMoveNote: showsLineMoveNote(market, v.level === "official", edgeNow),
    };
  };
  return { spread: one("spread"), total: one("total") };
}

export type CardHeadline = {
  market: Market | null;
  verdict: VerdictLevel;
  /** Hold codes behind a Value that is past the bar; empty otherwise. */
  holds: HoldCode[];
  /** Posted edge for official plays; stored edge_now otherwise. Null if we don't have it. */
  edgePct: number | null;
  /** edgePct formatted for display; below the bar it is floored so it never reads as meeting it. */
  edgeLabel: string | null;
  /** No stored edge for either market (no line yet). */
  noLine: boolean;
  officialBet: ValueBet | null;
  edgeNow: number | null;
  edgeVsOpen: number | null;
  /** "vs now 4.5% (needs 4%)" or "Posted 4.2% · vs now 3.1%". Hidden when stored edges are missing. */
  subtitle: string | null;
  /** CZarp Play whose edge now is below the bar: show LINE_MOVE_NOTE under the headline. */
  lineMoveNote: boolean;
};

function officialBetsOnly(bets: ValueBet[]): ValueBet[] {
  return bets.filter((b) => b.pick_tier !== "track");
}

/** Headline market: an official market if any (Official wins ties); else the market highest on the
 *  ladder, ties broken by the higher stored edge_now. */
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
      ? `Posted ${postedLabel} · vs now ${fmtEdgeVsBar(edgeNow, market)}`
      : null;
    return {
      market,
      verdict: "official",
      holds: [],
      edgePct: FINITE(posted) ? posted : null,
      edgeLabel: postedLabel,
      noLine: false,
      officialBet,
      edgeNow,
      edgeVsOpen,
      subtitle,
      lineMoveNote: showsLineMoveNote(market, true, edgeNow),
    };
  }

  // No posted pick: rank both markets on the ladder (current line only). A market counts once it
  // has a stored edge now.
  const spreadNow = marketEdgeNow(g, "spread");
  const totalNow = marketEdgeNow(g, "total");
  const has = (m: Market) => marketEdgeNow(g, m) != null;
  let market: Market | null = null;
  if (has("spread") || has("total")) {
    const rs = has("spread") ? VERDICT_RANK[marketVerdict(g, [], "spread").level] : -1;
    const rt = has("total") ? VERDICT_RANK[marketVerdict(g, [], "total").level] : -1;
    market = rs !== rt ? (rs > rt ? "spread" : "total") : (spreadNow ?? -1) >= (totalNow ?? -1) ? "spread" : "total";
  }

  const v = market ? marketVerdict(g, [], market) : { level: "toss_up" as const, holds: [] };
  const edgeNow = market ? marketEdgeNow(g, market) : null;
  const edgeVsOpen = market ? marketEdgeVsOpen(g, market) : null;
  return {
    market,
    verdict: v.level,
    holds: v.holds,
    edgePct: edgeNow,
    edgeLabel: market && edgeNow != null ? fmtEdgeVsBar(edgeNow, market) : null,
    noLine: market == null,
    officialBet: null,
    edgeNow,
    edgeVsOpen,
    subtitle: notPostedSubtitle(market, edgeNow),
    lineMoveNote: false,
  };
}

function notPostedSubtitle(market: Market | null, edgeNow: number | null): string | null {
  if (!market || edgeNow == null) return null;
  return `vs now ${fmtEdgeVsBar(edgeNow, market)} (needs ${fmtThresholdPct(market)})`;
}

const COACH_SKIP = "new_coach_lock";
const LINE_MOVED_LABEL: string = HOLD_CODES.line_moved_against;

/** Label for a known skip code; null for the coach code and for codes we don't know. */
export function knownSkipCodeLabel(code: string, thresholdPct: string): string | null {
  switch (code) {
    case "below_open_threshold": return `edge vs open below ${thresholdPct}`;
    case "line_moved_against": return LINE_MOVED_LABEL;
    case "no_opening_line": return "no opening line yet";
    case "no_current_line": return "no current line";
    case "game_started": return "game already started";
    case "kelly_too_small": return HOLD_CODES.kelly_too_small;
    default: return null;
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

export type SkipDetail = {
  /** The one line the card shows after "Not a play: ". With a new-coach pause, the pause leads. */
  primary: string | null;
  /** Other reasons, shown in the game sheet as "Also: …" (only when the pause is primary). */
  also: string[];
};

/** Raw snake_case code stored as text ("some_new_code", "feed vendor_feed_down"). */
const SNAKE_TOKEN = /\b[a-z0-9]+(?:_[a-z0-9]+)+\b/i;
const NOT_A_PLAY_PREFIX = /^\s*not\s+a\s+play\s*:\s*/i;

/** Pipeline text split into its "; " parts:
 *  - a leading "Not a play:" is stripped (the UI adds its own prefix);
 *  - parts with a raw snake_case code are dropped, and so are opening-line parts ("edge vs open
 *    below 4%"): the ladder is judged on the current line;
 *  - old "moved … against" text maps to the new line-move wording; any other "against" wording
 *    ("market went against us") is dropped, so "against us/our side" never shows;
 *  - lock words are rewritten, then any part that is still coach/lock copy is dropped. */
function textReasons(text: string): string[] {
  return text
    .split(/\s*;\s*/)
    .map((x) => x.replace(NOT_A_PLAY_PREFIX, "").trim())
    .filter((x) => x && !SNAKE_TOKEN.test(x) && !/\bvs\.?\s+open\b/i.test(x))
    .map((x) => (/\bmoved\b.*\bagainst\b/i.test(x) ? LINE_MOVED_LABEL : /\bagainst\b/i.test(x) ? "" : rewriteLockWords(x)))
    .filter((x) => x && !isCoachOrLockCopy(x));
}

/** Known code labels win over the pipeline's human text. The stored text is the fallback when there
 *  are no codes (older rows) or none of the codes has a known label; see textReasons for how that
 *  text is cleaned. Nothing left → no line. Exported for tests. */
export function formatSkip(
  human: string | null | undefined,
  codes: string[] | null | undefined,
  market: Market,
  locks?: SlateGame["coachLocks"],
): SkipDetail {
  const threshold = fmtThresholdPct(market);
  const all = codes ?? [];
  const text = human?.trim() ?? "";
  const coach = all.includes(COACH_SKIP) || /\bnew\s*coach\b/i.test(text);
  const known = all
    .filter((c) => c !== COACH_SKIP)
    .map((c) => knownSkipCodeLabel(c, threshold))
    .filter((x): x is string => !!x);
  const others = known.length ? known : textReasons(text);
  const uniq = [...new Set(others)];
  if (coach) {
    // Pause leads: built from the team's lock data when we have it, else the stored text.
    const pause = marketPauseText(locks, market === "total" ? "total" : "spread") ?? coachPauseText(text) ?? "paused, new coach";
    return { primary: pause, also: uniq };
  }
  return { primary: uniq.length ? uniq.join("; ") : null, also: [] };
}

/** Threshold codes: the ladder re-derives these from edge now, so the stored ones are not shown
 *  (below_open_threshold is about the opening line, which the ladder ignores). */
const BAR_CODES = new Set(["below_open_threshold", "below_current_threshold"]);

/** Customer reason line for a non-official market, matching its ladder level:
 *  - Late Value: LATE_VALUE_NOTE, shown as-is (any other
 *    stored reason, e.g. "game already started", still shows).
 *  - Value past the bar: the hold rule(s) as the note (new-coach pause leads).
 *  - Below the bar (Toss Up, Lean, Value): no bar reason (the chip and the edge say it); only other
 *    stored reasons (e.g. stake under $1, game already started), or the stored text with
 *    opening-line wording removed. */
function skipForMarket(g: SlateGame, officialBets: ValueBet[], market: Market): SkipDetail {
  const m: Market = market === "moneyline" ? "spread" : market;
  const v = marketVerdict(g, officialBets, m);
  if (v.level === "official") return { primary: null, also: [] };
  const stored = m === "total" ? g.skip_reasons_total : g.skip_reasons_spread;
  const codes = (stored ?? []).filter((c) => !BAR_CODES.has(c));
  // A structured row whose remaining codes all have labels doesn't need the stored text (which can
  // still carry opening-line or stale wording). Unknown codes and the coach pause still read it.
  const structured = !!stored?.length && codes.every((c) => c !== COACH_SKIP && knownSkipCodeLabel(c, "") != null);
  const human = structured ? null : m === "total" ? g.skip_reason_total : g.skip_reason_spread;
  const d = formatSkip(human, codes, market === "moneyline" ? "moneyline" : m, g.coachLocks);
  // Late Value: nothing on the ladder held it back. Any other stored reason (game started, a new
  // pipeline rule) still shows; otherwise it's value that came after the 7:15 AM CT run.
  if (v.level === "late_value" && !d.primary) return { primary: LATE_VALUE_NOTE, also: [] };
  return d;
}

/** "Not a play: <reason>", except the Late Value note (lib/verdicts.ts), which stands on its own. */
const reasonLine = (primary: string) => (primary === LATE_VALUE_NOTE ? LATE_VALUE_NOTE : `Not a play: ${primary}`);

/** Skip detail for the headline (or given) non-play market; null for official plays. */
export function notAPlayDetail(g: SlateGame, officialBets: ValueBet[], market?: Market | null): SkipDetail | null {
  const h = cardHeadline(g, officialBets);
  const m = market ?? h.market;
  if (!m || h.verdict === "official") return null;
  if (market && (isOfficialMarket(g, officialBets, market) || officialBetFor(officialBets, market))) return null;
  return skipForMarket(g, officialBets, m);
}

/** One-line reason for a non-play headline market, e.g. "Not a play: edge vs open below 4%"
 *  or, with a new-coach pause, "Not a play: paused until game 4 (new coach, 3 played)". */
export function notAPlayLine(g: SlateGame, officialBets: ValueBet[], market?: Market | null): string | null {
  const d = notAPlayDetail(g, officialBets, market);
  return d?.primary ? reasonLine(d.primary) : null;
}

/** "Also: …" lines for the game sheet (reasons behind a new-coach pause). */
export function notAPlayAlso(g: SlateGame, officialBets: ValueBet[], market?: Market | null): string[] {
  const d = notAPlayDetail(g, officialBets, market);
  return d?.primary ? d.also.map((r) => `Also: ${r}`) : [];
}

/** Reasons for skipped markets (team page). A coach skip reads "Not a play: paused until game N (…)",
 *  followed by "Also: …" lines for any other reasons. */
export function notAPlayLines(g: SlateGame, officialMarkets: Iterable<string>): string[] {
  const mk = new Set(officialMarkets);
  const out: string[] = [];
  const push = (d: SkipDetail) => {
    if (!d.primary) return;
    out.push(reasonLine(d.primary));
    for (const r of d.also) out.push(`Also: ${r}`);
  };
  if (!mk.has("spread") && !mk.has("moneyline")) push(skipForMarket(g, [], "spread"));
  if (!mk.has("total")) push(skipForMarket(g, [], "total"));
  return [...new Set(out)];
}
