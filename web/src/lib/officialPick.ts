/** Official-pick destination helpers: game-sheet deep links, ledger record merge, and the
 *  Official Pick banner view. The UI never computes projections or edges; it only formats
 *  stored official-record and slate fields. Customers see official picks only. */

import type { LedgerRow, SlateGame, ValueBet } from "./types";
import { fmtClv, fmtEdgeVsBar, fmtOne, fmtPlayPct, fmtSpread, fmtTipoff, fmtUnits, MARKET_LABEL, type Market } from "./labels";
import { marketRatings } from "./verdict";
import { currentLineForBet } from "./posted";

/** Link to the slate with that game's sheet open: /cbb/slate?date=YYYY-MM-DD&game=123 */
export function gameSheetHref(sportKey: string, date: string, gameId: number): string {
  return `/${sportKey}/slate?date=${date}&game=${gameId}`;
}

export type OfficialRecord = {
  game_id: number;
  pick?: string | null;
  side?: string | null;
  stake_units: number | null;
  result: "W" | "L" | "P" | null;
  profit_units: number | null;
  clv: number | null;
  logged_at?: string | null;
  pick_hash?: string | null;
};

export function ledgerToOfficialRecord(r: Pick<LedgerRow, "game_id" | "pick" | "stake_units" | "result" | "profit_units" | "clv" | "logged_at" | "pick_hash" | "pick_tier">): OfficialRecord | null {
  if (r.pick_tier && r.pick_tier !== "official") return null;
  return {
    game_id: r.game_id,
    pick: r.pick ?? null,
    stake_units: r.stake_units,
    result: r.result,
    profit_units: r.profit_units,
    clv: r.clv ?? null,
    logged_at: r.logged_at ?? null,
    pick_hash: r.pick_hash ?? null,
  };
}

function recordMatches(b: ValueBet, r: OfficialRecord): boolean {
  if (r.pick_hash && b.pick_hash && r.pick_hash === b.pick_hash) return true;
  if (b.game_id !== r.game_id) return false;
  const pick = (r.pick ?? "").trim();
  const side = (r.side ?? "").trim();
  const hit = (token: string) => !!token && (b.side === token || b.side.startsWith(token + " ") || token.startsWith(b.side + " "));
  return hit(pick) || hit(side);
}

/** Attach stake / result / P/L / CLV from the official ledger onto official ValueBets.
 *  Tracked rows are ignored. Does not invent numbers; unmatched bets are left as they were. */
export function applyOfficialRecord(bets: ValueBet[], rows: OfficialRecord[]): ValueBet[] {
  const officialRows = rows.filter((r) => r.game_id != null);
  if (!officialRows.length) return bets;
  return bets.map((b) => {
    if ((b.pick_tier ?? "official") !== "official") return b;
    const hit = officialRows.find((r) => recordMatches(b, r));
    if (!hit) return b;
    return {
      ...b,
      stake_units: hit.stake_units,
      result: hit.result,
      profit_units: hit.profit_units,
      clv: hit.clv,
      logged_at: b.logged_at ?? hit.logged_at ?? null,
      posted_at: b.posted_at ?? hit.logged_at ?? null,
    };
  });
}

export function officialBetsOf(bets: ValueBet[]): ValueBet[] {
  return bets.filter((b) => (b.pick_tier ?? "official") === "official");
}

export function stakeUnitsLabel(units: number | null | undefined): string | null {
  if (units == null || !Number.isFinite(units)) return null;
  return `${units.toFixed(2)}u`;
}

/** Short, neutral, pre-grade line. No dollars, no promise of a win. */
export const OFFICIAL_PENDING_NOTE = "Time-stamped. Graded in public after the game.";

export type OfficialPickView = {
  side: string;
  market: Market;
  marketLabel: string;
  edgeLabel: string;
  belowBar: boolean;
  stakeLabel: string | null;
  postedLineLabel: string | null;
  postedAtLabel: string | null;
  fairLabel: string | null;
  priceLabel: string | null;
  lineMoveNote: boolean;
  openNowContext: string | null;
  result: "W" | "L" | "P" | null;
  resultLabel: string | null;
  profitLabel: string | null;
  clvLabel: string | null;
  pendingNote: string | null;
};

function fmtMarketLine(market: Market, n: number): string {
  return market === "total" ? fmtOne(n) : fmtSpread(n);
}

function finite(n: unknown): n is number {
  return typeof n === "number" && Number.isFinite(n);
}

/** One banner block per official bet on the game. Tracked picks are omitted. */
export function officialPickViews(g: SlateGame, bets: ValueBet[]): OfficialPickView[] {
  const official = officialBetsOf(bets);
  if (!official.length) return [];
  const ratings = marketRatings(g, official);
  return official.map((b) => {
    const market: Market = b.market === "moneyline" ? "spread" : b.market;
    const rating = market === "total" ? ratings.total : ratings.spread;
    const edgeNow = rating.edgeNow ?? b.edge_at_post_pct ?? (finite(b.line_edge_pct) ? b.line_edge_pct : null);
    const belowBar = rating.belowBar === true;
    const edgeLabel = finite(edgeNow)
      ? (belowBar ? fmtEdgeVsBar(edgeNow, market) : fmtPlayPct(edgeNow))
      : "—";
    const postedLine = finite(b.line_at_post) ? b.line_at_post : (finite(b.posted_line) ? b.posted_line : null);
    const fair = finite(b.fair_at_post) ? b.fair_at_post : null;
    const price = currentLineForBet(g, b.market, b.side);
    const priceN = finite(price) ? price : postedLine;
    const postedAt = b.posted_at ?? b.logged_at ?? null;
    const result = b.result ?? null;
    return {
      side: b.side,
      market: b.market,
      marketLabel: MARKET_LABEL[b.market],
      edgeLabel,
      belowBar,
      stakeLabel: stakeUnitsLabel(b.stake_units),
      postedLineLabel: finite(postedLine) ? fmtMarketLine(market, postedLine) : null,
      postedAtLabel: postedAt ? fmtTipoff(postedAt) : null,
      fairLabel: finite(fair) ? fmtMarketLine(market, fair) : null,
      priceLabel: finite(priceN) ? fmtMarketLine(market, priceN) : null,
      lineMoveNote: rating.lineMoveNote === true,
      openNowContext: rating.openNowContext,
      result,
      resultLabel: result === "W" ? "W" : result === "L" ? "L" : result === "P" ? "P" : null,
      profitLabel: result != null && finite(b.profit_units) ? fmtUnits(b.profit_units) : null,
      clvLabel: result != null && finite(b.clv) ? fmtClv(b.clv) : null,
      pendingNote: result == null ? OFFICIAL_PENDING_NOTE : null,
    };
  });
}
