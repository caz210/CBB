/** "As of" stamp for the line a game's ladder ratings use. Only REAL stored timestamps; never a
 *  made-up or hard-coded time. Sources, first readable one wins:
 *    1. projections.edges_as_of   when the pipeline computed edge_now (czarp-v2; may not exist yet)
 *    2. slate_games.market_fetched_at  when the line on the card was fetched (may not exist yet)
 *    3. newest market_lines.fetched_at for source='vegas', line_type='current' (the row slate_games
 *       shows, readable with anon today)
 *  Nothing readable → null, and the UI omits the "as of" text. */
import { fmtUpdated } from "./labels";

export type LinesAsOfSource = "edges_as_of" | "market_fetched_at" | "market_lines";

export type LinesAsOf = { at: string; source: LinesAsOfSource };

const valid = (v: unknown): v is string => typeof v === "string" && Number.isFinite(Date.parse(v));

export function pickLinesAsOf(x: { edgesAsOf?: unknown; marketFetchedAt?: unknown; marketLinesFetchedAt?: unknown }): LinesAsOf | null {
  if (valid(x.edgesAsOf)) return { at: x.edgesAsOf, source: "edges_as_of" };
  if (valid(x.marketFetchedAt)) return { at: x.marketFetchedAt, source: "market_fetched_at" };
  if (valid(x.marketLinesFetchedAt)) return { at: x.marketLinesFetchedAt, source: "market_lines" };
  return null;
}

/** Newest valid stamp per game from rows like { game_id, fetched_at }. */
export function newestByGame(rows: readonly Record<string, unknown>[], field: string): Map<number, string> {
  const out = new Map<number, string>();
  for (const r of rows) {
    const v = r[field];
    if (!valid(v)) continue;
    const id = Number(r.game_id);
    const cur = out.get(id);
    if (!cur || Date.parse(v) > Date.parse(cur)) out.set(id, v);
  }
  return out;
}

/** "as of 7:13 AM CT" (weekday added when the stamp isn't today in Central time); null if none. */
export function fmtLinesAsOf(at: string | null | undefined, now = new Date()): string | null {
  if (!at) return null;
  const t = fmtUpdated(at, now);
  return t ? `as of ${t}` : null;
}
