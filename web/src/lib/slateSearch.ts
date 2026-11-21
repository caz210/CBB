// Filter the daily slate by a team-name query. Matching is the same as the header team
// search (case, punctuation, St./State/Saint, localAliases, plus team_aliases when the
// cached /api/teams index is available), applied to either side of a game. The UI never
// computes projections; it only hides or shows cards.
import { parseSlateSort } from "./tvRating";
import { buildIndex, normalize, searchTeams, type SearchTeam, type TeamIndexEntry } from "./teamSearch";

export type NamedSides = { home: string; away: string };

/** Canonical team name → extra spellings from /api/teams (already normalised in that payload). */
export type AliasByName = ReadonlyMap<string, readonly string[]>;

export function aliasByNameFromIndex(index: TeamIndexEntry[]): AliasByName {
  const m = new Map<string, string[]>();
  for (const e of index) {
    if (e.team.aliases?.length) m.set(e.team.name, e.team.aliases);
  }
  return m;
}

function asTeam(name: string, id: number, aliases?: readonly string[]): SearchTeam {
  return aliases ? { id, name, slug: "", next: null, aliases: [...aliases] } : { id, name, slug: "", next: null };
}

/** True when the query is empty (or only punctuation/spaces) and every game should stay visible. */
export function isEmptySlateQuery(query: string): boolean {
  return !normalize(query);
}

/** True when `name` matches the typed query (aliases and St./State included). An empty query matches. */
export function teamMatchesQuery(name: string, query: string, aliases?: AliasByName): boolean {
  if (isEmptySlateQuery(query)) return true;
  return searchTeams(buildIndex([asTeam(name, 1, aliases?.get(name))]), query, 1).length > 0;
}

/** True when either team on the game matches the query. An empty query matches every game. */
export function slateGameMatches(game: NamedSides, query: string, aliases?: AliasByName): boolean {
  if (isEmptySlateQuery(query)) return true;
  return searchTeams(buildIndex([
    asTeam(game.away, 1, aliases?.get(game.away)),
    asTeam(game.home, 2, aliases?.get(game.home)),
  ]), query, 2).length > 0;
}

/** Games whose home or away team matches the query. An empty query returns the same list. */
export function filterSlateGames<T extends NamedSides>(games: T[], query: string, aliases?: AliasByName): T[] {
  if (isEmptySlateQuery(query)) return games;
  return games.filter((g) => slateGameMatches(g, query, aliases));
}

/** How long to wait on /api/teams before giving up on DB aliases and showing the real empty state. */
export const ALIAS_INDEX_TIMEOUT_MS = 3000;

/** Server-sent slate aliases (SlateData.aliases) as an AliasByName. null/undefined → undefined. */
export function aliasByNameFromRecord(rec: Record<string, string[]> | null | undefined): AliasByName | undefined {
  return rec ? new Map(Object.entries(rec)) : undefined;
}

/** True only for a possible alias-only lookup: no local matches, the slate's own aliases didn't come with the
 *  page (slateAliasesKnown false), and the /api/teams index is still loading. When the server already sent the
 *  slate teams' aliases, a no-match is a real no-match, so the empty state shows straight away.
 *  False on error, timeout, or a hit. */
export function isSlateAliasSearchPending(opts: {
  query: string;
  matchCount: number;
  load: "loading" | "ready" | "error";
  timedOut: boolean;
  slateAliasesKnown?: boolean;
}): boolean {
  if (opts.slateAliasesKnown) return false;
  return !isEmptySlateQuery(opts.query) && opts.matchCount === 0 && opts.load === "loading" && !opts.timedOut;
}

/** Quoted query in the empty state. Long unbroken strings are cut so they cannot blow out the layout. */
export const SLATE_QUERY_ECHO_MAX = 40;

export function echoSlateQuery(query: string, max = SLATE_QUERY_ECHO_MAX): string {
  const t = query.trim();
  if (t.length <= max) return t;
  return `${t.slice(0, max)}…`;
}

/**
 * Date-nav href from the *live* URL (after replaceState), not the searchParams the
 * server saw when it first rendered the page. Typing updates ?q= on the client only;
 * Prev/Next/Today must read that live state or they drop or stale the query.
 */
export function slateDateHref(opts: {
  sportKey: string;
  date: string;
  today: string;
  search: string | { get(name: string): string | null };
}): string {
  const sp = typeof opts.search === "string"
    ? new URLSearchParams(opts.search.startsWith("?") ? opts.search.slice(1) : opts.search)
    : opts.search;
  const q = new URLSearchParams();
  if (opts.date !== opts.today) q.set("date", opts.date);
  const sort = parseSlateSort(sp.get("sort"));
  if (sort !== "time") q.set("sort", sort);
  if (sp.get("hide") === "1") q.set("hide", "1");
  const query = sp.get("q");
  if (query) q.set("q", query);
  const qs = q.toString();
  return `/${opts.sportKey}/slate${qs ? `?${qs}` : ""}`;
}
