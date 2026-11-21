// Filter the daily slate by a team-name query. Matching is the same as the header team
// search (case, punctuation, St./State/Saint, aliases via localAliases), applied to
// either side of a game. The UI never computes projections; it only hides or shows cards.
import { buildIndex, normalize, searchTeams, type SearchTeam } from "./teamSearch";

export type NamedSides = { home: string; away: string };

function asTeam(name: string, id: number): SearchTeam {
  return { id, name, slug: "", next: null };
}

/** True when the query is empty (or only punctuation/spaces) and every game should stay visible. */
export function isEmptySlateQuery(query: string): boolean {
  return !normalize(query);
}

/** True when `name` matches the typed query (aliases and St./State included). An empty query matches. */
export function teamMatchesQuery(name: string, query: string): boolean {
  if (isEmptySlateQuery(query)) return true;
  return searchTeams(buildIndex([asTeam(name, 1)]), query, 1).length > 0;
}

/** True when either team on the game matches the query. An empty query matches every game. */
export function slateGameMatches(game: NamedSides, query: string): boolean {
  if (isEmptySlateQuery(query)) return true;
  return searchTeams(buildIndex([asTeam(game.away, 1), asTeam(game.home, 2)]), query, 2).length > 0;
}

/** Games whose home or away team matches the query. An empty query returns the same list. */
export function filterSlateGames<T extends NamedSides>(games: T[], query: string): T[] {
  if (isEmptySlateQuery(query)) return games;
  return games.filter((g) => slateGameMatches(g, query));
}
