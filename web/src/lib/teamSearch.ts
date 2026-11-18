// Team search: normalisation, alias handling and ranking. Pure functions (no React, no network)
// so they are unit-tested in tests/teamSearch.test.ts.
import { localAliases, type AliasLookup } from "./teamAliases";

export type TeamNextGame = {
  gameId: number;
  /** YYYY-MM-DD, America/Chicago local date */
  date: string;
  opponent: string;
  home: boolean;
  tipoff: string | null;
};

export type SearchTeam = {
  id: number;
  name: string;
  slug: string;
  next: TeamNextGame | null;
};

export type TeamIndexEntry = {
  team: SearchTeam;
  /** normalised canonical name, e.g. "michigan st" */
  name: string;
  /** every normalised full-string spelling to match against (name, "st" -> "state"/"saint", aliases) */
  forms: string[];
  /** word tokens of all forms, for word-start matching */
  tokens: string[];
  /** normalised aliases */
  aliases: string[];
};

export const MAX_RESULTS = 8;

/** Lowercase, drop punctuation, "A&M" -> "am", "&"/"and" removed, collapse spaces. */
export function normalize(s: string): string {
  return s
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/\b([a-z])\s*&\s*([a-z])\b/g, "$1$2")
    .replace(/&/g, " ")
    .replace(/[.'’`]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .split(" ")
    .filter((t) => t && t !== "and")
    .join(" ");
}

const ST_WORDS = ["state", "saint"];

/** All spellings of a normalised string where a standalone "st" may mean "state" or "saint". */
export function expandSt(n: string): string[] {
  const toks = n.split(" ").filter(Boolean);
  if (!toks.includes("st")) return [n];
  const out = new Set<string>([n]);
  // Names have at most a couple of "st" tokens; expand each independently.
  let acc: string[][] = [[]];
  for (const t of toks) {
    const opts = t === "st" ? ["st", ...ST_WORDS] : [t];
    acc = acc.flatMap((p) => opts.map((o) => [...p, o]));
  }
  for (const a of acc) out.add(a.join(" "));
  return [...out];
}

export function buildIndex(teams: SearchTeam[], aliasesFor: AliasLookup = localAliases): TeamIndexEntry[] {
  return teams.map((team) => {
    const name = normalize(team.name);
    const aliases = aliasesFor(team).map(normalize).filter(Boolean);
    const forms = [...new Set([name, ...aliases].flatMap(expandSt))];
    const tokens = [...new Set(forms.flatMap((f) => f.split(" ")))];
    return { team, name, forms, tokens, aliases };
  });
}

/** Score for one entry (lower is better), or null when it doesn't match. */
function score(e: TeamIndexEntry, qForms: string[], qTokens: string[]): number | null {
  let best: number | null = null;
  const take = (n: number) => { if (best == null || n < best) best = n; };
  for (const q of qForms) {
    if (e.forms.includes(q)) take(0);
    else if (e.forms.some((f) => f.startsWith(q))) take(1);
  }
  if (best != null) return best;
  // every typed word starts a word of the team ("mich st" -> "michigan st")
  const wordsOk = qTokens.every((t) => e.tokens.some((w) => w.startsWith(t) || (t === "st" && ST_WORDS.some((s) => w === s))));
  if (wordsOk) take(2);
  // plain substring, spaces ignored ("ichigan", "texasst")
  const squashed = qForms.map((q) => q.replace(/ /g, ""));
  if (e.forms.some((f) => squashed.some((q) => f.replace(/ /g, "").includes(q)))) take(3);
  return best;
}

export function searchTeams(index: TeamIndexEntry[], query: string, limit = MAX_RESULTS): SearchTeam[] {
  const q = normalize(query);
  if (!q) return [];
  const qForms = expandSt(q);
  const qTokens = q.split(" ");
  const hits: { e: TeamIndexEntry; s: number }[] = [];
  for (const e of index) {
    const s = score(e, qForms, qTokens);
    if (s != null) hits.push({ e, s });
  }
  hits.sort((a, z) =>
    a.s - z.s
    // within a rank, teams playing soon come first, then shorter (closer) names, then A-Z
    || Number(!!z.e.team.next) - Number(!!a.e.team.next)
    || a.e.name.length - z.e.name.length
    || a.e.team.name.localeCompare(z.e.team.name));
  return hits.slice(0, limit).map((h) => h.e.team);
}

/** Link to the slate with a game sheet open: /cbb/slate?date=YYYY-MM-DD&game=123 */
export function nextGameHref(sportKey: string, g: TeamNextGame): string {
  return `/${sportKey}/slate?date=${g.date}&game=${g.gameId}`;
}
