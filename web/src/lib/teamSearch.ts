// Team search: normalisation, alias handling and ranking. Pure functions (no React, no network)
// so they are unit-tested in tests/teamSearch.test.ts.
import { localAliases, type AliasLookup } from "./teamAliases";

export type TeamNextGame = {
  gameId: number;
  /** YYYY-MM-DD, America/Chicago local date */
  date: string;
  opponent: string;
  home: boolean;
  /** games.neutral: no home team, so the label reads "vs" and carries a Neutral tag */
  neutral?: boolean;
  tipoff: string | null;
};

export type SearchTeam = {
  id: number;
  name: string;
  slug: string;
  next: TeamNextGame | null;
  /** extra search spellings from the `team_aliases` table (already normalised); absent when that query failed */
  aliases?: string[];
};

export type TeamIndexEntry = {
  team: SearchTeam;
  /** normalised canonical name, e.g. "michigan st" */
  name: string;
  /** normalised spellings of the canonical name ("st" -> "state"/"saint" where it makes sense) */
  nameForms: string[];
  /** normalised spellings of the aliases */
  aliasForms: string[];
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

/** "st" is ambiguous: leading ("St. John's") means Saint, trailing ("Texas St.") means State, and in the
 *  middle ("Cal St. Fullerton", "Mount St. Mary's") either. State and Saint are NOT interchangeable
 *  with each other: "state" never finds Saint schools and "saint" never finds X St. schools. */
const STATE = "state";
const SAINT = "saint";

/** Spellings of a normalised string where a standalone "st" may be written out as state/saint. */
export function expandSt(n: string): string[] {
  const toks = n.split(" ").filter(Boolean);
  if (!toks.includes("st")) return [n];
  let acc: string[][] = [[]];
  toks.forEach((t, i) => {
    let opts = [t];
    if (t === "st") {
      const lead = i === 0, trail = i === toks.length - 1;
      opts = ["st", ...(lead && !trail ? [SAINT] : trail && !lead ? [STATE] : [STATE, SAINT])];
    }
    acc = acc.flatMap((p) => opts.map((o) => [...p, o]));
  });
  return [...new Set(acc.map((a) => a.join(" ")))];
}

/** DB aliases (team_aliases, loaded by /api/teams) plus the local map. The union means a failed or partial
 *  alias query only costs the DB extras; the local map keeps working either way. */
export const teamAliases: AliasLookup = (team) => [...((team as SearchTeam).aliases ?? []), ...localAliases(team)];

export function buildIndex(teams: SearchTeam[], aliasesFor: AliasLookup = teamAliases): TeamIndexEntry[] {
  return teams.map((team) => {
    const name = normalize(team.name);
    const nameForms = expandSt(name);
    const aliasForms = [...new Set(aliasesFor(team).map(normalize).filter((a) => a && a !== name).flatMap(expandSt))];
    return { team, name, nameForms, aliasForms };
  });
}

/** Ranks, lower is better:
 *  0 exact · 1 prefix · 2 words in order · 3 substring · 4 words in any order (names only).
 *  Prefix and in-order word matches (1 and 2) that only come from an alias sit half a rank lower, so a plain
 *  "state" lists every X St. team before LSU ("Louisiana State") and "saint" lists the Saint schools before
 *  Siena ("Saints"). Exact alias hits (unc, uconn) keep full rank. */
function scoreForms(forms: string[], qForms: string[], qTokens: string[], anyOrder: boolean): { r: number; word: boolean } | null {
  let best: { r: number; word: boolean } | null = null;
  const take = (r: number, word = false) => { if (!best || r < best.r) best = { r, word }; };
  for (const q of qForms) {
    if (forms.includes(q)) take(0);
    else if (forms.some((f) => f.startsWith(q))) take(1);
  }
  if (best && (best as { r: number }).r <= 1) return best;
  // A typed "st" only stands for st/state/saint, not every word starting with "st".
  const wordHit = (t: string, w: string) => (t === "st" ? w === "st" || w === STATE || w === SAINT : w.startsWith(t));
  // every typed word starts a word of one spelling, in the same order ("mich st" -> "michigan st", but
  // "st louis" must not match "louisiana state")
  if (forms.some((f) => {
    const words = f.split(" ");
    let from = 0;
    return qTokens.every((t) => {
      const i = words.findIndex((w, k) => k >= from && wordHit(t, w));
      if (i < 0) return false;
      from = i + 1;
      return true;
    });
  })) take(2, true);
  // plain substring, spaces ignored ("ichigan", "texasst")
  const squashed = qForms.map((q) => q.replace(/ /g, ""));
  if (forms.some((f) => squashed.some((q) => f.replace(/ /g, "").includes(q)))) take(3);
  // same words, any order ("carolina north"), ranked last. Real names only: letting aliases match this loosely
  // made "st louis" list LSU ("Louisiana State").
  if (anyOrder && forms.some((f) => {
    const words = f.split(" ");
    const used = new Set<number>();
    return qTokens.every((t) => {
      const i = words.findIndex((w, k) => !used.has(k) && wordHit(t, w));
      if (i < 0) return false;
      used.add(i);
      return true;
    });
  })) take(4, true);
  return best;
}

/** Score for one entry (lower is better), or null when it doesn't match. */
function score(e: TeamIndexEntry, qForms: string[], qTokens: string[]): number | null {
  const n = scoreForms(e.nameForms, qForms, qTokens, true);
  const a = e.aliasForms.length ? scoreForms(e.aliasForms, qForms, qTokens, false) : null;
  const sa = a ? a.r + (a.word || a.r === 1 ? 0.5 : 0) : null;
  if (!n) return sa;
  return sa == null ? n.r : Math.min(n.r, sa);
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

// ---- next-game freshness ----------------------------------------------------
// `games.status` only flips to final in the morning pipeline run, so a game that has clearly tipped and
// finished still says "scheduled". Treat a game as over ~3h after tip (CBB games run ~2h). Used by /api/teams and again in the browser, so a cached list can't go stale.
export const GAME_WINDOW_MS = 3 * 60 * 60 * 1000;

/** YYYY-MM-DD in America/Chicago (game dates are stored as CT local dates). */
export function chicagoDate(now = new Date()): string {
  return new Intl.DateTimeFormat("en-CA", { timeZone: "America/Chicago", year: "numeric", month: "2-digit", day: "2-digit" }).format(now);
}

/** True when a game can still be worth linking to. With a valid tip time the only rule is "not tipped more than
 *  ~3h ago", so a 10:59 PM CT game keeps its button after midnight while it's still being played. Without a
 *  tip time, fall back to the date: today or later (CT). */
export function isUpcoming(g: { date: string; tipoff: string | null }, now = new Date()): boolean {
  const t = g.tipoff ? Date.parse(g.tipoff) : NaN;
  if (Number.isFinite(t)) return t + GAME_WINDOW_MS >= now.getTime();
  return g.date >= chicagoDate(now);
}

/** Label for the game button: "Today's game" when it's today (CT) or still being played after midnight. */
export function nextGameLabel(g: { date: string }, now = new Date()): string {
  return g.date <= chicagoDate(now) ? "Today's game" : "Next game";
}

/** Same teams with any no-longer-upcoming `next` removed. */
export function withFreshNext(teams: SearchTeam[], now = new Date()): SearchTeam[] {
  return teams.map((t) => (t.next && !isUpcoming(t.next, now) ? { ...t, next: null } : t));
}
