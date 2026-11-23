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

/** "st" is ambiguous: a leading "st" ("St. John's", "St. Mary's") means Saint, and so does "st" after
 *  Mount/Mt ("Mount St. Mary's"). Everywhere else ("Texas St.", "Cal St. Fullerton", a mid-alias like
 *  "wichita st shockers") it means State. A bare "st" can be either. State and Saint are NOT
 *  interchangeable: "state" never finds Saint schools and "saint" never finds X St. schools. */
const STATE = "state";
const SAINT = "saint";

/** True when the "st" at `i` should be read as Saint, not State. */
function stMeansSaint(i: number, toks: string[]): boolean {
  if (i === 0) return true;
  const prev = toks[i - 1];
  return prev === "mount" || prev === "mt";
}

/** Spellings of a normalised string where a standalone "st" may be written out as state/saint. */
export function expandSt(n: string): string[] {
  const toks = n.split(" ").filter(Boolean);
  if (!toks.includes("st")) return [n];
  let acc: string[][] = [[]];
  toks.forEach((t, i) => {
    let opts = [t];
    if (t === "st") {
      const lead = i === 0, trail = i === toks.length - 1;
      // A bare "st" can be either. Leading (or Mount/Mt + st) is Saint; other mid/trailing "st" is State.
      opts = lead && trail ? ["st", STATE, SAINT] : stMeansSaint(i, toks) ? ["st", SAINT] : ["st", STATE];
    }
    acc = acc.flatMap((p) => opts.map((o) => [...p, o]));
  });
  return [...new Set(acc.map((a) => a.join(" ")))];
}

/** Forms that wrongly rewrite st/state/saint in this string (State "st" as "saint", or Saint "st" as "state").
 *  Used to drop pre-expanded DB aliases like "cal saint fullerton". */
function spuriousStForms(n: string): Set<string> {
  const toks = n.split(" ").filter(Boolean);
  const out = new Set<string>();
  toks.forEach((t, i) => {
    if (t !== "st" && t !== STATE && t !== SAINT) return;
    const asSaint = stMeansSaint(i, toks);
    const wrong = (t === "st" || t === STATE) && !asSaint ? SAINT
      : (t === "st" || t === SAINT) && asSaint && !(i === 0 && toks.length === 1) ? STATE
      : null;
    if (!wrong) return;
    const copy = toks.slice();
    copy[i] = wrong;
    out.add(copy.join(" "));
  });
  return out;
}

/** DB aliases (team_aliases, loaded by /api/teams) plus the local map. The union means a failed or partial
 *  alias query only costs the DB extras; the local map keeps working either way. */
export const teamAliases: AliasLookup = (team) => [...((team as SearchTeam).aliases ?? []), ...localAliases(team)];

export function buildIndex(teams: SearchTeam[], aliasesFor: AliasLookup = teamAliases): TeamIndexEntry[] {
  return teams.map((team) => {
    const name = normalize(team.name);
    const nameForms = expandSt(name);
    const raw = aliasesFor(team).map(normalize).filter((a) => a && a !== name);
    // The alias table may already contain "cal saint fullerton"-style expansions of a mid "st". Drop those.
    const spurious = spuriousStForms(name);
    for (const a of raw) {
      if (spurious.has(a)) continue;
      for (const f of spuriousStForms(a)) spurious.add(f);
    }
    const aliasForms = [...new Set(raw.flatMap(expandSt).filter((f) => !spurious.has(f)))];
    return { team, name, nameForms, aliasForms };
  });
}

/** Ranks, lower is better:
 *  0 exact · 1 prefix · 2 words in order · 3 substring · 4 words in any order (names only).
 *  Prefix and in-order word matches (1 and 2) that only come from an alias sit half a rank lower, so a plain
 *  "state" lists every X St. team before LSU ("Louisiana State"). An alias prefix that stops mid-word (Siena's
 *  "saints" for "saint") ranks 2.5, below in-order name matches like "Mount St. Mary's". Exact alias hits
 *  (unc, uconn) keep full rank. */
function scoreForms(forms: string[], qForms: string[], qTokens: string[], anyOrder: boolean): { r: number; word: boolean; part?: boolean } | null {
  let best: { r: number; word: boolean; part?: boolean } | null = null;
  const take = (r: number, word = false, part = false) => { if (!best || r < best.r || (r === best.r && best.part && !part)) best = { r, word, part }; };
  for (const q of qForms) {
    if (forms.includes(q)) take(0);
    else if (forms.some((f) => f.startsWith(q + " "))) take(1);
    // Prefix that stops mid-word ("saint" in "saints", "mich" in "michigan"). score() demotes these on aliases.
    else if (forms.some((f) => f.startsWith(q))) take(1, false, true);
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
  // plain substring, spaces ignored ("ichigan", "texasst"). A bare "st" already matches
  // st/state/saint as words and "Stanford"-style prefixes; don't also hit Boston / Austin.
  const squashed = qForms.map((q) => q.replace(/ /g, "")).filter((q) => q !== "st");
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
  // Alias-only hits rank half a step below the same name hit. A mid-word alias prefix (a mascot: "saint" in
  // Siena's "saints") drops below every in-order name match, so it can't push "Mount St. Mary's" out of the top 8.
  const sa = a ? (a.part ? 2.5 : a.r + (a.word || a.r === 1 ? 0.5 : 0)) : null;
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
