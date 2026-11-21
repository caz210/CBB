// Plain node:test + tsc, no extra deps. Run: npm test
import { test } from "node:test";
import assert from "node:assert/strict";
import { buildIndex, type SearchTeam } from "../src/lib/teamSearch";
import {
  aliasByNameFromIndex, aliasByNameFromRecord, echoSlateQuery, filterSlateGames, isEmptySlateQuery,
  isSlateAliasSearchPending, slateDateHref, slateGameMatches, teamMatchesQuery,
} from "../src/lib/slateSearch";

const slate = [
  { id: 1, away: "Texas St.", home: "Duke" },
  { id: 2, away: "Michigan St.", home: "North Carolina" },
  { id: 3, away: "Connecticut", home: "Gonzaga" },
  { id: 4, away: "Texas", home: "Kansas" },
  { id: 5, away: "N.C. State", home: "Saint Louis" },
  { id: 6, away: "St. John's", home: "Texas Tech" },
  { id: 7, away: "Albany", home: "Canisius" },
  { id: 8, away: "San Jose St.", home: "UNLV" },
];
const ids = (q: string, aliases?: Parameters<typeof filterSlateGames>[2]) =>
  filterSlateGames(slate, q, aliases).map((g) => g.id);

test("empty query returns every game and matches either side", () => {
  assert.equal(isEmptySlateQuery(""), true);
  assert.equal(isEmptySlateQuery("   "), true);
  assert.equal(isEmptySlateQuery("..."), true);
  assert.equal(isEmptySlateQuery("duke"), false);
  assert.deepEqual(ids(""), [1, 2, 3, 4, 5, 6, 7, 8]);
  assert.deepEqual(ids("   "), [1, 2, 3, 4, 5, 6, 7, 8]);
  assert.equal(slateGameMatches(slate[0], ""), true);
  assert.equal(teamMatchesQuery("Duke", ""), true);
});

test("matching is case-insensitive", () => {
  for (const q of ["duke", "DUKE", "Duke", "dUkE"]) {
    assert.deepEqual(ids(q), [1], q);
    assert.equal(slateGameMatches(slate[0], q), true, q);
  }
});

test("a query matches either team on the game", () => {
  assert.deepEqual(ids("duke"), [1]);
  assert.deepEqual(ids("texas st"), [1]);
  assert.deepEqual(ids("kansas"), [4]);
  assert.deepEqual(ids("texas"), [1, 4, 6]); // Texas St., Texas, Texas Tech
  assert.equal(slateGameMatches({ away: "Duke", home: "Texas St." }, "duke"), true);
  assert.equal(slateGameMatches({ away: "Duke", home: "Texas St." }, "texas st"), true);
  assert.equal(slateGameMatches({ away: "Duke", home: "Texas St." }, "gonzaga"), false);
});

test("St. and State are interchangeable", () => {
  assert.ok(ids("texas state").includes(1));
  assert.ok(ids("Texas State").includes(1));
  assert.ok(ids("michigan state").includes(2));
  assert.ok(ids("mich st").includes(2));
  assert.ok(ids("nc state").includes(5));
  assert.ok(ids("saint louis").includes(5));
  assert.ok(ids("st louis").includes(5));
  assert.ok(ids("saint johns").includes(6));
  assert.ok(ids("st john's").includes(6));
  // "state" finds the St./State schools, not Duke or Gonzaga
  const stateIds = ids("state");
  assert.ok(stateIds.includes(1) && stateIds.includes(2) && stateIds.includes(5));
  assert.ok(!stateIds.includes(3) && !stateIds.includes(4));
});

test("partial names match", () => {
  assert.deepEqual(ids("mich"), [2]);
  assert.ok(ids("conn").includes(3));
  assert.ok(ids("gonz").includes(3));
  assert.deepEqual(ids("xyz"), []);
  assert.equal(slateGameMatches(slate[3], "kans"), true);
});

test("punctuation is normalised and local aliases work without the DB table", () => {
  assert.ok(ids("n.c. state").includes(5));
  assert.ok(ids("st. john's").includes(6));
  assert.ok(ids("uconn").includes(3)); // Connecticut alias from localAliases
  assert.ok(ids("unc").includes(2)); // North Carolina alias
  assert.ok(ids("UNC").includes(2));
  assert.equal(teamMatchesQuery("Texas St.", "texas-state"), true);
  // DB-only spellings do not match until the /api/teams index is passed in
  assert.deepEqual(ids("ualbany"), []);
  assert.deepEqual(ids("sjsu"), []);
});

test("the team_aliases table (via the cached index) is used when loaded, and skipped when it isn't", () => {
  const teams: SearchTeam[] = [
    { id: 7, name: "Albany", slug: "albany", next: null, aliases: ["ualbany", "great danes"] },
    { id: 5, name: "N.C. State", slug: "nc-state", next: null, aliases: ["ncsu", "wolfpack"] },
    { id: 8, name: "San Jose St.", slug: "san-jose-st", next: null, aliases: ["sjsu"] },
  ];
  const aliases = aliasByNameFromIndex(buildIndex(teams));
  assert.ok(ids("ualbany", aliases).includes(7));
  assert.ok(ids("UAlbany", aliases).includes(7));
  assert.ok(ids("great danes", aliases).includes(7));
  assert.ok(ids("wolfpack", aliases).includes(5));
  assert.ok(ids("ncsu", aliases).includes(5));
  assert.ok(ids("sjsu", aliases).includes(8));
  // fail-safe: no index (load failed or still in flight) still matches local aliases
  assert.ok(ids("uconn").includes(3));
  assert.ok(ids("wolfpack").includes(5)); // also in localAliases for N.C. State
  assert.deepEqual(ids("ualbany"), []);
});

test("type, then Prev: date links read the live query, not the server-rendered URL", () => {
  const today = "2026-11-21";
  // Server rendered /cbb/slate with no q. The user types 'duke' (replaceState).
  const serverSearch = "";
  const liveSearch = "q=duke";
  assert.equal(
    slateDateHref({ sportKey: "cbb", date: "2026-11-20", today, search: liveSearch }),
    "/cbb/slate?date=2026-11-20&q=duke",
  );
  // Building from the stale server snapshot is exactly the bug — Prev would drop q.
  assert.equal(
    slateDateHref({ sportKey: "cbb", date: "2026-11-20", today, search: serverSearch }),
    "/cbb/slate?date=2026-11-20",
  );
});

test("retype, then Prev: the new query wins over the one the server rendered", () => {
  const today = "2026-11-21";
  const serverSearch = "q=duke"; // landed on ?q=duke
  const liveSearch = "q=mercer"; // retyped
  assert.equal(
    slateDateHref({ sportKey: "cbb", date: "2026-11-20", today, search: liveSearch }),
    "/cbb/slate?date=2026-11-20&q=mercer",
  );
  assert.equal(
    slateDateHref({ sportKey: "cbb", date: "2026-11-20", today, search: serverSearch }),
    "/cbb/slate?date=2026-11-20&q=duke",
  );
});

test("Today and Next keep the live q, sort and hide", () => {
  const today = "2026-11-21";
  const live = "date=2026-11-20&q=duke&sort=tv&hide=1";
  assert.equal(
    slateDateHref({ sportKey: "cbb", date: today, today, search: live }),
    "/cbb/slate?sort=tv&hide=1&q=duke",
  );
  assert.equal(
    slateDateHref({ sportKey: "cbb", date: "2026-11-22", today, search: live }),
    "/cbb/slate?date=2026-11-22&sort=tv&hide=1&q=duke",
  );
  // Next/Today from a page the user typed on (no server q, live q only)
  assert.equal(
    slateDateHref({ sportKey: "cbb", date: today, today, search: "q=duke" }),
    "/cbb/slate?q=duke",
  );
  assert.equal(
    slateDateHref({ sportKey: "cbb", date: "2026-11-22", today, search: "q=duke" }),
    "/cbb/slate?date=2026-11-22&q=duke",
  );
});

test("a long unbroken query is truncated in the empty-state echo", () => {
  const long = "x".repeat(300);
  const echo = echoSlateQuery(long);
  assert.equal(echo.length, 41); // 40 chars + ellipsis
  assert.equal(echo, `${"x".repeat(40)}…`);
  assert.equal(echoSlateQuery("xyzzy"), "xyzzy");
  assert.equal(echoSlateQuery("  mercer  "), "mercer");
});

test("alias-only queries stay in Searching… until the index loads, then show the real empty state on failure or timeout", () => {
  const pending = (over: Partial<{ query: string; matchCount: number; load: "loading" | "ready" | "error"; timedOut: boolean }>) =>
    isSlateAliasSearchPending({ query: "ualbany", matchCount: 0, load: "loading", timedOut: false, ...over });
  assert.equal(pending({}), true);
  assert.equal(pending({ matchCount: 1 }), false); // a local hit should not wait
  assert.equal(pending({ load: "ready" }), false);
  assert.equal(pending({ load: "error" }), false);
  assert.equal(pending({ timedOut: true }), false);
  assert.equal(pending({ query: "" }), false);
  assert.equal(pending({ query: "   " }), false);
});

test("Searching… only while an alias-only lookup is possible: not once the page carries the slate's aliases", () => {
  const base = { query: "zzzz", matchCount: 0, load: "loading" as const, timedOut: false };
  // Page shipped this slate's team_aliases: a no-match is a real no-match, no Searching… flash.
  assert.equal(isSlateAliasSearchPending({ ...base, slateAliasesKnown: true }), false);
  assert.equal(isSlateAliasSearchPending({ ...base, query: "ualbany", slateAliasesKnown: true }), false);
  // Slate aliases failed to load: fall back to waiting on /api/teams.
  assert.equal(isSlateAliasSearchPending({ ...base, query: "ualbany", slateAliasesKnown: false }), true);
  // And the server aliases match on first paint, before /api/teams answers.
  const aliases = aliasByNameFromRecord({ Albany: ["ualbany", "great danes"] });
  assert.equal(slateGameMatches({ away: "Albany", home: "Duke" }, "ualbany", aliases), true);
  assert.equal(slateGameMatches({ away: "Albany", home: "Duke" }, "zzzz", aliases), false);
  assert.equal(aliasByNameFromRecord(null), undefined);
});
