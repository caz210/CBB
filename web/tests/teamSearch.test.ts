// Plain node:test + tsc, no extra deps. Run: npm test
import { test } from "node:test";
import assert from "node:assert/strict";
import { buildIndex, chicagoDate, expandSt, isUpcoming, nextGameHref, nextGameLabel, normalize, searchTeams, withFreshNext, type SearchTeam } from "../src/lib/teamSearch";
import { teamSlug } from "../src/lib/teams";

const NAMES = [
  "Texas", "Texas St.", "Texas Tech", "Texas A&M", "Texas Southern", "UT Arlington", "UTEP", "UTSA", "Utah", "Utah St.",
  "North Carolina", "N.C. State", "UNC Asheville", "UNC Greensboro", "UNC Wilmington", "Duke", "VCU", "Virginia", "Virginia Tech",
  "San Diego", "San Diego St.", "South Dakota St.", "Michigan", "Michigan St.", "Mississippi St.", "Saint Louis", "St. John's",
  "Saint Mary's", "William & Mary", "Connecticut", "Mississippi", "Miami FL", "Miami OH", "Stony Brook", "Stephen F. Austin",
  "Alabama", "Alabama St.", "Alabama A&M", "American", "Ball St.", "LSU", "Kent St.", "Saint Joseph's", "Saint Peter's", "St. Thomas", "Cal St. Fullerton", "Prairie View A&M", "Arizona", "Arizona St.", "Arkansas", "Arkansas St.",
];
const mk = (withNext: string[] = []): SearchTeam[] =>
  NAMES.map((name, i) => ({
    id: i + 1, name, slug: teamSlug(name),
    next: withNext.includes(name) ? { gameId: 100 + i, date: "2026-11-18", opponent: "Duke", home: true, tipoff: null } : null,
  }));
const find = (q: string, withNext: string[] = [], limit?: number) =>
  searchTeams(buildIndex(mk(withNext)), q, limit).map((t) => t.name);

test("normalize drops punctuation, case, ampersands and 'and'", () => {
  assert.equal(normalize("Michigan St."), "michigan st");
  assert.equal(normalize("  St. John's "), "st johns");
  assert.equal(normalize("N.C. State"), "nc state");
  assert.equal(normalize("Texas A&M"), "texas am");
  assert.equal(normalize("William & Mary"), "william mary");
  assert.equal(normalize("William and Mary"), "william mary");
  assert.equal(normalize("!!"), "");
});

test("expandSt: trailing st is State, leading st is Saint, middle st is either", () => {
  assert.deepEqual(expandSt("texas st").sort(), ["texas st", "texas state"]);
  assert.deepEqual(expandSt("st johns").sort(), ["saint johns", "st johns"]);
  assert.deepEqual(expandSt("cal st fullerton").sort(), ["cal saint fullerton", "cal st fullerton", "cal state fullerton"]);
  assert.deepEqual(expandSt("st").sort(), ["saint", "st", "state"]);
  assert.deepEqual(expandSt("stony brook"), ["stony brook"]);
});

test("'texas st' finds Texas St. first, in any spelling", () => {
  for (const q of ["texas st", "Texas St.", "TEXAS STATE", "texas st ", "texas  state"]) {
    assert.equal(find(q)[0], "Texas St.", q);
  }
  assert.ok(!find("texas st").includes("Texas Tech"));
});

test("St/State/Saint are interchangeable on both sides", () => {
  assert.equal(find("michigan state")[0], "Michigan St.");
  assert.equal(find("mich st")[0], "Michigan St.");
  assert.ok(find("st louis").includes("Saint Louis"));
  assert.ok(find("saint johns").includes("St. John's"));
  assert.ok(find("st marys").includes("Saint Mary's"));
  assert.ok(find("mississippi state").includes("Mississippi St."));
});

test("initials and short forms: UT, UNC, VCU, SDSU", () => {
  assert.equal(find("ut")[0], "Texas");
  assert.ok(find("ut").includes("UT Arlington"));
  assert.equal(find("unc")[0], "North Carolina");
  assert.deepEqual(find("unc").slice(1, 4).sort(), ["UNC Asheville", "UNC Greensboro", "UNC Wilmington"]);
  assert.equal(find("vcu")[0], "VCU");
  assert.deepEqual(find("sdsu").sort(), ["San Diego St.", "South Dakota St."]);
  assert.equal(find("uconn")[0], "Connecticut");
  assert.equal(find("ole miss")[0], "Mississippi");
  assert.equal(find("nc state")[0], "N.C. State");
});

test("punctuation and '&' are ignored", () => {
  assert.equal(find("texas a&m")[0], "Texas A&M");
  assert.equal(find("texas am")[0], "Texas A&M");
  assert.equal(find("william and mary")[0], "William & Mary");
  assert.equal(find("n.c. state")[0], "N.C. State");
  assert.equal(find("johns")[0], "St. John's");
});

test("ranking: exact, then prefix, then word-start, then substring", () => {
  const r = find("virginia");
  assert.equal(r[0], "Virginia");           // exact
  assert.equal(r[1], "Virginia Tech");      // prefix
  const w = find("tech");
  assert.ok(w.includes("Texas Tech") && w.includes("Virginia Tech")); // word-start
  const sub = find("rizona");               // substring only
  assert.deepEqual(sub.sort(), ["Arizona", "Arizona St."]);
  // prefix hits come before substring-only hits
  const ark = find("ark");
  assert.deepEqual(ark.slice(0, 2), ["Arkansas", "Arkansas St."]);
  assert.ok(ark.indexOf("Arkansas St.") < ark.indexOf("Stephen F. Austin") || !ark.includes("Stephen F. Austin"));
});

test("within a rank, teams with a game come first, then shorter names", () => {
  assert.deepEqual(find("alabama").slice(0, 3), ["Alabama", "Alabama A&M", "Alabama St."]); // same length: A-Z
  assert.deepEqual(find("alabama", ["Alabama St."]).slice(0, 3), ["Alabama", "Alabama St.", "Alabama A&M"]);
  assert.deepEqual(find("ala").slice(0, 2), ["Alabama", "Alabama A&M"]);                  // shorter name first
});

test("at most 8 results by default, limit is respected, empty query returns nothing", () => {
  assert.equal(find("a").length, 8);
  assert.equal(find("a", [], 3).length, 3);
  assert.deepEqual(find(""), []);
  assert.deepEqual(find("   "), []);
  assert.deepEqual(find("zzzzqq"), []);
});

test("no match when a typed word has no counterpart", () => {
  assert.ok(!find("texas duke").length);
});

test("reversed word order still matches, ranked below in-order matches", () => {
  assert.equal(find("carolina north")[0], "North Carolina");
  assert.ok(find("state kent").includes("Kent St."));
  assert.equal(find("state arizona")[0], "Arizona St.");
  const r = find("st alabama");           // reversed: only Alabama St. has both words
  assert.equal(r[0], "Alabama St.");
  // an in-order hit always beats a reversed one
  const idx = buildIndex(mk(), () => []);
  const names = searchTeams(idx, "state mich").map((t) => t.name);
  assert.deepEqual(names.slice(0, 1), ["Michigan St."]);
  assert.ok(find("mich st")[0] === "Michigan St.");
});

test("nextGameHref opens that game's sheet on its slate date", () => {
  assert.equal(
    nextGameHref("cbb", { gameId: 1281, date: "2026-11-17", opponent: "Notre Dame", home: true, tipoff: null }),
    "/cbb/slate?date=2026-11-17&game=1281",
  );
});

test("alias lookup is replaceable (e.g. by a DB-backed team_aliases join)", () => {
  const idx = buildIndex(mk(), (t) => (t.name === "Duke" ? ["Cameron Crazies"] : []));
  assert.deepEqual(searchTeams(idx, "cameron").map((t) => t.name), ["Duke"]);
  assert.deepEqual(searchTeams(idx, "unc").map((t) => t.name).slice(0, 1), ["UNC Asheville"]); // no local map applied
});

test("'state' finds only X St. teams and 'saint' only Saint schools", () => {
  const state = find("state", [], 40);
  assert.ok(state.includes("Michigan St.") && state.includes("Ball St."));
  for (const n of ["St. John's", "Saint Louis", "Saint Mary's", "Saint Peter's", "St. Thomas", "Saint Joseph's"]) assert.ok(!state.includes(n), n);
  const saint = find("saint", [], 40);
  assert.ok(saint.includes("St. John's") && saint.includes("Saint Louis") && saint.includes("St. Thomas"));
  for (const n of ["Ball St.", "Michigan St.", "Kent St."]) assert.ok(!saint.includes(n), n);
});

test("short 'st' queries stay on St/Saint: 'st louis' does not find LSU", () => {
  const r = find("st louis");
  assert.equal(r[0], "Saint Louis");
  // LSU (Louisiana State) only matches with the words reversed, so it can only trail the real hit
  assert.ok(!r.includes("LSU") || r.indexOf("LSU") > 0);
  assert.ok(find("st").includes("St. John's"));
  assert.equal(find("cal st fullerton")[0], "Cal St. Fullerton");
  assert.equal(find("cal state fullerton")[0], "Cal St. Fullerton");
});

test("a&m ranks the A&M schools above American", () => {
  const r = find("a&m");
  assert.ok(r.indexOf("Texas A&M") < r.indexOf("American") || !r.includes("American"));
  assert.ok(r.includes("Alabama A&M") && r.includes("Prairie View A&M"));
  assert.ok(find("a and m")[0] !== "American");
});

test("'st joes' finds Saint Joseph's", () => {
  assert.equal(find("st joes")[0], "Saint Joseph's");
  assert.equal(find("saint joes")[0], "Saint Joseph's");
  assert.equal(find("st josephs")[0], "Saint Joseph's");
});

// ---- next-game freshness ----------------------------------------------------
// 2026-11-18 03:00 UTC = Tue 2026-11-17 21:00 CST
const at = (iso: string) => new Date(iso);
const game = (date: string, tipoff: string | null) => ({ date, tipoff });

test("chicagoDate follows America/Chicago, not UTC", () => {
  assert.equal(chicagoDate(at("2026-11-18T03:00:00Z")), "2026-11-17");
  assert.equal(chicagoDate(at("2026-11-18T06:30:00Z")), "2026-11-18");
});

test("isUpcoming: a game that tipped more than 3h ago is not upcoming, even if status still says scheduled", () => {
  const now = at("2026-11-18T05:50:00Z"); // 11:50 PM CST on the 17th
  assert.equal(isUpcoming(game("2026-11-17", "2026-11-17T17:00:00Z"), now), false); // 11 AM tip
  assert.equal(isUpcoming(game("2026-11-17", "2026-11-18T04:59:00Z"), now), true);   // 10:59 PM tip, under 3h ago
  assert.equal(isUpcoming(game("2026-11-17", "2026-11-18T02:00:00Z"), now), false);  // 8 PM tip, 3h50m ago
});

test("isUpcoming: 3h boundary, future tips, missing tip time, past dates", () => {
  const now = at("2026-11-18T01:00:00Z");
  assert.equal(isUpcoming(game("2026-11-17", "2026-11-17T22:00:01Z"), now), true);  // 2h59m59s ago
  assert.equal(isUpcoming(game("2026-11-17", "2026-11-17T21:59:59Z"), now), false); // just over 3h
  assert.equal(isUpcoming(game("2026-11-17", "2026-11-18T03:00:00Z"), now), true);  // tonight
  assert.equal(isUpcoming(game("2026-11-18", "2026-11-19T01:00:00Z"), now), true);  // tomorrow
  assert.equal(isUpcoming(game("2026-11-17", null), now), true);                    // no tip time: keep, date is today
  assert.equal(isUpcoming(game("2026-11-16", null), now), false);                   // past date
  assert.equal(isUpcoming(game("2026-11-16", "2026-11-18T03:00:00Z"), now), true);  // past date but tipped under 3h ago: still on
  assert.equal(isUpcoming(game("2026-11-17", "garbage"), now), true);               // unparseable tip: date rule only
});

test("after midnight CT: a late tip keeps its button for 3h, an earlier game is gone", () => {
  // 10:59 PM CST on the 17th tips at 04:59Z on the 18th
  const late = game("2026-11-17", "2026-11-18T04:59:00Z");
  assert.equal(isUpcoming(late, at("2026-11-18T06:05:00Z")), true);   // 12:05 AM CT, one hour in
  assert.equal(isUpcoming(late, at("2026-11-18T07:59:00Z")), true);   // 1:59 AM CT, just under 3h
  assert.equal(isUpcoming(late, at("2026-11-18T08:00:00Z")), false);  // over 3h after tip
  assert.equal(isUpcoming(game("2026-11-17", "2026-11-18T03:00:00Z"), at("2026-11-18T06:30:00Z")), false); // 9 PM tip, 3.5h in
  assert.equal(nextGameLabel(late, at("2026-11-18T06:05:00Z")), "Today's game"); // still tonight's game
});

test("withFreshNext clears stale next games and keeps live ones", () => {
  const now = at("2026-11-18T05:50:00Z");
  const t = (id: number, next: SearchTeam["next"]): SearchTeam => ({ id, name: `T${id}`, slug: `t${id}`, next });
  const g = (date: string, tipoff: string) => ({ gameId: 1, date, opponent: "X", home: true, tipoff });
  const out = withFreshNext([t(1, g("2026-11-17", "2026-11-17T17:00:00Z")), t(2, g("2026-11-18", "2026-11-19T01:00:00Z")), t(3, null)], now);
  assert.equal(out[0].next, null);
  assert.equal(out[1].next?.date, "2026-11-18");
  assert.equal(out[2].next, null);
});

test("button label: Today's game on the day, Next game otherwise", () => {
  const now = at("2026-11-18T01:00:00Z"); // 7 PM CST on the 17th
  assert.equal(nextGameLabel({ date: "2026-11-17" }, now), "Today's game");
  assert.equal(nextGameLabel({ date: "2026-11-18" }, now), "Next game");
});

test("a team with a stale next game is no longer ranked as 'has a game'", () => {
  const now = at("2026-11-18T05:50:00Z");
  const teams = mk(["Alabama St."]).map((t) => (t.next ? { ...t, next: { ...t.next, date: "2026-11-17", tipoff: "2026-11-17T17:00:00Z" } } : t));
  const out = searchTeams(buildIndex(withFreshNext(teams, now)), "alabama").map((t) => t.name);
  assert.deepEqual(out.slice(0, 3), ["Alabama", "Alabama A&M", "Alabama St."]);
});

// ---- DB aliases (team_aliases) + the local map as fallback -----------------------------------------------------

test("DB aliases feed the index; the local map still works when they are absent", () => {
  const teams = mk().map((t) => (t.name === "Duke" ? { ...t, aliases: ["blue devils", "dook"] } : { ...t }));
  const idx = buildIndex(teams);
  assert.equal(searchTeams(idx, "dook")[0].name, "Duke");        // from the DB list
  assert.equal(searchTeams(idx, "unc")[0].name, "North Carolina"); // from the local map (no DB aliases on that team)
  // alias query failed: no `aliases` anywhere, local map only
  assert.equal(searchTeams(buildIndex(mk()), "unc")[0].name, "North Carolina");
  assert.deepEqual(searchTeams(buildIndex(mk()), "dook"), []);
});

test("a DB alias containing the typed word ranks below real name matches", () => {
  const teams = mk().map((t) => (t.name === "Duke" ? { ...t, aliases: ["texas state fans"] } : { ...t }));
  const r = searchTeams(buildIndex(teams), "texas state").map((t) => t.name);
  assert.equal(r[0], "Texas St.");
});

test("'louisiana state' finds LSU, but a plain 'state' lists X St. teams first", () => {
  const idx = buildIndex(mk());
  assert.equal(searchTeams(idx, "louisiana state")[0].name, "LSU");
  const all = searchTeams(idx, "state", 100).map((t) => t.name);
  assert.notEqual(all[0], "LSU");
  // LSU matches only through its alias, so it trails every team whose own name has "St."
  const lsu = all.indexOf("LSU");
  assert.ok(lsu > 0);
  for (const n of all.slice(lsu + 1)) assert.ok(!/ St\.( |$)/.test(n), n);
});

test("new aliases: Brigham Young, North Carolina State, Mt St Marys", () => {
  const names = [...NAMES, "BYU", "Mount St. Mary's"];
  const idx = buildIndex(names.map((name, i) => ({ id: i + 1, name, slug: teamSlug(name), next: null })));
  assert.equal(searchTeams(idx, "brigham young")[0].name, "BYU");
  assert.equal(searchTeams(idx, "north carolina state")[0].name, "N.C. State");
  assert.equal(searchTeams(idx, "mt st marys")[0].name, "Mount St. Mary's");
  assert.equal(searchTeams(idx, "mt st mary's")[0].name, "Mount St. Mary's");
});
