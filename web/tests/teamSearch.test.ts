// Plain node:test + tsc, no extra deps. Run: npm test
import { test } from "node:test";
import assert from "node:assert/strict";
import { buildIndex, expandSt, nextGameHref, normalize, searchTeams, type SearchTeam } from "../src/lib/teamSearch";
import { teamSlug } from "../src/lib/teams";

const NAMES = [
  "Texas", "Texas St.", "Texas Tech", "Texas A&M", "Texas Southern", "UT Arlington", "UTEP", "UTSA", "Utah", "Utah St.",
  "North Carolina", "N.C. State", "UNC Asheville", "UNC Greensboro", "UNC Wilmington", "Duke", "VCU", "Virginia", "Virginia Tech",
  "San Diego", "San Diego St.", "South Dakota St.", "Michigan", "Michigan St.", "Mississippi St.", "Saint Louis", "St. John's",
  "Saint Mary's", "William & Mary", "Connecticut", "Mississippi", "Miami FL", "Miami OH", "Stony Brook", "Stephen F. Austin",
  "Alabama", "Alabama St.", "Alabama A&M", "Arizona", "Arizona St.", "Arkansas", "Arkansas St.",
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

test("expandSt maps a standalone 'st' to state and saint", () => {
  assert.deepEqual(expandSt("texas st").sort(), ["texas saint", "texas st", "texas state"]);
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
