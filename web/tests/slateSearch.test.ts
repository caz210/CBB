// Plain node:test + tsc, no extra deps. Run: npm test
import { test } from "node:test";
import assert from "node:assert/strict";
import { filterSlateGames, isEmptySlateQuery, slateGameMatches, teamMatchesQuery } from "../src/lib/slateSearch";

const slate = [
  { id: 1, away: "Texas St.", home: "Duke" },
  { id: 2, away: "Michigan St.", home: "North Carolina" },
  { id: 3, away: "Connecticut", home: "Gonzaga" },
  { id: 4, away: "Texas", home: "Kansas" },
  { id: 5, away: "N.C. State", home: "Saint Louis" },
  { id: 6, away: "St. John's", home: "Texas Tech" },
];
const ids = (q: string) => filterSlateGames(slate, q).map((g) => g.id);

test("empty query returns every game and matches either side", () => {
  assert.equal(isEmptySlateQuery(""), true);
  assert.equal(isEmptySlateQuery("   "), true);
  assert.equal(isEmptySlateQuery("..."), true);
  assert.equal(isEmptySlateQuery("duke"), false);
  assert.deepEqual(ids(""), [1, 2, 3, 4, 5, 6]);
  assert.deepEqual(ids("   "), [1, 2, 3, 4, 5, 6]);
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

test("punctuation is normalised and aliases work", () => {
  assert.ok(ids("n.c. state").includes(5));
  assert.ok(ids("st. john's").includes(6));
  assert.ok(ids("uconn").includes(3)); // Connecticut alias
  assert.ok(ids("unc").includes(2)); // North Carolina alias
  assert.ok(ids("UNC").includes(2));
  assert.equal(teamMatchesQuery("Texas St.", "texas-state"), true);
});
