import { test } from "node:test";
import assert from "node:assert/strict";
import { gameTitle, retitle, sideLabel, isNeutral, atsSideLabel, spreadRef, spreadNotationNote } from "../src/lib/gameTitle";

test("neutral game reads 'Away vs Home' and sets the flag", () => {
  const t = gameTitle("Houston", "Kansas", true);
  assert.equal(t.text, "Houston vs Kansas");
  assert.equal(t.sep, "vs");
  assert.equal(t.neutral, true);
  assert.equal(t.away, "Houston");
  assert.equal(t.home, "Kansas");
});

test("home game keeps '@' and no flag", () => {
  const t = gameTitle("Houston", "Kansas", false);
  assert.equal(t.text, "Houston @ Kansas");
  assert.equal(t.sep, "@");
  assert.equal(t.neutral, false);
});

test("missing or null neutral is false", () => {
  for (const v of [undefined, null]) {
    const t = gameTitle("A", "B", v);
    assert.equal(t.text, "A @ B");
    assert.equal(t.neutral, false);
  }
  assert.equal(gameTitle("A", "B").neutral, false);
  assert.equal(isNeutral(undefined), false);
  assert.equal(isNeutral(null), false);
  assert.equal(isNeutral(false), false);
  assert.equal(isNeutral(true), true);
});

test("retitle only touches neutral games and only the first ' @ '", () => {
  assert.equal(retitle("Houston @ Kansas", true), "Houston vs Kansas");
  assert.equal(retitle("Houston @ Kansas", false), "Houston @ Kansas");
  assert.equal(retitle("Houston @ Kansas", null), "Houston @ Kansas");
  assert.equal(retitle("Houston @ Kansas"), "Houston @ Kansas");
  assert.equal(retitle("No separator", true), "No separator");
  assert.equal(retitle("Texas A&M @ St. John's", true), "Texas A&M vs St. John's");
});

test("a neutral game has no 'Home' row label; a home game keeps Away/Home", () => {
  assert.equal(sideLabel("Home", true), "Team");
  assert.equal(sideLabel("Away", true), "Team");
  assert.equal(sideLabel("Home", false), "Home");
  assert.equal(sideLabel("Away", null), "Away");
  assert.equal(sideLabel("Home", undefined), "Home");
});

test("ATS side names the team on a neutral site and is untouched otherwise", () => {
  assert.equal(atsSideLabel("home", "Houston", "Kansas", true), "Kansas");
  assert.equal(atsSideLabel("away", "Houston", "Kansas", true), "Houston");
  assert.equal(atsSideLabel("push", "Houston", "Kansas", true), "push");
  assert.equal(atsSideLabel("home", "Houston", "Kansas", false), "home");
  assert.equal(atsSideLabel("away", "Houston", "Kansas", null), "away");
  assert.equal(atsSideLabel("home", "Houston", "Kansas"), "home");
});

test("spreadRef names the home-listed team only on a neutral site", () => {
  assert.equal(spreadRef("+1.3", "Kansas", true), "Kansas +1.3");
  assert.equal(spreadRef("+2.5", "Kansas", true), "Kansas +2.5");
  assert.equal(spreadRef("+1.3", "Kansas", false), "+1.3");
  assert.equal(spreadRef("+1.3", "Kansas", null), "+1.3");
  assert.equal(spreadRef("+1.3", "Kansas"), "+1.3");
});

test("spread footnote stays accurate for home games and adds the neutral rule only when needed", () => {
  assert.equal(spreadNotationNote(false), "Spreads use home-line notation (−5.5 = home favored).");
  const n = spreadNotationNote(true);
  assert.ok(n.includes("home team favored"));
  assert.ok(n.includes("neutral-site"));
  assert.ok(!/v1|v2/i.test(n));
});
