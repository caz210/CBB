import { test } from "node:test";
import assert from "node:assert/strict";
import { SHOW_TRACKED_TIER } from "../src/lib/pickTier";
import { METHODOLOGY_SECTIONS, SHOW_METHODOLOGY_PLACEHOLDERS, visibleMethodologySections } from "../src/lib/methodology";

test("methodology placeholders and tracked copy are gated off by default", () => {
  assert.equal(SHOW_METHODOLOGY_PLACEHOLDERS, false);
  assert.equal(SHOW_TRACKED_TIER, false);
  assert.deepEqual(visibleMethodologySections(), []);
  assert.ok(METHODOLOGY_SECTIONS.some((s) => s.tracked && /Official vs tracked/i.test(s.title)));
});

test("flipping the flags restores placeholder sections; tracked stays hidden unless its flag is on", () => {
  const placeholdersOn = visibleMethodologySections(true, false);
  assert.ok(placeholdersOn.length > 0);
  assert.ok(placeholdersOn.every((s) => s.title !== "Official vs tracked"));
  assert.ok(!placeholdersOn.some((s) => /tracked picks/i.test(s.body)));
  const bothOn = visibleMethodologySections(true, true);
  assert.ok(bothOn.some((s) => s.title === "Official vs tracked"));
  assert.equal(bothOn.length, METHODOLOGY_SECTIONS.length);
});

test("SHOW_TRACKED_TIER gates the customer tracked UI (slate section, methodology, portfolio tab)", () => {
  assert.equal(SHOW_TRACKED_TIER, false);
});
