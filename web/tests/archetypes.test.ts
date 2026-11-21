import { test } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import {
  compactSlateTags,
  HIDDEN_TAGS,
  isHiddenTag,
  mostExtremeTags,
  parseTagList,
  PUBLIC_TAG_NAMES,
  visibleCopy,
  splitSentences,
  visibleNotes,
  visibleTags,
  weaknessesOnly,
  type ArchetypeTag,
} from "../src/lib/archetypes";

const tag = (name: string, kind: ArchetypeTag["kind"], extra: Partial<ArchetypeTag> = {}): ArchetypeTag => ({
  name, kind, ...extra,
});

test("visibleTags is the single filter that hides Soft Glass and Open Arc", () => {
  const mixed = parseTagList([
    { name: "Wall", kind: "strength" },
    { name: "Soft Glass", kind: "weakness" },
    { name: "Open Arc", kind: "weakness" },
    { name: "Crash", kind: "strength" },
    "open arc",
  ]);
  const vis = visibleTags(mixed);
  assert.deepEqual(vis.map((t) => t.name), ["Wall", "Crash"]);
  assert.ok(HIDDEN_TAGS.every((n) => isHiddenTag(n)));
  assert.ok(!vis.some((t) => isHiddenTag(t.name)));
});

test("slate cards show tags of every kind, not only weaknesses", () => {
  const all = [
    tag("Sharp", "strength"),
    tag("Cold", "weakness"),
    tag("Slow", "style"),
    tag("Open Arc", "weakness"),
  ];
  const vis = visibleTags(all);
  assert.deepEqual(vis.map((t) => t.kind).sort(), ["strength", "style", "weakness"]);
  assert.equal(weaknessesOnly(all, 2).length, 1);
  assert.ok(vis.length > weaknessesOnly(all, 2).length);
});

test("320px compact card shows the 2 most extreme tags plus a leftover count", () => {
  // Stored top-2 from the pipeline (already ranked by tail extremity).
  const top = [tag("Crash", "strength"), tag("Soft D", "weakness")];
  const all = [
    tag("Secure", "strength"),
    tag("Crash", "strength"),
    tag("Soft D", "weakness"),
    tag("Slow", "style"),
    tag("Open Arc", "weakness"),
  ];
  const compact = compactSlateTags(all, top, 2);
  assert.deepEqual(compact.shown.map((t) => t.name), ["Crash", "Soft D"]);
  assert.equal(compact.more, 2); // Secure + Slow; Open Arc is hidden

  // Explicit extremity scores beat the stored top list.
  const scored = [
    tag("Slow", "style", { extremity: 0.9 }),
    tag("Cold", "weakness", { extremity: 0.4 }),
    tag("Fast", "strength", { extremity: 0.7 }),
    tag("Wall", "strength", { extremity: 0.1 }),
  ];
  const byScore = mostExtremeTags(scored, 2);
  assert.deepEqual(byScore.map((t) => t.name), ["Slow", "Fast"]);
  assert.equal(compactSlateTags(scored, [], 2).more, 2);
});

test("a team with two or fewer visible tags has no +N leftover", () => {
  const two = [tag("Fast", "strength"), tag("Soft Glass", "weakness"), tag("Cold", "weakness")];
  const compact = compactSlateTags(two, two, 2);
  assert.deepEqual(compact.shown.map((t) => t.name), ["Fast", "Cold"]);
  assert.equal(compact.more, 0);
});

/**
 * Snapshot of TAG_DEFS in czarp/teamstats.py (pipeline module; not on the
 * main web tree). Soft Glass and Open Arc are hidden signals defined next
 * to TAG_DEFS, not members of it.
 */
const TAG_DEFS_SNAPSHOT = [
  "Fast", "Slow", "Sharp", "Cold", "Volume 3s", "Paint First", "Secure", "Careless",
  "Disrupt", "Crash", "Control Glass", "At the Line", "Foul Prone", "Wall", "Soft D",
  "Crowd 3s",
] as const;

function parseTagDefs(src: string): string[] {
  const block = src.match(/TAG_DEFS\s*=\s*\[([\s\S]*?)\]/);
  if (!block) return [];
  return [...block[1].matchAll(/\(\s*"([^"]+)"/g)].map((m) => m[1]);
}

test("UI public tag list matches the pipeline TAG_DEFS snapshot", () => {
  assert.deepEqual([...PUBLIC_TAG_NAMES], [...TAG_DEFS_SNAPSHOT]);
  assert.equal(PUBLIC_TAG_NAMES.length, 16);
  assert.ok(HIDDEN_TAGS.every((n) => !(PUBLIC_TAG_NAMES as readonly string[]).includes(n)));

  // When the pipeline file is present (full repo checkout), parse it too.
  const candidates = [
    path.resolve(__dirname, "../../../czarp/teamstats.py"),
    path.resolve(__dirname, "../../../../czarp/teamstats.py"),
    path.resolve(process.cwd(), "czarp/teamstats.py"),
    path.resolve(process.cwd(), "../czarp/teamstats.py"),
  ];
  const file = candidates.find((f) => fs.existsSync(f));
  if (file) {
    const names = parseTagDefs(fs.readFileSync(file, "utf8"));
    assert.deepEqual(names, [...TAG_DEFS_SNAPSHOT], `parsed TAG_DEFS from ${file}`);
  }
});

test("visibleCopy strips Soft Glass and Open Arc mentions from notes and reason text", () => {
  assert.equal(
    visibleCopy("Illinois: Fast vs Soft D (+0.25) · Illinois: Volume 3s vs Open Arc (+0.50)"),
    "Illinois: Fast vs Soft D (+0.25)",
  );
  const reason = "Eastern Illinois (Soft D, Careless) at Illinois (Fast, Crash). Projected pace about 74 possessions. Matchup notes: Illinois: Fast vs Soft D; Illinois: Volume 3s vs Open Arc.";
  const cleaned = visibleCopy(reason);
  assert.match(cleaned, /Soft D/);
  assert.match(cleaned, /Projected pace about 74 possessions/);
  assert.match(cleaned, /Matchup notes: Illinois: Fast vs Soft D/);
  assert.doesNotMatch(cleaned, /Open Arc|Soft Glass/i);
  assert.deepEqual(visibleNotes(["Illinois: Volume 3s vs Open Arc (+0.50)", "Illinois: Fast vs Soft D (+0.25)"]), ["Illinois: Fast vs Soft D (+0.25)"]);
});

test("visibleCopy keeps the Why / pace sentence when a reason names a hidden tag with no ';' or '·'", () => {
  // Real slate_games.reason values (Houston @ California, 2026-11-08; Fairleigh Dickinson @ East Texas A&M, 2026-11-11).
  const houston = "Houston (Wall, Slow) at California (Volume 3s, Sharp). Projected pace about 69 possessions. Matchup notes: California: Volume 3s vs Open Arc.";
  const houstonOut = visibleCopy(houston);
  assert.equal(houstonOut, "Houston (Wall, Slow) at California (Volume 3s, Sharp). Projected pace about 69 possessions.");
  assert.doesNotMatch(houstonOut, /Open Arc|Soft Glass/i);

  const etamu = "Fairleigh Dickinson (Soft D, Cold) at East Texas A&M (Crash). Projected pace about 68 possessions. Matchup notes: East Texas A&M: Crash vs Soft Glass.";
  const etamuOut = visibleCopy(etamu);
  assert.equal(etamuOut, "Fairleigh Dickinson (Soft D, Cold) at East Texas A&M (Crash). Projected pace about 68 possessions.");
  assert.doesNotMatch(etamuOut, /Open Arc|Soft Glass/i);

  // Mixed matchup-notes clauses: drop only the hidden one, keep the public one.
  const mixed = "Loyola Chicago (Wall, Crash) at Air Force (Cold, At the Line). Projected pace about 70 possessions. Matchup notes: Crash vs Soft Glass; Air Force: At the Line vs Wall.";
  const mixedOut = visibleCopy(mixed);
  assert.equal(mixedOut, "Loyola Chicago (Wall, Crash) at Air Force (Cold, At the Line). Projected pace about 70 possessions. Matchup notes: Air Force: At the Line vs Wall.");
  assert.doesNotMatch(mixedOut, /Open Arc|Soft Glass/i);
});

test("visibleCopy filters hidden names out of upset_reason without dropping the rest", () => {
  const clean = "California Volume 3s vs Houston soft 3-point defense; California Sharp vs Houston soft 3-point defense";
  assert.equal(visibleCopy(clean), clean);
  assert.equal(
    visibleCopy("California Volume 3s vs Houston Open Arc; California Sharp vs Houston soft 3-point defense"),
    "California Sharp vs Houston soft 3-point defense",
  );
});

test("visibleCopy does not split on abbreviations (prod notes: Green Bay @ St. Thomas 11-08, Troy @ N.C. State 11-13)", () => {
  // Whole note is a hidden-tag matchup, so nothing is left (was "St." / "N.C.").
  assert.equal(visibleCopy("St. Thomas: Volume 3s vs Open Arc (+0.50)"), "");
  assert.equal(visibleCopy("N.C. State: Crash vs Soft Glass (+0.50)"), "");
  assert.deepEqual(visibleNotes(["St. Thomas: Volume 3s vs Open Arc (+0.50)"]), []);
  assert.deepEqual(visibleNotes(["N.C. State: Crash vs Soft Glass (+0.50)"]), []);
  // Public notes with abbreviations are kept whole.
  assert.equal(visibleCopy("St. Thomas: Volume 3s vs Soft D (+0.25)"), "St. Thomas: Volume 3s vs Soft D (+0.25)");
  assert.equal(visibleCopy("N.C. State: Crash vs Careless (+0.25)"), "N.C. State: Crash vs Careless (+0.25)");
  // Prod reasons.
  const gb = "Green Bay (Careless, Slow) at St. Thomas (Volume 3s, Secure). Projected pace about 68 possessions.";
  assert.equal(visibleCopy(gb), gb);
  const troy = "Troy (no standout style tags) at N. C. State (Sharp, Crash).";
  assert.equal(visibleCopy(troy), troy);
  assert.equal(
    visibleCopy("Green Bay (Careless, Slow) at St. Thomas (Volume 3s, Secure). Projected pace about 68 possessions. Matchup notes: St. Thomas: Volume 3s vs Open Arc."),
    gb,
  );
  assert.deepEqual(splitSentences("At Mt. St. Mary's. Pace 70. N.C. State next."), ["At Mt. St. Mary's.", "Pace 70.", "N.C. State next."]);
});
