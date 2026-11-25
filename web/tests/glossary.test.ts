import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { HIDDEN_TAGS, PUBLIC_TAG_NAMES } from "../src/lib/archetypes";
import { VERDICT_LADDER } from "../src/lib/verdicts";
import { EXPLAINERS } from "../src/lib/explainers";
import {
  GLOSSARY_ANCHOR,
  GLOSSARY_ENTRIES,
  GLOSSARY_HREF,
  glossaryEntriesIn,
  glossaryEntry,
} from "../src/lib/glossary";

/** Live chips and style tags the glossary is responsible for (verdicts are checked separately). */
const RENDERED_IDS = [
  "neutral",
  "tv-rating",
  "upset-alert",
  "czarp-lean",
  "line-moved-away",
  "new-coach-pause",
  "tentative",
  ...PUBLIC_TAG_NAMES,
] as const;

const LADDER_WORDS = /\bToss Up\b|\bLate Value\b|CZarp Play/i;

test("every live chip and style tag has a glossary entry", () => {
  for (const id of RENDERED_IDS) {
    const e = glossaryEntry(id);
    assert.ok(e, `missing glossary entry for ${id}`);
    assert.ok(e!.title.trim(), id);
    assert.ok(e!.body.trim().length > 20, `${id} body is too short`);
    assert.match(e!.body, /[A-Za-z]/);
    assert.doesNotMatch(e!.title + e!.body, /placeholder|TODO|v1|v2|KenPom/i);
    assert.doesNotMatch(e!.title + e!.body, LADDER_WORDS);
  }
});

test("glossary style tags are exactly the 16 public TAG_DEFS names", () => {
  const style = glossaryEntriesIn("style").map((e) => e.id);
  assert.deepEqual([...style].sort(), [...PUBLIC_TAG_NAMES].sort());
  assert.ok(HIDDEN_TAGS.every((n) => !style.includes(n)));
  assert.equal(style.length, 16);
});

test("glossary covers live game chips; ladder words appear only in the verdict group", () => {
  assert.deepEqual(glossaryEntriesIn("chip").map((e) => e.title), [
    "Neutral", "TV Rating", "Upset alert", "CZarp Lean", "Line moved away, possible news", "New-coach pause", "Tentative",
  ]);
  assert.equal(glossaryEntriesIn("style").length, 16);
  assert.equal(GLOSSARY_ENTRIES.length, 5 + 7 + 16);
  assert.equal(glossaryEntry("TV Rating")?.id, "tv-rating");
  assert.equal(GLOSSARY_HREF, `/methodology#${GLOSSARY_ANCHOR}`);
  for (const e of GLOSSARY_ENTRIES) {
    if (e.group !== "verdict") assert.doesNotMatch(e.id + e.title + e.body, LADDER_WORDS);
  }
});

test("verdict group is built from VERDICT_LADDER, weakest to strongest, with no restated copy", () => {
  const v = glossaryEntriesIn("verdict");
  assert.deepEqual(v.map((e) => e.title), VERDICT_LADDER.map((d) => d.label));
  assert.deepEqual(v.map((e) => e.body), VERDICT_LADDER.map((d) => d.short));
  assert.deepEqual(v.map((e) => e.id), ["toss-up", "small-edge", "value", "late-value", "czarp-play"]);
  assert.equal(glossaryEntry("CZarp Play")?.body, EXPLAINERS.czarpPlay.body);
  assert.equal(glossaryEntry("Late Value")?.body, EXPLAINERS.lateValue.body);
  assert.equal(EXPLAINERS.czarpPlay.title, "CZarp Play");
  // Ladder copy is not duplicated as string literals in glossary.ts.
  const candidates = [resolve(__dirname, "../src/lib/glossary.ts"), resolve(__dirname, "../../src/lib/glossary.ts")];
  const file = candidates.find((f) => { try { readFileSync(f); return true; } catch { return false; } });
  assert.ok(file, "glossary.ts not found");
  const src = readFileSync(file!, "utf8");
  for (const d of VERDICT_LADDER) assert.ok(!src.includes(d.short), `glossary.ts restates ${d.label}`);
  assert.doesNotMatch(src, /"Toss Up"|"Lean"|"Value"|"Late Value"|"CZarp Play"/);
});

test("CZarp Play is defined once, in lib/verdicts.ts", () => {
  const read = (rel: string) => {
    const f = [resolve(__dirname, `../${rel}`), resolve(__dirname, `../../${rel}`)].find((x) => { try { readFileSync(x); return true; } catch { return false; } });
    return readFileSync(f!, "utf8");
  };
  assert.equal((read("src/lib/explainers.ts").match(/czarpPlay\s*:/g) ?? []).length, 1);
  assert.match(read("src/lib/explainers.ts"), /czarpPlay:\s*ladderTip\("official"\)/);
  assert.equal((read("src/lib/verdicts.ts").match(/label: "CZarp Play"/g) ?? []).length, 1);
  // No retired ladder names anywhere in the app source.
  for (const rel of ["src/lib/verdicts.ts", "src/lib/explainers.ts", "src/lib/verdict.ts", "src/lib/methodology.ts", "src/components/StatusChips.tsx", "src/components/SlateBoard.tsx"]) {
    assert.doesNotMatch(read(rel), /"Favor"|Official Play|officialPlay|FAVOR_REASON/, rel);
  }
});
