import { test } from "node:test";
import assert from "node:assert/strict";
import { HIDDEN_TAGS, PUBLIC_TAG_NAMES } from "../src/lib/archetypes";
import {
  GLOSSARY_ANCHOR,
  GLOSSARY_ENTRIES,
  GLOSSARY_HREF,
  glossaryEntriesIn,
  glossaryEntry,
} from "../src/lib/glossary";

/** Every chip and tag the UI can render, plus the new-ladder verdicts. */
const RENDERED_IDS = [
  "toss-up",
  "lean",
  "favor",
  "official-play",
  "neutral",
  "tv-rating",
  "upset-alert",
  "new-coach-pause",
  "tentative",
  ...PUBLIC_TAG_NAMES,
] as const;

test("every rendered chip and style tag has a glossary entry", () => {
  for (const id of RENDERED_IDS) {
    const e = glossaryEntry(id);
    assert.ok(e, `missing glossary entry for ${id}`);
    assert.ok(e!.title.trim(), id);
    assert.ok(e!.body.trim().length > 20, `${id} body is too short`);
    assert.match(e!.body, /[A-Za-z]/);
    assert.doesNotMatch(e!.title + e!.body, /placeholder|TODO|v1|v2|KenPom/i);
  }
});

test("glossary style tags are exactly the 16 public TAG_DEFS names", () => {
  const style = glossaryEntriesIn("style").map((e) => e.id);
  assert.deepEqual([...style].sort(), [...PUBLIC_TAG_NAMES].sort());
  assert.ok(HIDDEN_TAGS.every((n) => !style.includes(n)));
  assert.equal(style.length, 16);
});

test("glossary covers the new verdict ladder and the named game chips", () => {
  assert.deepEqual(glossaryEntriesIn("verdict").map((e) => e.title), [
    "Toss Up", "Lean", "Favor", "Official Play",
  ]);
  assert.deepEqual(glossaryEntriesIn("chip").map((e) => e.title), [
    "Neutral", "TV Rating", "Upset alert", "New-coach pause", "Tentative",
  ]);
  assert.equal(glossaryEntry("Official Play")?.id, "official-play");
  assert.equal(GLOSSARY_HREF, `/methodology#${GLOSSARY_ANCHOR}`);
  assert.equal(GLOSSARY_ENTRIES.length, 4 + 5 + 16);
});
