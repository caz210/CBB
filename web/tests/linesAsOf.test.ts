// "As of" stamp for the line the ladder ratings use. Run: npm test
import { test } from "node:test";
import assert from "node:assert/strict";
import { fmtLinesAsOf, newestByGame, pickLinesAsOf } from "../src/lib/linesAsOf";

test("source order: edges_as_of, then market_fetched_at, then market_lines", () => {
  const a = "2026-11-21T13:20:00+00:00", b = "2026-11-21T13:15:00+00:00", c = "2026-11-21T13:13:00+00:00";
  assert.deepEqual(pickLinesAsOf({ edgesAsOf: a, marketFetchedAt: b, marketLinesFetchedAt: c }), { at: a, source: "edges_as_of" });
  assert.deepEqual(pickLinesAsOf({ marketFetchedAt: b, marketLinesFetchedAt: c }), { at: b, source: "market_fetched_at" });
  assert.deepEqual(pickLinesAsOf({ marketLinesFetchedAt: c }), { at: c, source: "market_lines" });
});

test("missing or invalid stamps are skipped; nothing readable → null (UI omits 'as of')", () => {
  assert.equal(pickLinesAsOf({}), null);
  assert.equal(pickLinesAsOf({ edgesAsOf: null, marketFetchedAt: "not a date", marketLinesFetchedAt: undefined }), null);
  assert.deepEqual(pickLinesAsOf({ edgesAsOf: "", marketLinesFetchedAt: "2026-11-21T13:13:00+00:00" })?.source, "market_lines");
  assert.equal(fmtLinesAsOf(null), null);
});

test("newestByGame keeps the newest stamp per game", () => {
  const m = newestByGame([
    { game_id: 1, fetched_at: "2026-11-10T13:15:00+00:00" },
    { game_id: 1, fetched_at: "2026-11-10T16:17:00+00:00" },
    { game_id: 2, fetched_at: null },
    { game_id: 3, fetched_at: "2026-11-11T13:11:00+00:00" },
  ], "fetched_at");
  assert.equal(m.get(1), "2026-11-10T16:17:00+00:00");
  assert.equal(m.has(2), false);
  assert.equal(m.size, 2);
});

test("formats the real stamp in Central time", () => {
  // Real: 11-21 slate lines fetched 13:13 UTC = 7:13 AM CT.
  const now = new Date("2026-11-21T20:00:00Z");
  assert.equal(fmtLinesAsOf("2026-11-21T13:13:41+00:00", now), "as of 7:13 AM CT");
  assert.equal(fmtLinesAsOf("2026-11-20T13:12:00+00:00", now), "as of Fri 7:12 AM CT");
});
