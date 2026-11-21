import { test } from "node:test";
import assert from "node:assert/strict";
import { compareTvRating, fmtTvRating, parseSlateSort, tvHighlightIds, TV_HIGHLIGHT_FLOOR, TV_HIGHLIGHT_MIN, TV_HIGHLIGHT_TOP_N } from "../src/lib/tvRating";
import { loadSlateFrom } from "../src/lib/data";

test("fmtTvRating shows an integer and treats null as unknown, not 0", () => {
  assert.equal(fmtTvRating(77.3), 77);
  assert.equal(fmtTvRating(67.6), 68);
  assert.equal(fmtTvRating(60.3), 60);
  assert.equal(fmtTvRating(0), 0);
  assert.equal(fmtTvRating(50), 50);
  assert.equal(fmtTvRating(99.5), 100);
  assert.equal(fmtTvRating(null), null);
  assert.equal(fmtTvRating(undefined), null);
  assert.equal(fmtTvRating(Number.NaN), null);
});

test("parseSlateSort accepts time, value and tv; anything else is time", () => {
  assert.equal(parseSlateSort("tv"), "tv");
  assert.equal(parseSlateSort("value"), "value");
  assert.equal(parseSlateSort("time"), "time");
  assert.equal(parseSlateSort(null), "time");
  assert.equal(parseSlateSort("nope"), "time");
});

const tip = (h: number) => `2026-11-21T${String(h).padStart(2, "0")}:00:00+00:00`;
const g = (id: number, rating: number | null, hour = id) => ({ game_id: id, tv_rating: rating, tipoff: tip(hour) });

test("TV Rating sort is highest first, nulls last, then earlier tip", () => {
  const rows = [g(1, 10, 10), g(2, null, 1), g(3, 60.3, 8), g(4, 60.3, 4), g(5, 77.3, 12), g(6, null, 2)];
  const order = rows.slice().sort(compareTvRating).map((x) => x.game_id);
  // 77.3 first; the two 60.3s tie on rating so earlier tip (id 4 at 04:00) beats id 3 at 08:00; then 10; nulls last by tip
  assert.deepEqual(order, [5, 4, 3, 1, 2, 6]);
});

test("highlight is 50+ or the day's top 5 by rating, whichever set is larger; nulls never count", () => {
  assert.equal(TV_HIGHLIGHT_MIN, 50);
  assert.equal(TV_HIGHLIGHT_TOP_N, 5);
  assert.equal(TV_HIGHLIGHT_FLOOR, 25);
  // only 3 games at/above 50 → still highlight the next two (top 5), all above the floor
  const day = [g(1, 77.3), g(2, 67.6), g(3, 60.3), g(4, 47.3), g(5, 43.0), g(6, 40.3), g(7, null)];
  assert.deepEqual([...tvHighlightIds(day)].sort((a, b) => a - b), [1, 2, 3, 4, 5]);
  // eight games at 50+ → all eight, not just the top 5
  const many = Array.from({ length: 8 }, (_, i) => g(i + 1, 50 + i));
  assert.deepEqual([...tvHighlightIds(many)].sort((a, b) => a - b), [1, 2, 3, 4, 5, 6, 7, 8]);
  // below 50: top 5 that also meet the rounded floor of 25 (20 and under drop out)
  const low = [g(1, 40), g(2, 30), g(3, 20), g(4, 10), g(5, 5), g(6, 1), g(7, null)];
  assert.deepEqual([...tvHighlightIds(low)].sort((a, b) => a - b), [1, 2]);
  // ties at 5th place are included when they meet the floor; 10 does not
  const ties = [g(1, 80), g(2, 70), g(3, 60), g(4, 40), g(5, 40), g(6, 40), g(7, 10)];
  assert.deepEqual([...tvHighlightIds(ties)].sort((a, b) => a - b), [1, 2, 3, 4, 5, 6]);
  // fewer than 5 rated games: a 12 or 8 is below the floor, so no flame; nulls never count
  const few = [g(1, 12), g(2, 8), g(3, null)];
  assert.deepEqual([...tvHighlightIds(few)], []);
  assert.equal(tvHighlightIds([]).size, 0);
});

test("highlight uses the rounded displayed value, so 49.6 shows 50 and gets a flame", () => {
  assert.equal(fmtTvRating(49.6), 50);
  assert.equal(fmtTvRating(49.4), 49);
  // 49.6 is not in the top 5 by actual rating, but displays as 50 so it is highlighted
  const day = [g(1, 80), g(2, 70), g(3, 60), g(4, 55), g(5, 54), g(6, 53), g(7, 49.6), g(8, 40)];
  const ids = tvHighlightIds(day);
  assert.ok(ids.has(7), "49.6 displays as 50 and must get a flame");
  assert.ok(!ids.has(8), "40 is below 50 and outside the top 5");
  // 49.4 displays as 49: no 50+ flame, and outside the top 5
  const justUnder = [g(1, 80), g(2, 70), g(3, 60), g(4, 55), g(5, 54), g(6, 53), g(7, 49.4)];
  assert.ok(!tvHighlightIds(justUnder).has(7));
});

test("top-5 floor is the rounded displayed value (~25); below that never gets a flame", () => {
  // 24.6 displays as 25 → eligible for a top-5 flame; 24.4 displays as 24 → never
  assert.equal(fmtTvRating(24.6), 25);
  assert.equal(fmtTvRating(24.4), 24);
  const atFloor = [g(1, 40), g(2, 30), g(3, 28), g(4, 26), g(5, 24.6), g(6, 20)];
  assert.deepEqual([...tvHighlightIds(atFloor)].sort((a, b) => a - b), [1, 2, 3, 4, 5]);
  const below = [g(1, 40), g(2, 30), g(3, 28), g(4, 26), g(5, 24.4), g(6, 20)];
  assert.deepEqual([...tvHighlightIds(below)].sort((a, b) => a - b), [1, 2, 3, 4]);
  // even with fewer than 5 rated games, a sub-floor rating is not flamed
  assert.deepEqual([...tvHighlightIds([g(1, 24.4), g(2, 10)])], []);
  assert.deepEqual([...tvHighlightIds([g(1, 24.6)])], [1]);
});

test("highlight stays on the full slate, so hide-completed can show fewer than 5 flames", () => {
  // game 1 is a completed 80; the live games are 40, 35, 30, 28, 26.
  // Full-slate top 5 are 1-5; 26 is 6th and below 50, so it stays unhighlighted
  // even after game 1 is hidden. Recomputing on the visible subset would wrongly flame 26.
  const slate = [g(1, 80), g(2, 40), g(3, 35), g(4, 30), g(5, 28), g(6, 26)];
  const ids = tvHighlightIds(slate);
  assert.deepEqual([...ids].sort((a, b) => a - b), [1, 2, 3, 4, 5]);
  assert.ok(!ids.has(6));
  const visible = slate.filter((x) => x.game_id !== 1);
  assert.ok(tvHighlightIds(visible).has(6), "documents the wrong result if someone recomputes on the visible subset");
});

test("top-5 ties use the rounded displayed value, so 40.4 and 40.3 (both shown as 40) get the same treatment", () => {
  assert.equal(fmtTvRating(40.4), 40);
  assert.equal(fmtTvRating(40.3), 40);
  const day = [g(1, 80), g(2, 70), g(3, 60), g(4, 50), g(5, 40.4), g(6, 40.3)];
  assert.deepEqual([...tvHighlightIds(day)].sort((a, b) => a - b), [1, 2, 3, 4, 5, 6]);
  // 39.6 also displays as 40, so it ties with the group; 39.4 displays as 39 and does not
  const more = [g(1, 80), g(2, 70), g(3, 60), g(4, 50), g(5, 40.4), g(6, 40.3), g(7, 39.6), g(8, 39.4)];
  const ids = tvHighlightIds(more);
  assert.ok(ids.has(7), "39.6 shows 40 and ties the 5th-place displayed rating");
  assert.ok(!ids.has(8), "39.4 shows 39, below the displayed cutoff");
});

function fakeClient(withRating: boolean) {
  const N = 61;
  const slateRows = Array.from({ length: N }, (_, i) => {
    const row: Record<string, unknown> = {
      sport: "cbb", game_date: "2026-11-18", game_id: i + 1, home: `Home ${i + 1}`, away: `Away ${i + 1}`,
      tipoff: "2026-11-19T01:00:00+00:00", game: `Away ${i + 1} @ Home ${i + 1}`, archetypes: {},
    };
    if (withRating) row.tv_rating = i === 0 ? 77.3 : null;
    return row;
  });
  const statusRows = slateRows.map((r) => ({
    id: r.game_id, status: "final", updated_at: "2026-11-19T08:00:00Z", final_at: "2026-11-19T03:00:00Z", final_at_estimated: false,
    home: r.home, away: r.away, home_team_id: null, away_team_id: null, home_score: 79, away_score: 63,
  }));
  const from = (table: string) => {
    const st = { cols: "", neutralEq: false };
    const run = () => {
      if (table === "games" && (st.neutralEq || st.cols === "id" || st.cols === "id,neutral")) {
        return Promise.resolve({ data: [], error: null });
      }
      if (table === "slate_games") return Promise.resolve({ data: slateRows, error: null });
      if (table === "games") return Promise.resolve({ data: statusRows, error: null });
      return Promise.resolve({ data: [], error: null });
    };
    const chain: Record<string, unknown> = {};
    for (const m of ["eq", "in", "gte", "gt", "lt", "order", "limit", "not", "or", "abortSignal"]) {
      chain[m] = (...a: unknown[]) => { if (m === "eq" && a[0] === "neutral") st.neutralEq = true; return chain; };
    }
    chain.select = (cols: string) => { st.cols = cols; return chain; };
    chain.then = (res: (v: unknown) => unknown, rej: (e: unknown) => unknown) => run().then(res, rej);
    return chain;
  };
  return { from } as never;
}

test("a missing tv_rating column is fail-safe: the slate still loads all 61 finals, ratings stay unknown", async () => {
  const r = await loadSlateFrom(fakeClient(false), { key: "cbb", league: "NCAAM", status: "live" } as never, "2026-11-18");
  assert.equal(r.state, "live");
  if (r.state !== "live") return;
  assert.equal(r.data.games.length, 61);
  assert.equal(r.data.games.filter((x) => x.completed && x.status === "final" && x.home_score === 79).length, 61);
  assert.ok(r.data.games.every((x) => x.tv_rating == null), "missing column → unknown, not 0");
});

test("when the column is present, tv_rating is read without changing status or scores", async () => {
  const r = await loadSlateFrom(fakeClient(true), { key: "cbb", league: "NCAAM", status: "live" } as never, "2026-11-18");
  assert.equal(r.state, "live");
  if (r.state !== "live") return;
  assert.equal(r.data.games.length, 61);
  assert.equal(r.data.games.filter((x) => x.completed).length, 61);
  assert.equal(r.data.games[0].tv_rating, 77.3);
  assert.equal(fmtTvRating(r.data.games[0].tv_rating), 77);
  assert.ok(r.data.games.slice(1).every((x) => x.tv_rating == null));
});
