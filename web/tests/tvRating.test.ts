import { test } from "node:test";
import assert from "node:assert/strict";
import { compareTvRating, fmtTvRating, parseSlateSort, tvHighlightIds, TV_HIGHLIGHT_MIN, TV_HIGHLIGHT_TOP_N } from "../src/lib/tvRating";
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
  // only 3 games at/above 50 → still highlight the next two (top 5)
  const day = [g(1, 77.3), g(2, 67.6), g(3, 60.3), g(4, 47.3), g(5, 43.0), g(6, 40.3), g(7, null)];
  assert.deepEqual([...tvHighlightIds(day)].sort((a, b) => a - b), [1, 2, 3, 4, 5]);
  // eight games at 50+ → all eight, not just the top 5
  const many = Array.from({ length: 8 }, (_, i) => g(i + 1, 50 + i));
  assert.deepEqual([...tvHighlightIds(many)].sort((a, b) => a - b), [1, 2, 3, 4, 5, 6, 7, 8]);
  // every rating below 50 → just the top 5
  const low = [g(1, 40), g(2, 30), g(3, 20), g(4, 10), g(5, 5), g(6, 1), g(7, null)];
  assert.deepEqual([...tvHighlightIds(low)].sort((a, b) => a - b), [1, 2, 3, 4, 5]);
  // ties at 5th place are included
  const ties = [g(1, 80), g(2, 70), g(3, 60), g(4, 40), g(5, 40), g(6, 40), g(7, 10)];
  assert.deepEqual([...tvHighlightIds(ties)].sort((a, b) => a - b), [1, 2, 3, 4, 5, 6]);
  // fewer than 5 rated games: all rated are highlighted; nulls are not
  const few = [g(1, 12), g(2, 8), g(3, null)];
  assert.deepEqual([...tvHighlightIds(few)].sort((a, b) => a - b), [1, 2]);
  assert.equal(tvHighlightIds([]).size, 0);
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
