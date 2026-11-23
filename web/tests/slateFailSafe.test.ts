// Simulates the Supabase client to prove the neutral-site read is optional: when it fails (error value, thrown
// error or hang-then-abort) the slate keeps every final status and score; only titles lose the "vs"/Neutral treatment.
import { test } from "node:test";
import assert from "node:assert/strict";
import { loadSlateFrom, neutralIds } from "../src/lib/data";

type Mode = "ok" | "error" | "throw";
const N = 61;
const NEUTRAL_IDS = new Set([3, 17, 40]);

function fakeClient(mode: Mode) {
  const slateRows = Array.from({ length: N }, (_, i) => ({
    sport: "cbb", game_date: "2026-11-18", game_id: i + 1, home: `Home ${i + 1}`, away: `Away ${i + 1}`,
    tipoff: "2026-11-19T01:00:00+00:00", game: `Away ${i + 1} @ Home ${i + 1}`, archetypes: {},
  }));
  const statusRows = slateRows.map((r) => ({
    id: r.game_id, status: "final", updated_at: "2026-11-19T08:00:00Z", final_at: "2026-11-19T03:00:00Z", final_at_estimated: false,
    home: r.home, away: r.away, home_team_id: null, away_team_id: null, home_score: 79, away_score: 63,
  }));
  const calls: { table: string; cols: string }[] = [];
  const from = (table: string) => {
    const st = { table, cols: "", neutralEq: false, ids: null as number[] | null };
    const run = () => {
      calls.push({ table, cols: st.cols });
      // the neutral lookups: games.select("id") or ("id,neutral") filtered on neutral
      if (table === "games" && (st.neutralEq || st.cols === "id,neutral")) {
        if (mode === "throw") return Promise.reject(new Error("aborted"));
        if (mode === "error") return Promise.resolve({ data: null, error: { code: "42703", message: "column games.neutral does not exist" } });
        return Promise.resolve({ data: [...NEUTRAL_IDS].filter((id) => !st.ids || st.ids.includes(id)).map((id) => ({ id, neutral: true })), error: null });
      }
      if (table === "slate_games") return Promise.resolve({ data: slateRows, error: null });
      if (table === "games") return Promise.resolve({ data: statusRows, error: null });
      return Promise.resolve({ data: [], error: null });
    };
    const chain: Record<string, unknown> = {};
    for (const m of ["eq", "in", "gte", "gt", "lt", "order", "limit", "not", "or", "abortSignal"]) {
      chain[m] = (...a: unknown[]) => { if (m === "eq" && a[0] === "neutral") st.neutralEq = true; if (m === "in" && a[0] === "id") st.ids = a[1] as number[]; return chain; };
    }
    chain.select = (cols: string) => { st.cols = cols; return chain; };
    chain.then = (res: (v: unknown) => unknown, rej: (e: unknown) => unknown) => run().then(res, rej);
    return chain;
  };
  return { client: { from } as never, calls };
}

for (const mode of ["ok", "error", "throw"] as const) {
  test(`slate with neutral read "${mode}": all ${N} finals and scores survive`, async () => {
    const { client, calls } = fakeClient(mode);
    const r = await loadSlateFrom(client, { key: "cbb", league: "NCAAM", status: "live" } as never, "2026-11-18");
    assert.equal(r.state, "live");
    if (r.state !== "live") return;
    const g = r.data.games;
    assert.equal(g.length, N);
    assert.equal(g.filter((x) => x.completed).length, N, "finals count");
    assert.ok(g.every((x) => x.status === "final" && x.home_score === 79 && x.away_score === 63 && x.final_at), "status, scores, final_at");
    assert.equal(r.data.finished, true);
    const neutral = g.filter((x) => x.neutral).map((x) => x.game_id).sort((a, b) => a - b);
    assert.deepEqual(neutral, mode === "ok" ? [...NEUTRAL_IDS] : []);
    // the status query is the one main has: it never mentions neutral
    const status = calls.find((c) => c.table === "games" && c.cols.includes("status"));
    assert.ok(status && !/neutral/.test(status.cols), "status select unchanged");
    assert.equal(status!.cols, "id,status,updated_at,final_at,final_at_estimated,home,away,home_team_id,away_team_id,home_score,away_score,external_id");
  });
}

test("neutralIds never throws and returns an empty set on failure", async () => {
  for (const mode of ["error", "throw"] as const) {
    const { client } = fakeClient(mode);
    assert.equal((await neutralIds(client, [1, 2, 3])).size, 0);
  }
  const { client } = fakeClient("ok");
  assert.deepEqual([...(await neutralIds(client, [1, 3, 17]))].sort((a, b) => a - b), [3, 17]);
  assert.equal((await neutralIds(client, [])).size, 0);
});
