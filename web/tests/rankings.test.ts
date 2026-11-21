import { test } from "node:test";
import assert from "node:assert/strict";
import {
  RANKING_SNAPSHOT_COLUMNS,
  TOP_N,
  fmtPollDate,
  fmtRating,
  fmtRecord,
  isEarlySeason,
  longNameDemoRows,
  movementDemoRows,
  movementLabel,
  movementMark,
  rankMovement,
  rankingsErrorCopy,
  rankingsErrorKind,
  sortRankings,
  updatedLabel,
  weekLabel,
  withMovement,
} from "../src/lib/rankings";
import { loadRankingsFrom } from "../src/lib/data";

test("fmtRating shows a sign and one decimal; null is a dash, not 0", () => {
  assert.equal(fmtRating(28.8), "+28.8");
  assert.equal(fmtRating(28.84), "+28.8");
  assert.equal(fmtRating(28.85), "+28.9");
  assert.equal(fmtRating(0), "0.0");
  assert.equal(fmtRating(-0), "0.0");
  assert.equal(fmtRating(-1.24), "−1.2");
  assert.equal(fmtRating(-1.25), "−1.3");
  assert.equal(fmtRating(null), "—");
  assert.equal(fmtRating(undefined), "—");
  assert.equal(fmtRating(Number.NaN), "—");
});

test("fmtRecord is di_wins–di_losses; missing either side is unknown, not 0-0", () => {
  assert.equal(fmtRecord(4, 0), "4–0");
  assert.equal(fmtRecord(0, 0), "0–0");
  assert.equal(fmtRecord(12, 3), "12–3");
  assert.equal(fmtRecord(null, 0), "—");
  assert.equal(fmtRecord(4, null), "—");
  assert.equal(fmtRecord(undefined, undefined), "—");
  assert.equal(fmtRecord(Number.NaN, 1), "—");
});

test("rankMovement covers up, down, same, and new", () => {
  assert.deepEqual(rankMovement(1, 3), { kind: "up", delta: 2 });
  assert.deepEqual(rankMovement(5, 1), { kind: "down", delta: 4 });
  assert.deepEqual(rankMovement(3, 3), { kind: "same", delta: 0 });
  assert.deepEqual(rankMovement(4, null), { kind: "new", delta: 0 });
  assert.deepEqual(rankMovement(4, undefined), { kind: "new", delta: 0 });
  assert.deepEqual(rankMovement(4, 0), { kind: "new", delta: 0 });
});

test("withMovement: no prior poll (empty list) is a dash, never fake movement or New", () => {
  const rows = [
    { team_id: 1, team: "Purdue", rank: 1, prev_rank: 99, rating: 28.8, di_wins: 4, di_losses: 0 },
    { team_id: 2, team: "Duke", rank: 2, prev_rank: 1, rating: 27, di_wins: 5, di_losses: 0 },
  ];
  const out = withMovement(rows, []);
  assert.ok(out.every((r) => r.movement.kind === "none" && r.prev_rank === null));
  assert.equal(movementLabel(out[0].movement), "No previous poll");
});

test("withMovement: mocked prior week yields up, down, same, and New", () => {
  const current = [
    { team_id: 1, team: "Purdue", rank: 1, rating: 28.8, di_wins: 4, di_losses: 0 },
    { team_id: 2, team: "Duke", rank: 2, rating: 27.4, di_wins: 5, di_losses: 0 },
    { team_id: 3, team: "Houston", rank: 3, rating: 25.1, di_wins: 4, di_losses: 0 },
    { team_id: 4, team: "UConn", rank: 4, rating: 23.6, di_wins: 4, di_losses: 1 },
  ];
  const previous = [
    { team_id: 1, rank: 3 },
    { team_id: 2, rank: 1 },
    { team_id: 3, rank: 3 },
  ];
  const out = withMovement(current, previous);
  assert.equal(out[0].movement.kind, "up");
  assert.equal(out[0].movement.delta, 2);
  assert.equal(out[1].movement.kind, "down");
  assert.equal(out[1].movement.delta, 1);
  assert.equal(out[2].movement.kind, "same");
  assert.equal(out[3].movement.kind, "new");
  assert.equal(out[3].prev_rank, null);
  assert.equal(movementLabel(out[0].movement), "Up 2");
  assert.equal(movementLabel(out[3].movement), "New");
});

test("withMovement: missing prior poll (unknown) falls back to stored prev_rank, or dash if none", () => {
  const stored = [
    { team_id: 1, team: "Purdue", rank: 1, prev_rank: 3, rating: 28, di_wins: 4, di_losses: 0 },
    { team_id: 2, team: "Duke", rank: 2, prev_rank: null, rating: 27, di_wins: 5, di_losses: 0 },
  ];
  const fromStored = withMovement(stored, undefined);
  assert.equal(fromStored[0].movement.kind, "up");
  assert.equal(fromStored[0].movement.delta, 2);
  assert.equal(fromStored[1].movement.kind, "new");

  const firstPoll = [
    { team_id: 1, team: "Purdue", rank: 1, prev_rank: null, rating: 28, di_wins: 4, di_losses: 0 },
  ];
  const dashed = withMovement(firstPoll, null);
  assert.equal(dashed[0].movement.kind, "none");
});

test("sortRankings is by rank, then team name, and does not mutate the input", () => {
  const rows = [
    { rank: 2, team: "Duke" },
    { rank: 1, team: "Arizona" },
    { rank: 2, team: "Auburn" },
    { rank: 25, team: "Zed" },
  ];
  const copy = rows.slice();
  const out = sortRankings(rows);
  assert.deepEqual(out.map((r) => r.team), ["Arizona", "Auburn", "Duke", "Zed"]);
  assert.deepEqual(rows, copy);
  assert.equal(TOP_N, 25);
});

test("week and as-of labels are calendar dates in CT copy; early season covers Nov–Jan", () => {
  assert.equal(fmtPollDate("2026-11-16", { year: true }), "Nov 16, 2026");
  assert.equal(weekLabel("2026-11-16"), "Week of Nov 16, 2026");
  assert.equal(updatedLabel("2026-11-21"), "Updated Nov 21 CT");
  assert.equal(fmtPollDate("nope"), null);
  assert.equal(isEarlySeason("2026-11-16"), true);
  assert.equal(isEarlySeason("2026-12-28"), true);
  assert.equal(isEarlySeason("2027-01-04"), true);
  assert.equal(isEarlySeason("2027-02-01"), false);
});

test("the snapshot select never requests unused W-L columns", () => {
  assert.equal(RANKING_SNAPSHOT_COLUMNS.includes("di_wins"), true);
  assert.equal(RANKING_SNAPSHOT_COLUMNS.includes("di_losses"), true);
  assert.equal(/kp_/i.test(RANKING_SNAPSHOT_COLUMNS), false);
  assert.equal(/kenpom/i.test(RANKING_SNAPSHOT_COLUMNS), false);
});

test("movement demo rows cover up, down, same, and New", () => {
  const kinds = movementDemoRows().map((r) => r.movement.kind);
  assert.deepEqual(kinds, ["up", "down", "same", "new", "up"]);
});

test("no change and no previous poll use distinct marks and aria labels", () => {
  const same = rankMovement(3, 3);
  const none = { kind: "none" as const, delta: 0 };
  assert.equal(movementMark(same), "=");
  assert.equal(movementLabel(same), "No change");
  assert.equal(movementMark(none), "—");
  assert.equal(movementLabel(none), "No previous poll");
  assert.notEqual(movementMark(same), movementMark(none));
});

test("long-name Top 25 fixture includes Texas A&M-Corpus Christi and stays dashed (no prior poll)", () => {
  const rows = longNameDemoRows();
  assert.ok(rows.some((r) => r.team === "Texas A&M-Corpus Christi" && r.rank <= TOP_N));
  assert.ok(rows.every((r) => r.movement.kind === "none"));
});

test("rankingsErrorKind maps network failures vs data/schema errors, and copy never leaks schema details", () => {
  assert.equal(rankingsErrorKind({ message: "fetch failed" }), "unreachable");
  assert.equal(rankingsErrorKind({ message: "Failed to fetch", code: "" }), "unreachable");
  assert.equal(rankingsErrorKind({ code: "ETIMEDOUT", message: "connect timeout" }), "unreachable");
  assert.equal(rankingsErrorKind({ code: "42703", message: "column ranking_snapshots.foo does not exist" }), "unavailable");
  assert.equal(rankingsErrorKind({ code: "PGRST204", message: "Could not find the 'rating' column" }), "unavailable");
  assert.equal(rankingsErrorKind({ message: "boom", code: "500" }), "unavailable");
  assert.equal(rankingsErrorKind(null), "unreachable");
  const down = rankingsErrorCopy("unreachable");
  const bad = rankingsErrorCopy("unavailable");
  assert.equal(down.title, "Couldn't load the rankings");
  assert.match(down.body, /didn't get a response/i);
  assert.equal(bad.title, "Rankings are temporarily unavailable");
  for (const text of [down.title, down.body, bad.title, bad.body]) {
    assert.equal(/schema|column|postgres|postgrest|42703|kp_/i.test(text), false);
  }
});

type Call = { table: string; cols: string; eq: Record<string, unknown>; lt?: Record<string, unknown> };

function fakeRankingsClient(opts: {
  latest?: { week_of: string; as_of: string }[];
  latestError?: { message: string; code?: string };
  rows?: Record<string, unknown>[];
  rowsError?: { message: string; code?: string };
  prevWeek?: { week_of: string }[];
  prevWeekError?: { message: string; code?: string };
  prevRows?: Record<string, unknown>[];
}) {
  const calls: Call[] = [];
  const from = (table: string) => {
    const st: Call = { table, cols: "", eq: {} };
    calls.push(st);
    const run = () => {
      const hasWeek = st.eq.week_of != null;
      const lookingPrevWeek = st.cols === "week_of" && st.lt?.week_of != null;
      const lookingPrevRows = st.cols === "team_id,rank" && hasWeek;
      if (lookingPrevWeek) {
        if (opts.prevWeekError) return Promise.resolve({ data: null, error: opts.prevWeekError });
        return Promise.resolve({ data: opts.prevWeek ?? [], error: null });
      }
      if (lookingPrevRows) {
        return Promise.resolve({ data: opts.prevRows ?? [], error: null });
      }
      if (!hasWeek && st.cols.includes("week_of")) {
        if (opts.latestError) return Promise.resolve({ data: null, error: opts.latestError });
        return Promise.resolve({ data: opts.latest ?? [], error: null });
      }
      if (hasWeek) {
        if (opts.rowsError) return Promise.resolve({ data: null, error: opts.rowsError });
        return Promise.resolve({ data: opts.rows ?? [], error: null });
      }
      return Promise.resolve({ data: [], error: null });
    };
    const chain: Record<string, unknown> = {};
    for (const m of ["eq", "in", "gte", "gt", "lt", "order", "limit", "range", "not", "or", "abortSignal"]) {
      chain[m] = (...a: unknown[]) => {
        if (m === "eq") st.eq[String(a[0])] = a[1];
        if (m === "lt") st.lt = { ...(st.lt ?? {}), [String(a[0])]: a[1] };
        return chain;
      };
    }
    chain.select = (cols: string) => {
      st.cols = cols;
      return chain;
    };
    chain.then = (res: (v: unknown) => unknown, rej: (e: unknown) => unknown) => run().then(res, rej);
    return chain;
  };
  return { from, calls } as never as { from: typeof from; calls: Call[] };
}

const sport = { key: "cbb", league: "ncaam", status: "live" } as never;

function row(partial: Record<string, unknown>) {
  return {
    team_id: 1,
    rank: 1,
    prev_rank: null,
    rating: 28.84,
    di_wins: 4,
    di_losses: 0,
    as_of: "2026-11-21",
    week_of: "2026-11-16",
    season: 2027,
    teams: { name: "Purdue" },
    ...partial,
  };
}

test("loadRankingsFrom: first poll (no prior week) is live, sorted, dashed movement", async () => {
  const sb = fakeRankingsClient({
    latest: [{ week_of: "2026-11-16", as_of: "2026-11-21" }],
    rows: [
      row({ team_id: 2, rank: 2, rating: 27.4, teams: { name: "Duke" }, di_wins: 5, di_losses: 0 }),
      row({ team_id: 1, rank: 1, rating: 28.84, teams: { name: "Purdue" } }),
    ],
    prevWeek: [],
  });
  const r = await loadRankingsFrom(sb as never, sport);
  assert.equal(r.state, "live");
  if (r.state !== "live") return;
  assert.equal(r.data.week_of, "2026-11-16");
  assert.equal(r.data.as_of, "2026-11-21");
  assert.deepEqual(r.data.rows.map((x) => x.team), ["Purdue", "Duke"]);
  assert.equal(fmtRating(r.data.rows[0].rating), "+28.8");
  assert.equal(fmtRecord(r.data.rows[0].di_wins, r.data.rows[0].di_losses), "4–0");
  assert.ok(r.data.rows.every((x) => x.movement.kind === "none"));
  const select = (sb as unknown as { calls: Call[] }).calls.find((c) => c.cols.includes("di_wins"));
  assert.ok(select && !/kp_/i.test(select.cols));
});

test("loadRankingsFrom: mocked prior week attaches up/down/same/new", async () => {
  const sb = fakeRankingsClient({
    latest: [{ week_of: "2026-11-23", as_of: "2026-11-23" }],
    rows: [
      row({ team_id: 1, rank: 1, week_of: "2026-11-23", as_of: "2026-11-23", teams: { name: "Purdue" } }),
      row({ team_id: 2, rank: 2, week_of: "2026-11-23", as_of: "2026-11-23", rating: 27.4, teams: { name: "Duke" } }),
      row({ team_id: 3, rank: 3, week_of: "2026-11-23", as_of: "2026-11-23", rating: 25.1, teams: { name: "Houston" } }),
      row({ team_id: 4, rank: 4, week_of: "2026-11-23", as_of: "2026-11-23", rating: 23.6, teams: { name: "UConn" } }),
    ],
    prevWeek: [{ week_of: "2026-11-16" }],
    prevRows: [
      { team_id: 1, rank: 3 },
      { team_id: 2, rank: 1 },
      { team_id: 3, rank: 3 },
    ],
  });
  const r = await loadRankingsFrom(sb as never, sport);
  assert.equal(r.state, "live");
  if (r.state !== "live") return;
  assert.deepEqual(r.data.rows.map((x) => x.movement.kind), ["up", "down", "same", "new"]);
});

test("loadRankingsFrom: query error is an error state; 0 rows is a live empty poll", async () => {
  const err = await loadRankingsFrom(
    fakeRankingsClient({ latestError: { message: "boom", code: "500" } }) as never,
    sport,
  );
  assert.equal(err.state, "error");
  if (err.state === "error") assert.equal(err.reason, "unavailable");
  const net = await loadRankingsFrom(
    fakeRankingsClient({ latestError: { message: "fetch failed" } }) as never,
    sport,
  );
  assert.equal(net.state, "error");
  if (net.state === "error") assert.equal(net.reason, "unreachable");

  const empty = await loadRankingsFrom(fakeRankingsClient({ latest: [] }) as never, sport);
  assert.equal(empty.state, "live");
  if (empty.state !== "live") return;
  assert.equal(empty.data.rows.length, 0);

  const unconfigured = await loadRankingsFrom(null, sport);
  assert.equal(unconfigured.state, "unconfigured");
});
