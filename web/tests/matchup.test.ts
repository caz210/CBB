// Plain node:test + tsc, no extra deps. Run: npm test
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  ageHours, favorite, fetchMatchup, firstUnknown, fmtAsOf, fmtScore, fmtSpread, spreadParts, fmtVenueDelta, fmtWinPct, isStale, mapRpcError,
  matchupHref, parseMatchup, parseMatchupParams, swapVenue, venueDelta, venueLabel, winBarPct, type Matchup,
} from "../src/lib/matchup";

const NEUTRAL = {
  team_a: 39, team_b: 255, venue: "neutral", as_of: "2026-11-19T01:31:48+00:00",
  score_a: 72.2, score_b: 72.8, total: 145.0, spread_a: 0.6, win_pct_a: 0.479, win_pct_b: 0.521,
};
const A_HOME = { ...NEUTRAL, venue: "a_home", score_a: 74.0, score_b: 70.5, total: 144.5, spread_a: -3.5, win_pct_a: 0.621, win_pct_b: 0.379 };
const m = (o: Partial<Matchup> = {}) => ({ ...NEUTRAL, ...o }) as Matchup;

// ---- formatting ----------------------------------------------------------------------------------------------

test("scores show the RPC's exact value: one decimal, or two only when it ends in .x5", () => {
  assert.equal(fmtScore(72.2), "72.2");
  assert.equal(fmtScore(74), "74.0");
  assert.equal(fmtScore(145), "145.0");
  assert.equal(fmtScore(70.5), "70.5");
  assert.equal(fmtScore(68.15), "68.15");
  assert.equal(fmtScore(81.85), "81.85");
  assert.equal(fmtScore(104.15), "104.15"); // used to display as 104.2
  assert.equal(fmtScore(68.85), "68.85");   // used to display as 68.8
  assert.equal(fmtScore(65.05), "65.05");
  assert.equal(fmtScore(84.95), "84.95");
  assert.equal(fmtScore(72.20), "72.2");    // numeric(…,2) "72.20" arrives as 72.2
  assert.equal(fmtScore(71.5), "71.5");
});

// cents as an integer, so the checks below are exact (no float noise)
const cents = (shown: string) => Math.round(Number(shown) * 100);

test("displayed score gap equals the displayed spread and displayed sum equals the total, for every .x5 case", () => {
  // Rebuild the RPC's rule: total on a 0.5 grid, spread on a 0.1 grid, score_a=(total-spread)/2, score_b=(total+spread)/2.
  // The Texas St./Duke values from the live RPC come first.
  const cases: [number, number][] = [[150.0, 13.7], [150.0, 16.4], [150.0, 19.9], [145.0, 0.6], [144.5, -3.5], [161.0, -8.0]];
  for (let t = 100; t <= 190; t += 0.5) for (let sp = -40; sp <= 40; sp = Math.round((sp + 0.1) * 10) / 10) cases.push([t, sp]);
  let x5 = 0;
  for (const [total, spread] of cases) {
    const aC = Math.round((total * 100 - spread * 100) / 2), bC = Math.round((total * 100 + spread * 100) / 2);
    const sa = aC / 100, sb = bC / 100;
    const shownA = fmtScore(sa), shownB = fmtScore(sb);
    if (shownA.split(".")[1].length === 2) x5++;
    // the page shows spread to one decimal and total to one decimal
    const shownSpread = Math.round(spread * 10), shownTotal = Math.round(total * 10);
    assert.equal(cents(shownB) - cents(shownA), shownSpread * 10, `gap for total ${total} spread ${spread}: ${shownA} / ${shownB}`);
    assert.equal(cents(shownA) + cents(shownB), shownTotal * 10, `sum for total ${total} spread ${spread}`);
  }
  assert.ok(x5 > 1000, `expected plenty of .x5 cases, got ${x5}`); // about half of all pairs
});

test("Texas St. vs Duke with Texas St. home: 68.15 - 81.85 sits next to Duke \u221213.7 and the gap is 13.7", () => {
  const mm = m({ team_a: 520, team_b: 78, venue: "a_home", score_a: 68.15, score_b: 81.85, total: 150, spread_a: 13.7, win_pct_a: 0.115, win_pct_b: 0.885 });
  assert.equal(fmtScore(mm.score_a), "68.15");
  assert.equal(fmtScore(mm.score_b), "81.85");
  assert.equal(fmtSpread(mm, "Texas St.", "Duke"), "Duke \u221213.7");
  assert.equal((cents(fmtScore(mm.score_b)) - cents(fmtScore(mm.score_a))) / 100, 13.7);
  assert.equal(fmtScore(mm.total), "150.0");
});

test("firstUnknown flags ids missing from the team list so the RPC can be skipped", () => {
  const known = (id: number) => id === 520 || id === 78;
  assert.equal(firstUnknown(520, 78, known), null);
  assert.equal(firstUnknown(999999, 78, known), "a");
  assert.equal(firstUnknown(520, 999999, known), "b");
  assert.equal(firstUnknown(null, 999999, known), "b");
  assert.equal(firstUnknown(null, null, known), null);
});

test("win % shows one decimal and the bounds become <0.1% and >99.9%", () => {
  assert.equal(fmtWinPct(0.479), "47.9%");
  assert.equal(fmtWinPct(0.521), "52.1%");
  assert.equal(fmtWinPct(0.5), "50.0%");
  assert.equal(fmtWinPct(0), "<0.1%");
  assert.equal(fmtWinPct(1), ">99.9%");
  assert.equal(fmtWinPct(0.0004), "<0.1%");
  assert.equal(fmtWinPct(0.9996), ">99.9%");
  assert.equal(fmtWinPct(0.001), "0.1%");
  assert.equal(fmtWinPct(0.999), "99.9%");
});

test("win bar keeps both sides visible at the extremes", () => {
  assert.equal(winBarPct(0.479), 47.9);
  assert.equal(winBarPct(0), 1);
  assert.equal(winBarPct(1), 99);
});

test("spread is in betting notation for the favorite", () => {
  assert.equal(fmtSpread({ spread_a: -3.5 }, "Texas St.", "Duke"), "Texas St. \u22123.5"); // A favored: negative
  assert.equal(fmtSpread({ spread_a: 0.6 }, "Auburn", "West Virginia"), "West Virginia \u22120.6"); // B favored
  assert.equal(fmtSpread({ spread_a: 12 }, "A", "B"), "B \u221212.0");
  assert.equal(fmtSpread({ spread_a: 0 }, "A", "B"), "Pick'em");
  assert.equal(favorite({ spread_a: -0.1 }), "a");
  assert.equal(favorite({ spread_a: 0.1 }), "b");
  assert.equal(favorite({ spread_a: 0 }), null);
});

test("stale flag turns on after 30 hours", () => {
  const asOf = "2026-11-19T01:31:48+00:00";
  const at = (h: number) => new Date(Date.parse(asOf) + h * 3_600_000);
  assert.equal(isStale(asOf, at(0)), false);
  assert.equal(isStale(asOf, at(29.9)), false);
  assert.equal(isStale(asOf, at(30)), false);
  assert.equal(isStale(asOf, at(30.1)), true);
  assert.equal(isStale(asOf, at(-1)), false); // slightly-future clock skew is not stale
  assert.equal(isStale("not a date", at(0)), true);
  assert.equal(Math.round(ageHours(asOf, at(5))!), 5);
});

test("as-of is shown in Central time with a CT label, whatever the viewer's zone", () => {
  assert.equal(fmtAsOf("2026-11-19T01:31:48+00:00"), "Nov 18, 7:31 PM CT"); // CST, UTC-6
  assert.equal(fmtAsOf("2026-07-04T17:05:00Z"), "Jul 4, 12:05 PM CT"); // CDT, UTC-5
  assert.equal(fmtAsOf("2026-11-19T06:00:00Z"), "Nov 19, 12:00 AM CT"); // CT date, not the UTC date
  assert.equal(fmtAsOf("nope"), null);
});

test("venue labels and swap", () => {
  assert.equal(venueLabel("neutral", "A", "B"), "Neutral court");
  assert.equal(venueLabel("a_home", "Texas St.", "Duke"), "At Texas St.");
  assert.equal(venueLabel("b_home", "Texas St.", "Duke"), "At Duke");
  assert.equal(swapVenue("a_home"), "b_home");
  assert.equal(swapVenue("b_home"), "a_home");
  assert.equal(swapVenue("neutral"), "neutral");
});

test("vs-neutral delta is the difference between two RPC replies", () => {
  const n = parseMatchup(NEUTRAL)!, h = parseMatchup(A_HOME)!;
  const d = venueDelta(n, h)!;
  assert.deepEqual(d, { home: "a", spread: 4.1, total: -0.5 }); // Auburn's margin moves from -0.6 to +3.5
  assert.equal(fmtVenueDelta(d, "Auburn", "WVU"), "Auburn +4.1 pts vs neutral \u00B7 total \u22120.5");
  assert.equal(venueDelta(n, n), null);
  // team B at home: A's spread rises, B's margin improves
  const bh = m({ venue: "b_home", spread_a: 4.7, total: 145 });
  assert.deepEqual(venueDelta(n, bh), { home: "b", spread: 4.1, total: 0 });
  assert.equal(fmtVenueDelta({ home: "b", spread: 0, total: 0 }, "A", "B"), "B no change vs neutral");
});

// ---- RPC reply parsing + error mapping -------------------------------------------------------------------------

test("parseMatchup accepts the RPC shape (numbers or numeric strings) and rejects junk", () => {
  assert.deepEqual(parseMatchup(NEUTRAL)?.score_a, 72.2);
  assert.equal(parseMatchup({ ...NEUTRAL, score_a: "72.20", total: "145.0" })?.total, 145);
  assert.equal(parseMatchup([NEUTRAL])?.team_a, 39);
  assert.equal(parseMatchup({ ...NEUTRAL, venue: "home" }), null);
  assert.equal(parseMatchup({ ...NEUTRAL, win_pct_a: null }), null);
  assert.equal(parseMatchup({ ...NEUTRAL, as_of: undefined }), null);
  assert.equal(parseMatchup(null), null);
  assert.equal(parseMatchup("x"), null);
});

test("RPC errors map to friendly messages, never raw database text", () => {
  const same = mapRpcError({ code: "22023", message: "team_a and team_b must be different teams (both are 39)" });
  assert.equal(same.kind, "invalid");
  assert.ok(!same.message.includes("team_a"));
  assert.equal(mapRpcError({ code: "22P02", message: 'invalid input syntax for type bigint: "x"' }).kind, "bad_id");
  assert.equal(mapRpcError({ code: "55000" }).message, "Projections are updating, try again shortly.");
  assert.equal(mapRpcError({ code: "55000" }).kind, "updating");
  for (const e of [{ code: "XX000", message: "boom" }, { message: "network" }, null, undefined]) {
    const r = mapRpcError(e);
    assert.equal(r.kind, "other");
    assert.ok(!/boom|network/.test(r.message));
  }
});

test("fetchMatchup passes anon-RPC args through and maps every failure", async () => {
  const calls: unknown[] = [];
  const ok = { rpc: (fn: string, args: Record<string, unknown>) => { calls.push([fn, args]); return Promise.resolve({ data: NEUTRAL, error: null }); } };
  const r = await fetchMatchup(ok, 39, 255, "neutral");
  assert.ok(r.ok && r.data.total === 145);
  assert.deepEqual(calls[0], ["project_matchup", { team_a: 39, team_b: 255, venue: "neutral" }]);

  const bad = await fetchMatchup({ rpc: () => Promise.resolve({ data: null, error: { code: "55000", message: "inputs not loaded" } }) }, 1, 2, "a_home");
  assert.ok(!bad.ok && bad.error.kind === "updating");
  const junk = await fetchMatchup({ rpc: () => Promise.resolve({ data: { hello: 1 }, error: null }) }, 1, 2, "neutral");
  assert.ok(!junk.ok && junk.error.kind === "other");
  const boom = await fetchMatchup({ rpc: () => Promise.reject(new Error("offline")) }, 1, 2, "neutral");
  assert.ok(!boom.ok && boom.error.kind === "other");
});

// ---- URL state -----------------------------------------------------------------------------------------------

test("URL state parses ?a=&b=&venue= and never throws", () => {
  assert.deepEqual(parseMatchupParams({ a: "39", b: "255", venue: "a_home" }), { a: 39, b: 255, venue: "a_home" });
  assert.deepEqual(parseMatchupParams({ a: "39" }), { a: 39, b: null, venue: "neutral" });
  assert.deepEqual(parseMatchupParams({}), { a: null, b: null, venue: "neutral" });
  assert.deepEqual(parseMatchupParams({ a: "abc", b: "-4", venue: "zzz" }), { a: null, b: null, venue: "neutral" });
  assert.deepEqual(parseMatchupParams({ a: "0", b: "1.5" }), { a: null, b: null, venue: "neutral" });
  assert.deepEqual(parseMatchupParams({ a: "99999999999999999999" }), { a: null, b: null, venue: "neutral" });
  assert.deepEqual(parseMatchupParams({ a: ["7", "8"], venue: ["b_home"] }), { a: 7, b: null, venue: "b_home" });
  assert.deepEqual(parseMatchupParams(new URLSearchParams("a=1&b=2&venue=b_home")), { a: 1, b: 2, venue: "b_home" });
  assert.deepEqual(parseMatchupParams({ venue: "A_HOME" }), { a: null, b: null, venue: "neutral" });
});

test("matchupHref round-trips through parseMatchupParams", () => {
  assert.equal(matchupHref("cbb"), "/cbb/matchup");
  assert.equal(matchupHref("cbb", { a: 520 }), "/cbb/matchup?a=520");
  assert.equal(matchupHref("cbb", { a: 520, venue: "neutral" }), "/cbb/matchup?a=520&venue=neutral");
  const href = matchupHref("cbb", { a: 520, b: 78, venue: "a_home" });
  assert.equal(href, "/cbb/matchup?a=520&b=78&venue=a_home");
  assert.deepEqual(parseMatchupParams(new URL(href, "https://x.test").searchParams), { a: 520, b: 78, venue: "a_home" });
});

test("spreadParts keeps the number apart from the name (UI truncates only the name)", () => {
  assert.deepEqual(spreadParts({ spread_a: 13.7 }, "Texas St.", "Duke"), { name: "Duke", value: "\u221213.7" });
  assert.deepEqual(spreadParts({ spread_a: -3.5 }, "Western Carolina", "Norfolk St."), { name: "Western Carolina", value: "\u22123.5" });
  assert.equal(spreadParts({ spread_a: 0 }, "A", "B"), null);
});
