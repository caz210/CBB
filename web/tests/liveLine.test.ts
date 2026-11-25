import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync, readdirSync } from "node:fs";
import { resolve } from "node:path";
import {
  SHARE_1H, SHARE_2H, MARGIN_SD, HALF_MIN, OT_MIN,
  remainingShare, timeLeftMin, roundToHalf, phi, erf, liveLine,
  defaultLiveState, defaultLiveScores, normalizeClock, sliderBounds,
  LIVE_LINE_DISCLAIMER, nextLiveLinePeriod, LIVE_LINE_PERIODS,
  displayLiveSpread, projectedFinalScores, fmtProjectedScore,
} from "../src/lib/liveLine";
import { fmtOne, fmtSpread } from "../src/lib/labels";

const duke = {
  czarpTotal: 151,
  projHome: 76.2, // Duke
  projAway: 72.0, // Michigan St.
};

test("1H at 20 equals tip: remaining share is 1 and the rounded total is the stored official total", () => {
  assert.equal(remainingShare("1H", 20), 1);
  assert.equal(remainingShare("TIP", 99), 1);
  assert.deepEqual(normalizeClock("TIP", 0), { period: "1H", minutesLeft: 20 });
  const L = liveLine({ ...duke, homeScore: 0, awayScore: 0, period: "1H", minutesLeft: 20 });
  assert.ok(L);
  assert.equal(L!.remShare, 1);
  assert.equal(L!.liveTotal, 151);
  assert.equal(L!.timeLeftMin, 40);
  // live_away − live_home, nearest half. 151 split by 76.2/72 → about −4.3, rounds to −4.5.
  assert.equal(L!.liveSpread, -4.5);
});

test("slider endpoints: 1H 20/1, Halftime, 2H 20/1, OT 5/1", () => {
  const s = (period: "1H" | "2H" | "OT" | "HALFTIME", m: number) => remainingShare(period, m);
  assert.equal(SHARE_1H, 0.47);
  assert.equal(SHARE_2H, 0.53);
  assert.equal(s("1H", 20), 1);
  assert.equal(s("1H", 1), 0.5535);
  assert.equal(s("HALFTIME", 0), 0.53);
  assert.equal(s("2H", 20), 0.53);
  assert.equal(s("HALFTIME", 99), s("2H", 20));
  assert.equal(s("2H", 10), 0.265);
  assert.equal(s("2H", 1), SHARE_2H * (1 / 20));
  assert.equal(s("OT", 5), 5 / 40);
  assert.equal(s("OT", 1), 1 / 40);
  assert.deepEqual(sliderBounds("1H"), { min: 1, max: 20 });
  assert.deepEqual(sliderBounds("2H"), { min: 1, max: 20 });
  assert.deepEqual(sliderBounds("OT"), { min: 1, max: 5 });
  assert.equal(sliderBounds("HALFTIME"), null);
  assert.equal(timeLeftMin("1H", 1), 21);
  assert.equal(timeLeftMin("2H", 1), 1);
  assert.equal(timeLeftMin("OT", 5), 5);
  assert.equal(timeLeftMin("HALFTIME", 0), 20);
});

test("Halftime matches 2H at 20: remaining is 53% of the official total, split by stored team scores", () => {
  const a = liveLine({ ...duke, homeScore: 34, awayScore: 25, period: "HALFTIME", minutesLeft: 20 });
  const b = liveLine({ ...duke, homeScore: 34, awayScore: 25, period: "2H", minutesLeft: 20 });
  assert.ok(a && b);
  assert.equal(a!.remShare, SHARE_2H);
  assert.equal(a!.liveTotal, b!.liveTotal);
  assert.equal(a!.liveSpread, b!.liveSpread);
  assert.equal(a!.liveTotal, 139);
});

test("round to the nearest half-point; PK at 0", () => {
  assert.equal(roundToHalf(138.275), 138.5);
  assert.equal(roundToHalf(-4.279), -4.5);
  assert.equal(roundToHalf(-4.2), -4);
  assert.equal(roundToHalf(0.24), 0);
  assert.equal(roundToHalf(0.25), 0.5);
});

test("win % uses Phi(margin / (12.5 * sqrt(time_left/40))); 0:00 is 100/0 or 50/50", () => {
  assert.equal(MARGIN_SD, 12.5);
  assert.ok(Math.abs(phi(0) - 0.5) < 1e-7);
  assert.ok(Math.abs(phi(1.0) - 0.841344746) < 1e-4);
  assert.ok(Math.abs(erf(0)) < 1e-7);
  const tied = liveLine({ ...duke, homeScore: 0, awayScore: 0, period: "2H", minutesLeft: 0 });
  // 2H at 0: rem_total = 0, scores 0-0 → tied at 0:00 → 50/50
  assert.ok(tied);
  assert.equal(tied!.homeWinProb, 0.5);
  const homeLead = liveLine({ czarpTotal: 140, projHome: 70, projAway: 70, homeScore: 80, awayScore: 70, period: "2H", minutesLeft: 0 });
  assert.equal(homeLead!.homeWinProb, 1);
  assert.equal(homeLead!.awayWinProb, 0);
  const awayLead = liveLine({ czarpTotal: 140, projHome: 70, projAway: 70, homeScore: 70, awayScore: 80, period: "2H", minutesLeft: 0 });
  assert.equal(awayLead!.homeWinProb, 0);
  assert.equal(awayLead!.awayWinProb, 1);
  const mid = liveLine({ ...duke, homeScore: 34, awayScore: 25, period: "2H", minutesLeft: 10 });
  assert.ok(mid);
  const sd = 12.5 * Math.sqrt(10 / 40);
  assert.ok(Math.abs(mid!.homeWinProb - phi(mid!.liveMargin / sd)) < 1e-12);
});

test("uses stored proj values only; falls back to proj_spread when a team score is missing; null when there is no total", () => {
  const full = liveLine({ ...duke, homeScore: 0, awayScore: 0, period: "1H", minutesLeft: 20 });
  const viaSpread = liveLine({
    czarpTotal: 151, projSpread: 72 - 76.2, homeScore: 0, awayScore: 0, period: "1H", minutesLeft: 20,
  });
  assert.ok(full && viaSpread);
  assert.equal(viaSpread!.liveTotal, 151);
  // Spread fallback reconstructs team scores from official total + stored proj_spread
  // (home + away may not equal official total, so the half-point may differ from the ratio path).
  assert.equal(viaSpread!.liveSpread, -4);
  assert.equal(full!.liveSpread, -4.5);
  assert.equal(liveLine({ homeScore: 0, awayScore: 0, period: "1H", minutesLeft: 20 }), null);
});

test("defaults: scheduled is 1H at 20 with 0-0; in_progress is 2H at 20 with stored scores", () => {
  assert.deepEqual(defaultLiveState("scheduled"), { period: "1H", minutesLeft: 20 });
  assert.deepEqual(defaultLiveState(null), { period: "1H", minutesLeft: 20 });
  assert.deepEqual(defaultLiveState("in_progress"), { period: "2H", minutesLeft: 20 });
  assert.deepEqual(defaultLiveScores("scheduled", { home_score: 10, away_score: 8 }), { home: 0, away: 0 });
  assert.deepEqual(defaultLiveScores("in_progress", { home_score: 34, away_score: 25 }), { home: 34, away: 25 });
  assert.deepEqual(defaultLiveScores("in_progress", {}), { home: 0, away: 0 });
  assert.equal(HALF_MIN, 20);
  assert.equal(OT_MIN, 5);
  // A scheduled-but-tipped row (LUC vs SJSU) must not use stale DB scores; ESPN fills those in.
  assert.deepEqual(defaultLiveState("scheduled"), { period: "1H", minutesLeft: 20 });
  assert.deepEqual(defaultLiveScores("scheduled", { home_score: 59, away_score: 66 }), { home: 0, away: 0 });
  assert.deepEqual(defaultLiveState("final"), { period: "FINAL", minutesLeft: 0 });
  assert.deepEqual(defaultLiveScores("final", { home_score: 72, away_score: 68 }), { home: 72, away: 68 });
  assert.equal(remainingShare("FINAL", 0), 0);
  const done = liveLine({ czarpTotal: 151, projHome: 76.2, projAway: 72, homeScore: 72, awayScore: 68, period: "FINAL", minutesLeft: 0 });
  assert.ok(done);
  assert.equal(done.remShare, 0);
  assert.equal(done.liveTotal, 140);
  assert.equal(done.liveSpread, -4);
  assert.equal(done.homeWinProb, 1);
});

test("disclaimer is the customer copy; live line source has no book line, Edge %, paper, KenPom, or v1/v2", () => {
  assert.match(LIVE_LINE_DISCLAIMER, /This is an estimate, not an official CZarp play\. It never goes on the official record\./);
  // Resolve src from both the TypeScript tests/ tree and the compiled .test-build/tests/ tree.
  const root = [resolve(__dirname, "../src"), resolve(__dirname, "../../src")].find((d) => {
    try { readFileSync(resolve(d, "lib/liveLine.ts")); return true; } catch { return false; }
  });
  assert.ok(root, "src/lib/liveLine.ts");
  const files = [
    "lib/liveLine.ts", "lib/espnClock.ts", "lib/liveClock.ts", "components/LiveLinePanel.tsx", "app/api/live-clock/route.ts",
  ];
  for (const rel of files) {
    const src = readFileSync(resolve(root, rel), "utf8");
    // Headers may name the things we do not implement ("No book line, no Edge %"). Scan the code only.
    const code = src.replace(/\/\*[\s\S]*?\*\//g, "").replace(/\/\/.*$/gm, "");
    assert.doesNotMatch(code, /KenPom|v1\/v2|\bpaper\b|book line|Edge %|edge_pct|bookSpread|bookTotal/i, rel);
  }
});

test("period radiogroup: Arrow, Home, and End move between 1H, Halftime, 2H, OT", () => {
  assert.deepEqual([...LIVE_LINE_PERIODS], ["1H", "HALFTIME", "2H", "OT"]);
  assert.equal(nextLiveLinePeriod("1H", "ArrowRight"), "HALFTIME");
  assert.equal(nextLiveLinePeriod("HALFTIME", "ArrowRight"), "2H");
  assert.equal(nextLiveLinePeriod("2H", "ArrowRight"), "OT");
  assert.equal(nextLiveLinePeriod("OT", "ArrowRight"), "1H");
  assert.equal(nextLiveLinePeriod("1H", "ArrowDown"), "HALFTIME");
  assert.equal(nextLiveLinePeriod("1H", "ArrowLeft"), "OT");
  assert.equal(nextLiveLinePeriod("OT", "ArrowLeft"), "2H");
  assert.equal(nextLiveLinePeriod("2H", "ArrowUp"), "HALFTIME");
  assert.equal(nextLiveLinePeriod("2H", "Home"), "1H");
  assert.equal(nextLiveLinePeriod("1H", "Home"), "1H");
  assert.equal(nextLiveLinePeriod("1H", "End"), "OT");
  assert.equal(nextLiveLinePeriod("OT", "End"), "OT");
  assert.equal(nextLiveLinePeriod("2H", "Enter"), null);
  assert.equal(nextLiveLinePeriod("FINAL", "Home"), "1H");
  assert.equal(nextLiveLinePeriod("FINAL", "End"), "OT");
});


test("Rice vs Illinois Chicago (neutral): leading away favorite displays as Rice -1.5, not +1.5 / Rice", () => {
  // Christian's screenshot: Rice 64, Illinois Chicago 62, 2H, 3:00 left.
  // UIC is in the home slot on a neutral floor; Rice is away and leading.
  // Pregame official total 146.5; remaining split leaves Rice a 1.5-pt live favorite at 67%.
  const rice = {
    czarpTotal: 146.5,
    projHome: 76.4, // Illinois Chicago, pregame favorite
    projAway: 70.1, // Rice
    homeScore: 62,
    awayScore: 64,
    period: "2H" as const,
    minutesLeft: 3,
  };
  const L = liveLine(rice);
  assert.ok(L);
  assert.equal(L.liveTotal, 137.5);
  // LIVE TOTAL vs-pregame delta is unchanged (still from rounded live total − rounded official total).
  assert.equal(L.liveTotal - 146.5, -9);
  assert.equal(`${L.liveTotal - 146.5 > 0 ? "+" : ""}${fmtOne(L.liveTotal - 146.5)} vs pregame`, "-9.0 vs pregame");
  // Internal value stays home-line (away − home): positive because the away team is ahead in the projection.
  assert.equal(L.liveSpread, 1.5);
  const view = displayLiveSpread(L.liveSpread, "Illinois Chicago", "Rice");
  assert.equal(view.line, -1.5);
  assert.equal(fmtSpread(view.line), "-1.5");
  assert.equal(view.team, "Rice");
  const pHome = Math.round(L.homeWinProb * 100);
  const pAway = 100 - pHome;
  assert.equal(pAway, 67);
  assert.equal(pHome, 33);
  const winTeam = pHome >= pAway ? "Illinois Chicago" : "Rice";
  assert.equal(view.team, winTeam);
  const proj = projectedFinalScores(L.liveTotal, L.liveSpread);
  assert.equal(proj.away, 69.5);
  assert.equal(proj.home, 68);
  assert.equal(fmtProjectedScore(proj.away), "69.5");
  assert.equal(fmtProjectedScore(proj.home), "68");
  assert.equal(proj.away + proj.home, L.liveTotal);
  // Swapping the slots still names Rice as the -1.5 favorite.
  const swapped = liveLine({
    czarpTotal: 146.5,
    projHome: 70.1,
    projAway: 76.4,
    homeScore: 64,
    awayScore: 62,
    period: "2H",
    minutesLeft: 3,
  });
  assert.ok(swapped);
  assert.equal(swapped.liveTotal, 137.5);
  assert.equal(swapped.liveSpread, -1.5);
  const viewHome = displayLiveSpread(swapped.liveSpread, "Rice", "Illinois Chicago");
  assert.equal(viewHome.line, -1.5);
  assert.equal(viewHome.team, "Rice");
  assert.equal(fmtSpread(viewHome.line), "-1.5");
});

test("live spread display: favorite is always negative, labeled with that team; PK at zero", () => {
  // Home favorite: home-line is already negative, display stays negative / home.
  assert.deepEqual(displayLiveSpread(-4.5, "Duke", "Michigan St."), { line: -4.5, team: "Duke" });
  assert.equal(fmtSpread(displayLiveSpread(-4.5, "Duke", "Michigan St.").line), "-4.5");
  // Away favorite: home-line is positive; flip the sign so the label and the number agree.
  assert.deepEqual(displayLiveSpread(1.5, "Illinois Chicago", "Rice"), { line: -1.5, team: "Rice" });
  assert.equal(fmtSpread(displayLiveSpread(1.5, "Illinois Chicago", "Rice").line), "-1.5");
  // Pick 'em.
  assert.deepEqual(displayLiveSpread(0, "Duke", "Michigan St."), { line: 0, team: "Pick 'em" });
  assert.equal(fmtSpread(0), "PK");
  assert.deepEqual(displayLiveSpread(-0, "Duke", "Michigan St."), { line: 0, team: "Pick 'em" });
});

test("live spread follows the live favorite, even when that team is trailing or was the pregame dog", () => {
  // Home is trailing 60-64 but is still the live favorite (large remaining share, home was the pregame favorite).
  const homeTrail = liveLine({
    czarpTotal: 160, projHome: 90, projAway: 70,
    homeScore: 60, awayScore: 64, period: "1H", minutesLeft: 20,
  });
  assert.ok(homeTrail);
  assert.ok(homeTrail.liveSpread < 0);
  assert.ok(homeTrail.homeWinProb > 0.5);
  const homeView = displayLiveSpread(homeTrail.liveSpread, "Home", "Away");
  assert.ok(homeView.line < 0);
  assert.equal(homeView.team, "Home");
  assert.equal(homeView.team, homeTrail.homeWinProb >= homeTrail.awayWinProb ? "Home" : "Away");
  // Away is trailing 60-64 but is still the live favorite.
  const awayTrail = liveLine({
    czarpTotal: 160, projHome: 70, projAway: 90,
    homeScore: 64, awayScore: 60, period: "1H", minutesLeft: 20,
  });
  assert.ok(awayTrail);
  assert.ok(awayTrail.liveSpread > 0);
  assert.ok(awayTrail.awayWinProb > 0.5);
  const awayView = displayLiveSpread(awayTrail.liveSpread, "Home", "Away");
  assert.equal(awayView.line, -awayTrail.liveSpread);
  assert.ok(awayView.line < 0);
  assert.equal(awayView.team, "Away");
});

test("projected final scores come from the displayed live total and spread only", () => {
  // Rice / UIC: 137.5 and 1.5 (away favorite) → Rice 69.5, Illinois Chicago 68.
  assert.deepEqual(projectedFinalScores(137.5, 1.5), { away: 69.5, home: 68 });
  // Same total and magnitude, home favorite.
  assert.deepEqual(projectedFinalScores(137.5, -1.5), { home: 69.5, away: 68 });
  // Pick 'em with a whole total splits evenly.
  assert.deepEqual(projectedFinalScores(140, 0), { home: 70, away: 70 });
  assert.equal(fmtProjectedScore(70), "70");
  assert.equal(fmtProjectedScore(69.5), "69.5");
  // Odd half-point total with a whole spread: preserve the total (fav rounded, dog = total − fav).
  const odd = projectedFinalScores(139.5, -1);
  assert.equal(odd.home + odd.away, 139.5);
  assert.equal(odd.home, 70.5);
  assert.equal(odd.away, 69);
  // Pick 'em on a half-point total keeps both scores equal (may not sum to the total).
  const pk = projectedFinalScores(137.5, 0);
  assert.equal(pk.home, pk.away);
  assert.equal(pk.home, 69);
});
