import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync, readdirSync } from "node:fs";
import { resolve } from "node:path";
import {
  SHARE_1H, SHARE_2H, MARGIN_SD, HALF_MIN, OT_MIN,
  remainingShare, timeLeftMin, roundToHalf, phi, erf, liveLine,
  defaultLiveState, defaultLiveScores, normalizeClock, sliderBounds,
  LIVE_LINE_DISCLAIMER,
} from "../src/lib/liveLine";

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
});

test("disclaimer is the customer copy; live line source has no book line, Edge %, paper, KenPom, or v1/v2", () => {
  assert.match(LIVE_LINE_DISCLAIMER, /This is an estimate, not an official CZarp play\. It never goes on the official record\./);
  const root = resolve(__dirname, "../src");
  const files = [
    "lib/liveLine.ts", "lib/espnClock.ts", "components/LiveLinePanel.tsx", "app/api/live-clock/route.ts",
  ].map((f) => resolve(root, f));
  for (const f of files) {
    let src: string;
    try { src = readFileSync(f, "utf8"); } catch { continue; }
    assert.doesNotMatch(src, /KenPom|v1\/v2|\bpaper\b|book line|Edge %|edge_pct|bookSpread|bookTotal/i, f);
  }
});
