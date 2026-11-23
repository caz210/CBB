import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import {
  clampRoundedMinutes,
  clockFromEspnPayload,
  clockFromEspnStatus,
  isEspnEventId,
  isEspnFinal,
  isEspnHalftime,
  parseDisplayClock,
  periodFromEspnPeriod,
  scoresFromEspnPayload,
  statusFromEspnPayload,
} from "../src/lib/espnClock";

test("parseDisplayClock: 12:34, 0:45.2, 20:00, and garbage", () => {
  assert.equal(parseDisplayClock("12:34")?.toFixed(4), (12 + 34 / 60).toFixed(4));
  assert.equal(parseDisplayClock("0:45.2")?.toFixed(4), (45.2 / 60).toFixed(4));
  assert.equal(parseDisplayClock("20:00"), 20);
  assert.equal(parseDisplayClock("5:00"), 5);
  assert.equal(parseDisplayClock("0:00"), 0);
  assert.equal(parseDisplayClock("8"), 8);
  assert.equal(parseDisplayClock(""), null);
  assert.equal(parseDisplayClock(null), null);
  assert.equal(parseDisplayClock("halftime"), null);
  assert.equal(parseDisplayClock("12:60"), null);
  assert.equal(parseDisplayClock("abc"), null);
  assert.equal(parseDisplayClock(":45"), null);
});

test("period 1 = 1H, 2 = 2H, 3 and 4 = OT; missing is null", () => {
  assert.equal(periodFromEspnPeriod(1), "1H");
  assert.equal(periodFromEspnPeriod("1"), "1H");
  assert.equal(periodFromEspnPeriod(2), "2H");
  assert.equal(periodFromEspnPeriod(3), "OT");
  assert.equal(periodFromEspnPeriod(4), "OT");
  assert.equal(periodFromEspnPeriod(0), null);
  assert.equal(periodFromEspnPeriod(null), null);
  assert.equal(periodFromEspnPeriod("OT"), null);
});

test("0:00 or 0:xx clamps to 1; 20:00 and 5:00 stay at the top of the slider", () => {
  assert.equal(clampRoundedMinutes("1H", 0), 1);
  assert.equal(clampRoundedMinutes("2H", 45.2 / 60), 1);
  assert.equal(clampRoundedMinutes("1H", 20), 20);
  assert.equal(clampRoundedMinutes("OT", 5), 5);
  assert.equal(clampRoundedMinutes("OT", 0), 1);
  assert.equal(clampRoundedMinutes("OT", 9), 5);
  assert.equal(clampRoundedMinutes("2H", 12 + 34 / 60), 13); // 12:34 → 13
});

test("halftime status sets Halftime even if the clock is 0:00", () => {
  const c = clockFromEspnStatus({
    period: 2,
    displayClock: "0:00",
    type: { name: "STATUS_HALFTIME", description: "Halftime", state: "in" },
  });
  assert.deepEqual(c, { period: "HALFTIME", minutesLeft: 20 });
  assert.equal(isEspnHalftime({ type: { description: "Halftime" } }), true);
  assert.equal(isEspnHalftime({ type: { detail: "12:34 - 2nd Half" } }), false);
});

test("end of 1H: period 1 and clock 0:00 is 1H at 1, not Halftime", () => {
  const c = clockFromEspnStatus({
    period: 1,
    displayClock: "0:00",
    type: { name: "STATUS_END_PERIOD", description: "End of 1st Half", state: "in" },
  });
  assert.deepEqual(c, { period: "1H", minutesLeft: 1 });
});

test("OT periods 3 and 4 use the OT slider (5–1)", () => {
  assert.deepEqual(
    clockFromEspnStatus({ period: 3, displayClock: "5:00", type: { name: "STATUS_IN_PROGRESS", state: "in" } }),
    { period: "OT", minutesLeft: 5 },
  );
  assert.deepEqual(
    clockFromEspnStatus({ period: 4, displayClock: "0:45.2", type: { name: "STATUS_IN_PROGRESS", state: "in" } }),
    { period: "OT", minutesLeft: 1 },
  );
  assert.deepEqual(
    clockFromEspnStatus({ period: 3, displayClock: "2:20", type: { name: "STATUS_IN_PROGRESS", state: "in" } }),
    { period: "OT", minutesLeft: 2 },
  );
});

test("12:34 in 2H rounds to 13 min left", () => {
  assert.deepEqual(
    clockFromEspnStatus({ period: 2, displayClock: "12:34", type: { name: "STATUS_IN_PROGRESS", state: "in" } }),
    { period: "2H", minutesLeft: 13 },
  );
  assert.deepEqual(
    clockFromEspnStatus({ period: 1, displayClock: "20:00", type: { name: "STATUS_IN_PROGRESS", state: "in" } }),
    { period: "1H", minutesLeft: 20 },
  );
});

test("missing or garbled input, and finals, return null", () => {
  assert.equal(clockFromEspnStatus(null), null);
  assert.equal(clockFromEspnStatus({}), null);
  assert.equal(clockFromEspnStatus({ period: 2, displayClock: "nope", type: { state: "in" } }), null);
  assert.equal(clockFromEspnStatus({ period: 2, type: { state: "in" } }), null);
  assert.equal(clockFromEspnStatus({ displayClock: "12:00", type: { state: "in" } }), null);
  const fin = { period: 2, displayClock: "0:00", type: { name: "STATUS_FINAL", state: "post", completed: true, description: "Final" } };
  assert.equal(isEspnFinal(fin), true);
  assert.equal(clockFromEspnStatus(fin), null);
});

test("status and scores walk summary and scoreboard payloads", () => {
  const summary = {
    header: {
      competitions: [{
        status: { period: 2, displayClock: "8:00", type: { name: "STATUS_IN_PROGRESS", state: "in" } },
        competitors: [
          { homeAway: "home", score: "54" },
          { homeAway: "away", score: "49" },
        ],
      }],
    },
  };
  assert.deepEqual(clockFromEspnPayload(summary), { period: "2H", minutesLeft: 8 });
  assert.deepEqual(scoresFromEspnPayload(summary), { home: 54, away: 49 });
  const board = {
    events: [{ competitions: [{ status: { period: 1, displayClock: "3:10", type: { state: "in" } } }] }],
  };
  assert.ok(statusFromEspnPayload(board));
  assert.deepEqual(clockFromEspnPayload(board), { period: "1H", minutesLeft: 3 });
});

test("espn event ids are numeric; mapping lives in games.external_id", () => {
  assert.equal(isEspnEventId("401920905"), true);
  assert.equal(isEspnEventId("abc"), false);
  assert.equal(isEspnEventId(""), false);
  const root = [resolve(__dirname, "../src"), resolve(__dirname, "../../src")].find((d) => {
    try { readFileSync(resolve(d, "lib/data.ts")); return true; } catch { return false; }
  });
  assert.ok(root);
  const src = readFileSync(resolve(root, "lib/data.ts"), "utf8");
  assert.match(src, /external_id/);
});
