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
  phaseFromEspnStatus,
  feedFromEspnStatus,
  applyEspnFeed,
  shouldFetchLiveFeed,
  competitorsFromEspnPayload,
  matchEspnScores,
  bindFeedScores,
  feedFromEspnPayload,
  teamMatchKey,
  teamAliasKeys,
  LIVE_LINE_TEAM_ALIAS_GROUPS,
  type EspnCompetitor,
  type LiveLineUiState,
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

const scheduledDefaults = (): LiveLineUiState => ({ period: "1H", minutesLeft: 20, home: 0, away: 0, fromFeed: false });
const staleInProgress = (): LiveLineUiState => ({ period: "2H", minutesLeft: 20, home: 10, away: 8, fromFeed: false });

test("ESPN pre keeps the panel defaults; in prefills period, clock, and scores; post is Final", () => {
  const pre = { period: 1, displayClock: "20:00", type: { name: "STATUS_SCHEDULED", state: "pre", description: "Scheduled" } };
  assert.equal(phaseFromEspnStatus(pre), "pre");
  const afterPre = applyEspnFeed(scheduledDefaults(), feedFromEspnStatus(pre, { home: 0, away: 0 })!, false);
  assert.deepEqual(afterPre, { period: "1H", minutesLeft: 20, home: 0, away: 0, fromFeed: false });

  const live = { period: 2, displayClock: "3:36", type: { name: "STATUS_IN_PROGRESS", state: "in", description: "In Progress" } };
  assert.equal(phaseFromEspnStatus(live), "in");
  const afterIn = applyEspnFeed(scheduledDefaults(), feedFromEspnStatus(live, { home: 59, away: 66 })!, false);
  assert.equal(afterIn.period, "2H");
  assert.equal(afterIn.minutesLeft, 4); // 3:36 rounds to 4
  assert.equal(afterIn.home, 59);
  assert.equal(afterIn.away, 66);
  assert.equal(afterIn.fromFeed, true);

  const post = { period: 2, displayClock: "0:00", type: { name: "STATUS_FINAL", state: "post", completed: true, description: "Final" } };
  assert.equal(phaseFromEspnStatus(post), "post");
  const afterPost = applyEspnFeed(scheduledDefaults(), feedFromEspnStatus(post, { home: 72, away: 68 })!, false);
  assert.deepEqual(afterPost, { period: "FINAL", minutesLeft: 0, home: 72, away: 68, fromFeed: true });
});

test("ESPN post wins over a stale DB in_progress default", () => {
  const post = { period: 2, displayClock: "0:00", type: { name: "STATUS_FINAL", state: "post", completed: true, description: "Final" } };
  const next = applyEspnFeed(staleInProgress(), feedFromEspnStatus(post, { home: 81, away: 77 })!, false);
  assert.deepEqual(next, { period: "FINAL", minutesLeft: 0, home: 81, away: 77, fromFeed: true });
  // dirty edits are left alone
  assert.deepEqual(applyEspnFeed(staleInProgress(), feedFromEspnStatus(post, { home: 81, away: 77 })!, true), staleInProgress());
});

test("shouldFetchLiveFeed: scheduled-but-tipped (LUC vs SJSU) fetches; too-early and closed games do not", () => {
  const luc = { externalId: "401911233", status: "scheduled", tipoff: "2026-11-23T04:59:00.000Z" };
  assert.equal(shouldFetchLiveFeed({ ...luc, now: Date.parse("2026-11-23T05:30:00.000Z") }), true);
  assert.equal(shouldFetchLiveFeed({ ...luc, now: Date.parse("2026-11-23T04:54:00.000Z") }), true); // tip - 5 min
  assert.equal(shouldFetchLiveFeed({ ...luc, now: Date.parse("2026-11-23T04:53:59.000Z") }), false);
  assert.equal(shouldFetchLiveFeed({ ...luc, status: "in_progress", now: Date.parse("2026-11-23T05:30:00.000Z") }), true);
  assert.equal(shouldFetchLiveFeed({ ...luc, status: "final", now: Date.parse("2026-11-23T08:00:00.000Z") }), false);
  assert.equal(shouldFetchLiveFeed({ ...luc, status: "postponed", now: Date.parse("2026-11-23T05:30:00.000Z") }), false);
  assert.equal(shouldFetchLiveFeed({ ...luc, status: "canceled", now: Date.parse("2026-11-23T05:30:00.000Z") }), false);
  assert.equal(shouldFetchLiveFeed({ externalId: null, status: "scheduled", tipoff: luc.tipoff, now: Date.parse("2026-11-23T05:30:00.000Z") }), false);
});

const lucSjsuPayload = {
  header: {
    competitions: [{
      status: { period: 2, displayClock: "0:00", type: { name: "STATUS_FINAL", state: "post", completed: true, description: "Final" } },
      competitors: [
        { homeAway: "home", score: "63", id: "23", team: { id: "23", displayName: "San José State Spartans", abbreviation: "SJSU", location: "San José State", name: "Spartans", nickname: "San José St" } },
        { homeAway: "away", score: "72", id: "2350", team: { id: "2350", displayName: "Loyola Chicago Ramblers", abbreviation: "LUC", location: "Loyola Chicago", name: "Ramblers", nickname: "Loyola Chicago" } },
      ],
    }],
  },
};

const oursLucHome = { home: "Loyola Chicago", away: "San Jose St." };

test("neutral-site swap: LUC vs SJSU maps ESPN's home (SJSU 63) onto our away", () => {
  assert.equal(teamMatchKey("San Jose St."), teamMatchKey("San José State"));
  const competitors = competitorsFromEspnPayload(lucSjsuPayload);
  assert.ok(competitors);
  // ESPN's own sides are the reverse of ours
  assert.equal(scoresFromEspnPayload(lucSjsuPayload)!.home, 63);
  assert.equal(scoresFromEspnPayload(lucSjsuPayload)!.away, 72);
  const scores = matchEspnScores(oursLucHome, competitors);
  assert.deepEqual(scores, { home: 72, away: 63 });
  const feed = bindFeedScores(feedFromEspnPayload(lucSjsuPayload)!, oursLucHome);
  assert.ok(feed);
  assert.deepEqual(feed.scores, { home: 72, away: 63 });
  const next = applyEspnFeed(scheduledDefaults(), feed, false);
  assert.equal(next.period, "FINAL");
  assert.equal(next.home, 72); // Loyola Chicago
  assert.equal(next.away, 63); // San Jose St.
});

test("normal home/away: ESPN sides already match ours", () => {
  const payload = {
    header: {
      competitions: [{
        competitors: [
          { homeAway: "home", score: "80", team: { id: "150", displayName: "Duke Blue Devils", abbreviation: "DUKE", location: "Duke" } },
          { homeAway: "away", score: "70", team: { id: "152", displayName: "Wake Forest Demon Deacons", abbreviation: "WAKE", location: "Wake Forest" } },
        ],
      }],
    },
  };
  const scores = matchEspnScores({ home: "Duke", away: "Wake Forest" }, competitorsFromEspnPayload(payload));
  assert.deepEqual(scores, { home: 80, away: 70 });
});

test("ambiguous or unmatched pairs do not prefill", () => {
  const competitors = competitorsFromEspnPayload(lucSjsuPayload);
  // Both of our names are the same team — SJSU hits both sides equally.
  assert.equal(matchEspnScores({ home: "San Jose St.", away: "San José State" }, competitors), null);
  // Neither name matches the payload.
  assert.equal(matchEspnScores({ home: "Duke", away: "Wake Forest" }, competitors), null);
  // Unbound teams: the feed is kept (clock/phase) but scores stay null — never guessed.
  assert.equal(bindFeedScores(feedFromEspnPayload(lucSjsuPayload)!, { home: "Duke", away: "Wake Forest" }).scores, null);
  const post = { period: 2, displayClock: "0:00", type: { name: "STATUS_FINAL", state: "post", completed: true, description: "Final" } };
  assert.deepEqual(applyEspnFeed(scheduledDefaults(), feedFromEspnStatus(post, null)!, false), scheduledDefaults());
});

test("partial match (only one side) means no prefill", () => {
  const competitors = competitorsFromEspnPayload(lucSjsuPayload);
  assert.equal(matchEspnScores({ home: "Loyola Chicago", away: "Wake Forest" }, competitors), null);
  assert.equal(matchEspnScores({ home: "Duke", away: "San Jose St." }, competitors), null);
  assert.equal(bindFeedScores(feedFromEspnPayload(lucSjsuPayload)!, { home: "Loyola Chicago", away: "Wake Forest" }).scores, null);
});

test("stored ESPN team ids match even when the names would not", () => {
  const competitors = competitorsFromEspnPayload(lucSjsuPayload);
  const scores = matchEspnScores(
    { home: "Team A", away: "Team B", homeEspnId: "2350", awayEspnId: "23" },
    competitors,
  );
  assert.deepEqual(scores, { home: 72, away: 63 });
});

function espnTeam(score: number, homeAway: "home" | "away", labels: {
  displayName: string; abbreviation: string; location: string; name?: string; nickname?: string; id?: string;
}): EspnCompetitor {
  return {
    score,
    homeAway,
    espnId: labels.id ?? null,
    labels: [labels.displayName, labels.abbreviation, labels.location, labels.name, labels.nickname].filter((x): x is string => !!x),
  };
}

test("site aliases: UIC, UAlbany, California Baptist, UT Martin, Seattle U match our canonical names", () => {
  assert.ok(LIVE_LINE_TEAM_ALIAS_GROUPS.length === 16);
  assert.ok(teamAliasKeys("Illinois Chicago").has(teamMatchKey("UIC")));
  assert.ok(teamAliasKeys("UIC").has(teamMatchKey("Illinois Chicago")));

  const uic = espnTeam(60, "home", { displayName: "UIC Flames", abbreviation: "UIC", location: "UIC", name: "Flames", nickname: "UIC" });
  const miamiOh = espnTeam(71, "away", { displayName: "Miami (OH) RedHawks", abbreviation: "M-OH", location: "Miami (OH)", nickname: "Miami OH" });
  assert.deepEqual(matchEspnScores({ home: "Illinois Chicago", away: "Miami OH" }, [uic, miamiOh]), { home: 60, away: 71 });
  // Bidirectional: if our row already said UIC
  assert.deepEqual(matchEspnScores({ home: "UIC", away: "Miami OH" }, [uic, miamiOh]), { home: 60, away: 71 });

  const ualbany = espnTeam(75, "home", { displayName: "UAlbany Great Danes", abbreviation: "UALB", location: "UAlbany", name: "Great Danes", nickname: "UAlbany" });
  const mercyhurst = espnTeam(78, "away", { displayName: "Mercyhurst Lakers", abbreviation: "MERC", location: "Mercyhurst", nickname: "Mercyhurst" });
  assert.deepEqual(matchEspnScores({ home: "Albany", away: "Mercyhurst" }, [ualbany, mercyhurst]), { home: 75, away: 78 });

  const calBap = espnTeam(79, "home", { displayName: "California Baptist Lancers", abbreviation: "CBU", location: "California Baptist", name: "Lancers", nickname: "CA Baptist" });
  const portland = espnTeam(71, "away", { displayName: "Portland Pilots", abbreviation: "PORT", location: "Portland", nickname: "Portland" });
  assert.deepEqual(matchEspnScores({ home: "Cal Baptist", away: "Portland" }, [calBap, portland]), { home: 79, away: 71 });

  const utm = espnTeam(76, "away", { displayName: "UT Martin Skyhawks", abbreviation: "UTM", location: "UT Martin", name: "Skyhawks", nickname: "UT Martin" });
  const uvu = espnTeam(78, "home", { displayName: "Utah Valley Wolverines", abbreviation: "UVU", location: "Utah Valley", nickname: "Utah Valley" });
  assert.deepEqual(matchEspnScores({ home: "Utah Valley", away: "Tennessee Martin" }, [uvu, utm]), { home: 78, away: 76 });

  const seattleU = espnTeam(90, "home", { displayName: "Seattle U Redhawks", abbreviation: "SEA", location: "Seattle U", name: "Redhawks", nickname: "Seattle U" });
  const ucsd = espnTeam(64, "away", { displayName: "UC San Diego Tritons", abbreviation: "UCSD", location: "UC San Diego", nickname: "UC San Diego" });
  assert.deepEqual(matchEspnScores({ home: "Seattle", away: "UC San Diego" }, [seattleU, ucsd]), { home: 90, away: 64 });
});

test("aliases do not create collisions with Illinois, Albany St., Tennessee, California, or other Seattle schools", () => {
  const uic = espnTeam(60, "home", { displayName: "UIC Flames", abbreviation: "UIC", location: "UIC", name: "Flames", nickname: "UIC" });
  const illinois = espnTeam(80, "home", { displayName: "Illinois Fighting Illini", abbreviation: "ILL", location: "Illinois", name: "Fighting Illini", nickname: "Illinois" });
  const iowa = espnTeam(70, "away", { displayName: "Iowa Hawkeyes", abbreviation: "IOWA", location: "Iowa", nickname: "Iowa" });
  // UIC is not Illinois
  assert.equal(matchEspnScores({ home: "Illinois", away: "Iowa" }, [uic, iowa]), null);
  // Illinois Chicago vs Illinois stays uniquely assigned
  assert.deepEqual(matchEspnScores({ home: "Illinois Chicago", away: "Illinois" }, [uic, illinois]), { home: 60, away: 80 });

  const albanySt = espnTeam(81, "home", { displayName: "Albany State Golden Rams", abbreviation: "ALST", location: "Albany State", name: "Golden Rams", nickname: "Albany St." });
  const mercyhurst = espnTeam(78, "away", { displayName: "Mercyhurst Lakers", abbreviation: "MERC", location: "Mercyhurst", nickname: "Mercyhurst" });
  const ualbany = espnTeam(75, "home", { displayName: "UAlbany Great Danes", abbreviation: "UALB", location: "UAlbany", name: "Great Danes", nickname: "UAlbany" });
  // Albany (NY) must not swallow Albany St. (GA)
  assert.equal(matchEspnScores({ home: "Albany", away: "Mercyhurst" }, [albanySt, mercyhurst]), null);
  // A real Albany vs Albany St. game still maps uniquely
  assert.deepEqual(matchEspnScores({ home: "Albany", away: "Albany St." }, [ualbany, albanySt]), { home: 75, away: 81 });

  const utm = espnTeam(76, "away", { displayName: "UT Martin Skyhawks", abbreviation: "UTM", location: "UT Martin", name: "Skyhawks", nickname: "UT Martin" });
  const kentucky = espnTeam(85, "home", { displayName: "Kentucky Wildcats", abbreviation: "UK", location: "Kentucky", nickname: "Kentucky" });
  const tennessee = espnTeam(90, "home", { displayName: "Tennessee Volunteers", abbreviation: "TENN", location: "Tennessee", name: "Volunteers", nickname: "Tennessee" });
  assert.equal(matchEspnScores({ home: "Tennessee", away: "Kentucky" }, [utm, kentucky]), null);
  assert.deepEqual(matchEspnScores({ home: "Tennessee", away: "Tennessee Martin" }, [tennessee, utm]), { home: 90, away: 76 });

  const calBap = espnTeam(79, "home", { displayName: "California Baptist Lancers", abbreviation: "CBU", location: "California Baptist", name: "Lancers", nickname: "CA Baptist" });
  const stanford = espnTeam(68, "away", { displayName: "Stanford Cardinal", abbreviation: "STAN", location: "Stanford", nickname: "Stanford" });
  const california = espnTeam(65, "home", { displayName: "California Golden Bears", abbreviation: "CAL", location: "California", name: "Golden Bears", nickname: "California" });
  assert.equal(matchEspnScores({ home: "California", away: "Stanford" }, [calBap, stanford]), null);
  assert.deepEqual(matchEspnScores({ home: "California", away: "Cal Baptist" }, [california, calBap]), { home: 65, away: 79 });

  const seattleU = espnTeam(90, "home", { displayName: "Seattle U Redhawks", abbreviation: "SEA", location: "Seattle U", name: "Redhawks", nickname: "Seattle U" });
  const seattlePac = espnTeam(55, "home", { displayName: "Seattle Pacific Falcons", abbreviation: "SPU", location: "Seattle Pacific", name: "Falcons", nickname: "Seattle Pacific" });
  const ucsd = espnTeam(64, "away", { displayName: "UC San Diego Tritons", abbreviation: "UCSD", location: "UC San Diego", nickname: "UC San Diego" });
  assert.equal(matchEspnScores({ home: "Seattle", away: "UC San Diego" }, [seattlePac, ucsd]), null);
  assert.equal(matchEspnScores({ home: "Seattle Pacific", away: "UC San Diego" }, [seattleU, ucsd]), null);
  assert.deepEqual(matchEspnScores({ home: "Seattle", away: "Seattle Pacific" }, [seattleU, seattlePac]), { home: 90, away: 55 });
});

// Live 11/23 10:20 AM CT: game 401927397, St. Bonaventure @ Appalachian St. (ESPN summary, trimmed).
const appStPayload = {
  header: {
    competitions: [{
      status: { period: 1, displayClock: "8:07", type: { id: "2", name: "STATUS_IN_PROGRESS", state: "in", completed: false, description: "In Progress", detail: "8:07 - 1st Half" } },
      competitors: [
        { homeAway: "home", score: "14", id: "2026", team: { id: "2026", displayName: "App State Mountaineers", abbreviation: "APP", location: "App State", name: "Mountaineers", shortDisplayName: "App State" } },
        { homeAway: "away", score: "19", id: "179", team: { id: "179", displayName: "St. Bonaventure Bonnies", abbreviation: "SBU", location: "St. Bonaventure", name: "Bonnies", shortDisplayName: "St Bonaventure" } },
      ],
    }],
  },
};
const oursAppSt = { home: "Appalachian St.", away: "St. Bonaventure" };

test("401927397: App State binds to Appalachian St. by name (no stored ids)", () => {
  assert.ok(teamAliasKeys("Appalachian St.").has(teamMatchKey("App State")));
  assert.ok(teamAliasKeys("App State").has(teamMatchKey("Appalachian St.")));
  const feed = bindFeedScores(feedFromEspnPayload(appStPayload)!, oursAppSt);
  assert.deepEqual(feed.scores, { home: 14, away: 19 });
  const next = applyEspnFeed(scheduledDefaults(), feed, false);
  assert.deepEqual(next, { period: "1H", minutesLeft: 8, home: 14, away: 19, fromFeed: true });
});

test("401927397: stored ESPN ids (2026 / 179) bind exactly, including a neutral-style flip", () => {
  const competitors = competitorsFromEspnPayload(appStPayload);
  assert.deepEqual(matchEspnScores({ ...oursAppSt, homeEspnId: "2026", awayEspnId: "179" }, competitors), { home: 14, away: 19 });
  // Our sides reversed vs ESPN's: ids still map the right score to the right team.
  assert.deepEqual(matchEspnScores({ home: "St. Bonaventure", away: "Appalachian St.", homeEspnId: "179", awayEspnId: "2026" }, competitors), { home: 19, away: 14 });
});

test("ESPN ids win over names; one known id is enough; contradicting ids fall back to names", () => {
  const competitors = competitorsFromEspnPayload(appStPayload);
  // Names that would never match, ids decide.
  assert.deepEqual(matchEspnScores({ home: "Team X", away: "Team Y", homeEspnId: "2026", awayEspnId: "179" }, competitors), { home: 14, away: 19 });
  // Only our away id stored; the other competitor is our home.
  assert.deepEqual(matchEspnScores({ home: "Team X", away: "Team Y", awayEspnId: "179" }, competitors), { home: 14, away: 19 });
  assert.deepEqual(matchEspnScores({ home: "Team X", away: "Team Y", homeEspnId: "179" }, competitors), { home: 19, away: 14 });
  // Our away id matches but our home id names a third team: ids contradict, so names decide.
  assert.deepEqual(matchEspnScores({ ...oursAppSt, homeEspnId: "9999", awayEspnId: "179" }, competitors), { home: 14, away: 19 });
  assert.equal(matchEspnScores({ home: "Team X", away: "Team Y", homeEspnId: "9999", awayEspnId: "179" }, competitors), null);
  // Same id on both of our sides is bad data: ignore ids.
  assert.equal(matchEspnScores({ home: "Team X", away: "Team Y", homeEspnId: "179", awayEspnId: "179" }, competitors), null);
  // Stale ids that match neither competitor: names still work.
  assert.deepEqual(matchEspnScores({ ...oursAppSt, homeEspnId: "1", awayEspnId: "2" }, competitors), { home: 14, away: 19 });
});

test("clock fallback: unbound teams still prefill period + clock; scores stay manual and are never swapped", () => {
  const feed = bindFeedScores(feedFromEspnPayload(appStPayload)!, { home: "Team X", away: "Team Y" });
  assert.equal(feed.scores, null);
  assert.deepEqual(feed.clock, { period: "1H", minutesLeft: 8 });
  const manual: LiveLineUiState = { period: "1H", minutesLeft: 20, home: 11, away: 17, fromFeed: false };
  const next = applyEspnFeed(manual, feed, false);
  assert.deepEqual(next, { period: "1H", minutesLeft: 8, home: 11, away: 17, fromFeed: true, scoresFromFeed: false });
  // Dirty panel is left alone.
  assert.deepEqual(applyEspnFeed(manual, feed, true), manual);
  // Halftime via the clock fallback.
  const half = { period: 1, displayClock: "0:00", type: { name: "STATUS_HALFTIME", state: "in", description: "Halftime" } };
  const h = applyEspnFeed(manual, feedFromEspnStatus(half, null)!, false);
  assert.equal(h.period, "HALFTIME");
  assert.equal(h.home, 11);
  // A final with unbound teams stays manual (no Final with made-up scores).
  const post = { period: 2, displayClock: "0:00", type: { name: "STATUS_FINAL", state: "post", completed: true, description: "Final" } };
  assert.deepEqual(applyEspnFeed(manual, feedFromEspnStatus(post, null)!, false), manual);
  // A later bound feed fills the scores and clears the clock-only flag.
  const bound = applyEspnFeed(next, bindFeedScores(feedFromEspnPayload(appStPayload)!, oursAppSt), false);
  assert.deepEqual(bound, { period: "1H", minutesLeft: 8, home: 14, away: 19, fromFeed: true });
});

test("Grambling St. matches ESPN's Grambling Tigers; it does not collide with other Tigers", () => {
  const gram = espnTeam(70, "home", { displayName: "Grambling Tigers", abbreviation: "GRAM", location: "Grambling", name: "Tigers", nickname: "Grambling" });
  const jsu = espnTeam(65, "away", { displayName: "Jackson State Tigers", abbreviation: "JKST", location: "Jackson State", name: "Tigers", nickname: "Jackson St" });
  assert.deepEqual(matchEspnScores({ home: "Grambling St.", away: "Jackson St." }, [gram, jsu]), { home: 70, away: 65 });
  const tsu = espnTeam(60, "away", { displayName: "Texas Southern Tigers", abbreviation: "TXSO", location: "Texas Southern", name: "Tigers", nickname: "Texas Southern" });
  assert.deepEqual(matchEspnScores({ home: "Texas Southern", away: "Grambling St." }, [gram, tsu]), { home: 60, away: 70 });
});

test("season-sweep aliases match ESPN locations and don't collide with neighbours", () => {
  const t = (score: number, ha: "home" | "away", displayName: string, location: string, abbreviation: string) =>
    espnTeam(score, ha, { displayName, location, abbreviation });
  const oleMiss = t(80, "home", "Ole Miss Rebels", "Ole Miss", "MISS");
  const msSt = t(70, "away", "Mississippi State Bulldogs", "Mississippi State", "MSST");
  assert.deepEqual(matchEspnScores({ home: "Mississippi", away: "Mississippi St." }, [oleMiss, msSt]), { home: 80, away: 70 });
  const clemson = t(66, "away", "Clemson Tigers", "Clemson", "CLEM");
  assert.deepEqual(matchEspnScores({ home: "Clemson", away: "Mississippi" }, [oleMiss, clemson]), { home: 66, away: 80 });

  const ncsu = t(75, "home", "NC State Wolfpack", "NC State", "NCSU");
  const unc = t(77, "away", "North Carolina Tar Heels", "North Carolina", "UNC");
  assert.deepEqual(matchEspnScores({ home: "N.C. State", away: "North Carolina" }, [ncsu, unc]), { home: 75, away: 77 });

  const uconn = t(81, "home", "UConn Huskies", "UConn", "CONN");
  const mich = t(79, "away", "Michigan Wolverines", "Michigan", "MICH");
  assert.deepEqual(matchEspnScores({ home: "Connecticut", away: "Michigan" }, [uconn, mich]), { home: 81, away: 79 });

  const miaFl = t(88, "home", "Miami Hurricanes", "Miami", "MIA");
  const miaOh = t(84, "away", "Miami (OH) RedHawks", "Miami (OH)", "M-OH");
  assert.deepEqual(matchEspnScores({ home: "Miami FL", away: "Miami OH" }, [miaFl, miaOh]), { home: 88, away: 84 });
  const wagner = t(50, "away", "Wagner Seahawks", "Wagner", "WAG");
  assert.equal(matchEspnScores({ home: "Miami OH", away: "Wagner" }, [miaFl, wagner]), null);

  const amcc = t(68, "away", "Texas A&M-Corpus Christi Islanders", "Texas A&M-Corpus Christi", "AMCC");
  const tamu = t(90, "home", "Texas A&M Aggies", "Texas A&M", "TA&M");
  assert.deepEqual(matchEspnScores({ home: "Texas A&M", away: "Texas A&M Corpus Chris" }, [tamu, amcc]), { home: 90, away: 68 });

  const upst = t(61, "away", "South Carolina Upstate Spartans", "South Carolina Upstate", "UPST");
  const sc = t(73, "home", "South Carolina Gamecocks", "South Carolina", "SC");
  assert.deepEqual(matchEspnScores({ home: "South Carolina", away: "USC Upstate" }, [sc, upst]), { home: 73, away: 61 });

  const ulm = t(59, "away", "UL Monroe Warhawks", "UL Monroe", "ULM");
  const ul = t(71, "home", "Louisiana Ragin' Cajuns", "Louisiana", "UL");
  assert.deepEqual(matchEspnScores({ home: "Louisiana", away: "Louisiana Monroe" }, [ul, ulm]), { home: 71, away: 59 });

  const sela = t(64, "away", "SE Louisiana Lions", "SE Louisiana", "SELA");
  const lsu = t(82, "home", "LSU Tigers", "LSU", "LSU");
  assert.deepEqual(matchEspnScores({ home: "LSU", away: "Southeastern Louisiana" }, [lsu, sela]), { home: 82, away: 64 });

  const omaha = t(70, "away", "Omaha Mavericks", "Omaha", "OMA");
  const neb = t(85, "home", "Nebraska Cornhuskers", "Nebraska", "NEB");
  assert.deepEqual(matchEspnScores({ home: "Nebraska", away: "Nebraska Omaha" }, [neb, omaha]), { home: 85, away: 70 });
});
