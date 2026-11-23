import { test } from "node:test";
import assert from "node:assert/strict";
import {
  SLATE_FUTURE_EMPTY_BODY,
  SLATE_MATCHUP_CTA_LABEL,
  SLATE_PAST_EMPTY_BODY,
  SLATE_TODAY_EMPTY_BODY,
  showSlateLiveBadge,
  showSlateMatchupCta,
  slateDateRelation,
  slateEmptyBody,
  slateMatchupHref,
  SLATE_UNPOSTED_EMPTY_BODY,
} from "../src/lib/slateEmpty";
import { OFFICIAL_POST_TIME } from "../src/lib/officialPost";

const TODAY = "2026-11-21";

test("slateDateRelation compares YYYY-MM-DD in America/Chicago calendar order", () => {
  assert.equal(slateDateRelation("2026-11-20", TODAY), "past");
  assert.equal(slateDateRelation("2026-11-21", TODAY), "today");
  assert.equal(slateDateRelation("2026-11-22", TODAY), "future");
});

test("today and future empty slates use the official post-time copy; past wording is unchanged", () => {
  const unposted = SLATE_UNPOSTED_EMPTY_BODY;
  assert.equal(unposted, `This slate posts by ${OFFICIAL_POST_TIME} on game day.`);
  assert.equal(slateEmptyBody("future"), unposted);
  assert.equal(slateEmptyBody("today"), unposted);
  assert.equal(slateEmptyBody("future"), SLATE_FUTURE_EMPTY_BODY);
  assert.equal(slateEmptyBody("today"), SLATE_TODAY_EMPTY_BODY);
  assert.equal(
    slateEmptyBody("past"),
    "We don't have a stored slate for this date.",
  );
  assert.equal(slateEmptyBody("past"), SLATE_PAST_EMPTY_BODY);
  assert.notEqual(slateEmptyBody("past"), unposted);
});

test("matchup CTA is on future and today empty slates, not on past dates", () => {
  assert.equal(showSlateMatchupCta("future"), true);
  assert.equal(showSlateMatchupCta("today"), true);
  assert.equal(showSlateMatchupCta("past"), false);
  assert.equal(SLATE_MATCHUP_CTA_LABEL, "Get an early line → Matchup maker");
  assert.equal(slateMatchupHref("cbb"), "/cbb/matchup");
});

test("Live badge is hidden when the date has no games or is in the future", () => {
  assert.equal(showSlateLiveBadge("today", 8), true);
  assert.equal(showSlateLiveBadge("past", 8), true);
  assert.equal(showSlateLiveBadge("today", 0), false);
  assert.equal(showSlateLiveBadge("past", 0), false);
  assert.equal(showSlateLiveBadge("future", 0), false);
  assert.equal(showSlateLiveBadge("future", 8), false);
});

test("today with nothing on the schedule gets a plain no-games line, not the morning-run promise", async () => {
  const m = await import("../src/lib/slateEmpty");
  assert.equal(m.slateEmptyBody("today", false), "No games are scheduled today.");
  assert.doesNotMatch(m.slateEmptyBody("today", false), /posts by/);
  assert.equal(m.slateMatchupCtaLabel("today", false), "Run a matchup → Matchup maker");
  assert.equal(m.showSlateMatchupCta("today"), true);
  // Games are scheduled (or we couldn't tell): the slate may just be late, keep the unposted copy.
  for (const known of [true, null, undefined]) {
    assert.equal(m.slateEmptyBody("today", known), m.SLATE_UNPOSTED_EMPTY_BODY);
    assert.equal(m.slateMatchupCtaLabel("today", known), m.SLATE_MATCHUP_CTA_LABEL);
  }
  // Future and past ignore the flag.
  assert.equal(m.slateEmptyBody("future", false), m.SLATE_UNPOSTED_EMPTY_BODY);
  assert.equal(m.slateEmptyBody("past", false), m.SLATE_PAST_EMPTY_BODY);
});

test("an empty games table only confirms 'no games' after the official morning run", async () => {
  const m = await import("../src/lib/slateEmpty");
  // 2026-11-21 is CST (UTC-6): 13:00Z = 7:00 AM CT, 13:35Z = 7:35 AM CT (after 7:30).
  const early = new Date("2026-11-21T13:00:00Z");
  const after = new Date("2026-11-21T13:35:00Z");
  assert.equal(m.minutesCT(early), 7 * 60);
  assert.equal(m.confirmedSchedule(false, early), null);
  assert.equal(m.slateEmptyBody("today", m.confirmedSchedule(false, early)), m.SLATE_UNPOSTED_EMPTY_BODY);
  assert.equal(m.confirmedSchedule(false, after), false);
  assert.equal(m.slateEmptyBody("today", m.confirmedSchedule(false, after)), "No games are scheduled today.");
  assert.equal(m.confirmedSchedule(true, after), true);
  assert.equal(m.confirmedSchedule(null, after), null);
});
