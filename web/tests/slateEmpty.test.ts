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
} from "../src/lib/slateEmpty";

const TODAY = "2026-11-21";

test("slateDateRelation compares YYYY-MM-DD in America/Chicago calendar order", () => {
  assert.equal(slateDateRelation("2026-11-20", TODAY), "past");
  assert.equal(slateDateRelation("2026-11-21", TODAY), "today");
  assert.equal(slateDateRelation("2026-11-22", TODAY), "future");
});

test("today and future empty slates use the 7:15 AM CT copy; past wording is unchanged", () => {
  const unposted = "This slate posts by 7:15 AM CT on game day.";
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
