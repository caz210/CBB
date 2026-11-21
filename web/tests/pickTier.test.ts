import { test } from "node:test";
import assert from "node:assert/strict";
import { SHOW_TRACKED_TIER, resolvePortfolioTier, resultsHref, resultsUrlStrippingTier, shouldStripPortfolioTierParam } from "../src/lib/pickTier";

test("SHOW_TRACKED_TIER is the single flag that hides the tracked portfolio book", () => {
  assert.equal(SHOW_TRACKED_TIER, false);
});

test("when the flag is off, every non-official ?tier= falls back to official", () => {
  assert.equal(resolvePortfolioTier(undefined), "official");
  assert.equal(resolvePortfolioTier(null), "official");
  assert.equal(resolvePortfolioTier(""), "official");
  assert.equal(resolvePortfolioTier("official"), "official");
  assert.equal(resolvePortfolioTier("track"), "official");
  assert.equal(resolvePortfolioTier("TRACK"), "official");
  assert.equal(resolvePortfolioTier("paper"), "official");
  assert.equal(resolvePortfolioTier("tracked"), "official");
});

test("when the flag is on, only exact ?tier=track selects the tracked book", () => {
  assert.equal(resolvePortfolioTier("track", true), "track");
  assert.equal(resolvePortfolioTier("official", true), "official");
  assert.equal(resolvePortfolioTier(undefined, true), "official");
  assert.equal(resolvePortfolioTier("TRACK", true), "official");
  assert.equal(resolvePortfolioTier("paper", true), "official");
});

test("dirty ?tier= params are stripped so the URL never shows tracked data while the flag is off", () => {
  assert.equal(shouldStripPortfolioTierParam("track"), true);
  assert.equal(shouldStripPortfolioTierParam("paper"), true);
  assert.equal(shouldStripPortfolioTierParam("official"), false);
  assert.equal(shouldStripPortfolioTierParam(undefined), false);
  assert.equal(shouldStripPortfolioTierParam(""), false);
  // with the flag on, a real track URL is kept
  assert.equal(shouldStripPortfolioTierParam("track", true), false);
  assert.equal(shouldStripPortfolioTierParam("paper", true), true);
});

test("resultsHref never writes ?tier=track while the flag is off, and keeps range", () => {
  assert.equal(resultsHref("cbb"), "/cbb/results");
  assert.equal(resultsHref("cbb", "season", "official"), "/cbb/results");
  assert.equal(resultsHref("cbb", "season", "track"), "/cbb/results");
  assert.equal(resultsHref("cbb", "30d", "track"), "/cbb/results?range=30d");
  assert.equal(resultsHref("cbb", "7d", "paper"), "/cbb/results?range=7d");
  // flag on: track is a real query again
  assert.equal(resultsHref("cbb", "season", "track", true), "/cbb/results?tier=track");
  assert.equal(resultsHref("cbb", "30d", "track", true), "/cbb/results?tier=track&range=30d");
  assert.equal(resultsHref("cbb", "season", "official", true), "/cbb/results");
});

test("the tier redirect keeps every query param except tier", () => {
  assert.equal(resultsUrlStrippingTier("cbb", { tier: "track" }), "/cbb/results");
  assert.equal(resultsUrlStrippingTier("cbb", { tier: "track", range: "30d" }), "/cbb/results?range=30d");
  assert.equal(
    resultsUrlStrippingTier("cbb", { tier: "track", range: "30d", utm_source: "newsletter" }),
    "/cbb/results?range=30d&utm_source=newsletter",
  );
  assert.equal(
    resultsUrlStrippingTier("cbb", { utm_source: "x", range: "7d", tier: "paper" }),
    "/cbb/results?utm_source=x&range=7d",
  );
  // repeated non-tier params are kept; every tier param is dropped
  const q = new URLSearchParams("tier=track&utm_source=a&utm_source=b&range=30d&tier=paper");
  assert.equal(resultsUrlStrippingTier("cbb", q), "/cbb/results?utm_source=a&utm_source=b&range=30d");
  // path is always /{sport}/results — query values cannot change the path
  assert.equal(resultsUrlStrippingTier("cbb", { tier: "//evil.com", next: "/elsewhere" }), "/cbb/results?next=%2Felsewhere");
});
