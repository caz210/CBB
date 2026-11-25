/** Lean and unknown pick tiers must never be treated as official — verdict, posted
 *  fair, Official Pick banner, ledger record, or official W-L/units. */
import { test } from "node:test";
import assert from "node:assert/strict";
import { isOfficialTier, parsePickTier } from "../src/lib/labels";
import { cardHeadline, marketRatings, marketVerdict } from "../src/lib/verdict";
import { totalFair } from "../src/lib/posted";
import {
  applyOfficialRecord,
  ledgerToOfficialRecord,
  officialBetsOf,
  officialPickViews,
} from "../src/lib/officialPick";
import { officialRecordFromLedger } from "../src/lib/lean";
import { officialPicksFromLedger } from "../src/lib/sheetExtras";
import { portfolioRisk } from "../src/lib/portfolio";
import { rowUnits } from "../src/lib/data";
import type { LedgerRow, SlateGame, ValueBet } from "../src/lib/types";

const game = (o: Partial<SlateGame> = {}): SlateGame => ({
  sport: "cbb", game_date: "2026-11-24", game_id: 1956, home: "Florida", away: "Milwaukee",
  tipoff: null, game: "Milwaukee @ Florida", proj_home: 74, proj_away: 67, proj_spread: -6.8,
  proj_total: 141, czarp_total: 141, home_win_prob: 0.7, home_cover_prob: 0.6,
  archetypes: { home: [], away: [], home_tentative: false, away_tentative: false, tentative: false, notes: [] },
  market_spread: -2.5, market_total: 142.5, market_source: null,
  edge_now_pct_spread: 0.029, edge_now_pct_total: 0.004,
  skip_reasons_spread: ["below_open_threshold"], skip_reasons_total: ["below_open_threshold"],
  ...o,
} as SlateGame);

const bet = (o: Partial<ValueBet> = {}): ValueBet => ({
  sport: "cbb", game_id: 1956, market: "spread", side: "Pittsburgh -2.5",
  model_prob: null, market_prob: 0, prob_edge: 0, line_edge_pct: 0.029,
  kelly_quarter_units: 0.4, pick_tier: "official",
  fair_at_post: -6.8, line_at_post: -2.5, posted_line: -2.5, edge_at_post_pct: 0.029,
  ...o,
} as ValueBet);

const ledger = (o: Partial<LedgerRow> = {}): LedgerRow => ({
  sport: "cbb", game_date: "2026-11-24", game_id: 1956, pick: "Pittsburgh -2.5",
  line: -2.5, odds: -110, stake_units: 0.4, result: "W", profit_units: 1.1,
  is_paper: false, pick_tier: "official", flat_units: 0.91, clv: 0.5, ...o,
});

const TIERS = ["lean", "mystery", undefined, null, "", "tracked", "paper"] as const;

test("isOfficialTier is a positive check: only the exact string official passes", () => {
  assert.equal(isOfficialTier("official"), true);
  for (const t of ["lean", "track", "TRACK", "Official", "mystery", "", undefined, null, 1]) {
    assert.equal(isOfficialTier(t), false, String(t));
  }
  assert.equal(parsePickTier("lean"), "lean");
  assert.equal(parsePickTier("mystery"), null);
});

test("a lean or unknown bet never becomes a CZarp Play headline or official market verdict", () => {
  const g = game();
  assert.equal(cardHeadline(g, [bet()]).verdict, "official");
  assert.equal(cardHeadline(g, [bet({ pick_tier: "lean" })]).verdict, "lean"); // ladder Small edge, not CZarp Play
  assert.equal(cardHeadline(g, [bet({ pick_tier: undefined })]).verdict, "lean");
  assert.equal(cardHeadline(g, [bet({ pick_tier: "track" })]).verdict, "lean");
  assert.notEqual(marketVerdict(g, [bet({ pick_tier: "lean" })], "spread").level, "official");
  assert.notEqual(marketVerdict(g, [bet({ pick_tier: undefined })], "spread").level, "official");
  assert.equal(marketRatings(g, [bet({ pick_tier: "lean" })]).spread.level, "lean");
  assert.equal(marketRatings(g, [bet({ pick_tier: "lean" })]).spread.lineMoveNote, false);
});

test("posted total fair is official-only: lean and unknown do not get the posted tag", () => {
  const g = game({ czarp_total: 150 });
  const official = bet({ market: "total", fair_at_post: 147.5, pick_tier: "official" });
  assert.equal(totalFair(g, [official]).tag, "posted");
  assert.equal(totalFair(g, [official]).num, 147.5);
  assert.equal(totalFair(g, [bet({ market: "total", fair_at_post: 147.5, pick_tier: "lean" })]).tag, undefined);
  assert.equal(totalFair(g, [bet({ market: "total", fair_at_post: 147.5, pick_tier: undefined })]).num, 150);
  assert.equal(totalFair(g, [bet({ market: "total", fair_at_post: 147.5, pick_tier: "track" })]).tag, undefined);
});

test("officialBetsOf, officialPickViews, and ledgerToOfficialRecord reject lean and unknown", () => {
  const lean = bet({ pick_tier: "lean" });
  const unknown = bet({ pick_tier: undefined });
  const official = bet();
  assert.deepEqual(officialBetsOf([official, lean, unknown]).map((b) => b.pick_tier), ["official"]);
  assert.equal(officialPickViews(game(), [lean, unknown]).length, 0);
  assert.equal(officialPickViews(game(), [official, lean]).length, 1);
  assert.equal(officialPickViews(game(), [official, lean])[0].side, official.side);

  assert.ok(ledgerToOfficialRecord(ledger({ pick_tier: "official" })));
  assert.equal(ledgerToOfficialRecord(ledger({ pick_tier: "lean" })), null);
  assert.equal(ledgerToOfficialRecord(ledger({ pick_tier: "track" })), null);
  assert.equal(ledgerToOfficialRecord(ledger({ pick_tier: undefined })), null);
});

test("applyOfficialRecord never writes official stake/result onto a lean or unknown bet", () => {
  const rec = {
    game_id: 1956, pick: "Pittsburgh -2.5", stake_units: 0.4, result: "W" as const,
    profit_units: 1.1, clv: 0.5, logged_at: "2026-11-24T13:16:18+00:00", pick_hash: "abc",
  };
  const lean = bet({ pick_tier: "lean", pick_hash: "abc", stake_units: undefined, result: undefined, profit_units: undefined });
  const unknown = bet({ pick_tier: undefined, pick_hash: "abc", stake_units: undefined, result: undefined, profit_units: undefined });
  const official = bet({ pick_hash: "abc" });
  const [a, b, c] = applyOfficialRecord([official, lean, unknown], [rec]);
  assert.equal(a.stake_units, 0.4);
  assert.equal(a.result, "W");
  assert.equal(b.stake_units, undefined);
  assert.equal(b.result, undefined);
  assert.equal(c.stake_units, undefined);
  assert.equal(c.result, undefined);
});

test("a lean or unknown ledger row never counts in official W-L/units, sheet chips, or official rowUnits", () => {
  const officialW = ledger({ pick_tier: "official", result: "W", profit_units: 1, flat_units: 0.91 });
  const officialL = ledger({ pick_tier: "official", result: "L", profit_units: -1.1, game_id: 2 });
  const leanW = ledger({ pick_tier: "lean", result: "W", profit_units: 0.91, flat_units: 0.91, game_id: 3 });
  const unknownW = ledger({ pick_tier: undefined, result: "W", profit_units: 5, flat_units: 1, game_id: 4 });
  const trackW = ledger({ pick_tier: "track", result: "W", profit_units: 1, flat_units: 1, game_id: 5 });

  const stats = officialRecordFromLedger([officialW, officialL, leanW, unknownW, trackW]);
  assert.equal(stats.record, "1-1-0");
  assert.equal(stats.units, -0.1);

  assert.equal(rowUnits(officialW), 1);
  assert.equal(rowUnits(leanW), 0.91);
  assert.equal(rowUnits(unknownW), 1);
  assert.equal(rowUnits(trackW), 1);

  const chips = officialPicksFromLedger([
    { tier: "official", pick: "Pittsburgh -2.5", result: "W" },
    { tier: "lean", pick: "Milwaukee +14.5", result: "W" },
    { tier: "track", pick: "Over 150.5", result: "L" },
    { tier: "mystery", pick: "Virginia -3.5", result: "W" },
    { tier: null, pick: "Duke -1.5", result: "W" },
  ]);
  assert.deepEqual(chips.map((c) => c.pick), ["Pittsburgh -2.5"]);

  // If a mixed book were passed to official risk by mistake, lean W must not inflate official P/L.
  const officialOnly = [officialW, officialL];
  const mixed = [officialW, officialL, leanW, unknownW];
  const riskOfficial = portfolioRisk(officialOnly);
  const riskIfFiltered = portfolioRisk(mixed.filter((r) => r.pick_tier === "official"));
  assert.deepEqual(riskIfFiltered, riskOfficial);
  // And the positive filter drops lean/unknown.
  assert.equal(mixed.filter((r) => isOfficialTier(r.pick_tier)).length, 2);
});

test("every audited official check is positive: lean and unknown never pass isOfficialTier", () => {
  for (const t of TIERS) {
    assert.equal(isOfficialTier(t), false);
    assert.equal(isOfficialTier(parsePickTier(t) ?? t), false);
  }
});
