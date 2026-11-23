// Projected scores at stored precision; FV and Price at exactly one decimal. Run: npm test
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { fmtLedgerPick, fmtLine, fmtOne, fmtScore } from "../src/lib/labels";
import { fmtScore as matchupFmtScore } from "../src/lib/matchup";
import { totalFair } from "../src/lib/posted";
import type { SlateGame, ValueBet } from "../src/lib/types";

type FixtureGame = Pick<SlateGame, "game_id" | "away" | "home" | "proj_away" | "proj_home" | "proj_spread" | "market_spread" | "market_total"> & { czarp_total: number | null };
type Fixture = { game_date: string; games: FixtureGame[]; posted: ValueBet[] };

function fixture(): Fixture {
  const name = "tests/fixtures/slate-2026-11-22.json";
  const f = [resolve(__dirname, `../${name}`), resolve(__dirname, `../../${name}`)].find((x) => { try { readFileSync(x); return true; } catch { return false; } });
  assert.ok(f, "fixture not found");
  return JSON.parse(readFileSync(f!, "utf8")) as Fixture;
}

test("fmtScore: stored precision, two decimals only when the second digit isn't zero", () => {
  assert.equal(fmtScore(74.05), "74.05");
  assert.equal(fmtScore(67.95), "67.95");
  assert.equal(fmtScore(74.0), "74.0");
  assert.equal(fmtScore(74), "74.0");
  assert.equal(fmtScore(74.5), "74.5");
  assert.equal(fmtScore(74.1), "74.1");
  assert.equal(fmtScore(100.05), "100.05");
  assert.equal(fmtScore(0), "0.0");
});

test("fmtScore: float artifacts never show as the wrong hundredth or more than two decimals", () => {
  assert.equal(fmtScore(74.0499999), "74.05");
  assert.equal(fmtScore(74.0500001), "74.05");
  assert.equal(fmtScore(67.9499999999), "67.95");
  assert.equal(fmtScore(74.0000001), "74.0");
  assert.equal(fmtScore(73.9999999), "74.0");
  assert.equal(fmtScore(0.1 + 0.2), "0.3");
  assert.equal(fmtScore(148.1 / 2), "74.05"); // 74.04999999999999 in binary
  for (const x of [74.05, 67.95, 68.15, 81.85, 104.15, 65.05, 0.1 + 0.2, 74.0499999]) {
    assert.match(fmtScore(x), /^\d+\.\d{1,2}$/, String(x));
  }
  assert.equal(matchupFmtScore, fmtScore); // matchup tool uses the same formatter
});

test("FV and Price read exactly one decimal, keeping .0 and never rounding to a half point", () => {
  assert.equal(fmtLine(-6.1), "-6.1");
  assert.equal(fmtLine(-2.5), "-2.5");
  assert.equal(fmtLine(-6), "-6.0");
  assert.equal(fmtLine(8.9), "+8.9");
  assert.equal(fmtLine(3), "+3.0");
  assert.equal(fmtLine(0), "0.0");
  assert.equal(fmtLine(-0.04), "0.0");
  assert.equal(fmtLine(-31.54), "-31.5");
  assert.equal(fmtLine(-6.0999999), "-6.1");
  assert.equal(fmtOne(142), "142.0");
  assert.equal(fmtOne(142.5), "142.5");
  assert.equal(fmtOne(142.3), "142.3"); // not 142.5
  assert.equal(fmtOne(144.29999999), "144.3");
  for (const s of [fmtLine(-6.1), fmtLine(-6), fmtOne(142), fmtOne(140.5)]) assert.match(s, /^[+-]?\d+\.\d$/);
});

test("Nov 22 slate: displayed scores add up to the displayed FV total and differ by the displayed FV margin", () => {
  const fx = fixture();
  assert.equal(fx.game_date, "2026-11-22");
  assert.equal(fx.games.length, 38);
  let checked = 0;
  for (const g of fx.games) {
    if (g.proj_away == null || g.proj_home == null || g.proj_spread == null) continue;
    const bets = fx.posted.filter((b) => b.game_id === g.game_id);
    const away = fmtScore(g.proj_away), home = fmtScore(g.proj_home);
    const fvTotal = totalFair(g as unknown as SlateGame, bets).value;
    const fvSpread = fmtLine(g.proj_spread); // home line: −6.1 = home by 6.1
    const near = (a: number, b: number) => Math.abs(a - b) < 0.05; // 0.1 precision
    assert.ok(near(Number(away) + Number(home), Number(fvTotal)), `${g.away} @ ${g.home}: ${away} + ${home} vs FV ${fvTotal}`);
    assert.ok(near(Number(home) - Number(away), -Number(fvSpread)), `${g.away} @ ${g.home}: ${home} − ${away} vs FV ${fvSpread}`);
    assert.match(fvTotal, /^\d+\.\d$/);
    assert.match(fvSpread, /^[+-]?\d+\.\d$/);
    if (g.market_spread != null) assert.match(fmtLine(g.market_spread), /^[+-]?\d+\.\d$/);
    if (g.market_total != null) assert.match(fmtOne(g.market_total), /^\d+\.\d$/);
    checked++;
  }
  assert.equal(checked, 38);
});

test("McNeese @ Texas St. 11-22: 67.95 / 74.05 with FV Texas St. -6.1 and FV 142.0", () => {
  const g = fixture().games.find((x) => x.home === "Texas St." && x.away === "McNeese")!;
  assert.equal(fmtScore(g.proj_away!), "67.95");
  assert.equal(fmtScore(g.proj_home!), "74.05");
  assert.equal(fmtLine(g.proj_spread!), "-6.1");
  assert.equal(totalFair(g as unknown as SlateGame, []).value, "142.0");
  assert.equal(fmtLine(g.market_spread!), "-2.5");
  assert.equal(fmtOne(g.market_total!), "142.5");
});

test("the old one-decimal score rounding broke the sum or margin on 16 of 38 games today", () => {
  const fx = fixture();
  const old = (x: number) => Number(x.toFixed(1));
  const broken = fx.games.filter((g) => {
    const a = old(g.proj_away!), h = old(g.proj_home!);
    const t = Number(totalFair(g as unknown as SlateGame, fx.posted.filter((b) => b.game_id === g.game_id)).value);
    return Math.abs(a + h - t) > 0.05 || Math.abs(h - a + Number(fmtLine(g.proj_spread!))) > 0.05;
  });
  assert.equal(broken.length, 16);
});

test("fmtLedgerPick uses PK on a 0 spread and does not duplicate a line already in the pick", () => {
  assert.equal(fmtLedgerPick("Over 135.5", 135.5), "Over 135.5");
  assert.equal(fmtLedgerPick("Under 158.5", 158.5), "Under 158.5");
  assert.equal(fmtLedgerPick("North Carolina Central +26.5", 26.5), "North Carolina Central +26.5");
  assert.equal(fmtLedgerPick("Home Team", 0), "Home Team PK");
  assert.equal(fmtLedgerPick("Home Team +0.0", 0), "Home Team PK");
  assert.equal(fmtLedgerPick("Home Team", 2.5), "Home Team +2.5");
  assert.equal(fmtLedgerPick("Over", 141.5), "Over 141.5");
  assert.equal(fmtLedgerPick("A pick", null), "A pick");
});
