// One FV source everywhere: the stored proj_spread (spreads) and the official czarp-v1 total (totals),
// never the projected-score difference or sum. Data: every game Nov 10–22. Run: npm test
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync, statSync } from "node:fs";
import { join, resolve } from "node:path";
import { fmtLine, fmtSpread } from "../src/lib/labels";
import { totalFair } from "../src/lib/posted";
import type { SlateGame } from "../src/lib/types";

const root = [resolve(__dirname, ".."), resolve(__dirname, "../..")].find((d) => { try { return statSync(join(d, "src/components/SlateBoard.tsx")).isFile(); } catch { return false; } })!;
const read = (rel: string) => readFileSync(join(root, rel), "utf8");
const between = (src: string, a: string, b: string) => src.slice(src.indexOf(a), src.indexOf(b, src.indexOf(a) + a.length));

type Row = {
  game_id: number; game_date: string; away: string; home: string;
  proj_home: number | null; proj_away: number | null; proj_spread: number | null; czarp_total: number | null;
  market_spread: number | null; market_total: number | null; model_spread: number | null; vegas_spread: number | null;
};
const rows = (): Row[] => JSON.parse(read("tests/fixtures/fv-2026-11-10_22.json")) as Row[];
const asGame = (r: Row) => ({ ...r, czarp_total: r.czarp_total }) as unknown as SlateGame;

test("fmtSpread: missing or non-numeric is '—', never 'PK'; PK only at a real 0", () => {
  assert.equal(fmtSpread(null), "—");
  assert.equal(fmtSpread(undefined), "—");
  assert.equal(fmtSpread(NaN), "—");
  assert.equal(fmtSpread(Infinity), "—");
  assert.equal(fmtSpread(-Infinity), "—");
  assert.equal(fmtSpread("3" as unknown as number), "—");
  assert.equal(fmtSpread(0), "PK");
  assert.equal(fmtSpread(-0), "PK");
  assert.equal(fmtSpread(0.04), "PK"); // rounds to 0.0
  assert.equal(fmtSpread(-2.5), "-2.5");
  assert.equal(fmtSpread(1.5), "+1.5");
});

test("every FV surface reads the stored proj_spread / official total, never the score diff or sum", () => {
  const slate = read("src/components/SlateBoard.tsx");
  const card = between(slate, "function GameCard(", "\nfunction ");
  const sheet = between(slate, "function GameSheet(", "\nfunction ");
  const team = read("src/app/[sport]/team/[slug]/page.tsx");
  const home = read("src/app/page.tsx");
  // Card footer.
  assert.match(card, /fair=\{g\.proj_spread == null \? "—" : fmtSpread\(g\.proj_spread\)\}/);
  assert.match(card, /totalFair\(g, bets\)/);
  // Sheet line table.
  assert.match(sheet, /const czLine = g\.proj_spread;/);
  assert.match(sheet, /k: "Spread", cz: czLine == null \? "—" : fmtSpread\(czLine\)/);
  assert.match(sheet, /const czTotal = totalFair\(g, bets\)/);
  // Team page next game and past rows (model_grades.model_spread is the stored proj_spread).
  assert.match(team, /fmtSpread\(next\.proj_spread\)/);
  assert.match(team, /spreadRef\(fmtSpread\(g\.model_spread\)/);
  assert.match(team, /spreadRef\(fmtSpread\(g\.vegas_spread\)/);
  // Home page Mini.
  assert.match(home, /<Mini label="Spread" value=\{fmtSpread\(g\.proj_spread\)\} \/>/);
  assert.match(home, /<Mini label="Total" value=\{totalFair\(g, \[\]\)\.value\} \/>/);
  // No surface derives FV from projected scores, and no spread FV/price goes through the zero-as-"0.0" formatter.
  for (const [name, src] of [["SlateBoard", slate], ["team page", team], ["home", home]] as const) {
    assert.doesNotMatch(src, /-\(\s*ph\s*-\s*pa\s*\)|proj_home\s*-\s*\w*\.?proj_away|proj_away\s*-\s*\w*\.?proj_home/, `${name} derives FV from the score difference`);
    assert.doesNotMatch(src, /fmtLine\(\w+\.(proj_spread|market_spread|model_spread|vegas_spread)/, `${name} formats a spread without PK`);
    assert.doesNotMatch(src, /proj_total/, `${name} shows the v2 proj_total`);
  }
});

test("Nov 10–22: card, sheet, team next, team past and home Mini show the same FV for every game", () => {
  const all = rows();
  assert.equal(all.length, 586);
  for (const r of all) {
    const g = asGame(r);
    // The exact expressions each surface renders (see the source test above).
    const card = g.proj_spread == null ? "—" : fmtSpread(g.proj_spread);
    const czLine = g.proj_spread;
    const sheet = czLine == null ? "—" : fmtSpread(czLine);
    const teamNext = g.proj_spread == null ? "—" : `${g.home} ${fmtSpread(g.proj_spread)}`;
    const teamPast = r.model_spread == null ? "—" : fmtSpread(r.model_spread);
    const mini = fmtSpread(g.proj_spread);
    const id = `${r.game_date} ${r.away} @ ${r.home}`;
    assert.equal(sheet, card, id);
    assert.equal(teamNext, `${r.home} ${card}`, id);
    assert.equal(teamPast, card, id);
    assert.equal(mini, card, id);
    // Totals: card and sheet both read the official total.
    assert.equal(totalFair(g, []).value, r.czarp_total == null ? "—" : r.czarp_total.toFixed(1), id);
  }
});

test("Nov 10–22: the three real FV zeros render PK on every surface (incl. team past rows)", () => {
  const byName = (a: string, h: string, d: string) => rows().find((r) => r.away === a && r.home === h && r.game_date === d)!;
  for (const [a, h, d] of [["Marist", "Army", "2026-11-17"], ["Mercer", "Presbyterian", "2026-11-18"], ["Cal St. Bakersfield", "Idaho", "2026-11-20"]]) {
    const r = byName(a, h, d);
    assert.ok(r, `${a} @ ${h}`);
    assert.equal(fmtSpread(r.proj_spread), "PK");
    assert.equal(fmtSpread(r.model_spread), "PK");
    assert.equal(fmtLine(r.model_spread!), "0.0"); // what the old past-row formatter showed
  }
  const zeros = rows().filter((r) => fmtSpread(r.proj_spread) === "PK");
  assert.equal(zeros.length, 3);
});

test("Nov 10–22: stored FV vs projected-score margin — 139 games differ by 0.1, all before Nov 21", () => {
  const all = rows();
  const off = all.filter((r) => r.proj_home != null && r.proj_away != null && r.proj_spread != null
    && Math.abs(-(r.proj_home - r.proj_away) - r.proj_spread) > 0.05);
  assert.equal(off.length, 139);
  for (const r of off) {
    assert.ok(r.game_date <= "2026-11-20", r.game_date);
    assert.ok(Math.abs(-(r.proj_home! - r.proj_away!) - r.proj_spread!) < 0.15); // a rounding step, never more
  }
  // Mercer @ Presbyterian: scores 76.8–76.9 would give -0.1; stored FV is 0.0 → PK. The sheet now says PK too.
  const mp = off.find((r) => r.away === "Mercer" && r.home === "Presbyterian")!;
  assert.equal(fmtSpread(mp.proj_spread), "PK");
  assert.equal(fmtSpread(-(mp.proj_home! - mp.proj_away!)), "-0.1");
});
