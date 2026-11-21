import { test } from "node:test";
import assert from "node:assert/strict";
import { readdir, readFile } from "node:fs/promises";
import path from "node:path";
import {
  TEAM_RANKING_COLUMNS,
  czarpAtsLine,
  czarpTotalLine,
  formatTeamSubtitle,
  NO_OFFICIAL_PICKS_YET,
  officialMarketDisplay,
  officialPicksCue,
  parseTeamRanking,
  teamSubtitleParts,
} from "../src/lib/teamPage";

test("czarpAtsLine names CZarp's team and spells out Win/Loss/Push", () => {
  assert.equal(
    czarpAtsLine({ side: "away", result: "W", away: "Grambling St.", home: "Kentucky" }),
    "CZarp ATS: Grambling St. · Win",
  );
  assert.equal(
    czarpAtsLine({ side: "home", result: "L", away: "Grambling St.", home: "Kentucky" }),
    "CZarp ATS: Kentucky · Loss",
  );
  assert.equal(
    czarpAtsLine({ side: "home", result: "P", away: "Grambling St.", home: "Kentucky" }),
    "CZarp ATS: Kentucky · Push",
  );
  assert.equal(
    czarpAtsLine({ side: "away", result: "win", away: "Grambling St.", home: "Kentucky" }),
    "CZarp ATS: Grambling St. · Win",
  );
});

test("czarpAtsLine handles no-pick and ungraded cases", () => {
  assert.equal(czarpAtsLine({ side: null, result: null, away: "Grambling St.", home: "Kentucky" }), null);
  assert.equal(czarpAtsLine({ side: "", result: "", away: "Grambling St.", home: "Kentucky" }), null);
  assert.equal(
    czarpAtsLine({ side: "away", result: null, away: "Grambling St.", home: "Kentucky" }),
    "CZarp ATS: Grambling St.",
  );
  assert.equal(
    czarpAtsLine({ side: "push", result: "P", away: "Grambling St.", home: "Kentucky" }),
    "CZarp ATS: Push",
  );
  assert.equal(
    czarpAtsLine({ side: null, result: "P", away: "Grambling St.", home: "Kentucky" }),
    "CZarp ATS: Push",
  );
});

test("czarpTotalLine uses Over/Under and Win/Loss/Push", () => {
  assert.equal(czarpTotalLine({ side: "under", result: "W" }), "CZarp: Under · Win");
  assert.equal(czarpTotalLine({ side: "over", result: "L" }), "CZarp: Over · Loss");
  assert.equal(czarpTotalLine({ side: "Under", result: "push" }), "CZarp: Under · Push");
  assert.equal(czarpTotalLine({ side: null, result: null }), null);
  assert.equal(czarpTotalLine({ side: "over", result: null }), "CZarp: Over");
  assert.equal(czarpTotalLine({ side: "push", result: "P" }), "CZarp: Push");
});

test("team subtitle includes ranking when a snapshot row is present", () => {
  const up = teamSubtitleParts({
    season: 2027,
    gamesPlayed: 6,
    ranking: { rank: 59, prev_rank: 61, rating: 12.8, di_wins: 4, di_losses: 2 },
  });
  assert.equal(formatTeamSubtitle(up), "2026–27 · 6 GP · #59 ▲2 · +12.8 · 4–2 D-I");

  const neu = teamSubtitleParts({
    season: 2027,
    gamesPlayed: 6,
    ranking: { rank: 59, prev_rank: null, rating: 12.8, di_wins: 4, di_losses: 2 },
  });
  assert.equal(formatTeamSubtitle(neu), "2026–27 · 6 GP · #59 New · +12.8 · 4–2 D-I");

  const same = teamSubtitleParts({
    season: 2027,
    gamesPlayed: 6,
    ranking: { rank: 12, prev_rank: 12, rating: -3.25, di_wins: 1, di_losses: 5 },
  });
  assert.equal(formatTeamSubtitle(same), "2026–27 · 6 GP · #12 = · −3.3 · 1–5 D-I");
});

test("team subtitle hides ranking when no snapshot row comes back", () => {
  assert.equal(
    formatTeamSubtitle(teamSubtitleParts({ season: 2027, gamesPlayed: 6 })),
    "2026–27 · 6 GP",
  );
  assert.equal(
    formatTeamSubtitle(teamSubtitleParts({ season: 2027, gamesPlayed: 6, ranking: null })),
    "2026–27 · 6 GP",
  );
  assert.equal(
    formatTeamSubtitle(teamSubtitleParts({ season: 2027, gamesPlayed: 6, tentative: true })),
    "2026–27 · 6 GP · tentative",
  );
});

test("parseTeamRanking hides incomplete or missing rows", () => {
  assert.equal(parseTeamRanking(null), null);
  assert.equal(parseTeamRanking({ rank: 59 }), null);
  assert.equal(parseTeamRanking({ rating: 12.8 }), null);
  const row = parseTeamRanking({
    rank: 59,
    prev_rank: 61,
    rating: 12.8,
    di_wins: 4,
    di_losses: 2,
    week_of: "2026-11-16",
    as_of: "2026-11-21",
  });
  assert.deepEqual(row, {
    rank: 59,
    prev_rank: 61,
    rating: 12.8,
    di_wins: 4,
    di_losses: 2,
    week_of: "2026-11-16",
    as_of: "2026-11-21",
  });
});

test("ranking select list is the requested columns and never asks for KenPom W-L", () => {
  assert.equal(TEAM_RANKING_COLUMNS, "rank,prev_rank,rating,di_wins,di_losses,week_of,as_of");
  assert.equal(/kp_wins|kp_losses|kenpom/i.test(TEAM_RANKING_COLUMNS), false);
});

test("official picks cue is a pick count", () => {
  assert.equal(officialPicksCue(12, 8), "20 picks");
  assert.equal(officialPicksCue(1, 0), "1 pick");
});

test("official markets with no picks show a quiet note, not 0-0 / 0 picks", () => {
  assert.deepEqual(officialMarketDisplay(0, 0), { value: null, hint: "No official picks yet", quiet: true });
  assert.equal(officialMarketDisplay(0, 0).hint, NO_OFFICIAL_PICKS_YET);
  assert.deepEqual(officialMarketDisplay(12, 8), { value: "12-8", hint: "20 picks", quiet: false });
  assert.deepEqual(officialMarketDisplay(1, 0), { value: "1-0", hint: "1 pick", quiet: false });
});

test("customer surface does not mention KenPom", async () => {
  const srcRoot = path.join(process.cwd(), "src");
  const files: string[] = [];
  async function walk(dir: string) {
    for (const ent of await readdir(dir, { withFileTypes: true })) {
      const p = path.join(dir, ent.name);
      if (ent.isDirectory()) await walk(p);
      else if (/\.(tsx?|jsx?|mdx?)$/.test(ent.name)) files.push(p);
    }
  }
  await walk(path.join(srcRoot, "app"));
  await walk(path.join(srcRoot, "components"));
  files.push(path.join(srcRoot, "lib/explainers.ts"));
  files.push(path.join(srcRoot, "lib/methodology.ts"));
  const hits: string[] = [];
  for (const f of files) {
    const text = await readFile(f, "utf8");
    if (/kenpom/i.test(text)) hits.push(path.relative(srcRoot, f));
  }
  assert.deepEqual(hits, [], `KenPom still appears in: ${hits.join(", ")}`);
});
