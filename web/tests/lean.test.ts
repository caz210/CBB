import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { parsePickTier } from "../src/lib/labels";
import { verdictLabel } from "../src/lib/verdicts";
import {
  CZARP_LEAN_EXPLAINER,
  CZARP_LEAN_LABEL,
  isMissingRelation,
  leanRecordLine,
  lineMovedAwayPtsLabel,
  lineMovedAwayTip,
  officialRecordFromLedger,
} from "../src/lib/lean";

test("parsePickTier never defaults an unknown or missing tier to official", () => {
  assert.equal(parsePickTier("official"), "official");
  assert.equal(parsePickTier("lean"), "lean");
  assert.equal(parsePickTier("track"), "track");
  assert.equal(parsePickTier("paper"), null);
  assert.equal(parsePickTier("tracked"), null);
  assert.equal(parsePickTier(""), null);
  assert.equal(parsePickTier(undefined), null);
  assert.equal(parsePickTier(null), null);
  assert.equal(parsePickTier(1), null);
});

test("a lean or unknown ledger row never counts in official W-L/units", () => {
  const row = (tier: unknown, result: "W" | "L" | "P" | null, units: number) => ({
    pick_tier: parsePickTier(tier) ?? undefined,
    result,
    profit_units: units,
  });
  const stats = officialRecordFromLedger([
    row("official", "W", 1),
    row("official", "L", -1.1),
    row("lean", "W", 0.91),
    row("lean", "L", -1),
    row("mystery", "W", 5),
    row(undefined, "W", 3),
    row("track", "W", 1),
    row("official", null, 0),
  ]);
  assert.equal(stats.record, "1-1-0");
  assert.equal(stats.wins, 1);
  assert.equal(stats.losses, 1);
  assert.equal(stats.pending, 1);
  assert.equal(stats.units, -0.1);
});

test("lean record line omits pushes when there are none", () => {
  assert.equal(leanRecordLine({ wins: 28, losses: 25, pushes: 0, units: 0.45 }), "CZarp Leans: 28-25, +0.45u");
  assert.equal(leanRecordLine({ wins: 28, losses: 25, pushes: 2, units: -1.2 }), "CZarp Leans: 28-25-2, −1.20u");
});

test("line-moved tooltip names the move in points and is not a play", () => {
  assert.equal(lineMovedAwayPtsLabel(3), "3");
  assert.equal(lineMovedAwayPtsLabel(3.0), "3");
  assert.equal(lineMovedAwayPtsLabel(2.5), "2.5");
  assert.equal(
    lineMovedAwayTip(3),
    "The spread moved 3 pts away from CZarp's side since it opened. That often means injury or lineup news. Not a play.",
  );
  assert.match(lineMovedAwayTip(null), /moved away from CZarp's side since it opened/);
  assert.doesNotMatch(lineMovedAwayTip(3), /against us|KenPom|v1|v2|paper/i);
});

test("CZarp Lean explainer uses the approved wording", () => {
  assert.equal(
    CZARP_LEAN_EXPLAINER,
    "CZarp Leans are sides where our number differs from the market by 2.5% to 4% and the line hasn't moved away from CZarp. Tracked at 1 unit, not part of the official record.",
  );
  assert.equal(CZARP_LEAN_LABEL, "CZarp Lean");
});

test("missing lean views/columns are detected so the slate can degrade to empty", () => {
  assert.equal(isMissingRelation({ code: "42P01", message: "relation does not exist" }), true);
  assert.equal(isMissingRelation({ code: "42703", message: "column does not exist" }), true);
  assert.equal(isMissingRelation({ code: "PGRST205", message: "could not find the table" }), true);
  assert.equal(isMissingRelation({ code: "PGRST204", message: "could not find the column" }), true);
  assert.equal(isMissingRelation({ code: "42501", message: "permission denied" }), false);
  assert.equal(isMissingRelation(null), false);
});

test("the ladder no longer ships a customer label of Lean", () => {
  assert.equal(verdictLabel("lean"), "Small edge");
  const candidates = [resolve(__dirname, "../src/lib/verdicts.ts"), resolve(__dirname, "../../src/lib/verdicts.ts")];
  const file = candidates.find((f) => { try { readFileSync(f); return true; } catch { return false; } });
  assert.ok(file, "verdicts.ts not found");
  const src = readFileSync(file!, "utf8");
  assert.doesNotMatch(src, /label:\s*"Lean"/);
  assert.match(src, /label:\s*"Small edge"/);
});
