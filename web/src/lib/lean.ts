/** CZarp Lean helpers: record copy, line-moved-away tooltip, and safe reads of lean views.
 *  Leans are flat 1u, never part of the official record. The UI never computes projections. */

import { fmtPlayPct, fmtSpread, fmtUnits } from "./labels";
import type { PickTier } from "./labels";
import type { LeanPick, LeanRecord, LedgerRow } from "./types";

export const CZARP_LEAN_LABEL = "CZarp Lean";
export const CZARP_LEANS_TAB = "CZarp Leans";
export const LEAN_STAKE_LABEL = "1u tracked";

/** Customer explainer on the CZarp Leans record. "Lean" appears only in this name. */
export const CZARP_LEAN_EXPLAINER =
  "CZarp Leans are sides where our number differs from the market by 2.5% to 4% and the line hasn't moved away from CZarp. Tracked at 1 unit, not part of the official record.";

export const LINE_MOVED_AWAY_LABEL = "Line moved away, possible news";

/** PostgREST / Postgres errors when lean_picks, lean_record, or new slate_games columns are not there yet. */
export function isMissingRelation(err: { code?: string | null; message?: string | null } | null | undefined): boolean {
  if (!err) return false;
  const code = err.code ?? "";
  const msg = (err.message ?? "").toLowerCase();
  return code === "42P01" || code === "42703" || code === "PGRST205" || code === "PGRST204"
    || /does not exist|schema cache|could not find the table|could not find the .*column|undefined table|undefined column/.test(msg);
}

export function lineMovedAwayPtsLabel(pts: number): string {
  const rounded = Math.round(pts * 10) / 10;
  return Number.isInteger(rounded) || Math.abs(rounded - Math.round(rounded)) < 1e-9
    ? String(Math.round(rounded))
    : rounded.toFixed(1);
}

/** Tooltip for the line-moved-away chip. pts is slate_games.line_moved_away_pts (> 0 = away from CZarp). */
export function lineMovedAwayTip(pts: number | null | undefined): string {
  const moved = pts != null && Number.isFinite(pts)
    ? `The spread moved ${lineMovedAwayPtsLabel(pts)} pts away from CZarp's side since it opened.`
    : "The spread moved away from CZarp's side since it opened.";
  return `${moved} That often means injury or lineup news. Not a play.`;
}

export function leanHeaderPick(l: LeanPick): string {
  const cz = l.czarp_line != null && Number.isFinite(l.czarp_line) ? `CZarp ${fmtSpread(l.czarp_line)}` : null;
  return cz ? `${l.pick} · ${cz}` : l.pick;
}

export function leanEdgeLabel(l: LeanPick): string | null {
  return l.edge_pct != null && Number.isFinite(l.edge_pct) ? fmtPlayPct(l.edge_pct) : null;
}

export function leansForGame<T extends { game_id: number }>(leans: T[] | null | undefined, gameId: number): T[] {
  return (leans ?? []).filter((l) => l.game_id === gameId);
}

/** "CZarp Leans: 28-25, +0.45u" — pushes only appear when there are some. */
export function leanRecordLine(r: Pick<LeanRecord, "wins" | "losses" | "pushes" | "units">): string {
  const wl = r.pushes ? `${r.wins}-${r.losses}-${r.pushes}` : `${r.wins}-${r.losses}`;
  return `${CZARP_LEANS_TAB}: ${wl}, ${fmtUnits(r.units)}`;
}

/** Official W-L/units from ledger-shaped rows. Lean, track, and unknown tiers never count. */
export function officialRecordFromLedger(rows: Array<Pick<LedgerRow, "pick_tier" | "result" | "profit_units">>): {
  wins: number; losses: number; pushes: number; pending: number; units: number; record: string;
} {
  let wins = 0, losses = 0, pushes = 0, pending = 0, units = 0;
  for (const r of rows) {
    if (r.pick_tier !== "official") continue;
    if (r.result == null) { pending++; continue; }
    if (r.result === "W") wins++;
    else if (r.result === "L") losses++;
    else if (r.result === "P") pushes++;
    units += r.profit_units ?? 0;
  }
  return { wins, losses, pushes, pending, units: Math.round(units * 100) / 100, record: `${wins}-${losses}-${pushes}` };
}

export function isPublicPortfolioTier(t: PickTier): t is "official" | "lean" {
  return t === "official" || t === "lean";
}
