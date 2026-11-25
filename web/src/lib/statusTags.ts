/** Status-tag picker for slate cards, game sheets, and the team-page next game.
 *
 *  Neutral, conference, and TV Rating are informational and are not status
 *  tags: they are uncapped and never compete for a slot here.
 *
 *  Each game has two ladder ratings (spread and total; see lib/verdicts.ts),
 *  shown per market in the card footer. The header's first slot is:
 *    official play → CZarp Play;
 *    else a CZarp Lean on the game → CZarp Lean (not the ladder "Small edge");
 *    else the STRONGER market's ladder verdict (VERDICT_RANK), with its market
 *    named ("Spread Small edge", "Total CZarp Play"; CZarp Play wins ties).
 *  That first tag is never dropped. The remaining slot follows STATUS_TAG_PRIORITY:
 *  Line moved away (outranks New coach), then a reserved Injury news slot
 *  (not built), then a single pause chip if any team is paused.
 *  The old VALUE / EDGE / HOT value badges are gone, so "value" never fills a
 *  slot; it stays in the priority list so the order is unchanged.
 */
import { lockedCoachLocks, type NewCoachLock } from "./coach";
import type { Market } from "./labels";
import type { SlateGame, ValueBet } from "./types";
import { cardHeadline } from "./verdict";
import type { HoldCode, VerdictLevel } from "./verdicts";

/** First slot is always the verdict (CZarp Play, CZarp Lean, or ladder). Then line-moved, reserved injury, pause. */
export const STATUS_TAG_PRIORITY = ["verdict", "line_moved", "injury", "pause", "value"] as const;
export const STATUS_TAG_MAX = 2;

export type StatusTagKind = (typeof STATUS_TAG_PRIORITY)[number];

export type StatusTag =
  | { kind: "pause"; locks: NewCoachLock[]; market: Market | null }
  | { kind: "verdict"; verdict: VerdictLevel; holds: HoldCode[]; noLine: boolean; market: Market | null }
  | { kind: "czarp_lean" }
  | { kind: "line_moved"; pts: number | null };
  /* injury is reserved in STATUS_TAG_PRIORITY and never emitted. */

/** Stable labels for tests and debugging. */
export function statusTagLabel(t: StatusTag): string {
  if (t.kind === "pause") return "pause";
  if (t.kind === "czarp_lean") return "czarp_lean";
  if (t.kind === "line_moved") return "line_moved";
  return t.verdict;
}

export function pickStatusTags(g: SlateGame, bets: ValueBet[]): StatusTag[] {
  const h = cardHeadline(g, bets);
  const locks = lockedCoachLocks(g.coachLocks);
  const official = Boolean(h.officialBet);
  const hasLean = (g.leans?.length ?? 0) > 0;
  const lineMoved = g.line_moved_away === true;

  const first: StatusTag = official
    ? { kind: "verdict", verdict: h.verdict, holds: h.holds, noLine: h.noLine, market: h.noLine ? null : h.market }
    : hasLean
      ? { kind: "czarp_lean" }
      : { kind: "verdict", verdict: h.verdict, holds: h.holds, noLine: h.noLine, market: h.noLine ? null : h.market };

  const byKind: Record<StatusTagKind, StatusTag[]> = {
    verdict: [first],
    line_moved: lineMoved ? [{ kind: "line_moved", pts: g.line_moved_away_pts ?? null }] : [],
    injury: [],
    pause: locks.length ? [{ kind: "pause", locks, market: h.market }] : [],
    value: [],
  };

  const out: StatusTag[] = [...byKind.verdict];
  for (const k of STATUS_TAG_PRIORITY) {
    if (k === "verdict") continue;
    for (const t of byKind[k]) {
      if (out.length >= STATUS_TAG_MAX) return out;
      out.push(t);
    }
  }
  return out;
}
