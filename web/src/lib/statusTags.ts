/** Status-tag picker for slate cards, game sheets, and the team-page next game.
 *
 *  Neutral, conference, and TV Rating are informational and are not status
 *  tags: they are uncapped and never compete for a slot here.
 *
 *  Each game has two ladder ratings (spread and total; see lib/verdicts.ts),
 *  shown per market in the card footer. The header's first slot is the
 *  STRONGER market's verdict with its market named ("Total Favor"; Official
 *  wins ties) and is never dropped. The remaining slot goes
 *  to the next entry in STATUS_TAG_PRIORITY: a single pause chip if any team
 *  is paused (two team pauses collapse into one "Both teams paused" chip).
 *  The old VALUE / EDGE / HOT value badges are gone, so "value" never fills a
 *  slot; it stays in the priority list so the order is unchanged.
 */
import { lockedCoachLocks, type NewCoachLock } from "./coach";
import type { Market } from "./labels";
import type { SlateGame, ValueBet } from "./types";
import { cardHeadline } from "./verdict";
import type { HoldCode, VerdictLevel } from "./verdicts";

export const STATUS_TAG_PRIORITY = ["verdict", "pause", "value"] as const;
export const STATUS_TAG_MAX = 2;

export type StatusTagKind = (typeof STATUS_TAG_PRIORITY)[number];

export type StatusTag =
  | { kind: "pause"; locks: NewCoachLock[]; market: Market | null }
  | { kind: "verdict"; verdict: VerdictLevel; holds: HoldCode[]; noLine: boolean; market: Market | null };

/** Stable labels for tests and debugging. */
export function statusTagLabel(t: StatusTag): string {
  if (t.kind === "pause") return "pause";
  return t.verdict;
}

export function pickStatusTags(g: SlateGame, bets: ValueBet[]): StatusTag[] {
  const h = cardHeadline(g, bets);
  const locks = lockedCoachLocks(g.coachLocks);

  const byKind: Record<StatusTagKind, StatusTag[]> = {
    verdict: [{ kind: "verdict", verdict: h.verdict, holds: h.holds, noLine: h.noLine, market: h.noLine ? null : h.market }],
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
