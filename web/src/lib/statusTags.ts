/** Status-tag picker for slate cards, game sheets, and the team-page next game.
 *
 *  VerdictChips used to always emit Official play or Low value (plus optional
 *  Lean / VALUE), and NewCoachChips was rendered as a sibling with no cap —
 *  so a paused Lean card could show three status tags (Low value, Lean, pause).
 *  There was no shared enforcer; comments mentioned a max of 2, but nothing
 *  applied it to the combined set.
 *
 *  Neutral, conference, and TV Rating are informational and are not status
 *  tags: they are uncapped and never compete for a slot here.
 *
 *  The verdict (Official play, Lean, or Low value) always takes the first
 *  slot and is never dropped. The remaining slot goes to the next entry in
 *  STATUS_TAG_PRIORITY: a single pause chip if any team is paused (two team
 *  pauses collapse into one "Both teams paused" chip), otherwise the value
 *  label (VALUE/EDGE/HOT, or Low value when Lean already took the verdict
 *  slot). CZarp can reorder the leftover priorities; the verdict is reserved.
 */
import { lockedCoachLocks, type NewCoachLock } from "./coach";
import { edgeTier, type EdgeTier, type Market } from "./labels";
import type { SlateGame, ValueBet } from "./types";
import { cardHeadline } from "./verdict";

export const STATUS_TAG_PRIORITY = ["verdict", "pause", "value"] as const;
export const STATUS_TAG_MAX = 2;

export type StatusTagKind = (typeof STATUS_TAG_PRIORITY)[number];

export type StatusTag =
  | { kind: "pause"; locks: NewCoachLock[]; market: Market | null }
  | { kind: "verdict"; verdict: "official" | "lean" | "low_value" }
  | { kind: "value"; value: "low_value" }
  | { kind: "value"; value: "tier"; tier: EdgeTier };

/** Stable labels for tests and debugging. */
export function statusTagLabel(t: StatusTag): string {
  if (t.kind === "pause") return "pause";
  if (t.kind === "verdict") return t.verdict;
  return t.value === "tier" ? t.tier : "low_value";
}

export function pickStatusTags(g: SlateGame, bets: ValueBet[]): StatusTag[] {
  const h = cardHeadline(g, bets);
  const official = h.verdict === "official";
  const locks = lockedCoachLocks(g.coachLocks);
  const tier = official && h.officialBet ? edgeTier(h.officialBet.line_edge_pct, h.officialBet.market) : null;

  // Low value with no Lean is the verdict; with Lean, Low value is the leftover value label.
  const byKind: Record<StatusTagKind, StatusTag[]> = {
    verdict: official
      ? [{ kind: "verdict", verdict: "official" }]
      : h.lean
        ? [{ kind: "verdict", verdict: "lean" }]
        : [{ kind: "verdict", verdict: "low_value" }],
    pause: locks.length ? [{ kind: "pause", locks, market: h.market }] : [],
    value: official
      ? (tier ? [{ kind: "value", value: "tier", tier }] : [])
      : h.lean
        ? [{ kind: "value", value: "low_value" }]
        : [],
  };

  const out: StatusTag[] = [];
  // Verdict always renders in the first slot when one exists (it always does
  // on a live card). Then fill from the rest of STATUS_TAG_PRIORITY.
  if (byKind.verdict.length) out.push(...byKind.verdict);
  for (const k of STATUS_TAG_PRIORITY) {
    if (k === "verdict") continue;
    for (const t of byKind[k]) {
      if (out.length >= STATUS_TAG_MAX) return out;
      out.push(t);
    }
  }
  return out;
}
