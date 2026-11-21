/** Status-tag picker for slate cards and game sheets.
 *
 *  VerdictChips used to always emit Official play or Low value (plus optional
 *  Lean / VALUE), and NewCoachChips was rendered as a sibling with no cap —
 *  so a paused Lean card could show three status tags (Low value, Lean, pause).
 *  There was no shared enforcer; comments mentioned a max of 2, but nothing
 *  applied it to the combined set.
 *
 *  Neutral and TV Rating are informational and are not status tags: they are
 *  uncapped and never compete for a slot here.
 *
 *  CZarp can reorder STATUS_TAG_PRIORITY; earlier entries win when we take
 *  the first STATUS_TAG_MAX tags. Default: pause > verdict (Lean / Official
 *  play) > value label (Low value, or the VALUE/EDGE/HOT tier on an official).
 */
import { lockedCoachLocks, type NewCoachLock } from "./coach";
import { edgeTier, type EdgeTier, type Market } from "./labels";
import type { SlateGame, ValueBet } from "./types";
import { cardHeadline } from "./verdict";

export const STATUS_TAG_PRIORITY = ["pause", "verdict", "value"] as const;
export const STATUS_TAG_MAX = 2;

export type StatusTagKind = (typeof STATUS_TAG_PRIORITY)[number];

export type StatusTag =
  | { kind: "pause"; lock: NewCoachLock; showName: boolean; market: Market | null }
  | { kind: "verdict"; verdict: "official" | "lean" }
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
  const showName = locks.length > 1;
  const tier = official && h.officialBet ? edgeTier(h.officialBet.line_edge_pct, h.officialBet.market) : null;

  const byKind: Record<StatusTagKind, StatusTag[]> = {
    pause: locks.map((lock) => ({ kind: "pause", lock, showName, market: h.market })),
    verdict: official
      ? [{ kind: "verdict", verdict: "official" }]
      : h.lean
        ? [{ kind: "verdict", verdict: "lean" }]
        : [],
    value: official
      ? (tier ? [{ kind: "value", value: "tier", tier }] : [])
      : [{ kind: "value", value: "low_value" }],
  };

  const out: StatusTag[] = [];
  for (const k of STATUS_TAG_PRIORITY) {
    for (const t of byKind[k]) {
      if (out.length >= STATUS_TAG_MAX) return out;
      out.push(t);
    }
  }
  return out;
}
