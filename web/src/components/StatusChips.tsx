"use client";
import type { SlateGame, ValueBet } from "@/lib/types";
import { pickStatusTags, statusTagLabel, type StatusTag } from "@/lib/statusTags";
import { ladderMarketLabel, verdictLabel, type VerdictLevel } from "@/lib/verdicts";
import type { Market } from "@/lib/labels";
import type { ExplainerKey } from "@/lib/explainers";
import { CZARP_LEAN_LABEL, LINE_MOVED_AWAY_LABEL, lineMovedAwayTip } from "@/lib/lean";
import { InfoTip } from "./InfoTip";
import { NewCoachChip } from "./NewCoachBadge";

/** normal-case tracking-normal: the team-page row is uppercase; the slate is not.
 *  These classes keep the verdict chips matching the slate. */
const CHIP = "normal-case tracking-normal";

const VERDICT_STYLE: Record<VerdictLevel, string> = {
  official: "bg-edge-400/15 text-edge-400 hover:bg-edge-400/25",
  late_value: "bg-edge-400/[0.07] text-edge-400/80 ring-1 ring-inset ring-edge-400/25 hover:bg-edge-400/15",
  value: "bg-ink-800 text-zinc-200 ring-1 ring-inset ring-zinc-500/40 hover:bg-ink-700",
  lean: "bg-ink-800 text-zinc-300 hover:bg-ink-700",
  toss_up: "bg-ink-800 text-zinc-500 hover:bg-ink-700 hover:text-zinc-300",
};

const VERDICT_TIP: Record<VerdictLevel, ExplainerKey> = {
  official: "czarpPlay",
  late_value: "lateValue",
  value: "value",
  lean: "lean",
  toss_up: "tossUp",
};

const CHIP_BOX = "rounded-md font-sans px-1.5 py-0.5 text-[10px] font-semibold";

/** Outline / neutral — visually distinct from the green CZarp Play chip. "Lean" is only used in this name. */
const LEAN_STYLE = "bg-transparent text-zinc-300 ring-1 ring-inset ring-zinc-500/50 hover:bg-ink-800";
const LINE_MOVED_STYLE = "bg-amber-400/10 text-amber-200/90 ring-1 ring-inset ring-amber-400/30 hover:bg-amber-400/15";

/** One verdict chip (Toss Up / Small edge / Value / Late Value / CZarp Play). Tap for the definition.
 *  market: name it in the chip ("Spread Small edge"), for the header's stronger-market slot.
 *  noLine: a card with no line yet gets a neutral tip, never "below the bar". */
export function VerdictChip({ level, noLine = false, market = null, wrap = false, className = "" }: { level: VerdictLevel; noLine?: boolean; market?: Market | null; wrap?: boolean; className?: string }) {
  return (
    <InfoTip term={noLine && level === "toss_up" ? "noLine" : VERDICT_TIP[level]} triggerClassName={`${wrap ? "text-left leading-tight" : "whitespace-nowrap"} ${CHIP_BOX} ${VERDICT_STYLE[level]} ${CHIP} ${className}`}>
      {market ? `${ladderMarketLabel(market)} ${verdictLabel(level)}` : verdictLabel(level)}
    </InfoTip>
  );
}

export function CzarpLeanChip({ wrap = false, className = "" }: { wrap?: boolean; className?: string }) {
  return (
    <InfoTip term="czarpLean" triggerClassName={`${wrap ? "text-left leading-tight" : "whitespace-nowrap"} ${CHIP_BOX} ${LEAN_STYLE} ${CHIP} ${className}`}>
      {CZARP_LEAN_LABEL}
    </InfoTip>
  );
}

export function LineMovedChip({ pts, className = "" }: { pts: number | null; className?: string }) {
  return (
    <InfoTip title={LINE_MOVED_AWAY_LABEL} body={lineMovedAwayTip(pts)} triggerClassName={`text-left leading-tight ${CHIP_BOX} ${LINE_MOVED_STYLE} ${CHIP} ${className}`}>
      {LINE_MOVED_AWAY_LABEL}
    </InfoTip>
  );
}

function StatusTagView({ tag }: { tag: StatusTag }) {
  if (tag.kind === "pause") {
    return <NewCoachChip locks={tag.locks} market={tag.market} className={CHIP} />;
  }
  if (tag.kind === "czarp_lean") {
    return <CzarpLeanChip />;
  }
  if (tag.kind === "line_moved") {
    return <LineMovedChip pts={tag.pts} />;
  }
  return <VerdictChip level={tag.verdict} noLine={tag.noLine} market={tag.market} />;
}

/** Up to STATUS_TAG_MAX status tags. Verdict first; Neutral and TV are not rendered here. */
export function StatusChips({ g, bets }: { g: SlateGame; bets: ValueBet[] }) {
  const tags = pickStatusTags(g, bets);
  return (
    <>
      {tags.map((t, i) => (
        <StatusTagView key={`${statusTagLabel(t)}-${t.kind === "pause" ? t.locks.map((l) => l.team).join(",") : i}`} tag={t} />
      ))}
    </>
  );
}
