"use client";
import type { SlateGame, ValueBet } from "@/lib/types";
import { pickStatusTags, statusTagLabel, type StatusTag } from "@/lib/statusTags";
import { ladderMarketLabel, verdictLabel, type VerdictLevel } from "@/lib/verdicts";
import type { Market } from "@/lib/labels";
import type { ExplainerKey } from "@/lib/explainers";
import { InfoTip } from "./InfoTip";
import { NewCoachChip } from "./NewCoachBadge";

/** normal-case tracking-normal: the team-page row is uppercase; the slate is not.
 *  These classes keep the verdict chips matching the slate. */
const CHIP = "normal-case tracking-normal";

const VERDICT_STYLE: Record<VerdictLevel, string> = {
  official: "bg-edge-400/15 text-edge-400 hover:bg-edge-400/25",
  favor: "bg-edge-400/[0.07] text-edge-400/80 ring-1 ring-inset ring-edge-400/25 hover:bg-edge-400/15",
  lean: "bg-ink-800 text-zinc-300 hover:bg-ink-700",
  toss_up: "bg-ink-800 text-zinc-500 hover:bg-ink-700 hover:text-zinc-300",
};

const VERDICT_TIP: Record<VerdictLevel, ExplainerKey> = {
  official: "officialPlay",
  favor: "favor",
  lean: "lean",
  toss_up: "tossUp",
};

/** One verdict chip (Toss Up / Lean / Favor / Official Play). Tap for the definition.
 *  market: name it in the chip ("Total Favor"), for the header's stronger-market slot.
 *  noLine: a card with no line yet gets a neutral tip, never "below the bar". */
export function VerdictChip({ level, noLine = false, market = null, wrap = false, className = "" }: { level: VerdictLevel; noLine?: boolean; market?: Market | null; wrap?: boolean; className?: string }) {
  return (
    <InfoTip term={noLine && level === "toss_up" ? "noLine" : VERDICT_TIP[level]} triggerClassName={`${wrap ? "text-left leading-tight" : "whitespace-nowrap"} rounded-md font-sans px-1.5 py-0.5 text-[10px] font-semibold ${VERDICT_STYLE[level]} ${CHIP} ${className}`}>
      {market ? `${ladderMarketLabel(market)} ${verdictLabel(level)}` : verdictLabel(level)}
    </InfoTip>
  );
}

function StatusTagView({ tag }: { tag: StatusTag }) {
  if (tag.kind === "pause") {
    return <NewCoachChip locks={tag.locks} market={tag.market} className={CHIP} />;
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
