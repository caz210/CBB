"use client";
import type { SlateGame, ValueBet } from "@/lib/types";
import { TIER_BADGE } from "@/lib/labels";
import { pickStatusTags, statusTagLabel, type StatusTag } from "@/lib/statusTags";
import { InfoTip } from "./InfoTip";
import { NewCoachChip } from "./NewCoachBadge";

function StatusTagView({ tag }: { tag: StatusTag }) {
  if (tag.kind === "pause") {
    return <NewCoachChip lock={tag.lock} name={tag.showName} market={tag.market} />;
  }
  if (tag.kind === "verdict" && tag.verdict === "official") {
    return <span className="rounded-md bg-edge-400/15 px-1.5 py-0.5 text-[10px] font-semibold text-edge-400">Official play</span>;
  }
  if (tag.kind === "verdict" && tag.verdict === "lean") {
    return (
      <InfoTip term="lean" triggerClassName="rounded-md bg-ink-800 px-1.5 py-0.5 text-[10px] font-semibold text-zinc-400 hover:bg-ink-700 hover:text-zinc-200">
        Lean
      </InfoTip>
    );
  }
  if (tag.kind === "value" && tag.value === "tier") {
    return <span className={`rounded-md px-1.5 py-0.5 text-[10px] font-bold ${TIER_BADGE[tag.tier]}`}>{tag.tier}</span>;
  }
  return <span className="rounded-md bg-ink-800 px-1.5 py-0.5 text-[10px] font-semibold text-zinc-400">Low value</span>;
}

/** Up to STATUS_TAG_MAX status tags, in STATUS_TAG_PRIORITY order. Neutral and TV are not rendered here. */
export function StatusChips({ g, bets }: { g: SlateGame; bets: ValueBet[] }) {
  const tags = pickStatusTags(g, bets);
  return (
    <>
      {tags.map((t, i) => (
        <StatusTagView key={`${statusTagLabel(t)}-${t.kind === "pause" ? t.lock.team : i}`} tag={t} />
      ))}
    </>
  );
}
