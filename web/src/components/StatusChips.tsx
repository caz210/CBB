"use client";
import type { SlateGame, ValueBet } from "@/lib/types";
import { TIER_BADGE } from "@/lib/labels";
import { pickStatusTags, statusTagLabel, type StatusTag } from "@/lib/statusTags";
import { InfoTip } from "./InfoTip";
import { NewCoachChip } from "./NewCoachBadge";

/** normal-case tracking-normal: the team-page row is uppercase; the slate is not.
 *  These classes keep Official play / Lean / Low value matching the slate. */
const CHIP = "normal-case tracking-normal";

function StatusTagView({ tag }: { tag: StatusTag }) {
  if (tag.kind === "pause") {
    return <NewCoachChip locks={tag.locks} market={tag.market} className={CHIP} />;
  }
  if (tag.kind === "verdict" && tag.verdict === "official") {
    return (
      <InfoTip term="official-play" triggerClassName={`rounded-md bg-edge-400/15 px-1.5 py-0.5 text-[10px] font-semibold text-edge-400 ${CHIP}`}>
        Official play
      </InfoTip>
    );
  }
  if (tag.kind === "verdict" && tag.verdict === "lean") {
    return (
      <InfoTip term="lean" triggerClassName={`rounded-md bg-ink-800 px-1.5 py-0.5 text-[10px] font-semibold text-zinc-400 hover:bg-ink-700 hover:text-zinc-200 ${CHIP}`}>
        Lean
      </InfoTip>
    );
  }
  if (tag.kind === "verdict" && tag.verdict === "low_value") {
    return <span className={`rounded-md bg-ink-800 px-1.5 py-0.5 text-[10px] font-semibold text-zinc-400 ${CHIP}`}>Low value</span>;
  }
  if (tag.kind === "value" && tag.value === "tier") {
    return <span className={`rounded-md px-1.5 py-0.5 text-[10px] font-bold ${TIER_BADGE[tag.tier]} ${CHIP}`}>{tag.tier}</span>;
  }
  return <span className={`rounded-md bg-ink-800 px-1.5 py-0.5 text-[10px] font-semibold text-zinc-400 ${CHIP}`}>Low value</span>;
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
