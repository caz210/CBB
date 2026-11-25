"use client";
import type { ReactNode } from "react";
import { NEUTRAL_CHIP_LABEL } from "@/lib/gameTitle";
import { InfoTip } from "./InfoTip";

/** Same muted look as the "Neutral site: no home-court edge applied" note. Never shrinks or wraps. */
export function NeutralChip({ className = "", interactive = true }: { className?: string; interactive?: boolean }) {
  if (!interactive) {
    return (
      <span className={`inline-block shrink-0 whitespace-nowrap rounded-md border border-ink-600 px-1.5 py-px align-middle text-[10px] font-semibold leading-4 text-zinc-400 ${className}`}>
        {NEUTRAL_CHIP_LABEL}
      </span>
    );
  }
  return (
    <InfoTip
      term="neutral"
      className={className}
      triggerClassName="inline-block shrink-0 whitespace-nowrap rounded-md border border-ink-600 px-1.5 py-px align-middle text-[10px] font-semibold leading-4 text-zinc-400"
    >
      {NEUTRAL_CHIP_LABEL}
    </InfoTip>
  );
}

/**
 * A one-line title that truncates. A normal game renders exactly the single truncating text it always did.
 * A neutral game puts the chip beside the title, so the chip is never what gets cut off.
 */
export function TitleLine({ text, neutral, suffix, className = "", wrap = false, interactive = true }: {
  text: string;
  neutral: boolean;
  /** text that follows the title, e.g. " · 7:30 PM" */
  suffix?: ReactNode;
  className?: string;
  /** neutral only: let the chip and suffix drop to a second line instead of squeezing the title (narrow cards) */
  wrap?: boolean;
  /** false inside a row Link/button so the Neutral chip cannot steal the tap */
  interactive?: boolean;
}) {
  if (!neutral) return <span className={`block truncate ${className}`}>{text}{suffix}</span>;
  return (
    <span className={`flex min-w-0 items-center ${wrap ? "flex-wrap gap-x-1.5 gap-y-0.5" : "gap-1.5"} ${className}`}>
      <span className={`min-w-0 truncate ${wrap ? "max-w-full" : ""}`}>{text}</span>
      <NeutralChip interactive={interactive} />
      {suffix != null && suffix !== "" && <span className="shrink-0 whitespace-nowrap">{suffix}</span>}
    </span>
  );
}
