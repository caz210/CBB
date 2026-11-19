import type { ReactNode } from "react";
import { NEUTRAL_CHIP_LABEL } from "@/lib/gameTitle";

/** Same muted look as the "Neutral site: no home-court edge applied" note. Never shrinks or wraps. */
export function NeutralChip({ className = "" }: { className?: string }) {
  return (
    <span className={`inline-block shrink-0 whitespace-nowrap rounded-md border border-ink-600 px-1.5 py-px align-middle text-[10px] font-semibold leading-4 text-zinc-400 ${className}`}>
      {NEUTRAL_CHIP_LABEL}
    </span>
  );
}

/**
 * A one-line title that truncates. A normal game renders exactly the single truncating text it always did.
 * A neutral game puts the chip beside the title, so the chip is never what gets cut off.
 */
export function TitleLine({ text, neutral, suffix, className = "" }: {
  text: string;
  neutral: boolean;
  /** text that follows the title, e.g. " · 7:30 PM" */
  suffix?: ReactNode;
  className?: string;
}) {
  if (!neutral) return <span className={`block truncate ${className}`}>{text}{suffix}</span>;
  return (
    <span className={`flex min-w-0 items-center gap-1.5 ${className}`}>
      <span className="min-w-0 truncate">{text}</span>
      <NeutralChip />
      {suffix != null && suffix !== "" && <span className="shrink-0 whitespace-nowrap">{suffix}</span>}
    </span>
  );
}
