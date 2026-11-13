"use client";
import { useEffect, useId, useState } from "react";
import { createPortal } from "react-dom";
import {
  isCoachLocked,
  lockedCoachLocks,
  newCoachLines,
  newCoachSheetCopy,
  newCoachUnlocked,
  pausedChipLabel,
  type NewCoachLock,
} from "@/lib/coach";
import { notAPlayLines } from "@/lib/verdict";
import type { SlateGame } from "@/lib/types";
import type { Market } from "@/lib/labels";

/** Full-text note for team pages. Shows the "back on" line once both markets are clear. */
export function NewCoachBadge({ lock, name = false, className = "" }: { lock: NewCoachLock; name?: boolean; className?: string }) {
  const locked = isCoachLocked(lock);
  const lines = locked ? newCoachLines(lock, name) : [newCoachUnlocked(lock)];
  return (
    <div className={`rounded-md border border-amber-400/40 bg-amber-400/10 px-2 py-1 text-[11px] leading-snug text-amber-100 ${className}`}>
      {lines.map((l) => <div key={l}>{l}</div>)}
    </div>
  );
}

/** Compact amber chip that opens a bottom-sheet explainer. Hidden once the team is fully clear. */
export function NewCoachChip({ lock, name = false, market, className = "" }: { lock: NewCoachLock; name?: boolean; market?: Market | null; className?: string }) {
  const [open, setOpen] = useState(false);
  const [mounted, setMounted] = useState(false);
  const id = useId();
  const copy = newCoachSheetCopy(lock);
  const label = pausedChipLabel(lock, name, market);

  useEffect(() => setMounted(true), []);
  useEffect(() => {
    if (!open) return;
    const onKey = (ev: KeyboardEvent) => {
      if (ev.key !== "Escape") return;
      ev.preventDefault();
      ev.stopPropagation();
      ev.stopImmediatePropagation();
      setOpen(false);
    };
    window.addEventListener("keydown", onKey, true);
    return () => window.removeEventListener("keydown", onKey, true);
  }, [open]);

  const sheet = open && mounted
    ? createPortal(
        <div className="fixed inset-0 z-[80] flex items-end justify-center sm:items-center" role="presentation">
          <button
            type="button"
            aria-label="Close"
            className="absolute inset-0 bg-black/70"
            onClick={(ev) => { ev.stopPropagation(); setOpen(false); }}
          />
          <div
            id={id}
            role="dialog"
            aria-modal="true"
            aria-label={copy.title}
            className="relative z-[81] w-full max-w-md rounded-t-3xl border border-ink-600 bg-ink-900 p-5 pb-[calc(1.25rem+env(safe-area-inset-bottom))] shadow-2xl normal-case tracking-normal sm:mx-4 sm:rounded-3xl"
            style={{ maxWidth: "min(28rem, calc(100vw - 32px))" }}
            onClick={(ev) => ev.stopPropagation()}
          >
            <div className="mx-auto mb-4 h-1 w-10 rounded-full bg-ink-600 sm:hidden" />
            <h2 className="text-base font-bold text-white normal-case tracking-normal">{copy.title}</h2>
            <p className="mt-2 text-[15px] leading-relaxed text-zinc-300 normal-case tracking-normal">{copy.body}</p>
            <button
              type="button"
              onClick={(ev) => { ev.stopPropagation(); setOpen(false); }}
              className="mt-5 min-h-12 w-full rounded-xl border border-ink-600 text-sm font-semibold text-white hover:bg-ink-800"
            >
              Got it
            </button>
          </div>
        </div>,
        document.body,
      )
    : null;

  return (
    <span className={`inline-flex ${className}`} onClick={(ev) => ev.stopPropagation()} onKeyDown={(ev) => ev.stopPropagation()}>
      <button
        type="button"
        aria-label={`${label} details`}
        aria-expanded={open}
        aria-controls={id}
        onClick={(ev) => { ev.stopPropagation(); setOpen((o) => !o); }}
        className="inline-flex items-center gap-1 rounded-md border border-amber-400/40 bg-amber-400/10 px-1.5 py-0.5 text-[10px] font-semibold normal-case tracking-normal text-amber-200 hover:bg-amber-400/20"
      >
        {label}
        <span className="inline-grid h-3.5 w-3.5 place-items-center rounded-full border border-amber-400/50 text-[8px] font-bold leading-none">i</span>
      </button>
      {sheet}
    </span>
  );
}

/** market: the card's headline market, so a totals card reads "Paused until game 4". Omit to list sides first. */
export function NewCoachChips({ locks, name, market, className = "" }: { locks?: NewCoachLock[] | null; name?: boolean; market?: Market | null; className?: string }) {
  const shown = lockedCoachLocks(locks);
  if (!shown.length) return null;
  const showName = name ?? shown.length > 1;
  return (
    <span className={`inline-flex flex-wrap items-center gap-1 ${className}`}>
      {shown.map((l) => <NewCoachChip key={l.team} lock={l} name={showName} market={market} />)}
    </span>
  );
}

/** @deprecated skip-code labels now live in lib/verdict.ts (no lock wording). */
export const SKIP_LABELS: Record<string, string> = {
  below_open_threshold: "edge vs open below threshold",
  below_current_threshold: "edge now below threshold",
  line_moved_against: "market moved 3+ pts away from CZarp since open",
  new_coach_lock: "new coach",
  kelly_too_small: "stake too small to post",
  game_started: "game already started",
  no_current_line: "no current line",
  no_opening_line: "no opening line yet",
};

/** Skip-reason lines for markets without an official bet: "Not a play: …" then "Also: …" for other reasons. */
export function whyNotPlayCopy(g: Partial<SlateGame> | undefined, officialMarkets: Iterable<string>): string[] {
  if (!g) return [];
  return notAPlayLines(g as SlateGame, officialMarkets);
}
