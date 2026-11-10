"use client";
import { useEffect, useId, useState } from "react";
import { createPortal } from "react-dom";
import {
  isCoachLocked,
  lockedCoachLocks,
  newCoachLines,
  newCoachSheetCopy,
  newCoachUnlocked,
  type NewCoachLock,
} from "@/lib/coach";

/** Full-text note for team pages. Unchanged: still shows the unlocked line once both markets open. */
export function NewCoachBadge({ lock, name = false, className = "" }: { lock: NewCoachLock; name?: boolean; className?: string }) {
  const locked = isCoachLocked(lock);
  const lines = locked ? newCoachLines(lock, name) : [newCoachUnlocked(lock)];
  return (
    <div className={`rounded-md border border-amber-400/40 bg-amber-400/10 px-2 py-1 text-[11px] leading-snug text-amber-100 ${className}`}>
      {lines.map((l) => <div key={l}>{l}</div>)}
    </div>
  );
}

/** Compact amber chip that opens a bottom-sheet explainer. Hidden once the team is fully unlocked. */
export function NewCoachChip({ lock, name = false, className = "" }: { lock: NewCoachLock; name?: boolean; className?: string }) {
  const [open, setOpen] = useState(false);
  const [mounted, setMounted] = useState(false);
  const id = useId();
  const copy = newCoachSheetCopy(lock);
  const label = name ? `${lock.team} new coach` : "New coach";

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
    <span className={`inline-flex ${className}`} onClick={(ev) => ev.stopPropagation()}>
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

export function NewCoachChips({ locks, name, className = "" }: { locks?: NewCoachLock[] | null; name?: boolean; className?: string }) {
  const shown = lockedCoachLocks(locks);
  if (!shown.length) return null;
  const showName = name ?? shown.length > 1;
  return (
    <span className={`inline-flex flex-wrap items-center gap-1 ${className}`}>
      {shown.map((l) => <NewCoachChip key={l.team} lock={l} name={showName} />)}
    </span>
  );
}

/** Machine-readable skip codes → short labels when human text is missing. */
export const SKIP_LABELS: Record<string, string> = {
  below_open_threshold: "below open threshold",
  below_current_threshold: "below current threshold",
  line_moved_against: "line moved against",
  new_coach_lock: "new coach lock",
  kelly_too_small: "Kelly stake too small",
  game_started: "game started",
  no_current_line: "no current line",
  no_opening_line: "no opening line",
};

type WhyNotSource = {
  coachLocks?: unknown; // ignored; the chip covers new-coach locks
  skip_reason_spread?: string | null;
  skip_reason_total?: string | null;
  skip_reasons_spread?: string[] | null;
  skip_reasons_total?: string[] | null;
};

function formatSkip(human?: string | null, codes?: string[] | null): string | null {
  if (human && human.trim()) return human.trim();
  if (codes && codes.length) return codes.map((c) => SKIP_LABELS[c] ?? c.replace(/_/g, " ")).join("; ");
  return null;
}

/** Skip-reason text/codes only, for markets without an official bet. Coach locks are covered by the chip. */
export function whyNotPlayCopy(g: WhyNotSource | undefined, officialMarkets: Iterable<string>): string[] {
  if (!g) return [];
  const mk = new Set(officialMarkets);
  const out: string[] = [];
  if (!mk.has("spread") && !mk.has("moneyline")) {
    const s = formatSkip(g.skip_reason_spread, g.skip_reasons_spread);
    if (s) out.push(`Spread: ${s}`);
  }
  if (!mk.has("total")) {
    const s = formatSkip(g.skip_reason_total, g.skip_reasons_total);
    if (s) out.push(`Total: ${s}`);
  }
  return out;
}
