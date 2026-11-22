"use client";
import { useEffect, useId, useState, type ReactNode } from "react";
import { createPortal } from "react-dom";
import { EXPLAINERS, type ExplainerKey } from "@/lib/explainers";
import { glossaryEntry } from "@/lib/glossary";

export type TipTerm = ExplainerKey | string;


function tipCopy(term?: TipTerm, title?: string, body?: string): { title: string; body: string } | null {
  if (title && body) return { title, body };
  if (!term) return null;
  if (term in EXPLAINERS) return EXPLAINERS[term as ExplainerKey];
  const g = glossaryEntry(term);
  return g ? { title: g.title, body: g.body } : null;
}

/** Small ⓘ that opens a bottom sheet via portal (never clipped by card/table overflow).
 *  With children, the children become the tap target (e.g. a chip) with a small ⓘ inside it;
 *  style that trigger with triggerClassName. Taps and keys never reach the card underneath.
 *  `term` may be an explainer key or a glossary id/title. Or pass title + body directly. */
export function InfoTip({ term, title, body, className = "", children, triggerClassName = "" }: {
  term?: TipTerm; title?: string; body?: string; className?: string; children?: ReactNode; triggerClassName?: string;
}) {
  const [open, setOpen] = useState(false);
  const [mounted, setMounted] = useState(false);
  const id = useId();
  const e = tipCopy(term, title, body);

  useEffect(() => setMounted(true), []);
  useEffect(() => {
    if (!open) return;
    // Capture + stop so Escape closes only this sheet, not a game sheet underneath.
    const onKey = (ev: KeyboardEvent) => {
      if (ev.key !== "Escape") return;
      ev.preventDefault();
      ev.stopImmediatePropagation();
      setOpen(false);
    };
    const prev = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    window.addEventListener("keydown", onKey, true);
    return () => {
      document.body.style.overflow = prev;
      window.removeEventListener("keydown", onKey, true);
    };
  }, [open]);

  const sheet = open && mounted && e
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
            aria-label={e.title}
            className="relative z-[81] w-full max-w-md rounded-t-3xl border border-ink-600 bg-ink-900 p-5 pb-[calc(1.25rem+env(safe-area-inset-bottom))] shadow-2xl normal-case tracking-normal sm:mx-4 sm:rounded-3xl"
            style={{ maxWidth: "min(28rem, calc(100vw - 32px))" }}
            onClick={(ev) => ev.stopPropagation()}
          >
            <div className="mx-auto mb-4 h-1 w-10 rounded-full bg-ink-600 sm:hidden" />
            <h2 className="text-base font-bold text-white normal-case tracking-normal">{e.title}</h2>
            <p className="mt-2 text-[15px] leading-relaxed text-zinc-300 normal-case tracking-normal">{e.body}</p>
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

  if (!e) {
    return children != null ? <>{children}</> : null;
  }

  return (
    <span
      className={`inline-flex align-middle ${className}`}
      onClick={(ev) => ev.stopPropagation()}
      onKeyDown={(ev) => ev.stopPropagation()}
    >
      {children != null ? (
        <button
          type="button"
          aria-label={`${e.title}: what does this mean?`}
          aria-expanded={open}
          aria-controls={id}
          onClick={(ev) => { ev.stopPropagation(); setOpen((o) => !o); }}
          className={`inline-flex items-center gap-1 ${triggerClassName}`}
        >
          {children}
          <span aria-hidden="true" className="inline-grid h-3.5 w-3.5 place-items-center rounded-full border border-current/50 text-[8px] font-bold leading-none opacity-80">i</span>
        </button>
      ) : (
        <button
          type="button"
          aria-label={`What is ${e.title}?`}
          aria-expanded={open}
          aria-controls={id}
          onClick={(ev) => { ev.stopPropagation(); setOpen((o) => !o); }}
          className="inline-grid h-5 w-5 shrink-0 place-items-center rounded-full border border-ink-600 text-[10px] font-bold normal-case tracking-normal text-zinc-400 hover:border-ink-500 hover:text-zinc-200"
        >
          i
        </button>
      )}
      {sheet}
    </span>
  );
}

/** Inline label + info tip for forms/KPIs. */
/** iconFirst: put the ⓘ before the label (right-aligned table headers, so neighbors don't run together). */
export function TermLabel({ term, children, className = "", gap = "gap-1", iconFirst = false }: { term: ExplainerKey; children: ReactNode; className?: string; gap?: string; iconFirst?: boolean }) {
  return (
    <span className={`inline-flex items-center ${gap} ${className}`}>
      {iconFirst && <InfoTip term={term} />}
      {children}
      {!iconFirst && <InfoTip term={term} />}
    </span>
  );
}
