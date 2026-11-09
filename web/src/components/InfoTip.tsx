"use client";
import { useEffect, useId, useRef, useState, type ReactNode } from "react";
import { EXPLAINERS, type ExplainerKey } from "@/lib/explainers";

/** Small ⓘ that opens a short popover/sheet with market-language copy. */
export function InfoTip({ term, className = "" }: { term: ExplainerKey; className?: string }) {
  const [open, setOpen] = useState(false);
  const id = useId();
  const btnRef = useRef<HTMLButtonElement>(null);
  const e = EXPLAINERS[term];

  useEffect(() => {
    if (!open) return;
    const onKey = (ev: KeyboardEvent) => ev.key === "Escape" && setOpen(false);
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [open]);

  return (
    <span className={`relative inline-flex align-middle ${className}`}>
      <button
        ref={btnRef}
        type="button"
        aria-label={`What is ${e.title}?`}
        aria-expanded={open}
        aria-controls={id}
        onClick={(ev) => { ev.stopPropagation(); setOpen((o) => !o); }}
        className="inline-grid h-5 w-5 place-items-center rounded-full border border-ink-600 text-[10px] font-bold text-zinc-500 hover:border-ink-500 hover:text-zinc-300"
      >
        i
      </button>
      {open && (
        <>
          <button type="button" aria-label="Close" className="fixed inset-0 z-40 cursor-default" onClick={(ev) => { ev.stopPropagation(); setOpen(false); }} />
          <div
            id={id}
            role="dialog"
            aria-label={e.title}
            className="absolute left-1/2 top-full z-50 mt-2 w-64 -translate-x-1/2 rounded-xl border border-ink-600 bg-ink-850 p-3 text-left shadow-xl sm:w-72"
            onClick={(ev) => ev.stopPropagation()}
          >
            <div className="text-xs font-bold text-white">{e.title}</div>
            <p className="mt-1 text-[12px] leading-relaxed text-zinc-400">{e.body}</p>
            <button type="button" onClick={() => setOpen(false)} className="mt-2 text-[11px] font-semibold text-court-400">Got it</button>
          </div>
        </>
      )}
    </span>
  );
}

/** Inline label + info tip for forms/KPIs. */
export function TermLabel({ term, children, className = "" }: { term: ExplainerKey; children: ReactNode; className?: string }) {
  return (
    <span className={`inline-flex items-center gap-1 ${className}`}>
      {children}
      <InfoTip term={term} />
    </span>
  );
}
