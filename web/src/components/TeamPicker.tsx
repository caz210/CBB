"use client";
// One team picker for the matchup maker. Same index, ranking and aliases as the header search
// (useTeamIndex + searchTeams); it just returns a team instead of navigating.
import { useEffect, useId, useMemo, useRef, useState, type KeyboardEvent } from "react";
import { searchTeams, type SearchTeam } from "@/lib/teamSearch";
import type { Load } from "@/lib/useTeamIndex";

export function TeamPicker({ label, side, selected, load, excludeId, onPick }: {
  label: string;
  side: "A" | "B";
  selected: SearchTeam | null;
  load: Load;
  /** the other side's team, hidden from results so the same team can't be picked twice */
  excludeId: number | null;
  onPick: (t: SearchTeam) => void;
}) {
  const uid = useId();
  const listId = `${uid}-list`;
  const [text, setText] = useState(selected?.name ?? "");
  const [open, setOpen] = useState(false);
  const [active, setActive] = useState(0);
  const root = useRef<HTMLDivElement>(null);

  // follow the selection (URL load, swap button, prefilled team)
  useEffect(() => { setText(selected?.name ?? ""); }, [selected?.id, selected?.name]);

  const results = useMemo(
    () => (load.state === "ready" && open && text.trim() && text !== selected?.name
      ? searchTeams(load.index, text, 9).filter((t) => t.id !== excludeId).slice(0, 8)
      : []),
    [load, open, text, selected?.name, excludeId],
  );

  useEffect(() => {
    if (!open) return;
    const outside = (e: Event) => { if (!root.current?.contains(e.target as Node)) { setOpen(false); setText(selected?.name ?? ""); } };
    document.addEventListener("mousedown", outside);
    document.addEventListener("focusin", outside);
    return () => { document.removeEventListener("mousedown", outside); document.removeEventListener("focusin", outside); };
  }, [open, selected?.name]);

  const choose = (t: SearchTeam) => { setOpen(false); setText(t.name); onPick(t); };

  const onKey = (e: KeyboardEvent<HTMLInputElement>) => {
    if (e.key === "Escape") { setOpen(false); setText(selected?.name ?? ""); return; }
    if (!results.length) return;
    if (e.key === "ArrowDown") { e.preventDefault(); setActive((a) => (a + 1) % results.length); }
    else if (e.key === "ArrowUp") { e.preventDefault(); setActive((a) => (a - 1 + results.length) % results.length); }
    else if (e.key === "Enter") { e.preventDefault(); choose(results[Math.min(active, results.length - 1)]); }
  };

  return (
    <div ref={root} className="relative">
      <label htmlFor={`${uid}-in`} className="mb-1.5 flex items-center gap-2 text-[11px] font-bold uppercase tracking-wider text-zinc-400">
        <span className="grid h-5 w-5 place-items-center rounded-md bg-ink-700 text-[10px] text-zinc-200">{side}</span>
        {label}
      </label>
      <input
        id={`${uid}-in`}
        type="search"
        value={text}
        onFocus={(e) => { setOpen(true); e.currentTarget.select(); }}
        onChange={(e) => { setText(e.target.value); setOpen(true); setActive(0); }}
        onKeyDown={onKey}
        role="combobox"
        aria-expanded={results.length > 0}
        aria-controls={listId}
        aria-autocomplete="list"
        aria-activedescendant={results.length ? `${listId}-${active}` : undefined}
        autoComplete="off" autoCorrect="off" autoCapitalize="off" spellCheck={false}
        placeholder={load.state === "loading" ? "Loading teams…" : "Search teams"}
        className="h-12 w-full rounded-xl border border-ink-600 bg-ink-900 px-3.5 text-[16px] font-semibold text-white placeholder:font-normal placeholder:text-zinc-500 focus:border-court-500 focus:outline-none [&::-webkit-search-cancel-button]:appearance-none"
      />
      {open && text.trim() && text !== selected?.name && (
        <div className="absolute inset-x-0 top-full z-30 mt-1.5 overflow-hidden rounded-xl border border-ink-600 bg-ink-900 shadow-2xl">
          {load.state === "error" ? (
            <p className="px-4 py-3 text-sm text-zinc-400">Team search is unavailable right now. Try again in a moment.</p>
          ) : load.state === "loading" ? (
            <p className="px-4 py-3 text-sm text-zinc-500">Loading teams…</p>
          ) : results.length === 0 ? (
            <p className="px-4 py-3 text-sm text-zinc-400">No team matches “{text.trim()}”.</p>
          ) : (
            <ul id={listId} role="listbox" aria-label={`Team ${side}`} className="max-h-72 divide-y divide-ink-700/60 overflow-y-auto">
              {results.map((t, i) => (
                <li key={t.id} id={`${listId}-${i}`} role="option" aria-selected={i === active}>
                  <button
                    type="button"
                    onMouseDown={(e) => e.preventDefault()}
                    onClick={() => choose(t)}
                    onMouseMove={() => setActive(i)}
                    className={`flex min-h-12 w-full items-center px-4 text-left text-[15px] font-semibold text-white ${i === active ? "bg-ink-800" : ""}`}
                  >
                    {t.name}
                  </button>
                </li>
              ))}
            </ul>
          )}
        </div>
      )}
    </div>
  );
}
