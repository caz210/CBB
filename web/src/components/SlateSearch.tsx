"use client";
import { useId } from "react";

const SearchIcon = () => (
  <svg viewBox="0 0 24 24" className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-zinc-500" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
    <circle cx="11" cy="11" r="7" /><path d="m20 20-3.5-3.5" />
  </svg>
);

/** Filters the slate as you type. The value lives in ?q=; the parent writes it with replaceState. */
export function SlateSearch({ query, onQuery }: { query: string; onQuery: (q: string) => void }) {
  const id = useId();
  return (
    <div className="relative mb-2 w-full min-w-0">
      <label htmlFor={id} className="sr-only">Search games by team</label>
      <SearchIcon />
      <input
        id={id}
        type="search"
        value={query}
        onChange={(e) => onQuery(e.target.value)}
        placeholder="Search teams"
        autoComplete="off"
        autoCorrect="off"
        autoCapitalize="off"
        spellCheck={false}
        enterKeyHint="search"
        className="h-11 w-full min-w-0 rounded-xl border border-ink-600 bg-ink-900 pl-9 pr-11 text-[16px] text-white placeholder:text-zinc-500 focus:border-court-500 focus:outline-none [&::-webkit-search-cancel-button]:appearance-none"
      />
      {query !== "" && (
        <button
          type="button"
          onClick={() => onQuery("")}
          aria-label="Clear search"
          className="absolute right-1 top-1/2 grid h-9 w-9 -translate-y-1/2 place-items-center rounded-lg text-zinc-400 hover:bg-ink-800 hover:text-white"
        >
          <svg viewBox="0 0 24 24" className="h-4 w-4" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" aria-hidden="true">
            <path d="M6 6l12 12M18 6L6 18" />
          </svg>
        </button>
      )}
    </div>
  );
}
