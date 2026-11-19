"use client";
// Team search in the site header. Desktop (xl+, 1280px and up): inline box with a dropdown. Phone: an icon that opens a
// full-screen sheet. Both share one results list. The team list loads once per session (module cache +
// sessionStorage) from /api/teams, then every keystroke filters locally.
import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import { useCallback, useEffect, useId, useMemo, useRef, useState, type KeyboardEvent, type ReactNode } from "react";
import { createPortal } from "react-dom";
import { DEFAULT_SPORT, SPORTS } from "@/lib/sports";
import { teamHref } from "@/lib/teams";
import { fmtGameDate } from "@/lib/labels";
import { nextGameHref, nextGameLabel, searchTeams, withFreshNext, type SearchTeam } from "@/lib/teamSearch";
import { useTeamIndex, type Load } from "@/lib/useTeamIndex";

const SearchIcon = ({ className = "h-5 w-5" }: { className?: string }) => (
  <svg viewBox="0 0 24 24" className={className} fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
    <circle cx="11" cy="11" r="7" /><path d="m20 20-3.5-3.5" />
  </svg>
);

function nextLabel(t: SearchTeam) {
  const n = t.next;
  if (!n) return null;
  return `${n.home ? "vs" : "at"} ${n.opponent} · ${fmtGameDate(n.date).replace(/^\w+, /, "")}`;
}

function Results({ id, sport, now, results, active, onPick, onHover, query, load }: {
  id: string; sport: string; now: Date; results: SearchTeam[]; active: number; onPick: () => void; onHover: (i: number) => void; query: string; load: Load["state"];
}) {
  if (!query.trim()) {
    return <p className="px-4 py-6 text-sm text-zinc-500">Type a team name, a short form (Texas St, UNC) or initials (UT, SDSU).</p>;
  }
  if (load === "loading") return <p className="px-4 py-6 text-sm text-zinc-500">Loading teams…</p>;
  if (load === "error") return <p className="px-4 py-6 text-sm text-zinc-400">Team search is unavailable right now. Close this and try again in a moment.</p>;
  if (!results.length) return <p className="px-4 py-6 text-sm text-zinc-400">No team matches “{query.trim()}”.</p>;
  return (
    <ul id={id} role="listbox" aria-label="Teams" className="divide-y divide-ink-700/60">
      {results.map((t, i) => {
        const sub = nextLabel(t);
        return (
          <li
            key={t.id}
            id={`${id}-${i}`}
            role="option"
            aria-selected={i === active}
            onMouseMove={() => onHover(i)}
            className={`flex items-center gap-3 px-4 py-2.5 ${i === active ? "bg-ink-800" : ""}`}
          >
            <span className="min-w-0 flex-1">
              <span className="block truncate text-[15px] font-semibold text-white">{t.name}</span>
              {sub && <span className="block truncate text-[11px] text-zinc-500">{sub}</span>}
            </span>
            <Link
              href={teamHref(sport, t.name)}
              onClick={onPick}
              className="flex min-h-10 shrink-0 items-center rounded-lg border border-ink-600 px-3 text-xs font-semibold text-zinc-200 hover:bg-ink-700"
            >
              Team page
            </Link>
            {t.next && (
              <Link
                href={nextGameHref(sport, t.next)}
                onClick={onPick}
                  className="flex min-h-10 shrink-0 items-center rounded-lg bg-court-500/15 px-3 text-xs font-semibold text-court-400 hover:bg-court-500/25"
              >
                {nextGameLabel(t.next, now)}
              </Link>
            )}
          </li>
        );
      })}
    </ul>
  );
}

/** footerSlot: spot under the results (inside the phone sheet and the desktop dropdown), used for the "Build a matchup" link. Pass it from SiteHeader. Nothing is rendered when it's omitted. */
export function TeamSearch({ footerSlot }: { footerSlot?: ReactNode }) {
  const pathname = usePathname();
  const router = useRouter();
  const seg = pathname.split("/")[1];
  const sport = SPORTS.some((s) => s.key === seg) ? seg : DEFAULT_SPORT;
  const uid = useId();
  const listId = `${uid}-teams`;

  const [query, setQuery] = useState("");
  const [sheet, setSheet] = useState(false);     // phone sheet
  const [focused, setFocused] = useState(false); // desktop dropdown
  const [active, setActive] = useState(0);
  const [mounted, setMounted] = useState(false);
  const deskRef = useRef<HTMLDivElement>(null);
  const deskInput = useRef<HTMLInputElement>(null);
  const sheetInput = useRef<HTMLInputElement>(null);
  const sheetRef = useRef<HTMLDivElement>(null);
  const btnRef = useRef<HTMLButtonElement>(null);

  useEffect(() => setMounted(true), []);
  const wanted = sheet || focused;
  const load = useTeamIndex(sport, wanted);
  // Re-check the clock on every search so a game that tipped while the page sat open never shows a button.
  const { results, now } = useMemo(() => {
    const at = new Date();
    return { now: at, results: load.state === "ready" ? withFreshNext(searchTeams(load.index, query), at) : [] };
  }, [load, query]);

  const close = useCallback(() => {
    setSheet(false);
    setFocused(false);
    setQuery("");
    setActive(0);
    deskInput.current?.blur();
  }, []);

  // close on navigation
  useEffect(() => { close(); }, [pathname, close]);

  // phone sheet: lock scroll, make the page behind it inert, focus the input, Escape closes,
  // and hand focus back to the search button when it closes.
  useEffect(() => {
    if (!sheet) return;
    const dlg = sheetRef.current;
    const opener = btnRef.current;
    const prev = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    const inerted: Element[] = [];
    for (const el of Array.from(document.body.children)) {
      if (el === dlg || (dlg && el.contains(dlg)) || el.hasAttribute("inert")) continue;
      el.setAttribute("inert", "");
      inerted.push(el);
    }
    sheetInput.current?.focus();
    const onEsc = (e: globalThis.KeyboardEvent) => { if (e.key === "Escape") close(); };
    window.addEventListener("keydown", onEsc);
    return () => {
      document.body.style.overflow = prev;
      window.removeEventListener("keydown", onEsc);
      inerted.forEach((el) => el.removeAttribute("inert"));
      opener?.focus();
    };
  }, [sheet, close]);

  // a link in the footer slot closes the search even when it points at the page we are already on
  const closeOnLink = (e: React.MouseEvent) => { if ((e.target as HTMLElement).closest("a")) close(); };

  // keep Tab / Shift+Tab inside the sheet
  const trapTab = (e: KeyboardEvent<HTMLDivElement>) => {
    if (e.key !== "Tab") return;
    const items = Array.from(e.currentTarget.querySelectorAll<HTMLElement>("input, button, a[href]"));
    if (!items.length) return;
    const first = items[0], last = items[items.length - 1];
    const at = document.activeElement;
    if (e.shiftKey && (at === first || !e.currentTarget.contains(at))) { e.preventDefault(); last.focus(); }
    else if (!e.shiftKey && (at === last || !e.currentTarget.contains(at))) { e.preventDefault(); first.focus(); }
  };

  // desktop: a click outside, or Tab moving focus outside, closes the dropdown
  useEffect(() => {
    if (!focused) return;
    const outside = (e: Event) => { if (!deskRef.current?.contains(e.target as Node)) setFocused(false); };
    document.addEventListener("mousedown", outside);
    document.addEventListener("focusin", outside);
    return () => { document.removeEventListener("mousedown", outside); document.removeEventListener("focusin", outside); };
  }, [focused]);

  const onKey = (e: KeyboardEvent<HTMLInputElement>) => {
    if (e.key === "Escape") { e.preventDefault(); close(); return; }
    if (!results.length) return;
    if (e.key === "ArrowDown") { e.preventDefault(); setActive((a) => (a + 1) % results.length); }
    else if (e.key === "ArrowUp") { e.preventDefault(); setActive((a) => (a - 1 + results.length) % results.length); }
    else if (e.key === "Enter") {
      e.preventDefault();
      const t = results[Math.min(active, results.length - 1)];
      // Enter opens the team page; Shift+Enter opens the next game when there is one.
      const to = e.shiftKey && t.next ? nextGameHref(sport, t.next) : teamHref(sport, t.name);
      close();
      router.push(to);
    }
  };

  const inputProps = (ref: React.RefObject<HTMLInputElement | null>) => ({
    ref,
    type: "search" as const,
    value: query,
    onChange: (e: React.ChangeEvent<HTMLInputElement>) => { setQuery(e.target.value); setActive(0); },
    onKeyDown: onKey,
    role: "combobox" as const,
    "aria-expanded": results.length > 0,
    "aria-controls": listId,
    "aria-activedescendant": results.length ? `${listId}-${active}` : undefined,
    "aria-autocomplete": "list" as const,
    autoComplete: "off",
    autoCorrect: "off",
    autoCapitalize: "off",
    spellCheck: false,
    enterKeyHint: "go" as const,
    placeholder: "Search teams",
    "aria-label": "Search teams",
  });

  const list = (
    <Results id={listId} sport={sport} now={now} results={results} active={active} onPick={close} onHover={setActive} query={query} load={load.state} />
  );

  const phoneSheet = sheet && mounted
    ? createPortal(
        <div ref={sheetRef} onKeyDown={trapTab} className="fixed inset-0 z-[90] flex flex-col bg-ink-950 pt-[env(safe-area-inset-top)]" role="dialog" aria-modal="true" aria-label="Search teams">
          <div className="flex items-center gap-2 border-b border-ink-700 px-3 py-2.5">
            <div className="relative flex-1">
              <SearchIcon className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-zinc-500" />
              <input
                {...inputProps(sheetInput)}
                className="h-11 w-full rounded-xl border border-ink-600 bg-ink-900 pl-9 pr-3 text-[16px] text-white placeholder:text-zinc-500 focus:border-court-500 focus:outline-none [&::-webkit-search-cancel-button]:appearance-none"
              />
            </div>
            <button type="button" onClick={close} className="min-h-11 rounded-lg px-3 text-sm font-semibold text-zinc-300 hover:text-white">Cancel</button>
          </div>
          <div className="flex-1 overflow-y-auto pb-[env(safe-area-inset-bottom)]">
            {list}
            {/* footerSlot: SiteHeader passes the "Build a matchup" link here */}
            {footerSlot && <div onClickCapture={closeOnLink} className="border-t border-ink-700/60 p-4">{footerSlot}</div>}
          </div>
        </div>,
        document.body,
      )
    : null;

  return (
    <>
      {/* phone: icon button */}
      <button
        ref={btnRef}
        type="button"
        onClick={() => setSheet(true)}
        aria-label="Search teams"
        className="grid h-11 w-11 place-items-center rounded-lg text-zinc-300 hover:text-white xl:hidden"
      >
        <SearchIcon />
      </button>
      {phoneSheet}

      {/* desktop: inline box + dropdown */}
      <div ref={deskRef} className="relative hidden w-40 xl:block">
        <SearchIcon className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-zinc-500" />
        <input
          {...inputProps(deskInput)}
          onFocus={() => setFocused(true)}
          className="h-10 w-full rounded-lg border border-ink-600 bg-ink-900 pl-9 pr-3 text-sm text-white placeholder:text-zinc-500 focus:border-court-500 focus:outline-none [&::-webkit-search-cancel-button]:appearance-none"
        />
        {focused && query.trim() !== "" && (
          <div className="absolute left-0 top-full z-50 mt-2 w-[26rem] max-w-[calc(100vw-2rem)] overflow-hidden rounded-xl border border-ink-600 bg-ink-900 shadow-2xl">
            {list}
            {footerSlot && <div onClickCapture={closeOnLink} className="border-t border-ink-700/60 p-3">{footerSlot}</div>}
          </div>
        )}
      </div>
    </>
  );
}
