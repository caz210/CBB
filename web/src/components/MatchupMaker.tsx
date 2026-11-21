"use client";
// Matchup maker: two team pickers, a court control, and CZarp's projection from the `project_matchup` RPC.
// The UI never computes a projection; it shows what the RPC returns (and the difference between two replies).
import { useCallback, useEffect, useMemo, useRef, useState, type KeyboardEvent } from "react";
import { useSearchParams } from "next/navigation";
import { createSupabaseBrowserClient } from "@/lib/supabase/client";
import { useTeamIndex } from "@/lib/useTeamIndex";
import type { SearchTeam } from "@/lib/teamSearch";
import {
  favorite, fetchMatchup, firstUnknown, fmtAsOf, fmtScore, fmtVenueDelta, fmtWinPct, isStale, matchupHref,
  parseMatchupParams, spreadParts, swapVenue, venueDelta, venueLabel, winBarPct, type Matchup, type MatchupError, type Venue,
} from "@/lib/matchup";
import { TeamPicker } from "./TeamPicker";

type Fetched = { key: string; match: Matchup; neutral: Matchup | null };
type View =
  | { s: "idle" }
  | { s: "same" }
  | { s: "unknown"; side: "a" | "b" }
  | { s: "loading" }
  | { s: "error"; error: MatchupError }
  | { s: "ok"; r: Fetched };

const keyOf = (a: number, b: number, v: Venue) => `${a}:${b}:${v}`;
const RETRY_COOLDOWN_MS = 3000;

export function MatchupMaker({ sportKey }: { sportKey: string }) {
  const load = useTeamIndex(sportKey, true);
  // The URL is the state. Picks and court are read from it, so Back/Forward, reloads and shared links all agree.
  const sp = useSearchParams();
  const { a, b, venue } = useMemo(() => parseMatchupParams(sp), [sp]);
  const [retry, setRetry] = useState(0);
  const [canRetry, setCanRetry] = useState(true);
  const [view, setView] = useState<View>({ s: "idle" });
  const cache = useRef(new Map<string, Fetched>());
  const seq = useRef(0);

  // replaceState(null, ...) lets the Next router adopt the new URL (and useSearchParams follow it) without
  // adding history entries or keeping a stale payload for this one.
  const setUrl = useCallback((next: { a: number | null; b: number | null; venue: Venue }) => {
    window.history.replaceState(null, "", matchupHref(sportKey, { a: next.a ?? undefined, b: next.b ?? undefined, venue: next.venue }));
  }, [sportKey]);

  const byId = useMemo(() => {
    const m = new Map<number, SearchTeam>();
    if (load.state === "ready") for (const e of load.index) m.set(e.team.id, e.team);
    return m;
  }, [load]);
  const teamA = a != null ? byId.get(a) ?? null : null;
  const teamB = b != null ? byId.get(b) ?? null : null;
  const nameA = teamA?.name ?? "Team A";
  const nameB = teamB?.name ?? "Team B";

  // (Re)query whenever the pair or the court changes. Replies are cached per pair+court so toggling is instant.
  useEffect(() => {
    const mine = ++seq.current; // invalidates any request still in flight
    if (a == null || b == null) { setView({ s: "idle" }); return; }
    if (a === b) { setView({ s: "same" }); return; }
    if (load.state === "loading") { setView({ s: "loading" }); return; }
    // ids that aren't in the team list can't work: say so without calling the RPC
    const bad = load.state === "ready" ? firstUnknown(a, b, (id) => byId.has(id)) : null;
    if (bad) { setView({ s: "unknown", side: bad }); return; }
    const key = keyOf(a, b, venue);
    const hit = cache.current.get(key);
    if (hit && retry === 0) { setView({ s: "ok", r: hit }); return; }
    const sb = createSupabaseBrowserClient();
    if (!sb) { setView({ s: "error", error: { kind: "other", message: "Something went wrong loading that matchup. Try again in a moment." } }); return; }
    setView({ s: "loading" });
    const nKey = keyOf(a, b, "neutral");
    const cachedNeutral = cache.current.get(nKey)?.match ?? null;
    (async () => {
      const [main, neutral] = await Promise.all([
        fetchMatchup(sb, a, b, venue),
        venue === "neutral" || cachedNeutral ? Promise.resolve(null) : fetchMatchup(sb, a, b, "neutral"),
      ]);
      if (mine !== seq.current) return; // a newer request superseded this one
      if (!main.ok) {
        setView({ s: "error", error: main.error });
        setCanRetry(false);
        setTimeout(() => { if (mine === seq.current) setCanRetry(true); }, RETRY_COOLDOWN_MS);
        return;
      }
      const nm = venue === "neutral" ? main.data : cachedNeutral ?? (neutral && neutral.ok ? neutral.data : null);
      const r: Fetched = { key, match: main.data, neutral: nm };
      cache.current.set(key, r);
      if (nm) cache.current.set(nKey, { key: nKey, match: nm, neutral: nm });
      setView({ s: "ok", r });
    })();
  }, [a, b, venue, retry, load.state, byId]);

  const pickA = useCallback((t: SearchTeam) => { setRetry(0); setUrl({ a: t.id, b, venue }); }, [setUrl, b, venue]);
  const pickB = useCallback((t: SearchTeam) => { setRetry(0); setUrl({ a, b: t.id, venue }); }, [setUrl, a, venue]);
  const swap = () => { setRetry(0); setUrl({ a: b, b: a, venue: swapVenue(venue) }); };
  const setVenue = (v: Venue) => { setRetry(0); setUrl({ a, b, venue: v }); };

  // radiogroup keyboard: arrows move and select, Home/End jump; only the selected radio is a tab stop
  const courtKey = (e: KeyboardEvent<HTMLButtonElement>, v: Venue) => {
    const order: Venue[] = ["neutral", "a_home", "b_home"];
    const i = order.indexOf(v);
    const to = e.key === "ArrowRight" || e.key === "ArrowDown" ? order[(i + 1) % 3]
      : e.key === "ArrowLeft" || e.key === "ArrowUp" ? order[(i + 2) % 3]
      : e.key === "Home" ? order[0] : e.key === "End" ? order[2] : null;
    if (!to) return;
    e.preventDefault();
    setVenue(to);
    // focus right away (a tabindex=-1 radio can take programmatic focus) so fast repeated presses keep moving
    e.currentTarget.parentElement?.querySelectorAll<HTMLElement>('[role="radio"]')[order.indexOf(to)]?.focus();
  };

  const ready = a != null && b != null && a !== b;
  const courts: { v: Venue; label: string }[] = [
    { v: "neutral", label: "Neutral" },
    { v: "a_home", label: `${nameA} home` },
    { v: "b_home", label: `${nameB} home` },
  ];

  return (
    <div className="mx-auto max-w-2xl">
      <section aria-label="Pick teams" className="grid gap-3">
        <TeamPicker label="Team" side="A" selected={teamA} load={load} excludeId={b} onPick={pickA} />
        <div className="flex justify-center">
          <button
            type="button"
            onClick={swap}
            disabled={a == null && b == null}
            aria-label="Swap teams"
            className="flex min-h-11 items-center gap-2 rounded-xl border border-ink-600 px-4 text-sm font-semibold text-zinc-200 hover:bg-ink-800 disabled:opacity-40"
          >
            <svg viewBox="0 0 24 24" className="h-4 w-4" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
              <path d="M7 4v14m0 0-3-3m3 3 3-3M17 20V6m0 0-3 3m3-3 3 3" />
            </svg>
            Swap teams
          </button>
        </div>
        <TeamPicker label="Team" side="B" selected={teamB} load={load} excludeId={a} onPick={pickB} />
      </section>

      <section aria-label="Court" className="mt-4">
        <div className="mb-1.5 text-[11px] font-bold uppercase tracking-wider text-zinc-400">Court</div>
        <div role="radiogroup" aria-label="Court" className="grid grid-cols-3 gap-1 rounded-xl border border-ink-600 bg-ink-900 p-1">
          {courts.map((c) => (
            <button
              key={c.v}
              type="button"
              role="radio"
              aria-checked={venue === c.v}
              tabIndex={venue === c.v ? 0 : -1}
              onClick={() => setVenue(c.v)}
              onKeyDown={(e) => courtKey(e, c.v)}
              className={`min-h-12 rounded-lg px-1.5 text-center text-[13px] font-semibold leading-tight transition ${venue === c.v ? "bg-court-500/20 text-court-400" : "text-zinc-400 hover:bg-ink-800"}`}
            >
              <span className="line-clamp-2 break-words">{c.label}</span>
            </button>
          ))}
        </div>
      </section>

      <section aria-label="Projection" aria-live="polite" className="mt-5">
        {view.s === "idle" && (
          <div className="card px-4 py-8 text-center">
            <p className="text-base font-bold text-white">Pick two teams</p>
            <p className="mx-auto mt-1.5 max-w-xs text-sm leading-relaxed text-zinc-400">
              Choose a team for each side and a court. CZarp shows the projected score, spread, total and win chance.
            </p>
          </div>
        )}
        {view.s === "same" && (
          <div className="card border-amber-400/30 px-4 py-5 text-sm text-amber-200">Pick two different teams.</div>
        )}
        {view.s === "unknown" && (
          <div className="card border-amber-400/30 px-4 py-5 text-sm text-amber-200" role="alert">
            That team isn&apos;t available. Pick team {view.side === "a" ? "A" : "B"} from the list.
          </div>
        )}
        {view.s === "loading" && (
          <div className="card px-4 py-10 text-center text-sm text-zinc-400" role="status">Loading projection…</div>
        )}
        {view.s === "error" && (
          <div className="card border-fade-400/30 px-4 py-5" role="alert">
            <p className="text-sm font-semibold text-white">{view.error.message}</p>
            {view.error.kind !== "bad_id" && view.error.kind !== "invalid" && (
              <button type="button" disabled={!canRetry} onClick={() => setRetry((r) => r + 1)} className="mt-3 min-h-11 rounded-xl border border-ink-600 px-4 text-sm font-semibold text-zinc-200 hover:bg-ink-800 disabled:cursor-not-allowed disabled:opacity-50">
                {canRetry ? "Try again" : "Try again in a moment"}
              </button>
            )}
          </div>
        )}
        {view.s === "ok" && ready && <ResultCard r={view.r} nameA={nameA} nameB={nameB} />}
      </section>

      <p className="mt-4 text-xs leading-relaxed text-zinc-500">
        Spread is in betting notation: the favorite carries the minus sign. For information and entertainment, not betting advice.
      </p>
    </div>
  );
}

function ResultCard({ r, nameA, nameB }: { r: Fetched; nameA: string; nameB: string }) {
  const m = r.match;
  const fav = favorite(m);
  const asOf = fmtAsOf(m.as_of);
  const stale = useMemo(() => isStale(m.as_of), [m.as_of]);
  const delta = r.neutral ? venueDelta(r.neutral, m) : null;
  const hi = "text-court-400";
  return (
    <div className="card overflow-hidden">
      <div className="border-b border-ink-700/70 px-4 py-2.5 text-[11px] font-bold uppercase tracking-wider text-zinc-400">
        {venueLabel(m.venue, nameA, nameB)}
      </div>

      <div className="grid grid-cols-2 divide-x divide-ink-700/70">
        {([["a", nameA, m.score_a], ["b", nameB, m.score_b]] as const).map(([side, name, score]) => (
          <div key={side} className="min-w-0 px-3 py-4 text-center">
            <div className={`truncate text-sm font-semibold ${fav === side ? "text-white" : "text-zinc-300"}`} title={name}>{name}</div>
            <div className={`mt-1 font-mono text-3xl font-black tabular-nums min-[400px]:text-4xl ${fav === side ? hi : "text-zinc-200"}`}>{fmtScore(score)}</div>
            <div className="mt-1 text-[11px] font-semibold uppercase tracking-wider text-zinc-500">{fav === side ? "Favorite" : fav ? "Underdog" : "Pick'em"}</div>
          </div>
        ))}
      </div>

      <div className="grid grid-cols-2 gap-px border-t border-ink-700/70 bg-ink-700/70">
        <div className="bg-ink-900 px-4 py-3">
          <div className="text-[11px] font-bold uppercase tracking-wider text-zinc-500">CZarp spread</div>
          {(() => {
            const sp = spreadParts(m, nameA, nameB);
            return sp ? (
              <div className="mt-0.5 min-w-0 text-white">
                <div className="truncate text-sm font-semibold" title={sp.name}>{sp.name}</div>
                <div className="whitespace-nowrap font-mono text-xl font-black">{sp.value}</div>
              </div>
            ) : (
              <div className="mt-0.5 font-mono text-xl font-black text-white">Pick&apos;em</div>
            );
          })()}
        </div>
        <div className="bg-ink-900 px-4 py-3">
          <div className="text-[11px] font-bold uppercase tracking-wider text-zinc-500">CZarp total</div>
          <div className="mt-0.5 font-mono text-xl font-black text-white">{fmtScore(m.total)}</div>
        </div>
      </div>

      {delta && (
        <div className="border-t border-ink-700/70 px-4 py-2.5 text-[13px] text-zinc-300">
          <span className="font-semibold text-court-400">Home court:</span> {fmtVenueDelta(delta, nameA, nameB)}
        </div>
      )}

      <div className="border-t border-ink-700/70 px-4 py-4">
        <div className="mb-2 text-[11px] font-bold uppercase tracking-wider text-zinc-500">Win chance</div>
        <div className="flex items-baseline justify-between gap-3 text-sm font-semibold">
          {/* the name truncates on its own; the percentage never shares that text node */}
          <span className="flex min-w-0 items-baseline gap-1.5 text-white">
            <span className="min-w-0 truncate" title={nameA}>{nameA}</span>
            <span className="shrink-0 whitespace-nowrap font-mono text-base font-black">{fmtWinPct(m.win_pct_a)}</span>
          </span>
          <span className="flex min-w-0 items-baseline justify-end gap-1.5 text-white">
            <span className="shrink-0 whitespace-nowrap font-mono text-base font-black">{fmtWinPct(m.win_pct_b)}</span>
            <span className="min-w-0 truncate" title={nameB}>{nameB}</span>
          </span>
        </div>
        <div className="mt-2 flex h-2.5 overflow-hidden rounded-full bg-ink-700" role="img" aria-label={`${nameA} ${fmtWinPct(m.win_pct_a)}, ${nameB} ${fmtWinPct(m.win_pct_b)}`}>
          <div className="bg-court-500" style={{ width: `${winBarPct(m.win_pct_a)}%` }} />
          <div className="bg-sky-400/70" style={{ width: `${100 - winBarPct(m.win_pct_a)}%` }} />
        </div>
      </div>

      <div className="flex flex-wrap items-center gap-x-2 gap-y-1 border-t border-ink-700/70 px-4 py-2.5 text-[11px] text-zinc-500">
        <span>As of {asOf ?? "unknown"}</span>
        {stale && (
          <span className="rounded-md border border-amber-400/40 bg-amber-400/10 px-1.5 py-0.5 font-bold uppercase tracking-wider text-amber-300">
            Stale · ratings are over 30 hours old
          </span>
        )}
      </div>
      <p className="border-t border-ink-700/70 px-4 py-2.5 text-[11px] leading-relaxed text-zinc-500">
        Projection only, not a CZarp Official Play.
      </p>
    </div>
  );
}
