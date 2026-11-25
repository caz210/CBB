"use client";

import { useCallback, useEffect, useId, useMemo, useRef, useState, type KeyboardEvent as ReactKeyboardEvent } from "react";
import { createPortal } from "react-dom";
import type { SlateGame } from "@/lib/types";
import { fmtOne, fmtSpread } from "@/lib/labels";
import {
  LIVE_LINE_DISCLAIMER,
  LIVE_LINE_PERIODS,
  defaultLiveScores,
  defaultLiveState,
  displayLiveSpread,
  fmtProjectedScore,
  liveLine,
  nextLiveLinePeriod,
  projectedFinalScores,
  sliderBounds,
  type LivePeriod,
} from "@/lib/liveLine";
import {
  ESPN_MEN_CBB_SUMMARY,
  applyEspnFeed,
  bindFeedScores,
  feedFromEspnPayload,
  feedFromEspnStatus,
  isEspnEventId,
  shouldFetchLiveFeed,
  type LiveFeedSnapshot,
  type OurTeamSides,
} from "@/lib/espnClock";
import { createLiveFeedPoller } from "@/lib/liveClock";

type PeriodKey = Exclude<LivePeriod, "TIP">;

const SEGMENTS: { k: PeriodKey; label: string }[] = [
  { k: "1H", label: "1H" },
  { k: "HALFTIME", label: "Halftime" },
  { k: "2H", label: "2H" },
  { k: "OT", label: "OT" },
];

const POLL_MS = 30_000;

function ourSides(g: { home: string; away: string; home_espn_id?: string | null; away_espn_id?: string | null }): OurTeamSides {
  return { home: g.home, away: g.away, homeEspnId: g.home_espn_id, awayEspnId: g.away_espn_id };
}

async function loadLiveFeed(eventId: string, ours: OurTeamSides): Promise<LiveFeedSnapshot | null> {
  if (!isEspnEventId(eventId)) return null;
  try {
    const res = await fetch(ESPN_MEN_CBB_SUMMARY(eventId), { cache: "no-store" });
    if (res.ok) {
      const raw = feedFromEspnPayload(await res.json());
      // Unbound teams still return the feed (clock only, scores null) — no reason to retry via the proxy.
      if (raw) return bindFeedScores(raw, ours);
    }
  } catch { /* CORS or network — try the read-only proxy */ }
  try {
    const res = await fetch(`/api/live-clock?event=${encodeURIComponent(eventId)}`, { cache: "no-store" });
    if (!res.ok) return null;
    const j = await res.json() as { status?: unknown; competitors?: LiveFeedSnapshot["competitors"] };
    const feed = feedFromEspnStatus(j.status, null, j.competitors ?? null);
    return feed ? bindFeedScores(feed, ours) : null;
  } catch {
    return null;
  }
}

function clampToPeriod(period: PeriodKey, minutes: number): number {
  if (period === "FINAL") return 0;
  if (period === "HALFTIME") return 20;
  const b = sliderBounds(period);
  if (!b) return minutes;
  return Math.min(b.max, Math.max(b.min, minutes));
}

function ScoreStepper({ name, value, onChange }: { name: string; value: number; onChange: (n: number) => void }) {
  return (
    <div className="rounded-[14px] border border-ink-700 bg-ink-850 px-2 pb-2.5 pt-2 text-center">
      <div className="flex min-h-8 items-center justify-center px-1 text-xs font-semibold leading-tight text-zinc-300">{name}</div>
      <div className="mt-1 flex items-center justify-between gap-1">
        <button type="button" aria-label={`Decrease ${name} score`} onClick={() => onChange(Math.max(0, value - 1))}
          className="grid h-9 w-9 shrink-0 place-items-center rounded-[10px] bg-ink-700 text-xl font-semibold text-white active:bg-ink-600">−</button>
        <input inputMode="numeric" aria-label={`${name} score`} value={Number.isFinite(value) ? String(value) : "0"}
          onChange={(e) => {
            const n = parseInt(e.target.value.replace(/\D/g, ""), 10);
            onChange(Number.isFinite(n) ? Math.max(0, n) : 0);
          }}
          className="w-12 bg-transparent text-center font-mono text-[28px] font-bold tracking-tight text-white [appearance:textfield] [&::-webkit-inner-spin-button]:appearance-none [&::-webkit-outer-spin-button]:appearance-none" />
        <button type="button" aria-label={`Increase ${name} score`} onClick={() => onChange(value + 1)}
          className="grid h-9 w-9 shrink-0 place-items-center rounded-[10px] bg-ink-700 text-xl font-semibold text-white active:bg-ink-600">+</button>
      </div>
    </div>
  );
}

function LivePanel({ g, onClose }: { g: SlateGame; onClose: () => void }) {
  const titleId = useId();
  const sliderId = useId();
  const init = defaultLiveState(g.status);
  const initScores = defaultLiveScores(g.status, g);
  const [period, setPeriod] = useState<PeriodKey>(init.period);
  const [minutes, setMinutes] = useState(init.minutesLeft);
  const [home, setHome] = useState(initScores.home);
  const [away, setAway] = useState(initScores.away);
  const dirty = useRef(false);
  const poller = useRef<ReturnType<typeof createLiveFeedPoller> | null>(null);
  const [fromFeed, setFromFeed] = useState(false);
  const [scoresFromFeed, setScoresFromFeed] = useState(true);
  const [mounted, setMounted] = useState(false);
  const uiRef = useRef({ period, minutesLeft: minutes, home, away, fromFeed });
  uiRef.current = { period, minutesLeft: minutes, home, away, fromFeed };

  useEffect(() => { setMounted(true); }, []);

  const markDirty = useCallback(() => {
    dirty.current = true;
    setFromFeed(false);
    poller.current?.stop();
  }, []);

  const applyFeed = useCallback((feed: LiveFeedSnapshot) => {
    if (dirty.current) return;
    const next = applyEspnFeed(uiRef.current, feed, false);
    setPeriod(next.period);
    setMinutes(clampToPeriod(next.period, next.minutesLeft));
    setHome(next.home);
    setAway(next.away);
    setFromFeed(next.fromFeed);
    setScoresFromFeed(next.scoresFromFeed !== false);
    uiRef.current = next;
    if (feed.phase === "post") poller.current?.stop();
  }, []);

  useEffect(() => {
    if (!shouldFetchLiveFeed({ externalId: g.external_id, status: g.status, tipoff: g.tipoff })) return;
    let cancelled = false;
    const p = createLiveFeedPoller(() => {
      if (dirty.current || cancelled) return;
      void loadLiveFeed(g.external_id!, ourSides(g)).then((feed) => {
        if (!cancelled && !dirty.current && feed) applyFeed(feed);
      });
    }, POLL_MS);
    poller.current = p;
    p.start();
    return () => { cancelled = true; p.stop(); if (poller.current === p) poller.current = null; };
  }, [g.status, g.external_id, g.tipoff, g.home, g.away, g.home_espn_id, g.away_espn_id, applyFeed]);

  useEffect(() => {
    const k = (e: KeyboardEvent) => {
      if (e.key !== "Escape") return;
      e.preventDefault();
      e.stopImmediatePropagation();
      onClose();
    };
    window.addEventListener("keydown", k, true);
    return () => window.removeEventListener("keydown", k, true);
  }, [onClose]);

  const setSeg = (next: PeriodKey) => {
    markDirty();
    setPeriod(next);
    setMinutes((m) => clampToPeriod(next, m));
  };

  const onPeriodKeyDown = (e: ReactKeyboardEvent<HTMLDivElement>) => {
    const next = nextLiveLinePeriod(period, e.key);
    if (!next) return;
    e.preventDefault();
    setSeg(next);
    const el = document.getElementById(`live-line-period-${next}`);
    if (el instanceof HTMLElement) el.focus();
  };

  const result = useMemo(
    () => liveLine({
      czarpTotal: g.czarp_total ?? null,
      projHome: g.proj_home,
      projAway: g.proj_away,
      projSpread: g.proj_spread,
      homeScore: home,
      awayScore: away,
      period,
      minutesLeft: period === "HALFTIME" ? 20 : minutes,
    }),
    [g.czarp_total, g.proj_home, g.proj_away, g.proj_spread, home, away, period, minutes],
  );

  const bounds = sliderBounds(period);
  const valueText = period === "HALFTIME" ? "20 min left" : `${minutes} min left`;
  const preTotal = result && (g.czarp_total != null && Number.isFinite(g.czarp_total) ? g.czarp_total : null);
  const preRounded = preTotal != null ? Math.round(preTotal * 2) / 2 : null;
  const totalSub = result && preRounded != null && result.liveTotal !== preRounded
    ? `${result.liveTotal - preRounded > 0 ? "+" : ""}${fmtOne(result.liveTotal - preRounded)} vs pregame`
    : (preRounded != null ? `Pregame ${fmtOne(preRounded)}` : "");
  const pHome = result ? Math.round(result.homeWinProb * 100) : 50;
  const pAway = 100 - pHome;
  const winFav = result ? (pHome >= pAway ? { pct: pHome, name: g.home } : { pct: pAway, name: g.away }) : null;
  const spreadView = result ? displayLiveSpread(result.liveSpread, g.home, g.away) : null;
  const proj = result ? projectedFinalScores(result.liveTotal, result.liveSpread) : null;

  const body = (
    <div className="fixed inset-0 z-[60] flex items-end justify-center md:items-center" role="dialog" aria-modal="true" aria-labelledby={titleId}>
      <button type="button" aria-label="Close Live Line" onClick={onClose} className="absolute inset-0 bg-black/55" />
      <div className="relative max-h-[88dvh] w-full max-w-lg overflow-y-auto rounded-t-[20px] border border-b-0 border-ink-700 bg-ink-900 px-3.5 pb-[calc(14px+env(safe-area-inset-bottom))] pt-2 shadow-2xl md:rounded-3xl md:border-b">
        <div className="mx-auto mb-2.5 h-1 w-9 rounded-full bg-ink-600" />
        <div className="mb-2.5 flex items-baseline justify-between">
          <h2 id={titleId} className="text-base font-bold tracking-tight">CZarp Live Line</h2>
          <button type="button" onClick={onClose} className="px-1 py-1 text-[13px] text-zinc-400">Close</button>
        </div>

        <div className="grid grid-cols-2 gap-2">
          <ScoreStepper name={g.away} value={away} onChange={(n) => { markDirty(); setAway(n); }} />
          <ScoreStepper name={g.home} value={home} onChange={(n) => { markDirty(); setHome(n); }} />
        </div>

        <div className="mb-1.5 mt-3 text-[11px] font-semibold uppercase tracking-wider text-zinc-500">Time left</div>
        <div role="radiogroup" aria-label="Period" onKeyDown={onPeriodKeyDown} className="grid grid-cols-4 gap-1 rounded-xl border border-ink-700 bg-ink-850 p-1">
          {LIVE_LINE_PERIODS.map((k) => {
            const on = period === k;
            const label = SEGMENTS.find((s) => s.k === k)!.label;
            return (
              <button key={k} type="button" role="radio" id={`live-line-period-${k}`} aria-checked={on} tabIndex={on || (period === "FINAL" && k === "1H") ? 0 : -1} onClick={() => setSeg(k)}
                className={`min-h-11 rounded-lg px-1 text-[13px] font-semibold focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-court-400 ${on ? "bg-court-500 text-ink-950" : "text-zinc-300 hover:bg-ink-700"}`}>
                {label}
              </button>
            );
          })}
        </div>

        {period === "FINAL" ? (
          <p className="mt-2 font-mono text-[13px] font-semibold text-zinc-200">Final</p>
        ) : bounds ? (
          <div className="mt-2">
            <label htmlFor={sliderId} className="block font-mono text-[13px] font-semibold text-zinc-200">{valueText}</label>
            <input
              id={sliderId}
              className="live-line-range"
              type="range"
              min={bounds.min}
              max={bounds.max}
              step={1}
              value={minutes}
              aria-valuemin={bounds.min}
              aria-valuemax={bounds.max}
              aria-valuenow={minutes}
              aria-valuetext={valueText}
              onInput={(e) => { markDirty(); setMinutes(Number((e.target as HTMLInputElement).value)); }}
              onChange={(e) => { markDirty(); setMinutes(Number(e.target.value)); }}
            />
          </div>
        ) : (
          <p className="mt-2 font-mono text-[13px] font-semibold text-zinc-200">20 min left</p>
        )}
        {fromFeed && <p className="mt-0.5 text-[11px] text-zinc-500">{scoresFromFeed ? "from live feed" : "clock from live feed · enter the score"}</p>}

        <div className="mt-3.5 rounded-2xl border border-ink-700 bg-ink-950 p-3">
          <div className="grid grid-cols-3 gap-1.5 text-center">
            <div>
              <div className="text-[10px] font-bold uppercase tracking-wider text-zinc-500">Live spread</div>
              <div className="mt-0.5 whitespace-nowrap font-mono text-[22px] font-bold tracking-tight">{spreadView ? fmtSpread(spreadView.line) : "—"}</div>
              <div className="mt-0.5 min-h-3.5 text-[11px] leading-snug text-zinc-400">{spreadView?.team ?? ""}</div>
            </div>
            <div>
              <div className="text-[10px] font-bold uppercase tracking-wider text-zinc-500">Live total</div>
              <div className="mt-0.5 whitespace-nowrap font-mono text-[22px] font-bold tracking-tight">{result ? fmtOne(result.liveTotal) : "—"}</div>
              <div className="mt-0.5 min-h-3.5 text-[11px] leading-snug text-zinc-400">{totalSub}</div>
            </div>
            <div>
              <div className="text-[10px] font-bold uppercase tracking-wider text-zinc-500">Win %</div>
              <div className="mt-0.5 whitespace-nowrap font-mono text-[22px] font-bold tracking-tight">{winFav ? `${winFav.pct}%` : "—"}</div>
              <div className="mt-0.5 min-h-3.5 text-[11px] leading-snug text-zinc-400">{winFav?.name ?? ""}</div>
            </div>
          </div>
          {proj && (
            <div className="mt-2.5 text-center">
              <div className="text-[10px] font-bold uppercase tracking-wider text-zinc-500">Projected final</div>
              <div className="mt-0.5 flex flex-wrap items-center justify-center gap-x-2 gap-y-0.5 text-[13px] leading-snug text-zinc-300">
                <span className="whitespace-nowrap">{g.away} <strong className="font-mono font-semibold text-zinc-100">{fmtProjectedScore(proj.away)}</strong></span>
                <span className="text-zinc-600" aria-hidden="true">·</span>
                <span className="whitespace-nowrap">{g.home} <strong className="font-mono font-semibold text-zinc-100">{fmtProjectedScore(proj.home)}</strong></span>
              </div>
            </div>
          )}
          <div className="mt-2.5 flex h-1.5 overflow-hidden rounded-full bg-ink-700">
            <i className="bg-court-400" style={{ width: `${pAway}%` }} />
            <b className="bg-[#60a5fa]" style={{ width: `${pHome}%` }} />
          </div>
          <div className="mt-1 flex justify-between text-[11px] text-zinc-400">
            <span>{g.away} <strong className="font-semibold text-zinc-100">{pAway}%</strong></span>
            <span>{g.home} <strong className="font-semibold text-zinc-100">{pHome}%</strong></span>
          </div>
        </div>

        <p className="mt-3 text-[11px] leading-snug text-zinc-500">{LIVE_LINE_DISCLAIMER}</p>
      </div>
    </div>
  );

  if (!mounted) return null;
  return createPortal(body, document.body);
}

/** Orange opener on the Bet tab, plus the Live Line panel. */
export function LiveLineEntry({ g }: { g: SlateGame }) {
  const [open, setOpen] = useState(false);
  return (
    <div className="mt-3.5">
      <button type="button" onClick={() => setOpen(true)}
        className="flex h-12 w-full items-center justify-center gap-2 rounded-[14px] bg-gradient-to-r from-[#ff8a2a] to-court-500 text-[15px] font-extrabold tracking-tight text-ink-950">
        <svg viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" strokeWidth="2.4" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
          <circle cx="12" cy="12" r="9" /><path d="M12 7v5l3 2" />
        </svg>
        CZarp Live Line
      </button>
      <p className="mt-2 text-center text-[11px] leading-snug text-zinc-500">Enter the score and time left. CZarp already has home or neutral in the pregame numbers.</p>
      {open && <LivePanel g={g} onClose={() => setOpen(false)} />}
    </div>
  );
}
