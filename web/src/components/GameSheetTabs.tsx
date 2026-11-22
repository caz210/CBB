"use client";
import { useEffect, useState, type ReactNode } from "react";
import type { SlateGame, ValueBet } from "@/lib/types";
import { fmtEdgeVsBar, fmtGameDate, fmtOne, fmtScore, fmtSpread, fmtWinProb, pct } from "@/lib/labels";
import { teamHref } from "@/lib/teams";
import { cardHeadline, marketRatings, type MarketRating } from "@/lib/verdict";
import { nowLine } from "@/lib/cardHeader";
import { betCoverProb, totalFair } from "@/lib/posted";
import { evPer100, fmtEv100 } from "@/lib/stats";
import {
  COMPARE_ROWS,
  DEFAULT_SHEET_TAB,
  MORE_STATS_ROWS,
  SHEET_TABS,
  TAPE_ROWS,
  betterSide,
  formCountLabel,
  formatTeamValue,
  locLabel,
  officialRecordLine,
  scoreLine,
  type SheetExtras,
  type SheetRecentGame,
  type SheetTab,
  type SheetTeamExtras,
} from "@/lib/sheetExtras";
import { ArchetypeChip } from "./ArchetypeChip";
import { InfoTip } from "./InfoTip";
import { VerdictChip } from "./StatusChips";
import Link from "next/link";

const EDGE_TONE = { official: "text-edge-400", late_value: "text-edge-400", value: "text-zinc-300", lean: "text-zinc-300", toss_up: "text-zinc-500" } as const;

export function useSheetExtras(g: SlateGame | null, sportKey: string) {
  const [data, setData] = useState<SheetExtras | null>(null);
  const [loading, setLoading] = useState(false);
  useEffect(() => {
    if (!g) { setData(null); return; }
    const ac = new AbortController();
    setLoading(true);
    const q = new URLSearchParams({ sport: sportKey, away: g.away, home: g.home, date: g.game_date });
    fetch(`/api/sheet-extras?${q}`, { signal: ac.signal })
      .then((r) => (r.ok ? r.json() : Promise.reject(r.status)))
      .then((d) => { if (!ac.signal.aborted) setData(d as SheetExtras); })
      .catch(() => { if (!ac.signal.aborted) setData(null); })
      .finally(() => { if (!ac.signal.aborted) setLoading(false); });
    return () => ac.abort();
  }, [g, g?.game_id, g?.away, g?.home, g?.game_date, sportKey]);
  return { extras: data, loading };
}

function spreadRef(n: number | null, team: string): string {
  if (n == null) return "—";
  return `${team} ${fmtSpread(n)}`;
}

function MarketBetRow({ label, fair, price, rating, official, now, edge }: {
  label: string;
  fair: string;
  price: string;
  rating: MarketRating;
  official: ValueBet | undefined;
  now: string | null;
  edge: string | null;
}) {
  const tone = rating.lineMoveNote ? "text-zinc-300" : EDGE_TONE[rating.level];
  return (
    <div className="rounded-xl border border-ink-700 bg-ink-850 px-3 py-2.5">
      <div className="flex items-start justify-between gap-2">
        <div className="min-w-0">
          <div className="mb-1 flex flex-wrap items-center gap-1.5">
            <span className="text-[10px] font-semibold uppercase tracking-wider text-zinc-500">{label}</span>
            <VerdictChip level={rating.level} noLine={rating.noLine} wrap />
          </div>
          <div className="font-mono text-[13px] leading-snug text-zinc-300">
            <div><span className="font-sans text-[10px] text-zinc-500">FV </span>{fair}</div>
            <div><span className="font-sans text-[10px] text-zinc-500">Price </span>{price}</div>
          </div>
          {official && (
            <div className="mt-1.5 text-[12px] font-semibold text-white">
              CZarp Play · {official.side}
              {now && <span className="ml-1.5 font-mono text-[11px] font-normal text-zinc-500">{now}</span>}
            </div>
          )}
        </div>
        <div className={`shrink-0 font-mono text-2xl font-black leading-none ${tone}`}>{edge ?? "—"}</div>
      </div>
    </div>
  );
}

export function QuickBetBlock({ g, bets }: { g: SlateGame; bets: ValueBet[] }) {
  const ratings = marketRatings(g, bets);
  const czSpread = g.proj_spread;
  const czTotal = totalFair(g, bets);
  const curS = g.insight?.cur_spread ?? g.market_spread;
  const curT = g.insight?.cur_total ?? g.market_total;
  const officialSpread = bets.find((b) => b.market === "spread" || b.market === "moneyline");
  const officialTotal = bets.find((b) => b.market === "total");
  const edgeS = ratings.spread.edgeNow != null ? fmtEdgeVsBar(ratings.spread.edgeNow, "spread") : null;
  const edgeT = ratings.total.edgeNow != null ? fmtEdgeVsBar(ratings.total.edgeNow, "total") : null;
  // Home-line notation still names the home team on a neutral floor.
  const spreadTeam = g.home;
  return (
    <div className="space-y-2">
      <MarketBetRow
        label="Spread"
        fair={czSpread == null ? "—" : spreadRef(czSpread, spreadTeam)}
        price={curS == null ? "—" : spreadRef(curS, spreadTeam)}
        rating={ratings.spread}
        official={officialSpread}
        now={officialSpread ? nowLine(officialSpread, g) : null}
        edge={edgeS}
      />
      <MarketBetRow
        label="Total"
        fair={czTotal.value}
        price={curT == null ? "—" : fmtOne(curT)}
        rating={ratings.total}
        official={officialTotal}
        now={officialTotal ? nowLine(officialTotal, g) : null}
        edge={edgeT}
      />
      <ProjStrip g={g} bets={bets} />
    </div>
  );
}

function ProjStrip({ g, bets }: { g: SlateGame; bets: ValueBet[] }) {
  const ph = g.insight?.proj_home ?? g.proj_home;
  const pa = g.insight?.proj_away ?? g.proj_away;
  const hw = g.insight?.home_win_prob ?? g.home_win_prob;
  const ov = g.czarp_over_prob ?? null;
  const poss = g.insight?.proj_possessions ?? g.proj_possessions;
  const edge = cardHeadline(g, bets);
  const cover = betCoverProb(g, edge.officialBet, edge.market ?? edge.officialBet?.market ?? null);
  const odds = edge.officialBet?.posted_odds;
  const ev = evPer100(cover, odds);
  return (
    <div className="grid grid-cols-2 gap-1.5 text-[11px]">
      <div className="rounded-lg bg-ink-850 px-2.5 py-2">
        <div className="text-[10px] uppercase tracking-wider text-zinc-500">CZarp projection</div>
        <div className="mt-0.5 font-mono text-sm text-zinc-200">
          <span className="block truncate text-zinc-400">{g.away} {pa == null ? "—" : fmtScore(pa)}</span>
          <span className="block truncate text-zinc-200">{g.home} {ph == null ? "—" : fmtScore(ph)}</span>
        </div>
      </div>
      <div className="rounded-lg bg-ink-850 px-2.5 py-2">
        <div className="text-[10px] uppercase tracking-wider text-zinc-500">{g.neutral ? `${g.home} win` : "Home win"} · Over</div>
        <div className="mt-0.5 font-mono text-sm text-zinc-200">{hw == null ? "—" : fmtWinProb(hw)} · {ov == null ? "—" : pct(ov)}</div>
        {poss != null && <div className="text-[10px] text-zinc-500">Pace {poss.toFixed(1)} poss.</div>}
        {edge.officialBet && ev != null && <div className="text-[10px] text-zinc-500">EV {fmtEv100(ev)}</div>}
      </div>
    </div>
  );
}

function CompareTable({ away, home, rows = COMPARE_ROWS, sportKey, showHeads = true }: {
  away: SheetTeamExtras | null;
  home: SheetTeamExtras | null;
  rows?: typeof COMPARE_ROWS;
  sportKey: string;
  showHeads?: boolean;
}) {
  return (
    <div>
      {showHeads && (
        <div className="mb-1 grid grid-cols-[1fr_auto_1fr] items-end gap-x-2 px-1">
          <TeamHead t={away} sportKey={sportKey} align="left" />
          <span />
          <TeamHead t={home} sportKey={sportKey} align="right" />
        </div>
      )}
      <div className="divide-y divide-ink-700/60 rounded-xl border border-ink-700">
        {rows.map((row) => {
          const win = betterSide(away, home, row);
          const a = formatTeamValue(away, row);
          const h = formatTeamValue(home, row);
          const ap = row.pctile ? away ? awayValuePctile(away, row.key) : null : null;
          const hp = row.pctile ? home ? awayValuePctile(home, row.key) : null : null;
          return (
            <div key={row.key} className="grid grid-cols-[1fr_auto_1fr] items-center gap-x-2 px-2.5 py-1.5">
              <div className={`min-w-0 text-left font-mono text-[13px] ${win === "away" ? "font-bold text-edge-400" : "text-zinc-300"}`}>{a}{row.pctile && a !== "—" ? <span className="text-[10px] font-normal text-zinc-500">%ile</span> : null}</div>
              <div className="flex items-center justify-center gap-0.5 text-center text-[10px] uppercase tracking-wider text-zinc-500">
                {row.hint ? <TermOrPlain label={row.label} hint={row.hint} /> : row.label}
              </div>
              <div className={`min-w-0 text-right font-mono text-[13px] ${win === "home" ? "font-bold text-edge-400" : "text-zinc-300"}`}>{h}{row.pctile && h !== "—" ? <span className="text-[10px] font-normal text-zinc-500">%ile</span> : null}</div>
              {row.pctile && (ap != null || hp != null) && (
                <div className="col-span-3 mb-1 mt-0.5 grid grid-cols-[1fr_auto_1fr] gap-x-2">
                  <PctBar value={ap} align="left" win={win === "away"} />
                  <span />
                  <PctBar value={hp} align="right" win={win === "home"} />
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}

function awayValuePctile(t: SheetTeamExtras, key: string): number | null {
  const map: Record<string, number | null> = {
    efg_pctile: t.efg_pctile, d_efg_pctile: t.d_efg_pctile,
    to_pct_pctile: t.to_pct_pctile, d_to_pct_pctile: t.d_to_pct_pctile,
    or_pct_pctile: t.or_pct_pctile, opp_or_pct_pctile: t.opp_or_pct_pctile,
    ftr_pctile: t.ftr_pctile, d_ftr_pctile: t.d_ftr_pctile,
  };
  const n = map[key];
  return typeof n === "number" && Number.isFinite(n) ? n : null;
}

function TermOrPlain({ label, hint }: { label: string; hint: string }) {
  return (
    <span className="inline-flex items-center gap-0.5">
      {label}
      <InfoTip title={label} body={hint} />
    </span>
  );
}

function PctBar({ value, align, win }: { value: number | null; align: "left" | "right"; win: boolean }) {
  if (value == null) return <div />;
  const w = Math.max(0, Math.min(100, value));
  return (
    <div className="h-1 overflow-hidden rounded-full bg-ink-700">
      <div
        className={`h-full rounded-full ${win ? "bg-edge-400" : "bg-zinc-500"} ${align === "right" ? "ml-auto" : ""}`}
        style={{ width: `${w}%` }}
      />
    </div>
  );
}

function TeamHead({ t, sportKey, align }: { t: SheetTeamExtras | null; sportKey: string; align: "left" | "right" }) {
  if (!t) return <div className={`text-[12px] text-zinc-500 ${align === "right" ? "text-right" : ""}`}>—</div>;
  return (
    <div className={align === "right" ? "min-w-0 text-right" : "min-w-0 text-left"}>
      <div className={`flex items-center gap-1 ${align === "right" ? "justify-end" : ""}`}>
        <Link href={teamHref(sportKey, t.name)} onClick={(e) => e.stopPropagation()} className="min-w-0 truncate text-[13px] font-bold hover:text-court-400">{t.name}</Link>
        {t.tentative && (
          <InfoTip title="Tentative" body="The sample is still early.">
            <span className="shrink-0 border-b border-dashed border-zinc-500 text-[10px] font-semibold leading-none text-zinc-500">tent.</span>
          </InfoTip>
        )}
      </div>
    </div>
  );
}

function TagsRow({ away, home }: { away: SheetTeamExtras | null; home: SheetTeamExtras | null }) {
  const a = away?.tags ?? [];
  const h = home?.tags ?? [];
  if (!a.length && !h.length) return null;
  return (
    <div className="mt-3 grid grid-cols-2 gap-2">
      {[{ n: away?.name, t: a }, { n: home?.name, t: h }].map((col) => (
        <div key={col.n ?? "x"} className="min-w-0">
          <div className="mb-1 truncate text-[11px] text-zinc-500">{col.n}</div>
          <div className="flex flex-wrap gap-1">{col.t.length ? col.t.map((tag) => <ArchetypeChip key={`${tag.kind}-${tag.name}`} tag={tag} />) : <span className="text-[11px] text-zinc-600">No style tags yet</span>}</div>
        </div>
      ))}
    </div>
  );
}

/** Official-play result only. Never a Toss Up / Lean / model-side chip. */
function OfficialBadge({ picks }: { picks: SheetRecentGame["official"] }) {
  if (!picks.length) return null;
  return (
    <div className="mt-0.5 space-y-0.5">
      {picks.map((p, i) => {
        const tone = p.result === "W" ? "text-edge-400" : p.result === "L" ? "text-fade-400" : "text-zinc-400";
        const res = p.result === "W" ? "Win" : p.result === "L" ? "Loss" : p.result === "P" ? "Push" : null;
        return (
          <div key={`${p.pick}-${i}`} className={`text-[11px] ${tone}`}>
            CZarp: {p.pick}{res ? ` · ${res}` : ""}
          </div>
        );
      })}
    </div>
  );
}

function RecentList({ t, max = 5 }: { t: SheetTeamExtras | null; max?: number }) {
  if (!t) return <p className="text-sm text-zinc-500">No team data.</p>;
  const official = officialRecordLine(t);
  const games = t.recent.slice(0, max);
  return (
    <div>
      <div className="mb-1.5 min-w-0">
        <div className="truncate text-[13px] font-bold">{t.name}</div>
        <div className="text-[11px] text-zinc-500">{formCountLabel(t, games.length)}</div>
      </div>
      {official && <div className="mb-1.5 text-[11px] text-zinc-400">Official picks: {official}</div>}
      {games.length === 0 ? (
        <p className="text-[12px] text-zinc-500">No recent results in our log.</p>
      ) : (
        <ul className="space-y-1.5">
          {games.map((g) => {
            const sc = scoreLine(g);
            const tone = g.result === "W" ? "text-edge-400" : g.result === "L" ? "text-fade-400" : "text-zinc-400";
            return (
              <li key={g.game_id} className="rounded-lg bg-ink-850 px-2.5 py-1.5">
                <div className="flex items-baseline justify-between gap-2">
                  <div className="min-w-0">
                    <div className="text-[10px] text-zinc-500">{fmtGameDate(g.date)}</div>
                    <div className="truncate text-[12px] font-semibold">
                      <span className={`mr-1.5 font-mono ${tone}`}>{g.result ?? "—"}</span>
                      {locLabel(g.location)} {g.opponent}
                    </div>
                  </div>
                  <div className="shrink-0 font-mono text-[12px] text-zinc-300">{sc ?? "—"}</div>
                </div>
                <OfficialBadge picks={g.official} />
              </li>
            );
          })}
        </ul>
      )}
    </div>
  );
}

function RecentBlock({ extras, max = 5 }: { extras: SheetExtras | null; max?: number }) {
  if (!extras) return null;
  return (
    <div className="grid grid-cols-1 gap-4 min-[360px]:grid-cols-2">
      <RecentList t={extras.away} max={max} />
      <RecentList t={extras.home} max={max} />
    </div>
  );
}


function MoreStats({ away, home, sportKey }: { away: SheetTeamExtras | null; home: SheetTeamExtras | null; sportKey: string }) {
  const [open, setOpen] = useState(false);
  return (
    <div className="mt-3">
      <button
        type="button"
        aria-expanded={open}
        onClick={() => setOpen((v) => !v)}
        className="flex min-h-10 w-full items-center justify-center rounded-xl border border-ink-600 text-sm font-semibold text-zinc-300 hover:bg-ink-800"
      >
        {open ? "Hide extra stats" : "More stats"}
      </button>
      {open && (
        <div className="mt-2">
          <CompareTable away={away} home={home} rows={MORE_STATS_ROWS} sportKey={sportKey} showHeads={false} />
        </div>
      )}
    </div>
  );
}

function ExtrasState({ loading, extras, children }: { loading: boolean; extras: SheetExtras | null; children: (extras: SheetExtras) => ReactNode }) {
  if (loading && !extras) return <p className="text-sm text-zinc-500">Loading comparison…</p>;
  if (!extras || (!extras.away && !extras.home)) return <p className="text-sm text-zinc-500">Comparison data isn’t available for this game.</p>;
  return <>{children(extras)}</>;
}

export function GameSheetTabs({ g, bets, sportKey, extras, loading }: { g: SlateGame; bets: ValueBet[]; sportKey: string; extras: SheetExtras | null; loading: boolean }) {
  const [tab, setTab] = useState<SheetTab>(DEFAULT_SHEET_TAB);
  return (
    <div>
      <div className="mb-3 grid grid-cols-3 gap-1 rounded-xl border border-ink-600 p-1">
        {SHEET_TABS.map((t) => (
          <button
            key={t.k}
            type="button"
            onClick={() => setTab(t.k)}
            className={`min-h-10 rounded-lg text-sm font-semibold ${tab === t.k ? "bg-ink-700 text-white" : "text-zinc-500 hover:text-zinc-300"}`}
          >
            {t.l}
          </button>
        ))}
      </div>
      {tab === "bet" && <QuickBetBlock g={g} bets={bets} />}
      {tab === "matchup" && (
        <ExtrasState loading={loading} extras={extras}>
          {(e) => (
            <>
              <CompareTable away={e.away} home={e.home} rows={TAPE_ROWS} sportKey={sportKey} />
              <TagsRow away={e.away} home={e.home} />
              <MoreStats away={e.away} home={e.home} sportKey={sportKey} />
            </>
          )}
        </ExtrasState>
      )}
      {tab === "form" && (
        <ExtrasState loading={loading} extras={extras}>
          {(e) => <RecentBlock extras={e} />}
        </ExtrasState>
      )}
    </div>
  );
}
