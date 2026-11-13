"use client";
import { useEffect, useState, type ReactNode } from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import type { SlateGame, ValueBet } from "@/lib/types";
import { LOCKED_NOTE, MARKET_LABEL, TIER_BADGE, edgeTier, finalLabel, fmtLine, fmtPostedStamp, fmtPlayPct, fmtWinProb, fmtTipoff, pct, signedPct } from "@/lib/labels";
import { useWatchlist } from "@/lib/watchlist";
import { weaknessesOnly } from "@/lib/archetypes";
import { teamHref } from "@/lib/teams";
import Link from "next/link";
import { ArchetypeChip, ArchetypeLegend } from "./ArchetypeChip";
import { InfoTip, TermLabel } from "./InfoTip";
import { NewCoachChips } from "./NewCoachBadge";
import { cardHeadline, notAPlayAlso, notAPlayLine } from "@/lib/verdict";
import { evPer100, fmtEv100 } from "@/lib/stats";
import { betCoverProb, postedFairVsProjections, postedVsNow } from "@/lib/posted";

type Sel = { game: SlateGame; bet: ValueBet } | null;

export function SlateBoard({ games, bets, tracked = [], sort = "time", hide = false, sportKey = "cbb" }: {
  games: SlateGame[]; bets: ValueBet[]; tracked?: ValueBet[]; sort?: "time" | "value"; hide?: boolean; sportKey?: string;
}) {
  const [sel, setSel] = useState<Sel>(null);
  const [gameSel, setGameSel] = useState<SlateGame | null>(null);
  const [watchOnly, setWatchOnly] = useState(false);
  const { has, toggle, count: watchCount, ready } = useWatchlist();
  const router = useRouter();
  const pathname = usePathname();
  const search = useSearchParams();
  const setParam = (k: string, v: string | null) => {
    const q = new URLSearchParams(search.toString());
    if (v == null) q.delete(k); else q.set(k, v);
    const qs = q.toString();
    router.replace(`${pathname}${qs ? `?${qs}` : ""}`, { scroll: false });
  };
  const byGame = (id: number) => bets.filter((b) => b.game_id === id);
  const raw = (g: SlateGame) => g.rawEdge ?? -Infinity;
  const tipMs = (g: SlateGame) => (g.tipoff ? Date.parse(g.tipoff) : Infinity);
  const byTime = (a: SlateGame, z: SlateGame) => tipMs(a) - tipMs(z) || a.game_id - z.game_id;
  const completedCount = games.filter((g) => g.completed).length;
  const shown = games
    .filter((g) => !(hide && g.completed))
    .filter((g) => !(watchOnly && ready && !has(g.game_id)))
    .slice()
    .sort(sort === "value"
      ? (a, z) => {
          const ea = raw(a), ez = raw(z);
          if (ea === ez) return byTime(a, z);
          return ez - ea; // no raw edge sorts last
        }
      : byTime);
  const officialShown = bets.filter((b) => !hide || !games.find((g) => g.game_id === b.game_id)?.completed);

  return (
    <div>
      <div className="mb-4 flex flex-wrap items-center gap-2">
        <div role="radiogroup" aria-label="Sort games" className="flex flex-1 rounded-xl border border-ink-600 bg-ink-900 p-1 sm:flex-none">
          {(["time", "value"] as const).map((k) => (
            <button key={k} role="radio" aria-checked={sort === k}
              onClick={() => setParam("sort", k === "time" ? null : "value")}
              className={`min-h-11 flex-1 rounded-lg px-4 text-sm font-semibold transition sm:flex-none ${sort === k ? "bg-court-500/20 text-court-400" : "text-zinc-400 hover:bg-ink-800"}`}>
              {k === "time" ? "Game time" : "Edge"}
            </button>
          ))}
        </div>
        <button role="switch" aria-checked={hide} onClick={() => setParam("hide", hide ? null : "1")}
          className="flex min-h-11 items-center gap-2.5 rounded-xl border border-ink-600 px-3 text-sm font-semibold text-zinc-300 hover:bg-ink-800">
          <span className={`relative h-6 w-10 rounded-full transition ${hide ? "bg-court-500" : "bg-ink-600"}`}>
            <span className={`absolute top-0.5 h-5 w-5 rounded-full bg-white transition-all ${hide ? "left-[18px]" : "left-0.5"}`} />
          </span>
          Hide completed{completedCount ? ` · ${completedCount}` : ""}
        </button>
        <button role="switch" aria-checked={watchOnly} onClick={() => setWatchOnly((w) => !w)}
          className={`flex min-h-11 items-center gap-2 rounded-xl border px-3 text-sm font-semibold ${watchOnly ? "border-amber-400/50 bg-amber-400/10 text-amber-300" : "border-ink-600 text-zinc-300 hover:bg-ink-800"}`}>
          <Star filled={watchOnly} />
          Watchlist{ready && watchCount ? ` · ${watchCount}` : ""}
        </button>
        <div className="ml-auto hidden md:block"><ArchetypeLegend /></div>
      </div>

      {watchOnly && ready && shown.length === 0 && (
        <div className="mb-4 rounded-xl border border-ink-700 bg-ink-900 px-3.5 py-3 text-sm text-zinc-400">
          {watchCount === 0
            ? <>No starred games yet. Tap the star on a game card to watch it.</>
            : <>None of your watched games are on this slate{hide ? " (or they are hidden as completed)" : ""}.</>}
          {watchCount > 0 && hide && (
            <>{" "}<button onClick={() => setParam("hide", null)} className="font-semibold text-court-400">Show completed</button></>
          )}
        </div>
      )}

      {shown.length === 0 && !watchOnly && (
        <div className="rounded-xl border border-ink-700 bg-ink-900 px-3.5 py-3 text-sm text-zinc-400">
          All {games.length} games on this slate are completed.{" "}
          <button onClick={() => setParam("hide", null)} className="font-semibold text-court-400">Show them</button>
        </div>
      )}

      {officialShown.length > 0 && (
        <section className="mb-4 rounded-2xl border border-edge-400/30 bg-edge-400/[0.04] p-3 md:p-4" aria-label="Official plays">
          <h2 className="mb-2 text-sm font-semibold text-edge-400">Official plays · {officialShown.length}</h2>
          <ul className="divide-y divide-ink-700/50">
            {officialShown.map((b) => {
              const g = games.find((x) => x.game_id === b.game_id);
              const tier = edgeTier(b.line_edge_pct, b.market);
              return (
                <li key={`${b.game_id}-${b.market}-${b.side}`}>
                  <button type="button" onClick={() => g && setSel({ game: g, bet: b })}
                    className="flex min-h-12 w-full items-center gap-3 py-2 text-left text-sm">
                    {tier ? <span className={`rounded-md px-1.5 py-0.5 text-[10px] font-bold ${TIER_BADGE[tier]}`}>{tier}</span> : <span className="rounded-md bg-edge-400/15 px-1.5 py-0.5 text-[10px] font-bold text-edge-400">Official</span>}
                    <span className="min-w-0 flex-1">
                      <span className="block truncate font-semibold text-white">{b.side} · {MARKET_LABEL[b.market]}</span>
                      <span className="block truncate text-[11px] text-zinc-500">{g?.game ?? `Game #${b.game_id}`}</span>
                    </span>
                    <span className="font-mono text-lg font-black text-edge-400">{pct(b.line_edge_pct, 1)}</span>
                    <span className="font-mono text-xs text-zinc-500">{b.kelly_quarter_units == null ? "—" : `${b.kelly_quarter_units.toFixed(2)}u`}</span>
                  </button>
                </li>
              );
            })}
          </ul>
        </section>
      )}

      <div className="grid gap-3 md:gap-4 lg:grid-cols-2">
        {shown.map((g) => (
          <GameCard
            key={g.game_id}
            g={g}
            sportKey={sportKey}
            bets={byGame(g.game_id)}
            watched={ready && has(g.game_id)}
            onWatch={() => toggle(g.game_id)}
            onOpen={(bet) => setSel({ game: g, bet })}
            onGame={() => setGameSel(g)}
          />
        ))}
      </div>

      {tracked.length > 0 && (
        <section className="mt-6 rounded-2xl border border-dashed border-ink-700 p-3 md:p-4" aria-label="Tracked picks">
          <div className="mb-2 flex flex-wrap items-baseline gap-x-2">
            <h2 className="text-sm font-semibold text-zinc-400">Tracked · {tracked.length}</h2>
            <span className="text-[11px] text-zinc-500">Paper-only 1u, never bet. Not part of the official portfolio.</span>
          </div>
          <ul className="divide-y divide-ink-700/50">
            {tracked.map((b) => {
              const g = games.find((x) => x.game_id === b.game_id);
              const proof = fmtPostedStamp(b.logged_at);
              return (
                <li key={`${b.game_id}-${b.market}-${b.side}`} className="flex items-center gap-3 py-2 text-sm text-zinc-400">
                  <span className="min-w-0 flex-1">
                    <span className="block truncate font-medium text-zinc-300">{b.side}</span>
                    <span className="block truncate text-[11px] text-zinc-500">{g?.game ?? `Game #${b.game_id}`} · {MARKET_LABEL[b.market]} · edge {pct(b.line_edge_pct, 1)}</span>
                    {proof && <span className="block text-[11px] text-zinc-600">{proof}</span>}
                  </span>
                  <span className="font-mono text-xs text-zinc-500">1u</span>
                </li>
              );
            })}
          </ul>
        </section>
      )}

      <div className="mt-4 md:hidden"><ArchetypeLegend /></div>
      <p className="mt-3 text-xs leading-relaxed text-zinc-500">
        Spreads use home-line notation (−5.5 = home favored). Price = Vegas consensus. Fair value = our model. Stakes are ¼-Kelly in units (1u = 1% of bankroll, capped at 2.5u). {LOCKED_NOTE}
      </p>

      <ValueSheet sel={sel} onClose={() => setSel(null)} />
      <GameSheet g={gameSel} sportKey={sportKey} bets={gameSel ? byGame(gameSel.game_id) : []} onClose={() => setGameSel(null)} />
    </div>
  );
}

function Star({ filled, className = "h-4 w-4" }: { filled: boolean; className?: string }) {
  return (
    <svg viewBox="0 0 24 24" className={className} fill={filled ? "currentColor" : "none"} stroke="currentColor" strokeWidth="2" strokeLinejoin="round">
      <path d="M12 3.5l2.6 5.3 5.9.9-4.3 4.2 1 5.8L12 17.8 6.8 19.7l1-5.8L3.5 9.7l5.9-.9L12 3.5z" />
    </svg>
  );
}

function VerdictChips({ g, bets }: { g: SlateGame; bets: ValueBet[] }) {
  const h = cardHeadline(g, bets);
  const official = h.verdict === "official";
  const tier = official && h.officialBet ? edgeTier(h.officialBet.line_edge_pct, h.officialBet.market) : null;
  return (
    <>
      {official
        ? <span className="rounded-md bg-edge-400/15 px-1.5 py-0.5 text-[10px] font-semibold text-edge-400">Official play</span>
        : <span className="rounded-md bg-ink-800 px-1.5 py-0.5 text-[10px] font-semibold text-zinc-400">Low value</span>}
      {tier && <span className={`rounded-md px-1.5 py-0.5 text-[10px] font-bold ${TIER_BADGE[tier]}`}>{tier}</span>}
      {!official && h.lean && (
        <InfoTip term="lean" triggerClassName="rounded-md bg-ink-800 px-1.5 py-0.5 text-[10px] font-semibold text-zinc-400 hover:bg-ink-700 hover:text-zinc-200">
          Lean
        </InfoTip>
      )}
    </>
  );
}

function GameCard({ g, sportKey, bets, watched, onWatch, onOpen, onGame }: {
  g: SlateGame; sportKey: string; bets: ValueBet[]; watched: boolean; onWatch: () => void; onOpen: (b: ValueBet) => void; onGame: () => void;
}) {
  const awayWin = g.home_win_prob == null ? null : 1 - g.home_win_prob;
  const awayCover = g.home_cover_prob == null ? null : 1 - g.home_cover_prob;
  const final = finalScore(g);
  const rows = [
    { side: "Away", name: g.away, proj: g.proj_away, win: awayWin, cover: awayCover, tags: g.archetypes.away, tent: g.archetypes.away_tentative, score: final?.away ?? null, won: final ? final.away > final.home : false },
    { side: "Home", name: g.home, proj: g.proj_home, win: g.home_win_prob, cover: g.home_cover_prob, tags: g.archetypes.home, tent: g.archetypes.home_tentative, score: final?.home ?? null, won: final ? final.home > final.away : false },
  ];
  const edge = cardHeadline(g, bets);
  const hasEdge = edge.edgePct != null;
  const official = edge.verdict === "official";
  const single = official && bets.length === 1 ? bets[0] : null;
  const whyNot = official ? null : notAPlayLine(g, bets);
  const timeLabel = g.completed
    ? finalLabel(g.final_at, g.final_at_estimated, g.game_date)
    : (g.tipoff ? fmtTipoff(g.tipoff) : "Tip-off TBD");
  const headline = edge.officialBet
    ? `${edge.officialBet.side} · ${MARKET_LABEL[edge.officialBet.market]}`
    : edge.market
      ? `${MARKET_LABEL[edge.market]} · fair vs price`
      : g.game;
  const openSingle = (e: { stopPropagation: () => void }) => { e.stopPropagation(); onOpen(single!); };
  const fair = totalFair(g, bets);
  const hasTeamProj = g.proj_home != null || g.proj_away != null;
  return (
    <article
      role="button" tabIndex={0} aria-label={`Insights for ${g.game}`}
      onClick={onGame} onKeyDown={(e) => (e.key === "Enter" || e.key === " ") && (e.preventDefault(), onGame())}
      className={`card cursor-pointer overflow-hidden transition hover:border-ink-500 active:scale-[0.995] ${g.completed ? "opacity-70" : ""}`}>
      <header className="flex items-start gap-2 border-b border-ink-700/60 px-4 py-2.5 text-xs">
        <div className="flex min-w-0 flex-1 flex-wrap items-center gap-1.5">
          <span className="font-semibold text-zinc-300">{timeLabel}</span>
          <VerdictChips g={g} bets={bets} />
          <NewCoachChips locks={g.coachLocks} market={edge.market} />
        </div>
        <button
          type="button"
          aria-label={watched ? "Remove from watchlist" : "Add to watchlist"}
          aria-pressed={watched}
          onClick={(e) => { e.stopPropagation(); onWatch(); }}
          className={`grid h-9 w-9 shrink-0 place-items-center rounded-lg transition ${watched ? "text-amber-300" : "text-zinc-600 hover:text-zinc-300"}`}
        >
          <Star filled={watched} className="h-5 w-5" />
        </button>
        <span aria-hidden="true" className="mt-2 shrink-0 font-sans text-court-400">›</span>
      </header>

      {/* Edge headline. With one official bet, tapping this opens the value sheet. */}
      {single ? (
        <button type="button" onClick={openSingle} aria-label={`Open ${single.side} details`}
          className="flex w-full items-end justify-between gap-3 border-b border-ink-700/40 px-4 py-3 text-left hover:bg-ink-850/60">
          <HeadlineBody headline={headline} subtitle={edge.subtitle} hasEdge={hasEdge} official={official} edgePct={edge.edgePct} />
        </button>
      ) : (
        <div className="flex items-end justify-between gap-3 border-b border-ink-700/40 px-4 py-3">
          <HeadlineBody headline={headline} subtitle={edge.subtitle} hasEdge={hasEdge} official={official} edgePct={edge.edgePct} />
        </div>
      )}

      <div className="divide-y divide-ink-700/40 px-4">
        {hasTeamProj && (
          <div className="flex justify-end pt-2 text-[9px] font-semibold uppercase tracking-wider text-zinc-600" title="Team scores are the czarp-v2 projection, a separate model from the official czarp-v1-totals total">
            v2 team projection
          </div>
        )}
        {rows.map((r) => (
          <div key={r.side} className="py-3">
            <div className="flex items-center gap-3">
              <span className="w-9 shrink-0 text-[10px] font-semibold uppercase text-zinc-600">{r.side}</span>
              <Link href={teamHref(sportKey, r.name)} onClick={(e) => e.stopPropagation()} className="min-w-0 flex-1 truncate text-[15px] font-semibold hover:text-court-400">{r.name}</Link>
              {final ? (
                <>
                  <span className="text-right font-mono text-xs text-zinc-500">{r.proj == null ? "—" : r.proj.toFixed(1)}<span className="ml-0.5 font-sans text-[10px] text-zinc-600">proj</span></span>
                  <span className={`w-12 text-right font-mono text-xl font-bold ${r.won ? "text-white" : "text-zinc-400"}`} aria-label={`${r.name} final score ${r.score}`}>{r.score}</span>
                </>
              ) : (
                <>
                  <span className={`text-right font-mono text-xs ${r.win != null && r.win >= 0.5 ? "text-white" : "text-zinc-500"}`}>{r.win == null ? "—" : fmtWinProb(r.win)}<span className="ml-0.5 font-sans text-[10px] text-zinc-600">win</span></span>
                  <span className="w-12 text-right font-mono text-xl font-bold">{r.proj == null ? "—" : r.proj.toFixed(1)}</span>
                </>
              )}
            </div>
            {weaknessesOnly(r.tags, 2).length > 0 && (
              <div className="mt-2 flex flex-wrap gap-1.5 pl-12">
                {weaknessesOnly(r.tags, 2).map((tag) => <ArchetypeChip key={tag.name} tag={tag} />)}
              </div>
            )}
          </div>
        ))}
      </div>

      {official && postedFairVsProjections(edge.officialBet, g) && (
        <div className="border-t border-ink-700/40 px-4 py-2 font-mono text-[11px] leading-snug text-zinc-400">
          {postedFairVsProjections(edge.officialBet, g)}
        </div>
      )}

      {whyNot && (
        <div className="border-t border-ink-700/40 px-4 py-2" onClick={(e) => e.stopPropagation()}>
          <NotAPlay line={whyNot} showTip={!edge.subtitle} />
        </div>
      )}

      <div className="grid grid-cols-2 gap-px border-t border-ink-700/60 bg-ink-700/40 text-center">
        <MarketStat
          label="Spread"
          fair={g.proj_spread == null ? "—" : fmtLine(g.proj_spread)}
          price={g.market_spread == null ? "—" : fmtLine(g.market_spread)}
        />
        <MarketStat
          label="Total"
          fair={fair.value}
          price={g.market_total?.toFixed(1) ?? "—"}
          tag={fair.tag}
        />
      </div>

      {(g.archetypes.notes.length > 0 || g.reason) && (
        <div className="space-y-1 border-t border-ink-700/60 px-4 py-2 text-[11px] text-zinc-400">
          {g.archetypes.notes.length > 0 && (
            <div>
              <span className="font-semibold text-zinc-500">Matchup: </span>{g.archetypes.notes.join(" · ")}
              {g.archetypes.tentative && <span className="text-zinc-600"> (tentative)</span>}
            </div>
          )}
          {g.reason && (
            <div><span className="font-semibold text-zinc-500">Why: </span>{g.reason}</div>
          )}
        </div>
      )}

      {bets.length > 1 && (
        <div className="space-y-2 border-t border-ink-700/60 bg-edge-400/[0.03] p-3">
          {bets.map((b) => {
            const tier = edgeTier(b.line_edge_pct, b.market);
            const muted = tier == null;
            return (
              <button
                key={`${b.market}-${b.side}`}
                onClick={(e) => { e.stopPropagation(); onOpen(b); }}
                className={`flex min-h-12 w-full items-center gap-3 rounded-xl border bg-ink-900 px-3 py-2 text-left transition active:scale-[0.99] ${muted ? "border-ink-600 hover:border-ink-500" : "border-edge-400/30 hover:border-edge-400/60"}`}
              >
                {tier ? <span className={`rounded-md px-1.5 py-1 text-[10px] font-bold ${TIER_BADGE[tier]}`}>{tier}</span> : <span className="rounded-md bg-ink-800 px-1.5 py-1 text-[10px] font-bold text-zinc-500">Official</span>}
                <span className="min-w-0 flex-1 truncate text-sm font-semibold">{b.side}</span>
                <span className={`font-mono text-lg font-black leading-none ${muted ? "text-zinc-500" : "text-edge-400"}`}>{pct(b.line_edge_pct, 1)}</span>
                <span className="font-mono text-[11px] text-zinc-500">{b.kelly_quarter_units == null ? "—" : `${b.kelly_quarter_units.toFixed(2)}u`}</span>
                <span className="text-zinc-600">›</span>
              </button>
            );
          })}
        </div>
      )}
    </article>
  );
}

/** Final score when the game is completed and both scores are stored; otherwise null. */
function finalScore(g: SlateGame): { home: number; away: number } | null {
  if (!g.completed || g.home_score == null || g.away_score == null) return null;
  return { home: g.home_score, away: g.away_score };
}

/** Fair total for a card or sheet. Always czarp-v1-totals (the official totals model):
 *  the posted fair for an official total, else the current v1 projection, else "—".
 *  Never the v2 team-score sum. */
function totalFair(g: SlateGame, bets: ValueBet[]): { value: string; num: number | null; tag?: "posted" | "v1" } {
  const posted = bets.find((b) => b.market === "total" && b.pick_tier !== "track" && typeof b.fair_at_post === "number" && Number.isFinite(b.fair_at_post));
  if (posted?.fair_at_post != null) return { value: posted.fair_at_post.toFixed(1), num: posted.fair_at_post, tag: "posted" };
  if (g.proj_total_v1 != null) return { value: g.proj_total_v1.toFixed(1), num: g.proj_total_v1, tag: "v1" };
  return { value: "—", num: null };
}

function totalModelDetail(g: SlateGame): string | null {
  const parts: string[] = [];
  if (g.proj_total_v1 != null) parts.push(`czarp-v1-totals ${g.proj_total_v1.toFixed(1)} (official)`);
  if (g.proj_total_v2 != null) parts.push(`czarp-v2 team sum ${g.proj_total_v2.toFixed(1)} (second look)`);
  return parts.length ? parts.join(" · ") : null;
}

function MarketStat({ label, fair, price, tag }: { label: string; fair: string; price: string; tag?: string }) {
  return (
    <div className="bg-ink-900 px-2 py-2.5">
      <div className="text-[10px] uppercase tracking-wider text-zinc-500">{label}</div>
      <div className="font-mono text-sm font-semibold">
        <span className="text-[10px] font-sans font-normal text-zinc-600">FV </span>{fair}
        {tag && <span className="ml-1 align-middle rounded px-1 py-px font-sans text-[9px] font-bold normal-case tracking-normal text-zinc-400 ring-1 ring-ink-600">{tag}</span>}
      </div>
      <div className="font-mono text-[11px] text-zinc-500"><span className="font-sans">Price </span>{price}</div>
    </div>
  );
}

function HeadlineBody({ headline, subtitle, hasEdge, official, edgePct }: {
  headline: string; subtitle?: string | null; hasEdge: boolean; official: boolean; edgePct: number | null;
}) {
  return (
    <>
      <div className="min-w-0">
        <div className="flex items-center gap-1 text-sm font-semibold text-zinc-300">
          <span className="truncate">{headline}</span>
          <InfoTip term="edge" />
        </div>
        {subtitle && (
          <div className="mt-0.5 flex items-start gap-1 font-mono text-[11px] leading-snug text-zinc-500">
            <span className="min-w-0">{subtitle}</span>
            <InfoTip term="openLineRule" />
          </div>
        )}
      </div>
      <div className={`shrink-0 font-mono text-4xl font-black leading-none ${!hasEdge ? "text-zinc-700" : official ? "text-edge-400" : "text-zinc-500"}`}>
        {hasEdge && edgePct != null ? fmtPlayPct(edgePct) : "—"}
      </div>
    </>
  );
}

function ValueSheet({ sel, onClose }: { sel: Sel; onClose: () => void }) {
  useEffect(() => {
    const k = (e: KeyboardEvent) => e.key === "Escape" && onClose();
    window.addEventListener("keydown", k);
    return () => window.removeEventListener("keydown", k);
  }, [onClose]);
  if (!sel) return null;
  const { game: g, bet: b } = sel;
  const tier = edgeTier(b.line_edge_pct, b.market);
  const ev = evPer100(b.model_prob, b.posted_odds);
  const evOdds = b.posted_odds == null || !Number.isFinite(b.posted_odds) || b.posted_odds === 0 ? null : b.posted_odds;
  return (
    <div className="fixed inset-0 z-50 flex items-end justify-center md:items-center" role="dialog" aria-modal="true">
      <button aria-label="Close" onClick={onClose} className="absolute inset-0 bg-black/70" />
      <div className="relative w-full max-w-lg rounded-t-3xl border border-ink-700 bg-ink-900 p-5 pb-[calc(1.25rem+env(safe-area-inset-bottom))] shadow-2xl md:rounded-3xl">
        <div className="mx-auto mb-4 h-1 w-10 rounded-full bg-ink-600 md:hidden" />
        <div className="text-xs text-zinc-500">{g.game}{g.tipoff ? ` · ${fmtTipoff(g.tipoff)}` : ""}</div>
        <div className="mt-1 flex items-center justify-between gap-3">
          <h3 className="text-xl font-bold">{b.side}</h3>
          <span className="flex flex-wrap items-center justify-end gap-1.5">
            <span className="rounded-md bg-edge-400/15 px-2 py-1 text-[10px] font-semibold text-edge-400">Official play</span>
            {tier && <span className={`rounded-md px-2 py-1 text-xs font-bold ${TIER_BADGE[tier]}`}>{tier}</span>}
          </span>
        </div>
        <div className="mt-1 text-sm text-zinc-400">{MARKET_LABEL[b.market]}</div>

        <div className="mt-4 text-center">
          <div className="inline-flex items-center gap-1.5 text-[10px] font-semibold uppercase tracking-wider text-zinc-500">
            <TermLabel term="edge">Edge</TermLabel>
          </div>
          <div className={`font-mono text-5xl font-black ${tier ? "text-edge-400" : "text-zinc-500"}`}>{pct(b.line_edge_pct, 1)}</div>
          {postedVsNow(b, g) && (
            <p className="mt-1 font-mono text-xs text-zinc-400">{postedVsNow(b, g)}</p>
          )}
          <p className="mt-1 text-[11px] text-zinc-500">Buy at value. One game proves nothing; every pick is time-stamped and graded in public.</p>
        </div>

        <div className="mt-5 grid grid-cols-2 gap-2">
          <Big label="Model prob" v={b.model_prob == null ? "—" : pct(b.model_prob, 1)} />
          <Big label="Market prob" v={pct(b.market_prob, 1)} sub="implied, incl. vig" />
          <Big label={<TermLabel term="edge">Edge</TermLabel>} v={pct(b.line_edge_pct, 1)} sub="vs price" accent />
          <Big label="Prob edge" v={signedPct(b.prob_edge)} sub="secondary" />
          <Big label="EV / $100" v={ev == null ? "—" : fmtEv100(ev)} sub={evOdds == null ? "at −110" : `at ${evOdds > 0 ? "+" : ""}${evOdds}`} />
        </div>

        <div className="mt-3 flex items-center justify-between rounded-2xl border border-edge-400/30 bg-edge-400/10 p-4">
          <div>
            <div className="text-xs font-semibold uppercase tracking-wider text-edge-400">Suggested stake</div>
            <div className="text-[11px] text-zinc-400">¼-Kelly · 1u = 1% of bankroll</div>
          </div>
          <div className="font-mono text-3xl font-black">{b.kelly_quarter_units == null ? "—" : `${b.kelly_quarter_units.toFixed(2)}u`}</div>
        </div>
        {b.logged_at && <p className="mt-3 text-[11px] text-zinc-400">{fmtPostedStamp(b.logged_at)}</p>}
        <p className="mt-3 text-[11px] text-zinc-500">Paper-trading season: logged to the public ledger, no real money.</p>
        <button onClick={onClose} className="mt-4 min-h-12 w-full rounded-xl border border-ink-600 font-semibold hover:bg-ink-800">Done</button>
      </div>
    </div>
  );
}

function GameSheet({ g, sportKey, bets, onClose }: { g: SlateGame | null; sportKey: string; bets: ValueBet[]; onClose: () => void }) {
  useEffect(() => {
    const k = (e: KeyboardEvent) => e.key === "Escape" && onClose();
    window.addEventListener("keydown", k);
    return () => window.removeEventListener("keydown", k);
  }, [onClose]);
  if (!g) return null;
  const i = g.insight;
  const ph = i?.proj_home ?? g.proj_home, pa = i?.proj_away ?? g.proj_away;
  const czLine = ph != null && pa != null ? -(ph - pa) : g.proj_spread;
  const czTotal = totalFair(g, bets); // v1 only, never the v2 team sum
  const openS = i?.open_spread ?? null, curS = i?.cur_spread ?? g.market_spread;
  const openT = i?.open_total ?? null, curT = i?.cur_total ?? g.market_total;
  const move = (o: number | null, c: number | null) => (o == null || c == null ? "—" : c === o ? "No move" : `${c > o ? "+" : "−"}${Math.abs(c - o).toFixed(1)}`);
  // Over probability from the official totals model (v1), never the default model's.
  const hw = i?.home_win_prob ?? g.home_win_prob, ov = g.over_prob_v1 ?? null;
  const edge = cardHeadline(g, bets);
  const hasEdge = edge.edgePct != null;
  return (
    <div className="fixed inset-0 z-50 flex items-end justify-center md:items-center" role="dialog" aria-modal="true" aria-label={`${g.game} insights`}>
      <button aria-label="Close" onClick={onClose} className="absolute inset-0 bg-black/70" />
      <div className="relative max-h-[88dvh] w-full max-w-lg overflow-y-auto rounded-t-3xl border border-ink-700 bg-ink-900 p-5 pb-[calc(1.25rem+env(safe-area-inset-bottom))] shadow-2xl md:rounded-3xl">
        <div className="mx-auto mb-4 h-1 w-10 rounded-full bg-ink-600 md:hidden" />
        <div className="text-xs text-zinc-500">{g.tipoff ? fmtTipoff(g.tipoff) : "Tip-off TBD"}{g.completed ? ` · ${finalLabel(g.final_at, g.final_at_estimated, g.game_date)}` : ""}</div>
        <h3 className="mt-1 text-xl font-bold">
          <Link href={teamHref(sportKey, g.away)} onClick={(e) => e.stopPropagation()} className="hover:text-court-400">{g.away}</Link>
          <span className="text-zinc-500"> @ </span>
          <Link href={teamHref(sportKey, g.home)} onClick={(e) => e.stopPropagation()} className="hover:text-court-400">{g.home}</Link>
        </h3>
        {finalScore(g) && (
          <div className="mt-1 font-mono text-sm font-semibold text-zinc-200">Final: {g.away} {finalScore(g)!.away}, {g.home} {finalScore(g)!.home}</div>
        )}
        {(i?.upset_alert || g.upset_alert) && (
          <div className="mt-2 rounded-xl border border-amber-400/40 bg-amber-400/10 px-3 py-2 text-sm text-amber-100">
            <div className="text-[10px] font-bold uppercase tracking-wider text-amber-300">Upset alert</div>
            <div className="mt-0.5 text-[13px] text-amber-50/90">{i?.upset_reason || g.upset_reason || "Underdog style matchup flagged by the model."}</div>
          </div>
        )}
        {i?.neutral && <div className="mt-2 inline-block rounded-md border border-ink-600 px-2 py-0.5 text-[11px] text-zinc-400">Neutral site: no home-court edge applied</div>}

        <div className="mt-4 flex items-end justify-between rounded-2xl border border-ink-700 bg-ink-850 px-4 py-3">
          <div className="min-w-0">
            <div className="flex flex-wrap items-center gap-1.5">
              <VerdictChips g={g} bets={bets} />
              {/* Same market as the card so the chip matches the "Not a play" line; the chip's sheet still lists both pause lengths. */}
              <NewCoachChips locks={g.coachLocks} market={edge.market} />
            </div>
            <div className="mt-1 truncate text-sm text-zinc-300">
              {edge.officialBet ? `${edge.officialBet.side} · ${MARKET_LABEL[edge.officialBet.market]}` : edge.market ? `${MARKET_LABEL[edge.market]} · fair vs price` : "Fair value vs price"}
            </div>
            {edge.subtitle && (
              <div className="mt-0.5 flex items-start gap-1 font-mono text-[11px] leading-snug text-zinc-500">
                <span className="min-w-0">{edge.subtitle}</span>
                <InfoTip term="openLineRule" />
              </div>
            )}
          </div>
          <div className={`shrink-0 font-mono text-4xl font-black ${!hasEdge ? "text-zinc-700" : edge.verdict === "official" ? "text-edge-400" : "text-zinc-500"}`}>
            {hasEdge && edge.edgePct != null ? fmtPlayPct(edge.edgePct) : "—"}
          </div>
        </div>
        <SheetSecondary g={g} bets={bets} />
        {edge.verdict === "official" && postedFairVsProjections(edge.officialBet, g) && (
          <p className="mt-2 font-mono text-[11px] leading-snug text-zinc-400">{postedFairVsProjections(edge.officialBet, g)}</p>
        )}
        <NotAPlay className="mt-2" line={edge.verdict === "official" ? null : notAPlayLine(g, bets)} showTip={!edge.subtitle} />
        {edge.verdict !== "official" && notAPlayAlso(g, bets).length > 0 && (
          <ul className="mt-1 space-y-0.5 text-[11px] leading-snug text-zinc-500">
            {notAPlayAlso(g, bets).map((l) => <li key={l}>{l}</li>)}
          </ul>
        )}

        <div className="mt-4 grid grid-cols-2 gap-2">
          <Big label={`${g.away} proj`} v={pa == null ? "—" : pa.toFixed(1)} sub="v2 team projection" />
          <Big label={`${g.home} proj`} v={ph == null ? "—" : ph.toFixed(1)} sub="v2 team projection" />
        </div>

        <div className="mt-3 overflow-hidden rounded-xl border border-ink-700 text-sm">
          <div className="grid grid-cols-5 bg-ink-850 px-3 py-2 text-[10px] uppercase tracking-wider text-zinc-500">
            <span className="col-span-1" />
            <span className="flex items-center justify-end gap-0.5 text-right"><TermLabel term="fairValue">FV</TermLabel></span>
            <span className="text-right">Open</span>
            <span className="flex items-center justify-end gap-0.5 text-right"><TermLabel term="price">Price</TermLabel></span>
            <span className="text-right">Move</span>
          </div>
          {[
            { k: "Spread", cz: czLine == null ? "—" : fmtLine(czLine), o: openS == null ? "—" : fmtLine(openS), c: curS == null ? "—" : fmtLine(curS), m: move(openS, curS) },
            { k: "Total", cz: czTotal.value, tag: czTotal.tag, o: openT?.toFixed(1) ?? "—", c: curT?.toFixed(1) ?? "—", m: move(openT, curT) },
          ].map((r: { k: string; cz: string; tag?: string; o: string; c: string; m: string }) => (
            <div key={r.k} className="grid grid-cols-5 border-t border-ink-700/60 px-3 py-2.5 font-mono">
              <span className="font-sans text-xs font-semibold text-zinc-400">{r.k}</span>
              <span className="text-right font-bold text-edge-400">
                {r.cz}
                {r.tag && <span className="ml-0.5 align-super font-sans text-[8px] font-semibold text-zinc-500">{r.tag}</span>}
              </span>
              <span className="text-right text-zinc-400">{r.o}</span>
              <span className="text-right">{r.c}</span>
              <span className="text-right text-xs text-zinc-500">{r.m}</span>
            </div>
          ))}
        </div>
        <p className="mt-1 text-[10px] text-zinc-600">Home-line notation (−5.5 = {g.home} favored). Price = Vegas consensus. Fair value = our model.</p>
        {totalModelDetail(g) && (
          <p className="mt-1 text-[11px] text-zinc-400">
            <TermLabel term="totalsModel">Totals models</TermLabel>
            {": "}{totalModelDetail(g)}
          </p>
        )}

        <div className="mt-3 grid grid-cols-2 gap-2">
          <Big label="Home win" v={hw == null ? "—" : fmtWinProb(hw)} />
          <Big label="Over" v={ov == null ? "—" : pct(ov)} sub={ov == null ? undefined : "czarp-v1-totals"} />
        </div>
        {(i?.proj_possessions ?? g.proj_possessions) != null && <p className="mt-2 text-xs text-zinc-400">Projected pace: <b className="font-mono text-white">{(i?.proj_possessions ?? g.proj_possessions)!.toFixed(1)}</b> possessions</p>}

        {(g.archetypes.away.length > 0 || g.archetypes.home.length > 0) && (
          <div className="mt-4 space-y-2">
            {[{ n: g.away, t: g.archetypes.away }, { n: g.home, t: g.archetypes.home }].filter((r) => r.t.length).map((r) => (
              <div key={r.n}>
                <div className="mb-1 text-[11px] font-semibold text-zinc-500">{r.n}</div>
                <div className="flex flex-wrap gap-1.5">{r.t.map((tag) => <ArchetypeChip key={`${tag.kind}-${tag.name}`} tag={tag} />)}</div>
              </div>
            ))}
            {g.archetypes.notes.length > 0 && <p className="text-[11px] text-zinc-400">{g.archetypes.notes.join(" · ")}</p>}
          </div>
        )}
        {(i?.reason || g.reason) && <div className="mt-4 rounded-xl bg-ink-850 p-3 text-sm text-zinc-300"><div className="mb-1 text-[10px] uppercase tracking-wider text-zinc-500">Why CZarp likes it</div>{i?.reason || g.reason}</div>}

        {bets.length > 0 && (
          <div className="mt-4 space-y-1.5">
            {bets.map((b) => {
              const tier = edgeTier(b.line_edge_pct, b.market);
              const compact = bets.length > 1 ? cardHeadline(g, [b]).subtitle : null;
              return (
                <div key={`${b.market}-${b.side}`} className="flex items-center gap-2 rounded-xl border border-edge-400/30 px-3 py-2 text-sm">
                  {tier && <span className={`rounded-md px-1.5 py-0.5 text-[10px] font-bold ${TIER_BADGE[tier]}`}>{tier}</span>}
                  <span className="min-w-0 flex-1 truncate">
                    <span className="block font-semibold">{b.side}</span>
                    {compact && <span className="block font-mono text-[11px] font-normal text-zinc-500">{compact}</span>}
                  </span>
                  <span className={`font-mono text-base font-black ${tier ? "text-edge-400" : "text-zinc-500"}`}>{pct(b.line_edge_pct, 1)}</span>
                  <span className="font-mono text-xs text-zinc-400">{b.kelly_quarter_units == null ? "—" : `${b.kelly_quarter_units.toFixed(2)}u`}</span>
                </div>
              );
            })}
          </div>
        )}
        <button onClick={onClose} className="mt-4 min-h-12 w-full rounded-xl border border-ink-600 font-semibold hover:bg-ink-800">Done</button>
      </div>
    </div>
  );
}

function NotAPlay({ line, className = "", showTip = true }: { line: string | null; className?: string; showTip?: boolean }) {
  if (!line) return null;
  return (
    <div className={`flex items-start gap-1 text-[11px] leading-snug text-zinc-400 ${className}`}>
      <span>{line}</span>
      {showTip && <InfoTip term="openLineRule" />}
    </div>
  );
}

function SheetSecondary({ g, bets }: { g: import("@/lib/types").SlateGame; bets: import("@/lib/types").ValueBet[] }) {
  const edge = cardHeadline(g, bets);
  const cover = betCoverProb(g, edge.officialBet, edge.market ?? edge.officialBet?.market ?? null);
  const odds = edge.officialBet?.posted_odds;
  const ev = evPer100(cover, odds);
  const usedDefault = odds == null || !Number.isFinite(odds) || odds === 0;
  return (
    <div className="mt-2 grid grid-cols-2 gap-2">
      <Big label="Cover probability" v={cover == null ? "—" : pct(cover)} sub={edge.officialBet ? edge.officialBet.side : edge.market ? MARKET_LABEL[edge.market] : undefined} />
      <Big label="EV / $100" v={ev == null ? "—" : fmtEv100(ev)} sub={usedDefault ? "at −110" : `at ${odds! > 0 ? "+" : ""}${odds}`} />
    </div>
  );
}

function Big({ label, v, sub, accent }: { label: ReactNode; v: string; sub?: string; accent?: boolean }) {
  return (
    <div className="rounded-xl bg-ink-850 p-3">
      <div className="text-[10px] uppercase tracking-wider text-zinc-500">{label}</div>
      <div className={`font-mono text-lg font-bold ${accent ? "text-edge-400" : ""}`}>{v}</div>
      {sub && <div className="text-[10px] text-zinc-600">{sub}</div>}
    </div>
  );
}
