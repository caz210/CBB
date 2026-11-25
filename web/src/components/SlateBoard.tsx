"use client";
import { useEffect, useState } from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import type { SlateGame, ValueBet } from "@/lib/types";
import { LOCKED_NOTE, MARKET_LABEL, finalLabel, fmtOne, fmtSpread, fmtPostedStamp, fmtScore, fmtWinProb, fmtTipoff, pct } from "@/lib/labels";
import { useWatchlist } from "@/lib/watchlist";
import { compactSlateTags, visibleCopy, visibleNotes, visibleTags, type ArchetypeTag } from "@/lib/archetypes";
import { teamHref } from "@/lib/teams";
import Link from "next/link";
import { ArchetypeChip, ArchetypeLegend } from "./ArchetypeChip";
import { InfoTip } from "./InfoTip";
import { StatusChips, VerdictChip } from "./StatusChips";
import { NeutralChip, TitleLine } from "./NeutralChip";
import { SlateSearch } from "./SlateSearch";
import { TvRatingChip } from "./TvRatingChip";
import { compareTvRating, tvHighlightIds, type SlateSort } from "@/lib/tvRating";
import { ALIAS_INDEX_TIMEOUT_MS, aliasByNameFromIndex, aliasByNameFromRecord, echoSlateQuery, filterSlateGames, isEmptySlateQuery, isSlateAliasSearchPending, slateGameMatches } from "@/lib/slateSearch";
import { useTeamIndex } from "@/lib/useTeamIndex";
import { SHOW_TRACKED_TIER } from "@/lib/pickTier";
import { gameTitle, sideLabel, spreadNotationNote } from "@/lib/gameTitle";
import { marketRatings, notAPlayAlso, type MarketRating } from "@/lib/verdict";
import { fmtLinesAsOf } from "@/lib/linesAsOf";
import { cardHeaderRow, cardNoteLine } from "@/lib/cardHeader";
import { HeaderRow } from "./HeaderRow";
import { totalFair } from "@/lib/posted";
import { officialPickViews, stakeUnitsLabel } from "@/lib/officialPick";
import { GameSheetTabs, useSheetExtras } from "./GameSheetTabs";
import { OfficialPickSection } from "./OfficialPickSection";

export function SlateBoard({ games, bets, tracked = [], sort = "time", hide = false, sportKey = "cbb", slateAliases }: {
  games: SlateGame[]; bets: ValueBet[]; tracked?: ValueBet[]; sort?: SlateSort; hide?: boolean; sportKey?: string;
  /** team_aliases for this slate's teams, sent with the page; null/undefined if that read failed */
  slateAliases?: Record<string, string[]> | null;
}) {
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
  // ?q= is the search box. replaceState keeps Back/Forward, reloads and shared links in sync
  // without a history entry per keystroke (same pattern as the matchup page).
  const query = search.get("q") ?? "";
  const hasQuery = !isEmptySlateQuery(query);
  // Reuse the header's cached /api/teams index (1,674 team_aliases). If it hasn't loaded
  // or the request fails, matching falls back to localAliases only — same as before.
  const teamLoad = useTeamIndex(sportKey, true);
  // The page already carries this slate's aliases, so ualbany works on first paint; the index is the fallback.
  const aliases = aliasByNameFromRecord(slateAliases) ?? (teamLoad.state === "ready" ? aliasByNameFromIndex(teamLoad.index) : undefined);
  // Don't flash "No games match" for an alias-only query (ualbany) while /api/teams is in flight.
  const [aliasTimedOut, setAliasTimedOut] = useState(false);
  useEffect(() => {
    if (teamLoad.state !== "loading") { setAliasTimedOut(false); return; }
    const t = window.setTimeout(() => setAliasTimedOut(true), ALIAS_INDEX_TIMEOUT_MS);
    return () => window.clearTimeout(t);
  }, [teamLoad.state]);
  const setQuery = (next: string) => {
    const q = new URLSearchParams(search.toString());
    if (next) q.set("q", next); else q.delete("q");
    const qs = q.toString();
    window.history.replaceState(null, "", `${pathname}${qs ? `?${qs}` : ""}`);
  };
  // ?game=<id> opens that game's sheet (used by the header team search's "Next game" link).
  const gameParam = search.get("game");
  useEffect(() => {
    if (!gameParam) return;
    const g = games.find((x) => String(x.game_id) === gameParam);
    if (g) setGameSel(g);
  }, [gameParam, games]);
  const closeGame = () => { setGameSel(null); if (gameParam) setParam("game", null); };
  const openGame = (g: SlateGame) => {
    setGameSel(g);
    if (gameParam !== String(g.game_id)) setParam("game", String(g.game_id));
  };
  const byGame = (id: number) => bets.filter((b) => b.game_id === id);
  const raw = (g: SlateGame) => g.rawEdge ?? -Infinity;
  const tipMs = (g: SlateGame) => (g.tipoff ? Date.parse(g.tipoff) : Infinity);
  const byTime = (a: SlateGame, z: SlateGame) => tipMs(a) - tipMs(z) || a.game_id - z.game_id;
  const completedCount = games.filter((g) => g.completed).length;
  const matched = filterSlateGames(games, query, aliases);
  const shown = matched
    .filter((g) => !(hide && g.completed))
    .filter((g) => !(watchOnly && ready && !has(g.game_id)))
    .slice()
    .sort(sort === "value"
      ? (a, z) => {
          const ea = raw(a), ez = raw(z);
          if (ea === ez) return byTime(a, z);
          return ez - ea; // no raw edge sorts last
        }
      : sort === "tv" ? compareTvRating : byTime);
  // Highlight is computed on the day's full slate, not the hidden/filtered subset.
  // Hide-completed can leave fewer than 5 flames visible; that is intended.
  const tvHot = tvHighlightIds(games);
  const officialShown = bets.filter((b) => {
    const g = games.find((x) => x.game_id === b.game_id);
    if (!g) return false;
    if (hide && g.completed) return false;
    if (hasQuery && !slateGameMatches(g, query, aliases)) return false;
    return true;
  });
  // Hide completed must not hide tracked rows (same as main). A team query still filters them.
  const trackedShown = tracked.filter((b) => {
    const g = games.find((x) => x.game_id === b.game_id);
    if (hasQuery && (!g || !slateGameMatches(g, query, aliases))) return false;
    return true;
  });

  const searchingAliases = isSlateAliasSearchPending({
    query, matchCount: matched.length, load: teamLoad.state, timedOut: aliasTimedOut, slateAliasesKnown: slateAliases != null,
  });

  return (
    <div>
      <SlateSearch query={query} onQuery={setQuery} />
      <div className="mb-4 flex flex-wrap items-center gap-2">
        <div className="flex w-full min-w-0 items-center gap-1 sm:w-auto">
          <div role="radiogroup" aria-label="Sort games" className="flex min-w-0 flex-1 rounded-xl border border-ink-600 bg-ink-900 p-1 sm:flex-none">
            {(["time", "value", "tv"] as const).map((k) => (
              <button key={k} role="radio" aria-checked={sort === k}
                onClick={() => setParam("sort", k === "time" ? null : k)}
                className={`min-h-11 flex-1 whitespace-nowrap rounded-lg px-2 text-sm font-semibold transition sm:flex-none sm:px-4 ${sort === k ? "bg-court-500/20 text-court-400" : "text-zinc-400 hover:bg-ink-800"}`}>
                {k === "time" ? "Game time" : k === "value" ? "Edge" : "TV Rating"}
              </button>
            ))}
          </div>
          <InfoTip term="tvRating" />
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

      {shown.length === 0 && hasQuery && searchingAliases && (
        <div className="mb-4 rounded-xl border border-ink-700 bg-ink-900 px-3.5 py-3 text-sm text-zinc-400" aria-live="polite">
          Searching…
        </div>
      )}

      {shown.length === 0 && hasQuery && !searchingAliases && (
        <div className="mb-4 min-w-0 overflow-hidden break-words rounded-xl border border-ink-700 bg-ink-900 px-3.5 py-3 text-sm text-zinc-400 [overflow-wrap:anywhere]">
          {matched.length === 0
            ? <>No games match “{echoSlateQuery(query)}”.</>
            : watchOnly
              ? <>No watched games match “{echoSlateQuery(query)}”.</>
              : <>No open games match “{echoSlateQuery(query)}”.</>}
          {" "}
          <button type="button" onClick={() => setQuery("")} className="font-semibold text-court-400">Clear search</button>
          {matched.length > 0 && hide && (
            <>{" "}<button type="button" onClick={() => setParam("hide", null)} className="font-semibold text-court-400">Show completed</button></>
          )}
        </div>
      )}

      {watchOnly && ready && shown.length === 0 && !hasQuery && (
        <div className="mb-4 rounded-xl border border-ink-700 bg-ink-900 px-3.5 py-3 text-sm text-zinc-400">
          {watchCount === 0
            ? <>No starred games yet. Tap the star on a game card to watch it.</>
            : <>None of your watched games are on this slate{hide ? " (or they are hidden as completed)" : ""}.</>}
          {watchCount > 0 && hide && (
            <>{" "}<button onClick={() => setParam("hide", null)} className="font-semibold text-court-400">Show completed</button></>
          )}
        </div>
      )}

      {shown.length === 0 && !watchOnly && !hasQuery && (
        <div className="rounded-xl border border-ink-700 bg-ink-900 px-3.5 py-3 text-sm text-zinc-400">
          All {games.length} games on this slate are completed.{" "}
          <button onClick={() => setParam("hide", null)} className="font-semibold text-court-400">Show them</button>
        </div>
      )}

      {officialShown.length > 0 && (
        <section className="mb-4 rounded-2xl border border-edge-400/30 bg-edge-400/[0.04] p-3 md:p-4" aria-label="CZarp Plays">
          <h2 className="mb-2 text-sm font-semibold text-edge-400">CZarp Plays · {officialShown.length}</h2>
          <ul className="divide-y divide-ink-700/50">
            {officialShown.map((b) => {
              const g = games.find((x) => x.game_id === b.game_id);
              return (
                <li key={`${b.game_id}-${b.market}-${b.side}`}>
                  {/* div, not <button>: NeutralChip's tooltip is itself a button, and <button> in <button> hydrates badly. */}
                  <div
                    role="button"
                    tabIndex={0}
                    onClick={() => g && openGame(g)}
                    onKeyDown={(e) => { if ((e.key === "Enter" || e.key === " ") && g) { e.preventDefault(); openGame(g); } }}
                    className="flex min-h-12 w-full cursor-pointer items-center gap-3 py-2 text-left text-sm"
                  >
                    <span className="min-w-0 flex-1">
                      <span className="block truncate font-semibold text-white">{b.side} · {MARKET_LABEL[b.market]}</span>
                      <TitleLine text={g ? gameTitle(g.away, g.home, g.neutral).text : `Game #${b.game_id}`} neutral={g?.neutral === true} interactive={false} className="text-[11px] text-zinc-500" />
                    </span>
                    <span className="font-mono text-lg font-black text-edge-400">{pct(b.line_edge_pct, 1)}</span>
                    <span className="font-mono text-xs text-zinc-500">{stakeUnitsLabel(b.stake_units) ?? "—"}</span>
                  </div>
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
            tvHot={tvHot.has(g.game_id)}
            onWatch={() => toggle(g.game_id)}
            onGame={() => openGame(g)}
          />
        ))}
      </div>

      {SHOW_TRACKED_TIER && trackedShown.length > 0 && (
        <section className="mt-6 rounded-2xl border border-dashed border-ink-700 p-3 md:p-4" aria-label="Tracked picks">
          <div className="mb-2 flex flex-wrap items-baseline gap-x-2">
            <h2 className="text-sm font-semibold text-zinc-400">Tracked · {trackedShown.length}</h2>
            <span className="text-[11px] text-zinc-500">Flat 1u, tracked for testing, not part of the official record.</span>
          </div>
          <ul className="divide-y divide-ink-700/50">
            {trackedShown.map((b) => {
              const g = games.find((x) => x.game_id === b.game_id);
              const proof = fmtPostedStamp(b.logged_at);
              return (
                <li key={`${b.game_id}-${b.market}-${b.side}`} className="flex items-center gap-3 py-2 text-sm text-zinc-400">
                  <span className="min-w-0 flex-1">
                    <span className="block truncate font-medium text-zinc-300">{b.side}</span>
                    <TitleLine text={g ? gameTitle(g.away, g.home, g.neutral).text : `Game #${b.game_id}`} neutral={g?.neutral === true} className="text-[11px] text-zinc-500" suffix={` · ${MARKET_LABEL[b.market]} · edge ${pct(b.line_edge_pct, 1)}`} />
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
        {spreadNotationNote(games.some((x) => x.neutral))} Price = market consensus. Fair value = our model. Stakes are ¼-Kelly in units (1u = 1% of bankroll, capped at 2.5u). {LOCKED_NOTE}
      </p>

      <GameSheet g={gameSel} sportKey={sportKey} bets={gameSel ? byGame(gameSel.game_id) : []} tvHot={gameSel ? tvHot.has(gameSel.game_id) : false} onClose={closeGame} />
    </div>
  );
}


function TeamStyleTags({ tags, top, onMore }: {
  tags: ArchetypeTag[];
  top?: ArchetypeTag[];
  onMore: (e: { stopPropagation: () => void }) => void;
}) {
  const vis = visibleTags(tags);
  if (!vis.length) return null;
  const compact = compactSlateTags(vis, top ?? [], 2);
  return (
    <>
      <div className="mt-2 hidden min-w-0 flex-wrap gap-1.5 pl-12 min-[360px]:flex">
        {vis.map((tag) => <ArchetypeChip key={`${tag.kind}-${tag.name}`} tag={tag} />)}
      </div>
      <div className="mt-2 flex min-w-0 flex-wrap gap-1.5 pl-12 min-[360px]:hidden">
        {compact.shown.map((tag) => <ArchetypeChip key={`${tag.kind}-${tag.name}`} tag={tag} />)}
        {compact.more > 0 && (
          <button
            type="button"
            onClick={onMore}
            aria-label={`${compact.more} more style tags`}
            className="inline-flex items-center whitespace-nowrap rounded-md border border-ink-600 px-1.5 py-0.5 text-[11px] font-semibold text-zinc-300 hover:bg-ink-800"
          >
            +{compact.more}
          </button>
        )}
      </div>
    </>
  );
}

function Star({ filled, className = "h-4 w-4" }: { filled: boolean; className?: string }) {
  return (
    <svg viewBox="0 0 24 24" className={className} fill={filled ? "currentColor" : "none"} stroke="currentColor" strokeWidth="2" strokeLinejoin="round">
      <path d="M12 3.5l2.6 5.3 5.9.9-4.3 4.2 1 5.8L12 17.8 6.8 19.7l1-5.8L3.5 9.7l5.9-.9L12 3.5z" />
    </svg>
  );
}

function GameCard({ g, sportKey, bets, watched, tvHot, onWatch, onGame }: {
  g: SlateGame; sportKey: string; bets: ValueBet[]; watched: boolean; tvHot: boolean; onWatch: () => void; onGame: () => void;
}) {
  const awayWin = g.home_win_prob == null ? null : 1 - g.home_win_prob;
  const awayCover = g.home_cover_prob == null ? null : 1 - g.home_cover_prob;
  const final = finalScore(g);
  const rows = [
    { side: sideLabel("Away", g.neutral), name: g.away, proj: g.proj_away, win: awayWin, cover: awayCover, tags: g.archetypes.away, top: g.archetypes.awayTop, tent: g.archetypes.away_tentative, score: final?.away ?? null, won: final ? final.away > final.home : false },
    { side: sideLabel("Home", g.neutral), name: g.home, proj: g.proj_home, win: g.home_win_prob, cover: g.home_cover_prob, tags: g.archetypes.home, top: g.archetypes.homeTop, tent: g.archetypes.home_tentative, score: final?.home ?? null, won: final ? final.home > final.away : false },
  ];
  const ratings = marketRatings(g, bets);
  const asOf = fmtLinesAsOf(g.lines_as_of);
  const note = cardNoteLine(g, bets);
  const row = cardHeaderRow(g, bets);
  const timeLabel = g.completed
    ? finalLabel(g.final_at, g.final_at_estimated, g.game_date)
    : (g.tipoff ? fmtTipoff(g.tipoff) : "Tip-off TBD");
  const fair = totalFair(g, bets);
  const hasTeamProj = g.proj_home != null || g.proj_away != null;
  return (
    <article
      role="button" tabIndex={0} aria-label={`Insights for ${gameTitle(g.away, g.home, g.neutral).text}${g.neutral ? ", neutral site" : ""}`}
      onClick={onGame} onKeyDown={(e) => (e.key === "Enter" || e.key === " ") && (e.preventDefault(), onGame())}
      className={`card cursor-pointer overflow-hidden transition hover:border-ink-500 active:scale-[0.995] ${g.completed ? "opacity-70" : ""}`}>
      <header className="flex items-start gap-2 border-b border-ink-700/60 px-4 py-2.5 text-xs">
        <div className="flex min-w-0 flex-1 flex-wrap items-center gap-1.5">
          <span className="font-semibold text-zinc-300">{timeLabel}</span>
          {g.neutral && <NeutralChip interactive={false} />}
          <TvRatingChip rating={g.tv_rating} highlight={tvHot} />
          <StatusChips g={g} bets={bets} />
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

      {/* Header row: one market, the side the edge favors, the edge now. Tapping anywhere on the card opens the game sheet. */}
      <div className="border-b border-ink-700/40 px-4 py-3">
        <HeaderRow row={row} />
      </div>

      <div className="divide-y divide-ink-700/40 px-4">
        {hasTeamProj && (
          <div className="flex justify-end pt-2 text-[9px] font-semibold uppercase tracking-wider text-zinc-600">
            CZarp projection
          </div>
        )}
        {rows.map((r) => (
          <div key={r.side} className="py-3">
            {/* Scores can carry two decimals (74.05), so below 360px the row tightens to leave the team name room. */}
            <div className="flex items-center gap-2 min-[360px]:gap-3">
              <span className="w-9 shrink-0 text-[10px] font-semibold uppercase text-zinc-600">{r.side}</span>
              <Link href={teamHref(sportKey, r.name)} onClick={(e) => e.stopPropagation()} className="min-w-0 flex-1 truncate text-[15px] font-semibold hover:text-court-400">{r.name}</Link>
              {final ? (
                <>
                  <span className="text-right font-mono text-xs text-zinc-500">{r.proj == null ? "—" : fmtScore(r.proj)}<span className="ml-0.5 font-sans text-[10px] text-zinc-600">proj</span></span>
                  <span className={`w-12 text-right font-mono text-xl font-bold ${r.won ? "text-white" : "text-zinc-400"}`} aria-label={`${r.name} final score ${r.score}`}>{r.score}</span>
                </>
              ) : (
                <>
                  <span className={`text-right font-mono text-xs ${r.win != null && r.win >= 0.5 ? "text-white" : "text-zinc-500"}`}>{r.win == null ? "—" : fmtWinProb(r.win)}<span className="ml-0.5 font-sans text-[10px] text-zinc-600">win</span></span>
                  <span className="min-w-12 text-right font-mono text-lg font-bold tabular-nums min-[360px]:text-xl">{r.proj == null ? "—" : fmtScore(r.proj)}</span>
                </>
              )}
            </div>
            <TeamStyleTags
              tags={r.tags}
              top={r.top}
              onMore={(e) => { e.stopPropagation(); onGame(); }}
            />
          </div>
        ))}
      </div>

      {note && (
        <div className="border-t border-ink-700/40 px-4 py-2" onClick={(e) => e.stopPropagation()}>
          <NotAPlay line={note} />
        </div>
      )}

      <div className="grid grid-cols-2 gap-px border-t border-ink-700/60 bg-ink-700/40 text-center">
        <MarketStat
          label="Spread"
          rating={ratings.spread}
          refTeam={g.home}
          fair={g.proj_spread == null ? "—" : fmtSpread(g.proj_spread)}
          price={g.market_spread == null ? "—" : fmtSpread(g.market_spread)}
        />
        <MarketStat
          label="Total"
          rating={ratings.total}
          fair={fair.value}
          price={g.market_total == null ? "—" : fmtOne(g.market_total)}
        />
      </div>
      {asOf && (
        <div className="border-t border-ink-700/40 px-4 py-1 text-center text-[10px] text-zinc-600">Ratings use the line {asOf}</div>
      )}

      {(visibleNotes(g.archetypes.notes).length > 0 || visibleCopy(g.reason)) && (
        <div className="space-y-1 border-t border-ink-700/60 px-4 py-2 text-[11px] text-zinc-400">
          {visibleNotes(g.archetypes.notes).length > 0 && (
            <div>
              <span className="font-semibold text-zinc-500">Matchup: </span>{visibleNotes(g.archetypes.notes).join(" · ")}
              {g.archetypes.tentative && <span className="text-zinc-600"> (tentative)</span>}
            </div>
          )}
          {visibleCopy(g.reason) ? (
            <div><span className="font-semibold text-zinc-500">Why: </span>{visibleCopy(g.reason)}</div>
          ) : null}
        </div>
      )}

      {bets.length > 1 && (
        <div className="space-y-2 border-t border-ink-700/60 bg-edge-400/[0.03] p-3">
          {bets.map((b) => {
            return (
              <button
                key={`${b.market}-${b.side}`}
                onClick={(e) => { e.stopPropagation(); onGame(); }}
                className="flex min-h-12 w-full items-center gap-3 rounded-xl border border-edge-400/30 bg-ink-900 px-3 py-2 text-left transition hover:border-edge-400/60 active:scale-[0.99]"
              >
                <span className="min-w-0 flex-1 truncate text-sm font-semibold">{b.side}</span>
                <span className="font-mono text-lg font-black leading-none text-edge-400">{pct(b.line_edge_pct, 1)}</span>
                <span className="font-mono text-[11px] text-zinc-500">{stakeUnitsLabel(b.stake_units) ?? "—"}</span>
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

/** Per-market ladder chip (no market name: the cell or row already says it). Stops taps reaching the card. */
function RatingChip({ r, wrap = false, className = "" }: { r: MarketRating; wrap?: boolean; className?: string }) {
  return <VerdictChip level={r.level} noLine={r.noLine} wrap={wrap} className={className} />;
}

/** Footer cell. FV and Price share one font, size, weight and color; the small labels carry the meaning and
 *  the ladder chip is the only thing that stands out. Both read exactly one decimal (fmtLine / fmtOne).
 *  Spread numbers always name the team the home-line number refers to ("FV Texas St. -6.1",
 *  "Price Texas St. -2.5"), home games included; never a bare number. The name may wrap; the number never
 *  splits from its sign. */
function MarketStat({ label, fair, price, refTeam, rating }: { label: string; fair: string; price: string; refTeam?: string; rating?: MarketRating }) {
  const row = (tag: string, value: string) => (
    <div className="font-mono text-xs leading-snug text-zinc-300">
      <span className="font-sans text-[10px] text-zinc-500">{tag} </span>
      {refTeam && value !== "—"
        ? <><span className="break-words font-sans">{refTeam} </span><span className="whitespace-nowrap">{value}</span></>
        : value}
    </div>
  );
  return (
    <div className="min-w-0 bg-ink-900 px-2 py-2.5">
      <div className="mb-1 flex flex-wrap items-center justify-center gap-x-1.5 gap-y-1">
        <span className="text-[10px] uppercase tracking-wider text-zinc-500">{label}</span>
        {rating && <RatingChip r={rating} />}
      </div>
      {row("FV", fair)}
      {row("Price", price)}
    </div>
  );
}

function GameSheet({ g, sportKey, bets, tvHot, onClose }: { g: SlateGame | null; sportKey: string; bets: ValueBet[]; tvHot: boolean; onClose: () => void }) {
  useEffect(() => {
    const k = (e: KeyboardEvent) => e.key === "Escape" && onClose();
    window.addEventListener("keydown", k);
    return () => window.removeEventListener("keydown", k);
  }, [onClose]);
  const extras = useSheetExtras(g, sportKey);
  if (!g) return null;
  const i = g.insight;
  const sheetNote = cardNoteLine(g, bets);
  const officialViews = officialPickViews(g, bets);
  return (
    <div className="fixed inset-0 z-50 flex items-end justify-center md:items-center" role="dialog" aria-modal="true" aria-label={`${gameTitle(g.away, g.home, g.neutral).text} insights`}>
      <button aria-label="Close" onClick={onClose} className="absolute inset-0 bg-black/70" />
      <div className="relative max-h-[88dvh] w-full max-w-lg overflow-y-auto rounded-t-3xl border border-ink-700 bg-ink-900 p-5 pb-[calc(1.25rem+env(safe-area-inset-bottom))] shadow-2xl md:rounded-3xl">
        <div className="mx-auto mb-4 h-1 w-10 rounded-full bg-ink-600 md:hidden" />
        <div className="text-xs text-zinc-500">{g.tipoff ? fmtTipoff(g.tipoff) : "Tip-off TBD"}{g.completed ? ` · ${finalLabel(g.final_at, g.final_at_estimated, g.game_date)}` : ""}</div>
        <h3 className="mt-1 text-xl font-bold">
          <Link href={teamHref(sportKey, g.away)} onClick={(e) => e.stopPropagation()} className="hover:text-court-400">{g.away}</Link>
          <span className="text-zinc-500"> {gameTitle(g.away, g.home, g.neutral).sep} </span>
          <Link href={teamHref(sportKey, g.home)} onClick={(e) => e.stopPropagation()} className="hover:text-court-400">{g.home}</Link>
          {g.neutral && <NeutralChip className="ml-2" />}
          <TvRatingChip rating={g.tv_rating} highlight={tvHot} className={g.neutral ? "" : "ml-2"} />
        </h3>
        {finalScore(g) && (
          <div className="mt-1 font-mono text-sm font-semibold text-zinc-200">Final: {g.away} {finalScore(g)!.away}, {g.home} {finalScore(g)!.home}</div>
        )}
        {(i?.upset_alert || g.upset_alert) && (
          <div className="mt-2 rounded-xl border border-amber-400/40 bg-amber-400/10 px-3 py-2 text-sm text-amber-100">
            <div className="text-[10px] font-bold uppercase tracking-wider text-amber-300">
              <InfoTip term="upset-alert" triggerClassName="font-bold uppercase tracking-wider text-amber-300">Upset alert</InfoTip>
            </div>
            <div className="mt-0.5 text-[13px] text-amber-50/90">{visibleCopy(i?.upset_reason || g.upset_reason) || "Underdog style matchup flagged by the model."}</div>
          </div>
        )}
        {i?.neutral && <div className="mt-2 inline-block rounded-md border border-ink-600 px-2 py-0.5 text-[11px] text-zinc-400">Neutral site: no home-court edge applied</div>}

        {/* Official picks get a dedicated banner; otherwise the same one-row headline as the card. */}
        <div className="mt-4">
          <div className="mb-2 flex flex-wrap items-center gap-1.5">
            <StatusChips g={g} bets={bets} />
          </div>
          {officialViews.length > 0 ? (
            <OfficialPickSection views={officialViews} />
          ) : (
            <div className="rounded-2xl border border-ink-700 bg-ink-850 px-4 py-3">
              <HeaderRow row={cardHeaderRow(g, bets)} />
            </div>
          )}
        </div>
        {sheetNote && <NotAPlay className="mt-2" line={sheetNote} />}
        {sheetNote && notAPlayAlso(g, bets).length > 0 && (
          <ul className="mt-1 space-y-0.5 text-[11px] leading-snug text-zinc-500">
            {notAPlayAlso(g, bets).map((l) => <li key={l}>{l}</li>)}
          </ul>
        )}

        <div className="mt-4">
          <GameSheetTabs g={g} bets={bets} sportKey={sportKey} extras={extras.extras} loading={extras.loading} />
          <button onClick={onClose} className="mt-4 min-h-12 w-full rounded-xl border border-ink-600 font-semibold hover:bg-ink-800">Done</button>
        </div>
      </div>
    </div>
  );
}

/** Headline Edge % color: CZarp Plays in brand green, Late Value and Value a step down, the rest muted. */

/** Keep tokens like "(-115)" and "0.1u+" from wrapping mid-token; wrap between chunks. */
function noteChunks(line: string) {
  const juice = line.match(/^(Not a play: price too juiced \([^)]+\)) (for a stake of [\d.]+u\+)$/);
  const parts = juice ? [juice[1], juice[2]] : line.split(" · ");
  if (parts.length === 1) return line;
  return parts.map((part, i) => (
    <span key={i}>
      {i > 0 ? (juice ? " " : " · ") : null}
      <span className="whitespace-nowrap">{part}</span>
    </span>
  ));
}

function NotAPlay({ line, className = "", showTip = true }: { line: string | null; className?: string; showTip?: boolean }) {
  if (!line) return null;
  return (
    <div className={`flex items-start gap-1 text-[11px] leading-snug text-zinc-400 ${className}`}>
      <span>{noteChunks(line)}</span>
      {showTip && <InfoTip term="ladderRule" />}
    </div>
  );
}
