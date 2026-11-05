"use client";
import { useEffect, useState } from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import type { SlateGame, ValueBet } from "@/lib/types";
import { LOCKED_NOTE, MARKET_LABEL, SOURCE_LABEL, TIER_BADGE, edgeTier, fmtLine, fmtPostedProof, fmtWinProb, fmtTipoff, pct, signedPct } from "@/lib/labels";
import { ArchetypeChip, ArchetypeLegend } from "./ArchetypeChip";

type Sel = { game: SlateGame; bet: ValueBet } | null;

export function SlateBoard({ games, bets, tracked = [], sort = "time", hide = false }: {
  games: SlateGame[]; bets: ValueBet[]; tracked?: ValueBet[]; sort?: "time" | "value"; hide?: boolean;
}) {
  const [sel, setSel] = useState<Sel>(null);
  const [gameSel, setGameSel] = useState<SlateGame | null>(null);
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
  const bestEdge = (id: number) => Math.max(-Infinity, ...byGame(id).map((b) => b.line_edge_pct));
  const tipMs = (g: SlateGame) => (g.tipoff ? Date.parse(g.tipoff) : Infinity);
  const byTime = (a: SlateGame, z: SlateGame) => tipMs(a) - tipMs(z) || a.game_id - z.game_id;
  const completedCount = games.filter((g) => g.completed).length;
  const shown = games
    .filter((g) => !(hide && g.completed))
    .slice()
    .sort(sort === "value"
      ? (a, z) => {
          const ea = bestEdge(a.game_id), ez = bestEdge(z.game_id);
          if (ea === ez) return byTime(a, z);
          return ez - ea; // -Infinity (no play) sorts last
        }
      : byTime);

  return (
    <div>
      <div className="mb-4 flex flex-wrap items-center gap-2">
        <div role="radiogroup" aria-label="Sort games" className="flex flex-1 rounded-xl border border-ink-600 bg-ink-900 p-1 sm:flex-none">
          {(["time", "value"] as const).map((k) => (
            <button key={k} role="radio" aria-checked={sort === k}
              onClick={() => setParam("sort", k === "time" ? null : "value")}
              className={`min-h-11 flex-1 rounded-lg px-4 text-sm font-semibold transition sm:flex-none ${sort === k ? "bg-court-500/20 text-court-400" : "text-zinc-400 hover:bg-ink-800"}`}>
              {k === "time" ? "Game time" : "Value"}
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
        <div className="ml-auto hidden md:block"><ArchetypeLegend /></div>
      </div>

      {shown.length === 0 && (
        <div className="rounded-xl border border-ink-700 bg-ink-900 px-3.5 py-3 text-sm text-zinc-400">
          All {games.length} games on this slate are completed.{" "}
          <button onClick={() => setParam("hide", null)} className="font-semibold text-court-400">Show them</button>
        </div>
      )}

      <div className="grid gap-3 md:gap-4 lg:grid-cols-2">
        {shown.map((g) => (
          <GameCard key={g.game_id} g={g} bets={byGame(g.game_id)} onOpen={(bet) => setSel({ game: g, bet })} onGame={() => setGameSel(g)} />
        ))}
      </div>

      {tracked.length > 0 && (
        <section className="mt-6 rounded-2xl border border-dashed border-ink-700 p-3 md:p-4" aria-label="Tracked picks">
          <div className="mb-2 flex flex-wrap items-baseline gap-x-2">
            <h2 className="text-sm font-semibold text-zinc-400">Tracked · {tracked.length}</h2>
            <span className="text-[11px] text-zinc-500">Paper-only 1u, never bet. Not part of the official record.</span>
          </div>
          <ul className="divide-y divide-ink-700/50">
            {tracked.map((b) => {
              const g = games.find((x) => x.game_id === b.game_id);
              const proof = fmtPostedProof(b.logged_at, g?.tipoff);
              return (
                <li key={`${b.game_id}-${b.market}-${b.side}`} className="flex items-center gap-3 py-2 text-sm text-zinc-400">
                  <span className="min-w-0 flex-1">
                    <span className="block truncate font-medium text-zinc-300">{b.side}</span>
                    <span className="block truncate text-[11px] text-zinc-500">{g?.game ?? `Game #${b.game_id}`} · {MARKET_LABEL[b.market]} · line edge {pct(b.line_edge_pct, 1)}</span>
                    {proof && <span className="block text-[11px] text-zinc-600">🔒 {proof}</span>}
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
        Spreads use home-line notation (−5.5 = home favored). Market = Vegas consensus. Stakes are ¼-Kelly in units (1u = 1% of bankroll, capped at 2.5u). {LOCKED_NOTE}
      </p>

      <ValueSheet sel={sel} onClose={() => setSel(null)} />
      <GameSheet g={gameSel} bets={gameSel ? byGame(gameSel.game_id) : []} onClose={() => setGameSel(null)} />
    </div>
  );
}

function GameCard({ g, bets, onOpen, onGame }: { g: SlateGame; bets: ValueBet[]; onOpen: (b: ValueBet) => void; onGame: () => void }) {
  const awayWin = g.home_win_prob == null ? null : 1 - g.home_win_prob;
  const awayCover = g.home_cover_prob == null ? null : 1 - g.home_cover_prob;
  const rows = [
    { side: "Away", name: g.away, proj: g.proj_away, win: awayWin, cover: awayCover, tags: g.archetypes.away, tent: g.archetypes.away_tentative },
    { side: "Home", name: g.home, proj: g.proj_home, win: g.home_win_prob, cover: g.home_cover_prob, tags: g.archetypes.home, tent: g.archetypes.home_tentative },
  ];
  return (
    <article
      role="button" tabIndex={0} aria-label={`Insights for ${g.game}`}
      onClick={onGame} onKeyDown={(e) => (e.key === "Enter" || e.key === " ") && (e.preventDefault(), onGame())}
      className={`card cursor-pointer overflow-hidden transition hover:border-ink-500 active:scale-[0.995] ${g.completed ? "opacity-70" : ""}`}>
      <header className="flex items-center justify-between border-b border-ink-700/60 px-4 py-2.5 text-xs">
        <span className="font-semibold text-zinc-300">{g.tipoff ? fmtTipoff(g.tipoff) : "Tip-off TBD"}</span>
        <span className="font-mono text-zinc-600"><span className="mr-2 text-court-400">Insights ›</span>{g.completed && <span className="mr-2 font-sans font-semibold uppercase text-zinc-500">Final</span>}#{g.game_id}</span>
      </header>

      <div className="divide-y divide-ink-700/40 px-4">
        {rows.map((r) => (
          <div key={r.side} className="py-3">
            <div className="flex items-center gap-3">
              <span className="w-9 shrink-0 text-[10px] font-semibold uppercase text-zinc-600">{r.side}</span>
              <span className="min-w-0 flex-1 truncate text-[15px] font-semibold">{r.name}</span>
              <span className={`text-right font-mono text-xs ${r.win != null && r.win >= 0.5 ? "text-white" : "text-zinc-500"}`}>{r.win == null ? "—" : fmtWinProb(r.win)}<span className="ml-0.5 font-sans text-[10px] text-zinc-600">win</span></span>
              <span className="w-12 text-right font-mono text-xl font-bold">{r.proj == null ? "—" : r.proj.toFixed(1)}</span>
            </div>
            {r.tags.length > 0 && (
              <div className="mt-2 flex flex-wrap gap-1.5 pl-12">
                {r.tags.map((t) => <ArchetypeChip key={t} name={t} tentative={r.tent} />)}
              </div>
            )}
          </div>
        ))}
      </div>

      <div className="grid grid-cols-3 gap-px border-t border-ink-700/60 bg-ink-700/40 text-center">
        <Stat label="Spread" model={g.proj_spread == null ? "—" : fmtLine(g.proj_spread)} mkt={g.market_spread == null ? "—" : fmtLine(g.market_spread)} />
        <Stat label="Total" model={g.proj_total == null ? "—" : g.proj_total.toFixed(1)} mkt={g.market_total?.toFixed(1) ?? "—"} />
        <Stat label="Home cover" model={g.home_cover_prob == null ? "—" : pct(g.home_cover_prob)} mkt={g.market_source ? SOURCE_LABEL[g.market_source] : ""} />
      </div>

      {g.archetypes.notes.length > 0 && (
        <div className="border-t border-ink-700/60 px-4 py-2 text-[11px] text-zinc-400">
          <span className="font-semibold text-zinc-500">Matchup: </span>{g.archetypes.notes.join(" · ")}
          {g.archetypes.tentative && <span className="text-zinc-600"> (tentative)</span>}
        </div>
      )}

      {bets.length > 0 && (
        <div className="space-y-2 border-t border-ink-700/60 bg-edge-400/[0.03] p-3">
          {bets.map((b) => {
            const tier = edgeTier(b.line_edge_pct, b.market);
            return (
              <button
                key={`${b.market}-${b.side}`}
                onClick={(e) => { e.stopPropagation(); onOpen(b); }}
                className="flex min-h-14 w-full items-center gap-3 rounded-xl border border-edge-400/30 bg-ink-900 px-3 py-2.5 text-left transition active:scale-[0.99] hover:border-edge-400/60"
              >
                {tier && <span className={`rounded-md px-1.5 py-1 text-[10px] font-bold ${TIER_BADGE[tier]}`}>{tier}</span>}
                <span className="min-w-0 flex-1">
                  <span className="block truncate text-sm font-semibold">{b.side}</span>
                  <span className="block text-[11px] text-zinc-500">{MARKET_LABEL[b.market]} · line edge {pct(b.line_edge_pct, 1)} <span className="text-zinc-600">· prob {signedPct(b.prob_edge)}</span></span>
                  {b.logged_at && <span className="block text-[10px] text-zinc-600">🔒 {fmtPostedProof(b.logged_at, g.tipoff)}</span>}
                </span>
                <span className="text-right">
                  <span className="block font-mono text-base font-bold">{b.kelly_quarter_units == null ? "—" : `${b.kelly_quarter_units.toFixed(1)}u`}</span>
                  <span className="block text-[10px] text-zinc-500">¼-Kelly</span>
                </span>
                <span className="text-zinc-600">›</span>
              </button>
            );
          })}
        </div>
      )}
    </article>
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
  return (
    <div className="fixed inset-0 z-50 flex items-end justify-center md:items-center" role="dialog" aria-modal="true">
      <button aria-label="Close" onClick={onClose} className="absolute inset-0 bg-black/70" />
      <div className="relative w-full max-w-lg rounded-t-3xl border border-ink-700 bg-ink-900 p-5 pb-[calc(1.25rem+env(safe-area-inset-bottom))] shadow-2xl md:rounded-3xl">
        <div className="mx-auto mb-4 h-1 w-10 rounded-full bg-ink-600 md:hidden" />
        <div className="text-xs text-zinc-500">{g.game}{g.tipoff ? ` · ${fmtTipoff(g.tipoff)}` : ""}</div>
        <div className="mt-1 flex items-center justify-between gap-3">
          <h3 className="text-xl font-bold">{b.side}</h3>
          {tier && <span className={`rounded-md px-2 py-1 text-xs font-bold ${TIER_BADGE[tier]}`}>{tier}</span>}
        </div>
        <div className="mt-1 text-sm text-zinc-400">{MARKET_LABEL[b.market]}</div>

        <div className="mt-5 grid grid-cols-2 gap-2">
          <Big label="Model prob" v={b.model_prob == null ? "—" : pct(b.model_prob, 1)} />
          <Big label="Market prob" v={pct(b.market_prob, 1)} sub="implied, incl. vig" />
          <Big label="Line edge" v={pct(b.line_edge_pct, 1)} sub="vs Vegas line" accent />
          <Big label="Prob edge" v={signedPct(b.prob_edge)} sub="secondary" />
        </div>

        <div className="mt-3 flex items-center justify-between rounded-2xl border border-edge-400/30 bg-edge-400/10 p-4">
          <div>
            <div className="text-xs font-semibold uppercase tracking-wider text-edge-400">Suggested stake</div>
            <div className="text-[11px] text-zinc-400">¼-Kelly · 1u = 1% of bankroll</div>
          </div>
          <div className="font-mono text-3xl font-black">{b.kelly_quarter_units == null ? "—" : `${b.kelly_quarter_units.toFixed(2)}u`}</div>
        </div>
        {b.logged_at && <p className="mt-3 text-[11px] text-zinc-400">🔒 {fmtPostedProof(b.logged_at, g.tipoff)} · locked, can&apos;t be edited</p>}
        <p className="mt-3 text-[11px] text-zinc-500">Paper-trading season: logged to the public ledger, no real money.</p>
        <button onClick={onClose} className="mt-4 min-h-12 w-full rounded-xl border border-ink-600 font-semibold hover:bg-ink-800">Done</button>
      </div>
    </div>
  );
}

function GameSheet({ g, bets, onClose }: { g: SlateGame | null; bets: ValueBet[]; onClose: () => void }) {
  useEffect(() => {
    const k = (e: KeyboardEvent) => e.key === "Escape" && onClose();
    window.addEventListener("keydown", k);
    return () => window.removeEventListener("keydown", k);
  }, [onClose]);
  if (!g) return null;
  const i = g.insight;
  const ph = i?.proj_home ?? g.proj_home, pa = i?.proj_away ?? g.proj_away;
  // CZarp line from projected margin (proj_home − proj_away), shown in home-line notation like Vegas.
  const czLine = ph != null && pa != null ? -(ph - pa) : g.proj_spread;
  const czTotal = ph != null && pa != null ? ph + pa : g.proj_total;
  const openS = i?.open_spread ?? null, curS = i?.cur_spread ?? g.market_spread;
  const openT = i?.open_total ?? null, curT = i?.cur_total ?? g.market_total;
  const move = (o: number | null, c: number | null) => (o == null || c == null ? "—" : c === o ? "No move" : `${c > o ? "+" : "−"}${Math.abs(c - o).toFixed(1)}`);
  const hw = i?.home_win_prob ?? g.home_win_prob, hc = i?.home_cover_prob ?? g.home_cover_prob, ov = i?.over_prob ?? null;
  return (
    <div className="fixed inset-0 z-50 flex items-end justify-center md:items-center" role="dialog" aria-modal="true" aria-label={`${g.game} insights`}>
      <button aria-label="Close" onClick={onClose} className="absolute inset-0 bg-black/70" />
      <div className="relative max-h-[88dvh] w-full max-w-lg overflow-y-auto rounded-t-3xl border border-ink-700 bg-ink-900 p-5 pb-[calc(1.25rem+env(safe-area-inset-bottom))] shadow-2xl md:rounded-3xl">
        <div className="mx-auto mb-4 h-1 w-10 rounded-full bg-ink-600 md:hidden" />
        <div className="text-xs text-zinc-500">{g.tipoff ? fmtTipoff(g.tipoff) : "Tip-off TBD"}{g.completed ? " · Final" : ""}</div>
        <h3 className="mt-1 text-xl font-bold">{g.game}</h3>
        {i?.neutral && <div className="mt-2 inline-block rounded-md border border-ink-600 px-2 py-0.5 text-[11px] text-zinc-400">Neutral site: no home-court edge applied</div>}

        <div className="mt-4 grid grid-cols-2 gap-2">
          <Big label={`${g.away} proj`} v={pa == null ? "—" : pa.toFixed(1)} />
          <Big label={`${g.home} proj`} v={ph == null ? "—" : ph.toFixed(1)} />
        </div>

        <div className="mt-3 overflow-hidden rounded-xl border border-ink-700 text-sm">
          <div className="grid grid-cols-5 bg-ink-850 px-3 py-2 text-[10px] uppercase tracking-wider text-zinc-500">
            <span className="col-span-1" /><span className="text-right">CZarp</span><span className="text-right">Open</span><span className="text-right">Now</span><span className="text-right">Move</span>
          </div>
          {[
            { k: "Spread", cz: czLine == null ? "—" : fmtLine(czLine), o: openS == null ? "—" : fmtLine(openS), c: curS == null ? "—" : fmtLine(curS), m: move(openS, curS) },
            { k: "Total", cz: czTotal == null ? "—" : czTotal.toFixed(1), o: openT?.toFixed(1) ?? "—", c: curT?.toFixed(1) ?? "—", m: move(openT, curT) },
          ].map((r) => (
            <div key={r.k} className="grid grid-cols-5 border-t border-ink-700/60 px-3 py-2.5 font-mono">
              <span className="font-sans text-xs font-semibold text-zinc-400">{r.k}</span>
              <span className="text-right font-bold text-edge-400">{r.cz}</span>
              <span className="text-right text-zinc-400">{r.o}</span>
              <span className="text-right">{r.c}</span>
              <span className="text-right text-xs text-zinc-500">{r.m}</span>
            </div>
          ))}
        </div>
        <p className="mt-1 text-[10px] text-zinc-600">Home-line notation (−5.5 = {g.home} favored). Vegas consensus.</p>

        <div className="mt-3 grid grid-cols-3 gap-2">
          <Big label="Home win" v={hw == null ? "—" : fmtWinProb(hw)} />
          <Big label="Home cover" v={hc == null ? "—" : pct(hc)} />
          <Big label="Over" v={ov == null ? "—" : pct(ov)} />
        </div>
        {i?.proj_possessions != null && <p className="mt-2 text-xs text-zinc-400">Projected pace: <b className="font-mono text-white">{i.proj_possessions.toFixed(1)}</b> possessions</p>}

        {(g.archetypes.away.length > 0 || g.archetypes.home.length > 0) && (
          <div className="mt-4 space-y-2">
            {[{ n: g.away, t: g.archetypes.away, x: g.archetypes.away_tentative }, { n: g.home, t: g.archetypes.home, x: g.archetypes.home_tentative }].filter((r) => r.t.length).map((r) => (
              <div key={r.n}>
                <div className="mb-1 text-[11px] font-semibold text-zinc-500">{r.n}</div>
                <div className="flex flex-wrap gap-1.5">{r.t.map((t) => <ArchetypeChip key={t} name={t} tentative={r.x} />)}</div>
              </div>
            ))}
            {g.archetypes.notes.length > 0 && <p className="text-[11px] text-zinc-400">{g.archetypes.notes.join(" · ")}</p>}
          </div>
        )}
        {i?.reason && <div className="mt-4 rounded-xl bg-ink-850 p-3 text-sm text-zinc-300"><div className="mb-1 text-[10px] uppercase tracking-wider text-zinc-500">Why CZarp likes it</div>{i.reason}</div>}

        {bets.length > 0 && (
          <div className="mt-4 space-y-1.5">
            {bets.map((b) => {
              const tier = edgeTier(b.line_edge_pct, b.market);
              return (
                <div key={`${b.market}-${b.side}`} className="flex items-center gap-2 rounded-xl border border-edge-400/30 px-3 py-2 text-sm">
                  {tier && <span className={`rounded-md px-1.5 py-0.5 text-[10px] font-bold ${TIER_BADGE[tier]}`}>{tier}</span>}
                  <span className="min-w-0 flex-1 truncate font-semibold">{b.side}</span>
                  <span className="font-mono text-xs text-zinc-400">{pct(b.line_edge_pct, 1)}</span>
                  <span className="font-mono font-bold">{b.kelly_quarter_units == null ? "—" : `${b.kelly_quarter_units.toFixed(2)}u`}</span>
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

function Stat({ label, model, mkt }: { label: string; model: string; mkt: string }) {
  return (
    <div className="bg-ink-900 px-2 py-2.5">
      <div className="text-[10px] uppercase tracking-wider text-zinc-500">{label}</div>
      <div className="font-mono text-sm font-semibold">{model}</div>
      <div className="font-mono text-[11px] text-zinc-500">{mkt && (label === "Home cover" ? mkt : `mkt ${mkt}`)}</div>
    </div>
  );
}
function Big({ label, v, sub, accent }: { label: string; v: string; sub?: string; accent?: boolean }) {
  return (
    <div className="rounded-xl bg-ink-850 p-3">
      <div className="text-[10px] uppercase tracking-wider text-zinc-500">{label}</div>
      <div className={`font-mono text-lg font-bold ${accent ? "text-edge-400" : ""}`}>{v}</div>
      {sub && <div className="text-[10px] text-zinc-600">{sub}</div>}
    </div>
  );
}
