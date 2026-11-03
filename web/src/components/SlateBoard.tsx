"use client";
import { useEffect, useState } from "react";
import type { SlateGame, ValueBet } from "@/lib/example-data";
import { MARKET_LABEL, SOURCE_LABEL, edgeTier, fmtLine, fmtTipoff, pct, signedPct } from "@/lib/labels";
import { ArchetypeChip, ArchetypeLegend } from "./ArchetypeChip";

type Sel = { game: SlateGame; bet: ValueBet } | null;

export function SlateBoard({ games, bets }: { games: SlateGame[]; bets: ValueBet[] }) {
  const [valueOnly, setValueOnly] = useState(false);
  const [sel, setSel] = useState<Sel>(null);
  const byGame = (id: number) => bets.filter((b) => b.game_id === id);
  const shown = valueOnly ? games.filter((g) => byGame(g.game_id).length) : games;
  const valueCount = games.filter((g) => byGame(g.game_id).length).length;

  return (
    <div>
      <div className="no-scrollbar -mx-4 mb-4 flex items-center gap-2 overflow-x-auto px-4 md:mx-0 md:px-0">
        <Toggle on={!valueOnly} onClick={() => setValueOnly(false)}>All games · {games.length}</Toggle>
        <Toggle on={valueOnly} onClick={() => setValueOnly(true)}>Value · {valueCount}</Toggle>
        <div className="ml-auto hidden md:block"><ArchetypeLegend /></div>
      </div>

      <div className="grid gap-3 md:gap-4 lg:grid-cols-2">
        {shown.map((g) => (
          <GameCard key={g.game_id} g={g} bets={byGame(g.game_id)} onOpen={(bet) => setSel({ game: g, bet })} />
        ))}
      </div>

      <div className="mt-4 md:hidden"><ArchetypeLegend /></div>
      <p className="mt-3 text-xs leading-relaxed text-zinc-500">
        Spreads use home-line notation (−5.5 = home favored). Market = Vegas consensus. Stakes are ¼-Kelly in units (1u = 1% of bankroll, capped at 2.5u).
      </p>

      <ValueSheet sel={sel} onClose={() => setSel(null)} />
    </div>
  );
}

function GameCard({ g, bets, onOpen }: { g: SlateGame; bets: ValueBet[]; onOpen: (b: ValueBet) => void }) {
  const awayWin = 1 - g.home_win_prob;
  const rows = [
    { side: "Away", name: g.away, proj: g.proj_away, win: awayWin, cover: 1 - g.home_cover_prob, tags: g.archetypes.away, tent: g.archetypes.away_tentative },
    { side: "Home", name: g.home, proj: g.proj_home, win: g.home_win_prob, cover: g.home_cover_prob, tags: g.archetypes.home, tent: g.archetypes.home_tentative },
  ];
  return (
    <article className="card overflow-hidden">
      <header className="flex items-center justify-between border-b border-ink-700/60 px-4 py-2.5 text-xs">
        <span className="font-semibold text-zinc-300">{fmtTipoff(g.tipoff)}</span>
        <span className="font-mono text-zinc-600">#{g.game_id}</span>
      </header>

      <div className="divide-y divide-ink-700/40 px-4">
        {rows.map((r) => (
          <div key={r.side} className="py-3">
            <div className="flex items-center gap-3">
              <span className="w-9 shrink-0 text-[10px] font-semibold uppercase text-zinc-600">{r.side}</span>
              <span className="min-w-0 flex-1 truncate text-[15px] font-semibold">{r.name}</span>
              <span className={`text-right font-mono text-xs ${r.win >= 0.5 ? "text-white" : "text-zinc-500"}`}>{pct(r.win)}<span className="ml-0.5 font-sans text-[10px] text-zinc-600">win</span></span>
              <span className="w-12 text-right font-mono text-xl font-bold">{r.proj.toFixed(1)}</span>
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
        <Stat label="Spread" model={fmtLine(g.proj_spread)} mkt={g.market_spread == null ? "—" : fmtLine(g.market_spread)} />
        <Stat label="Total" model={g.proj_total.toFixed(1)} mkt={g.market_total?.toFixed(1) ?? "—"} />
        <Stat label="Home cover" model={pct(g.home_cover_prob)} mkt={g.market_source ? SOURCE_LABEL[g.market_source] : ""} />
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
            const tier = edgeTier(b.prob_edge);
            return (
              <button
                key={b.side}
                onClick={() => onOpen(b)}
                className="flex min-h-14 w-full items-center gap-3 rounded-xl border border-edge-400/30 bg-ink-900 px-3 py-2.5 text-left transition active:scale-[0.99] hover:border-edge-400/60"
              >
                <span className="rounded-md bg-edge-400/15 px-1.5 py-1 text-[10px] font-bold text-edge-400">{tier ?? "EDGE"}</span>
                <span className="min-w-0 flex-1">
                  <span className="block truncate text-sm font-semibold">{b.side}</span>
                  <span className="block text-[11px] text-zinc-500">{MARKET_LABEL[b.market]} · edge {signedPct(b.prob_edge)}</span>
                </span>
                <span className="text-right">
                  <span className="block font-mono text-base font-bold">{b.kelly_quarter_units.toFixed(1)}u</span>
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
  const tier = edgeTier(b.prob_edge);
  return (
    <div className="fixed inset-0 z-50 flex items-end justify-center md:items-center" role="dialog" aria-modal="true">
      <button aria-label="Close" onClick={onClose} className="absolute inset-0 bg-black/70" />
      <div className="relative w-full max-w-lg rounded-t-3xl border border-ink-700 bg-ink-900 p-5 pb-[calc(1.25rem+env(safe-area-inset-bottom))] shadow-2xl md:rounded-3xl">
        <div className="mx-auto mb-4 h-1 w-10 rounded-full bg-ink-600 md:hidden" />
        <div className="text-xs text-zinc-500">{g.game} · {fmtTipoff(g.tipoff)}</div>
        <div className="mt-1 flex items-center justify-between gap-3">
          <h3 className="text-xl font-bold">{b.side}</h3>
          <span className="rounded-md bg-edge-400/15 px-2 py-1 text-xs font-bold text-edge-400">{tier ?? "EDGE"}</span>
        </div>
        <div className="mt-1 text-sm text-zinc-400">{MARKET_LABEL[b.market]}</div>

        <div className="mt-5 grid grid-cols-2 gap-2">
          <Big label="Model prob" v={pct(b.model_prob, 1)} />
          <Big label="Market prob" v={pct(b.market_prob, 1)} sub="implied, incl. vig" />
          <Big label="Prob edge" v={signedPct(b.prob_edge)} accent />
          <Big label="Line edge" v={pct(b.line_edge_pct, 1)} sub="of market total" />
        </div>

        <div className="mt-3 flex items-center justify-between rounded-2xl border border-edge-400/30 bg-edge-400/10 p-4">
          <div>
            <div className="text-xs font-semibold uppercase tracking-wider text-edge-400">Suggested stake</div>
            <div className="text-[11px] text-zinc-400">¼-Kelly · 1u = 1% of bankroll</div>
          </div>
          <div className="font-mono text-3xl font-black">{b.kelly_quarter_units.toFixed(2)}u</div>
        </div>
        <p className="mt-3 text-[11px] text-zinc-500">Paper-trading season: logged to the public ledger, no real money.</p>
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
function Toggle({ on, onClick, children }: { on: boolean; onClick: () => void; children: React.ReactNode }) {
  return (
    <button
      onClick={onClick}
      className={`min-h-10 shrink-0 rounded-full border px-4 text-sm font-semibold ${on ? "border-court-500 bg-court-500/15 text-court-400" : "border-ink-600 text-zinc-400 hover:bg-ink-800"}`}
    >
      {children}
    </button>
  );
}
