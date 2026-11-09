import Link from "next/link";
import { ExampleBadge, ExampleBanner } from "@/components/ExampleBadge";
import { ArchetypeChip } from "@/components/ArchetypeChip";
import { EXAMPLE_SLATE, EXAMPLE_VALUE_BETS, EXAMPLE_PICK_RECORD, summarize } from "@/lib/example-data";
import { MARKET_LABEL, edgeTier, fmtLine, fmtTipoff, pct, signedPct } from "@/lib/labels";
import { SPORTS, getSport } from "@/lib/sports";
import { loadHero, type Hero } from "@/lib/data";
import { RESULT_LABEL, fmtGameDate, fmtPostedProof, fmtUnits } from "@/lib/labels";

// Hero shows today's top official play, so render per request.
export const dynamic = "force-dynamic";

export default async function Landing() {
  const cbb = getSport("cbb");
  const heroRes = cbb ? await loadHero(cbb) : null;
  const hero = heroRes?.state === "live" ? heroRes.data : null;
  const g = EXAMPLE_SLATE[0];
  const v = EXAMPLE_VALUE_BETS.find((b) => b.game_id === g.game_id)!;
  const s = summarize(EXAMPLE_PICK_RECORD);
  return (
    <div>
      {!hero && <ExampleBanner />}
      <section className="grid-bg relative overflow-hidden border-b border-ink-700/60">
        <div className="pointer-events-none absolute -top-40 left-1/2 h-[500px] w-[900px] -translate-x-1/2 rounded-full bg-court-500/15 blur-3xl" />
        <div className="relative mx-auto grid max-w-7xl gap-8 px-4 py-10 md:gap-12 md:px-6 md:py-24 lg:grid-cols-[1.1fr_1fr] lg:items-center">
          <div>
            <div className="mb-4 inline-flex items-center gap-2 rounded-full border border-ink-600 bg-ink-850 px-3 py-1 text-xs text-zinc-300 md:mb-6">
              <span className="h-1.5 w-1.5 rounded-full bg-edge-400" /> 2026–27 · paper-trading in public
            </div>
            <h1 className="text-[2.5rem] font-black leading-[1.05] tracking-tight md:text-6xl">
              Buy bets like <span className="bg-gradient-to-r from-court-400 to-amber-300 bg-clip-text text-transparent">stocks</span> — at a price.
            </h1>
            <p className="mt-4 max-w-xl text-base text-zinc-400 md:mt-6 md:text-lg">
              Fair value vs market price on every D1 men&apos;s game. Edge % is the headline. A public portfolio grades every pick — wins, losses, and drawdowns.
            </p>
            <div className="mt-6 grid gap-3 sm:flex md:mt-8">
              <Link href="/signup" className="flex min-h-12 items-center justify-center rounded-xl bg-court-500 px-5 font-semibold text-ink-950 hover:bg-court-400">Create free account</Link>
              <Link href="/cbb/slate" className="flex min-h-12 items-center justify-center rounded-xl border border-ink-600 px-5 font-semibold hover:bg-ink-800">See today&apos;s slate →</Link>
            </div>
            <div className="mt-6 flex flex-wrap gap-2 text-xs text-zinc-500 md:mt-10">
              {SPORTS.map((sp) => (
                <span key={sp.key} className={`rounded-md border px-2 py-1 ${sp.status === "live" ? "border-court-500/50 text-court-400" : "border-ink-600"}`}>
                  {sp.label} {sp.status === "live" ? "· live" : "· soon"}
                </span>
              ))}
            </div>
          </div>

          {hero ? <LiveHero hero={hero} /> : (
          <div className="card relative p-4 shadow-2xl shadow-black/50 md:p-5">
            <div className="mb-2 flex items-center justify-between">
              <span className="text-xs font-semibold uppercase tracking-wider text-zinc-500">Tonight · {fmtTipoff(g.tipoff!)}</span>
              <ExampleBadge />
            </div>
            {[
              { n: g.away, p: g.proj_away, tags: g.archetypes.away, t: g.archetypes.away_tentative },
              { n: g.home, p: g.proj_home, tags: g.archetypes.home, t: g.archetypes.home_tentative },
            ].map((r) => (
              <div key={r.n} className="flex items-center justify-between gap-3 border-b border-ink-700/60 py-3 last:border-0">
                <div className="min-w-0">
                  <div className="font-semibold">{r.n}</div>
                  <div className="mt-1.5 flex flex-wrap gap-1">{r.tags.map((a) => <ArchetypeChip key={a.name} tag={a} />)}</div>
                </div>
                <div className="font-mono text-2xl font-bold">{r.p?.toFixed(1)}</div>
              </div>
            ))}
            <div className="mt-3 grid grid-cols-3 gap-2 text-center">
              <Mini label="Spread" value={fmtLine(g.proj_spread!)} />
              <Mini label="Total" value={g.proj_total!.toFixed(1)} />
              <Mini label="Away win" value={pct(1 - g.home_win_prob!)} />
            </div>
            <div className="mt-3 flex items-center justify-between gap-3 rounded-xl border border-edge-400/30 bg-edge-400/10 px-4 py-3">
              <div className="min-w-0">
                <div className="text-[11px] font-semibold uppercase tracking-wider text-edge-400">{edgeTier(v.line_edge_pct, v.market) ?? "No label"} · {MARKET_LABEL[v.market]} · Edge</div>
                <div className="truncate text-sm font-semibold">{v.side}</div>
                <div className="mt-0.5 text-[11px] text-zinc-500">prob {signedPct(v.prob_edge)} · ¼-Kelly {v.kelly_quarter_units!.toFixed(1)}u</div>
              </div>
              <div className="shrink-0 font-mono text-4xl font-black leading-none text-edge-400">{pct(v.line_edge_pct, 1)}</div>
            </div>
          </div>
          )}
        </div>
      </section>

      <section className="mx-auto max-w-7xl px-4 py-10 md:px-6 md:py-20">
        <div className="grid gap-3 md:grid-cols-3 md:gap-5">
          {[
            { t: "Price vs fair value", d: "Every game shows the market price next to our fair value. Edge % is the big number — buy when the price is wrong, not when you feel it." },
            { t: "Portfolio, not vibes", d: "ROI, units P/L, cumulative curve, CLV, and open variance (drawdowns and streaks) on the record page. Judge hundreds of bets." },
            { t: "Watchlist", d: "Star games on the slate and filter to what you're watching. No alerts yet — just a clean list you control." },
          ].map((f) => (
            <div key={f.t} className="card p-5 md:p-6">
              <h3 className="text-lg font-bold">{f.t}</h3>
              <p className="mt-1.5 text-sm leading-relaxed text-zinc-400">{f.d}</p>
            </div>
          ))}
        </div>

        <div className="card mt-6 p-5 md:mt-10 md:flex md:items-center md:justify-between md:gap-6 md:p-6">
          <div>
            <div className="flex items-center gap-2"><h3 className="text-lg font-bold">Season portfolio</h3><ExampleBadge /></div>
            <p className="mt-1 text-sm text-zinc-400">Paper-traded: a public book before anyone pays.</p>
          </div>
          <div className="mt-4 grid grid-cols-3 gap-3 font-mono md:mt-0 md:flex md:gap-8">
            <Big label="Record" value={s.record} />
            <Big label="ATS" value={pct(s.winPct, 1)} />
            <Big label="ROI" value={signedPct(s.roi)} />
          </div>
          <Link href="/cbb/results" className="mt-4 flex min-h-11 items-center justify-center rounded-xl border border-ink-600 px-4 text-sm font-semibold hover:bg-ink-800 md:mt-0">Full portfolio →</Link>
        </div>

        <div className="mt-6 rounded-2xl border border-court-500/30 bg-gradient-to-br from-court-500/10 to-transparent p-5 md:mt-10 md:flex md:items-center md:justify-between md:gap-6 md:p-8">
          <div>
            <div className="text-[11px] font-bold uppercase tracking-wider text-court-400">Free: 1 model · Pro: unlimited</div>
            <h3 className="mt-1.5 text-2xl font-bold">Build your own model</h3>
            <p className="mt-1.5 max-w-2xl text-sm text-zinc-400 md:text-base">Think pace matters more? Pick the features, set the weights, save it and compare it to CZarp night after night.</p>
          </div>
          <Link href="/models" className="mt-4 flex min-h-12 shrink-0 items-center justify-center rounded-xl bg-white px-5 font-semibold text-ink-950 hover:bg-zinc-200 md:mt-0">Try My Models</Link>
        </div>
      </section>
    </div>
  );
}

function Mini({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-lg bg-ink-850 px-2 py-2">
      <div className="text-[10px] uppercase tracking-wider text-zinc-500">{label}</div>
      <div className="mt-0.5 font-mono text-sm font-semibold">{value}</div>
    </div>
  );
}
function Big({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <div className="text-[10px] uppercase tracking-wider text-zinc-500">{label}</div>
      <div className="text-lg font-bold md:text-2xl">{value}</div>
    </div>
  );
}

function LiveHero({ hero }: { hero: Hero }) {
  if (hero.kind === "today") {
    const { game: g, bet: v } = hero;
    const tier = edgeTier(v.line_edge_pct, v.market);
    return (
      <Link href="/cbb/slate?sort=value" className="card relative block p-4 shadow-2xl shadow-black/50 md:p-5">
        <div className="mb-2 flex items-center justify-between text-xs">
          <span className="font-semibold uppercase tracking-wider text-edge-400">Highest edge today</span>
          <span className="text-zinc-500">{g.tipoff ? fmtTipoff(g.tipoff) : "Tip TBD"}</span>
        </div>
        {[{ n: g.away, p: g.proj_away, tags: g.archetypes.away, t: g.archetypes.away_tentative }, { n: g.home, p: g.proj_home, tags: g.archetypes.home, t: g.archetypes.home_tentative }].map((r) => (
          <div key={r.n} className="flex items-center justify-between gap-3 border-b border-ink-700/60 py-3 last:border-0">
            <div className="min-w-0">
              <div className="font-semibold">{r.n}</div>
              {r.tags.length > 0 && <div className="mt-1.5 flex flex-wrap gap-1">{r.tags.map((a) => <ArchetypeChip key={a.name} tag={a} />)}</div>}
            </div>
            <div className="font-mono text-2xl font-bold">{r.p == null ? "—" : r.p.toFixed(1)}</div>
          </div>
        ))}
        <div className="mt-3 flex items-center justify-between gap-3 rounded-xl border border-edge-400/30 bg-edge-400/10 px-4 py-3">
          <div className="min-w-0">
            <div className="text-[11px] font-semibold uppercase tracking-wider text-edge-400">{tier ?? "Official"} · {MARKET_LABEL[v.market]} · Edge</div>
            <div className="truncate text-sm font-semibold">{v.side}</div>
            {v.logged_at && <div className="text-[10px] text-zinc-500">🔒 {fmtPostedProof(v.logged_at, g.tipoff)}</div>}
            <div className="mt-0.5 text-[11px] text-zinc-500">¼-Kelly {v.kelly_quarter_units == null ? "—" : `${v.kelly_quarter_units.toFixed(2)}u`}</div>
          </div>
          <div className={`shrink-0 font-mono text-4xl font-black leading-none ${tier ? "text-edge-400" : "text-zinc-400"}`}>{pct(v.line_edge_pct, 1)}</div>
        </div>
      </Link>
    );
  }
  const r = hero.row;
  const tone = r.result === "W" ? "text-edge-400" : r.result === "L" ? "text-red-400" : "text-zinc-300";
  return (
    <Link href="/cbb/results" className="card relative block p-4 shadow-2xl shadow-black/50 md:p-5">
      <div className="mb-2 flex items-center justify-between text-xs">
        <span className="font-semibold uppercase tracking-wider text-zinc-400">{hero.when === "yesterday" ? "Yesterday's top play" : "Latest graded play"}</span>
        <span className="text-zinc-500">{fmtGameDate(r.game_date)}</span>
      </div>
      {hero.game && <div className="text-sm text-zinc-400">{hero.game}</div>}
      <div className="mt-1 text-xl font-bold">{r.pick}{r.line != null ? ` ${fmtLine(r.line)}` : ""}</div>
      <div className="mt-4 grid grid-cols-3 gap-2 text-center">
        <Mini label="Result" value={r.result ? RESULT_LABEL[r.result] : "Pending"} />
        <Mini label="Stake" value={r.stake_units == null ? "—" : `${r.stake_units.toFixed(2)}u`} />
        <Mini label="P/L" value={r.profit_units == null ? "—" : fmtUnits(r.profit_units)} />
      </div>
      {r.result && <div className={`mt-3 text-center text-sm font-semibold ${tone}`}>{RESULT_LABEL[r.result]}</div>}
      {r.logged_at && <div className="mt-2 text-[11px] text-zinc-500">🔒 {fmtPostedProof(r.logged_at, null)}</div>}
    </Link>
  );
}
