import { notFound } from "next/navigation";
import { SPORTS, getSport } from "@/lib/sports";
import {
  EXAMPLE_LEDGER, EXAMPLE_PICK_RECORD, EXAMPLE_GAME_NAMES, EXAMPLE_CUMULATIVE_UNITS, EXAMPLE_CLV_BEAT_PCT,
  summarize, type LedgerRow,
} from "@/lib/example-data";
import { MARKET_LABEL, RESULT_LABEL, fmtGameDate, fmtOdds, fmtUnits, pct, signedPct } from "@/lib/labels";
import { ComingSoon } from "@/components/ComingSoon";
import { PageHeader } from "@/components/PageHeader";

export function generateStaticParams() {
  return SPORTS.map((s) => ({ sport: s.key }));
}

export default async function ResultsPage({ params }: { params: Promise<{ sport: string }> }) {
  const { sport: key } = await params;
  const sport = getSport(key);
  if (!sport) notFound();
  if (sport.status !== "live") return <ComingSoon sport={sport} />;

  // TODO(supabase): from("pick_record").select("*").eq("league","ncaam").eq("season",2027)
  //                 from("ledger").select("*").eq("sport", key).order("game_date", { ascending: false })
  const s = summarize(EXAMPLE_PICK_RECORD);

  return (
    <div className="mx-auto max-w-7xl px-4 py-5 md:px-6 md:py-10">
      <PageHeader
        eyebrow={`${sport.long} · Results`}
        title="Every pick, graded in public"
        sub="Picks lock before tip-off and are graded from final scores. Nothing is deleted."
        right={
          <div className="no-scrollbar -mx-4 flex gap-2 overflow-x-auto px-4 text-sm md:mx-0 md:px-0">
            {["Season", "Last 30d", "Last 7d"].map((r, i) => (
              <span key={r} className={`flex min-h-10 shrink-0 items-center rounded-full border px-4 ${i === 0 ? "border-court-500 bg-court-500/15 text-court-400" : "border-ink-600 text-zinc-400"}`}>{r}</span>
            ))}
          </div>
        }
      />

      <div className="mt-4 rounded-xl border border-sky-400/30 bg-sky-400/5 p-3.5 text-sm text-sky-100/90 md:mt-6 md:p-4">
        <span className="mr-2 rounded bg-sky-400/20 px-1.5 py-0.5 text-[10px] font-bold uppercase text-sky-300">Paper trading</span>
        This season is paper-traded with notional units, no real money. The full record is public, losses included.
      </div>

      <div className="mt-4 grid grid-cols-2 gap-3 md:mt-6 md:grid-cols-5 md:gap-4">
        <Kpi label="Record (W-L-P)" value={s.record} wide />
        <Kpi label="ATS / win %" value={pct(s.winPct, 1)} sub="Break-even at −110: 52.4%" />
        <Kpi label="ROI" value={signedPct(s.roi)} good={s.roi > 0} />
        <Kpi label="Units" value={fmtUnits(s.units, 1)} good={s.units > 0} />
        <Kpi label="Beat closing line" value={pct(EXAMPLE_CLV_BEAT_PCT)} sub={`${s.graded} graded`} />
      </div>

      <div className="mt-4 grid gap-4 md:mt-6 md:gap-6 lg:grid-cols-[1.4fr_1fr]">
        <div className="card p-4 md:p-5">
          <div className="mb-2 flex items-center justify-between">
            <h2 className="font-bold">Cumulative units</h2>
            <span className="text-xs text-zinc-500">¼-Kelly · paper</span>
          </div>
          <UnitsChart data={EXAMPLE_CUMULATIVE_UNITS} />
        </div>
        <div className="card p-4 md:p-5">
          <h2 className="mb-2 font-bold">By market</h2>
          <div className="divide-y divide-ink-700/60">
            {EXAMPLE_PICK_RECORD.map((r) => {
              const roi = r.pnl / r.staked;
              return (
                <div key={r.market} className="flex items-center justify-between py-3 text-sm">
                  <div>
                    <div className="font-semibold">{MARKET_LABEL[r.market]}</div>
                    <div className="font-mono text-xs text-zinc-500">{r.wins}-{r.losses}-{r.pushes} · {r.pending} pending</div>
                  </div>
                  <div className="text-right font-mono">
                    <div>{pct(r.wins / (r.wins + r.losses), 1)}</div>
                    <div className={`text-xs ${roi >= 0 ? "text-edge-400" : "text-fade-400"}`}>{signedPct(roi)} ROI</div>
                  </div>
                </div>
              );
            })}
          </div>
          <p className="mt-3 text-xs leading-relaxed text-zinc-500">Small samples are noisy. Confidence intervals come once enough picks are graded.</p>
        </div>
      </div>

      <section className="mt-4 md:mt-6">
        <div className="mb-3 flex items-center justify-between">
          <h2 className="font-bold">Pick ledger</h2>
          <button className="min-h-10 rounded-lg border border-ink-600 px-3 text-xs text-zinc-400">Export CSV</button>
        </div>

        {/* phone: card list */}
        <ul className="space-y-2 md:hidden">
          {EXAMPLE_LEDGER.map((r, i) => (
            <li key={i} className="card flex items-center gap-3 p-3.5">
              <ResultPill r={r.result} />
              <div className="min-w-0 flex-1">
                <div className="truncate font-semibold">{r.pick}</div>
                <div className="truncate text-xs text-zinc-500">{EXAMPLE_GAME_NAMES[r.game_id]}</div>
                <div className="mt-0.5 font-mono text-[11px] text-zinc-500">{fmtGameDate(r.game_date)} · {fmtOdds(r.odds)} · {r.stake_units.toFixed(2)}u</div>
              </div>
              <PL v={r.profit_units} />
            </li>
          ))}
        </ul>

        {/* tablet/desktop: table (scrolls horizontally if squeezed) */}
        <div className="card hidden overflow-x-auto md:block">
          <table className="w-full min-w-[720px] text-sm">
            <thead className="bg-ink-850 text-left text-[11px] uppercase tracking-wider text-zinc-500">
              <tr>
                <th className="px-5 py-2.5">Date</th><th className="py-2.5">Game</th><th className="py-2.5">Pick</th>
                <th className="py-2.5 text-right">Odds</th><th className="py-2.5 text-right">Stake</th>
                <th className="py-2.5 text-center">Result</th><th className="px-5 py-2.5 text-right">P/L</th>
              </tr>
            </thead>
            <tbody>
              {EXAMPLE_LEDGER.map((r, i) => (
                <tr key={i} className="border-t border-ink-700/60 hover:bg-ink-850">
                  <td className="px-5 py-3 text-zinc-400">{fmtGameDate(r.game_date)}</td>
                  <td className="py-3 text-zinc-300">{EXAMPLE_GAME_NAMES[r.game_id]}</td>
                  <td className="py-3 font-semibold">{r.pick}</td>
                  <td className="py-3 text-right font-mono text-zinc-400">{fmtOdds(r.odds)}</td>
                  <td className="py-3 text-right font-mono">{r.stake_units.toFixed(2)}u</td>
                  <td className="py-3 text-center"><ResultPill r={r.result} /></td>
                  <td className="px-5 py-3 text-right"><PL v={r.profit_units} /></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}

function ResultPill({ r }: { r: LedgerRow["result"] }) {
  const cls = r === "W" ? "bg-edge-400/15 text-edge-400" : r === "L" ? "bg-fade-400/15 text-fade-400" : r === "P" ? "bg-ink-700 text-zinc-300" : "bg-amber-400/10 text-amber-300";
  return (
    <span title={r ? RESULT_LABEL[r] : "Pending"} className={`inline-grid h-8 min-w-8 shrink-0 place-items-center rounded-lg px-1.5 text-xs font-bold ${cls}`}>
      {r ?? "…"}
    </span>
  );
}
function PL({ v }: { v: number | null }) {
  if (v == null) return <span className="font-mono text-xs text-amber-300">Pending</span>;
  return <span className={`font-mono text-sm font-semibold ${v > 0 ? "text-edge-400" : v < 0 ? "text-fade-400" : "text-zinc-500"}`}>{fmtUnits(v)}</span>;
}
function Kpi({ label, value, sub, good, wide }: { label: string; value: string; sub?: string; good?: boolean; wide?: boolean }) {
  return (
    <div className={`card p-3.5 md:p-4 ${wide ? "col-span-2 md:col-span-1" : ""}`}>
      <div className="text-[10px] uppercase tracking-wider text-zinc-500 md:text-[11px]">{label}</div>
      <div className={`mt-1 font-mono text-xl font-bold md:text-2xl ${good ? "text-edge-400" : ""}`}>{value}</div>
      {sub && <div className="mt-0.5 text-[10px] text-zinc-500 md:text-[11px]">{sub}</div>}
    </div>
  );
}
function UnitsChart({ data }: { data: number[] }) {
  const w = 600, h = 220, pad = 28;
  const min = Math.min(...data, -2), max = Math.max(...data, 2);
  const x = (i: number) => pad + (i / (data.length - 1)) * (w - pad * 1.5);
  const y = (v: number) => 12 + ((max - v) / (max - min)) * (h - 24);
  const line = data.map((v, i) => `${i ? "L" : "M"}${x(i)},${y(v)}`).join(" ");
  const area = `${line} L${x(data.length - 1)},${y(0)} L${x(0)},${y(0)} Z`;
  return (
    <svg viewBox={`0 0 ${w} ${h}`} className="h-44 w-full md:h-52" preserveAspectRatio="none">
      <defs>
        <linearGradient id="u" x1="0" x2="0" y1="0" y2="1">
          <stop offset="0" stopColor="#ff7a1a" stopOpacity="0.35" />
          <stop offset="1" stopColor="#ff7a1a" stopOpacity="0" />
        </linearGradient>
      </defs>
      {[-2, -1, 0, 1, 2].map((t) => (
        <g key={t}>
          <line x1={pad} x2={w} y1={y(t)} y2={y(t)} stroke={t === 0 ? "#3a4659" : "#1f2836"} strokeDasharray={t === 0 ? "" : "3 4"} />
          <text x={0} y={y(t) + 4} fontSize="13" fill="#6b7280">{t > 0 ? `+${t}` : t}u</text>
        </g>
      ))}
      <path d={area} fill="url(#u)" />
      <path d={line} fill="none" stroke="#ff9a3d" strokeWidth="3" strokeLinejoin="round" vectorEffect="non-scaling-stroke" />
    </svg>
  );
}
