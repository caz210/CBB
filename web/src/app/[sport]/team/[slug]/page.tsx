import { NeutralChip } from "@/components/NeutralChip";
import { atsSideLabel, gameTitle, spreadRef } from "@/lib/gameTitle";
import Link from "next/link";
import type { Metadata } from "next";
import { notFound } from "next/navigation";
import { getSport } from "@/lib/sports";
import { loadTeam } from "@/lib/data";
import { teamHref } from "@/lib/teams";
import { matchupHref } from "@/lib/matchup";
import { GAME_STATUS_LABEL, finalLabel, fmtGameDate, fmtLine, fmtPlayPct, fmtTipoff } from "@/lib/labels";
import { ComingSoon } from "@/components/ComingSoon";
import { EmptyState } from "@/components/EmptyState";
import { PageHeader } from "@/components/PageHeader";
import { ArchetypeChip } from "@/components/ArchetypeChip";
import { InfoTip } from "@/components/InfoTip";
import { GLOSSARY_HREF } from "@/lib/glossary";
import { visibleTags } from "@/lib/archetypes";
import { TermLabel } from "@/components/InfoTip";
import { StatusChips } from "@/components/StatusChips";
import { NewCoachBadge } from "@/components/NewCoachBadge";
import { newCoachLock } from "@/lib/coach";
import { cardHeadline, notAPlayLine } from "@/lib/verdict";
import { totalFair } from "@/lib/posted";

export const dynamic = "force-dynamic";

export async function generateMetadata({ params }: { params: Promise<{ sport: string; slug: string }> }): Promise<Metadata> {
  const { sport: key, slug } = await params;
  const sport = getSport(key);
  const name = decodeSlug(slug).replace(/\b\w/g, (c) => c.toUpperCase());
  return { title: sport ? `${name} · ${sport.label}` : name };
}

export default async function TeamPage({ params }: { params: Promise<{ sport: string; slug: string }> }) {
  const { sport: key, slug } = await params;
  const sport = getSport(key);
  if (!sport) notFound();
  if (sport.status !== "live") return <ComingSoon sport={sport} />;

  const res = await loadTeam(sport, slug);
  if (res.state === "live" && res.data == null) notFound();

  return (
    <div className="mx-auto max-w-7xl px-4 py-5 md:px-6 md:py-10">
      <PageHeader
        badge={res.state === "live" ? "live" : "none"}
        eyebrow={`${sport.long} · Team`}
        title={res.state === "live" && res.data ? (res.data.profile?.team_name ?? res.data.record?.team_name ?? decodeSlug(slug)) : decodeSlug(slug)}
        sub={
          res.state === "live" && res.data?.profile
            ? `${res.data.profile.season - 1}–${String(res.data.profile.season).slice(2)} · ${res.data.profile.games_played ?? "—"} GP${res.data.profile.tentative ? " · tentative" : ""}`
            : "Profile, CZarp record, and game log."
        }
        right={
          <div className="flex gap-2">
            {res.state === "live" && (res.data?.profile?.team_id ?? res.data?.record?.team_id) != null && (
              <Link
                href={matchupHref(sport.key, { a: (res.data?.profile?.team_id ?? res.data?.record?.team_id) as number, venue: "neutral" })}
                className="flex min-h-10 items-center rounded-lg bg-court-500/15 px-3 text-sm font-semibold text-court-400 hover:bg-court-500/25"
              >
                Matchup
              </Link>
            )}
            <Link href={`/${sport.key}/slate`} className="flex min-h-10 items-center rounded-lg border border-ink-600 px-3 text-sm font-semibold text-zinc-300 hover:bg-ink-800">
              ← Slate
            </Link>
          </div>
        }
      />

      {res.state === "unconfigured" ? (
        <div className="mt-4"><EmptyState tone="warn" title="Live data isn't connected yet" body="Supabase settings are missing on this deployment." /></div>
      ) : res.state === "error" ? (
        <div className="mt-4"><EmptyState tone="warn" title="Couldn't load this team" body="The data service didn't respond. Please refresh in a minute." /></div>
      ) : res.data ? (
        <TeamBody sportKey={sport.key} data={res.data} />
      ) : null}
    </div>
  );
}

function decodeSlug(slug: string) {
  return slug.replace(/-/g, " ").replace(/\band\b/g, "&");
}

function TeamBody({ sportKey, data }: { sportKey: string; data: NonNullable<Extract<Awaited<ReturnType<typeof loadTeam>>, { state: "live" }>["data"]> }) {
  const p = data.profile;
  const r = data.record;
  const next = data.next;
  const coach = p ? newCoachLock(p.team_name, p) : null;
  const nextEdge = next ? cardHeadline(next, next.officialBets ?? []) : null;
  const nextWhy = next && nextEdge?.verdict !== "official" ? notAPlayLine(next, next.officialBets ?? []) : null;
  const nextTotal = next ? totalFair(next, next.officialBets ?? []) : null;

  return (
    <div className="mt-4 space-y-4 md:mt-6 md:space-y-6">
      {coach && <NewCoachBadge lock={coach} />}

      {/* Tags — every fired public tag from tags_all */}
      {visibleTags(data.tags).length > 0 && (
        <div>
          <div className="flex min-w-0 flex-wrap gap-1.5">
            {visibleTags(data.tags).map((t) => <ArchetypeChip key={`${t.kind}-${t.name}`} tag={t} />)}
          </div>
          <Link href={GLOSSARY_HREF} className="mt-2 inline-block text-xs font-semibold text-court-400 hover:text-court-300">
            What do these tags mean?
          </Link>
        </div>
      )}

      {/* Next game */}
      <section className="card p-4 md:p-5">
        <h2 className="mb-2 font-bold">Next game</h2>
        {!next ? (
          <p className="text-sm text-zinc-500">No upcoming game on the slate yet.</p>
        ) : (
          <div className="flex flex-wrap items-end justify-between gap-3">
            <div className="min-w-0">
              <div className="text-xs text-zinc-500">{fmtGameDate(next.game_date)}{next.tipoff ? ` · ${fmtTipoff(next.tipoff)}` : ""}</div>
              <div className="mt-0.5 text-lg font-bold">
                <Link className="hover:text-court-400" href={teamHref(sportKey, next.away)}>{next.away}</Link>
                <span className="text-zinc-500"> {gameTitle(next.away, next.home, next.neutral).sep} </span>
                <Link className="hover:text-court-400" href={teamHref(sportKey, next.home)}>{next.home}</Link>
                {next.neutral && <NeutralChip className="ml-2" />}
              </div>
              <div className="mt-1 font-mono text-xs text-zinc-400">
                FV {next.proj_spread == null ? "—" : spreadRef(fmtLine(next.proj_spread), next.home, next.neutral)} · Price {next.market_spread == null ? "—" : spreadRef(fmtLine(next.market_spread), next.home, next.neutral)}
                {/* Same helper as the slate card: posted fair for an official total, else czarp-v1-totals, else "—". Never v2. */}
                {" · Total FV "}{nextTotal!.value}
                {nextTotal!.tag && <span className="ml-1 rounded px-1 py-px align-middle font-sans text-[9px] font-bold text-zinc-400 ring-1 ring-ink-600">{nextTotal!.tag}</span>}
                {" / "}{next.market_total == null ? "—" : next.market_total.toFixed(1)}
              </div>
              {next.upset_alert && (
                <div className="mt-2 inline-block rounded-md border border-amber-400/40 bg-amber-400/10 px-2 py-1 text-[11px] text-amber-200">
                  <InfoTip term="upset-alert" triggerClassName="text-[11px] text-amber-200">Upset alert</InfoTip>
                  {next.upset_reason ? `: ${next.upset_reason}` : ""}
                </div>
              )}
              {nextWhy && (
                <p className="mt-2 text-[11px] text-zinc-400">{nextWhy}</p>
              )}
            </div>
            <div className="text-right">
              <div className="flex flex-wrap items-center justify-end gap-1 text-[10px] font-semibold uppercase tracking-wider text-zinc-500">
                <TermLabel term="edge">Edge</TermLabel>
                <StatusChips g={next} bets={next.officialBets ?? []} />
              </div>
              <div className={`font-mono text-4xl font-black ${nextEdge?.edgePct == null ? "text-zinc-700" : nextEdge.verdict === "official" ? "text-edge-400" : "text-zinc-500"}`}>
                {nextEdge?.edgePct == null ? "—" : fmtPlayPct(nextEdge.edgePct)}
              </div>
              <div className="text-[11px] text-zinc-500">{nextEdge?.subtitle ?? (nextEdge?.market ?? "fair vs price")}</div>
            </div>
          </div>
        )}
      </section>

      {/* Profile percentiles */}
      <section className="card p-4 md:p-5">
        <div className="mb-3 flex flex-wrap items-baseline justify-between gap-2">
          <h2 className="flex items-center gap-1.5 font-bold"><TermLabel term="percentile">Profile</TermLabel></h2>
          <span className="text-xs text-zinc-500">Percentiles only · not KenPom ranks</span>
        </div>
        {!p ? (
          <p className="text-sm text-zinc-500">No profile row yet for this team.</p>
        ) : (
          <>
            <div className="grid grid-cols-2 gap-2 sm:grid-cols-3 md:grid-cols-4">
              {[
                { l: "eFG%", v: p.efg_pctile },
                { l: "Opp eFG%", v: p.d_efg_pctile },
                { l: "TO%", v: p.to_pct_pctile },
                { l: "Opp TO%", v: p.d_to_pct_pctile },
                { l: "OR%", v: p.or_pct_pctile },
                { l: "Opp OR%", v: p.opp_or_pct_pctile },
                { l: "FTR", v: p.ftr_pctile },
                { l: "Opp FTR", v: p.d_ftr_pctile },
                { l: "Tempo", v: p.tempo_pctile },
                { l: "3PA rate", v: p.fg3a_rate_pctile },
                { l: "3P%", v: p.fg3_pct_pctile },
                { l: "Opp 3P%", v: p.opp_fg3_pct_pctile },
                { l: "FT%", v: p.ft_pct_pctile },
              ].map((x) => (
                <Pctile key={x.l} label={x.l} value={x.v} />
              ))}
            </div>
            {(p.adj_oe != null || p.adj_de != null || p.hca != null) && (
              <div className="mt-4 grid grid-cols-3 gap-2 border-t border-ink-700/60 pt-3">
                <Mini label="Adj OE" value={p.adj_oe == null ? "—" : p.adj_oe.toFixed(1)} hint="pts / 100" />
                <Mini label="Adj DE" value={p.adj_de == null ? "—" : p.adj_de.toFixed(1)} hint="pts / 100" />
                <Mini label="HCA" value={p.hca == null ? "—" : p.hca.toFixed(1)} hint="pts" />
              </div>
            )}
            <p className="mt-2 text-[11px] text-zinc-600">Adj OE/DE are efficiency (points per 100 possessions), not ranks.</p>
          </>
        )}
      </section>

      {/* CZarp record */}
      <section className="card p-4 md:p-5">
        <h2 className="mb-3 font-bold">CZarp record</h2>
        {!r ? (
          <p className="text-sm text-zinc-500">No graded games for this team yet.</p>
        ) : (
          <div className="grid gap-3 sm:grid-cols-2">
            <RecordCard
              title="All games (model)"
              rows={[
                { l: "ATS", v: `${r.model_ats_wins}-${r.model_ats_losses}-${r.model_ats_pushes}` },
                { l: "O/U", v: `${r.model_ou_wins}-${r.model_ou_losses}-${r.model_ou_pushes}` },
              ]}
            />
            <RecordCard
              title="Official picks"
              rows={[
                { l: "Overall", v: `${r.official_wins}-${r.official_losses}-${r.official_pushes}` },
                { l: "ATS", v: `${r.official_ats_wins}-${r.official_ats_losses}` },
                { l: "O/U", v: `${r.official_ou_wins}-${r.official_ou_losses}` },
              ]}
            />
          </div>
        )}
      </section>

      {/* Game log */}
      <section>
        <div className="mb-3 flex items-center justify-between">
          <h2 className="font-bold">Game log</h2>
          <span className="text-xs text-zinc-500">
            {p?.games_played != null && p.games_played > data.grades.length
              ? `${data.grades.length} of ${p.games_played} games · not every game is in our log`
              : `${data.grades.length} games`}
          </span>
        </div>
        {data.grades.length === 0 ? (
          <p className="card p-4 text-sm text-zinc-500">No graded or scheduled log rows yet.</p>
        ) : (
          <ul className="space-y-2">
            {data.grades.map((g) => {
              const opp = g.home_team_id === p?.team_id || g.home === p?.team_name ? g.away : g.home;
              // a neutral site has no road team, so it is always "vs" (the chip below carries the label)
              const loc = g.neutral || g.home_team_id === p?.team_id || g.home === p?.team_name ? "vs" : "@";
              return (
                <li key={g.game_id} className="card p-3.5">
                  <div className="flex items-start justify-between gap-3">
                    <div className="min-w-0">
                      <div className="text-[11px] text-zinc-500">{fmtGameDate(g.game_date)}{g.status === "final" || g.final_at ? ` · ${finalLabel(g.final_at, g.final_at_estimated, g.game_date)}` : g.status ? ` · ${GAME_STATUS_LABEL[g.status as keyof typeof GAME_STATUS_LABEL] ?? g.status}` : ""}</div>
                      <div className={g.neutral ? "flex min-w-0 items-center gap-1.5 font-semibold" : "truncate font-semibold"}>
                        <span className={g.neutral ? "min-w-0 truncate" : undefined}>
                          {loc}{" "}
                          <Link href={teamHref(sportKey, opp)} className="hover:text-court-400">{opp}</Link>
                          {g.home_score != null && g.away_score != null && (
                            <span className="ml-2 font-mono text-sm text-zinc-400">{g.away_score}–{g.home_score}</span>
                          )}
                        </span>
                        {g.neutral && <NeutralChip />}
                      </div>
                      <div className="mt-0.5 font-mono text-[11px] text-zinc-500">
                        FV {g.model_spread == null ? "—" : spreadRef(fmtLine(g.model_spread), g.home, g.neutral)} · Price {g.vegas_spread == null ? "—" : spreadRef(fmtLine(g.vegas_spread), g.home, g.neutral)}
                        {g.model_ats_side ? ` · ATS ${atsSideLabel(g.model_ats_side, g.away, g.home, g.neutral)}` : ""}
                        {g.model_ou_side ? ` · ${g.model_ou_side}` : ""}
                      </div>
                    </div>
                    <div className="shrink-0 text-right text-xs">
                      <Result r={g.model_ats_result} label="ATS" />
                      <Result r={g.model_ou_result} label="O/U" />
                    </div>
                  </div>
                </li>
              );
            })}
          </ul>
        )}
      </section>
    </div>
  );
}

function Pctile({ label, value }: { label: string; value: number | null }) {
  const v = value;
  const tone = v == null ? "text-zinc-600" : v >= 70 ? "text-edge-400" : v <= 30 ? "text-fade-400" : "text-white";
  return (
    <div className="rounded-xl bg-ink-850 px-3 py-2.5">
      <div className="text-[10px] uppercase tracking-wider text-zinc-500">{label}</div>
      <div className={`mt-0.5 font-mono text-xl font-bold ${tone}`}>{v == null ? "—" : `${v.toFixed(0)}`}<span className="text-xs font-semibold text-zinc-500">%</span></div>
      {v != null && (
        <div className="mt-1.5 h-1.5 overflow-hidden rounded-full bg-ink-700">
          <div className={`h-full rounded-full ${v >= 70 ? "bg-edge-400" : v <= 30 ? "bg-fade-400" : "bg-zinc-400"}`} style={{ width: `${Math.max(0, Math.min(100, v))}%` }} />
        </div>
      )}
    </div>
  );
}

function Mini({ label, value, hint }: { label: string; value: string; hint?: string }) {
  return (
    <div className="rounded-xl bg-ink-850 px-3 py-2">
      <div className="text-[10px] uppercase tracking-wider text-zinc-500">{label}</div>
      <div className="font-mono text-lg font-bold">{value}</div>
      {hint && <div className="text-[10px] text-zinc-600">{hint}</div>}
    </div>
  );
}

function RecordCard({ title, rows }: { title: string; rows: { l: string; v: string }[] }) {
  return (
    <div className="rounded-xl border border-ink-700 bg-ink-850 p-3">
      <div className="mb-2 text-xs font-semibold uppercase tracking-wider text-zinc-500">{title}</div>
      <div className="space-y-1.5">
        {rows.map((x) => (
          <div key={x.l} className="flex items-center justify-between text-sm">
            <span className="text-zinc-400">{x.l}</span>
            <span className="font-mono font-semibold">{x.v}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

function Result({ r, label }: { r: string | null; label: string }) {
  if (!r) return <div className="text-zinc-600">{label} —</div>;
  const up = r.toLowerCase();
  const cls = up.startsWith("w") ? "text-edge-400" : up.startsWith("l") ? "text-fade-400" : "text-zinc-400";
  return <div className={`font-mono font-semibold ${cls}`}>{label} {r}</div>;
}
