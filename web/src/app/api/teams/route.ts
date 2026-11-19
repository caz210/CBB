// Team search index: every team in the `teams` table for a sport, plus each team's next (or
// today's) scheduled game from `slate_games`. Anon key only, no cookies, so it can be cached.
// The browser fetches this once and keeps it (see components/TeamSearch.tsx).
import { createClient } from "@supabase/supabase-js";
import { NextResponse } from "next/server";
import { SUPABASE_ANON_KEY, SUPABASE_URL, supabaseConfigured } from "@/lib/supabase/env";
import { DEFAULT_SPORT, getSport } from "@/lib/sports";
import { teamSlug } from "@/lib/teams";
import { chicagoDate, isUpcoming, type SearchTeam, type TeamNextGame } from "@/lib/teamSearch";

// Short cache: the browser re-filters by clock too, but keep the CDN copy fresh.
export const revalidate = 60;

const CACHE = "public, s-maxage=60, stale-while-revalidate=60";

export async function GET(req: Request) {
  const sport = getSport(new URL(req.url).searchParams.get("sport") ?? DEFAULT_SPORT);
  if (!sport || sport.status !== "live") return NextResponse.json({ teams: [] }, { headers: { "Cache-Control": CACHE } });
  if (!supabaseConfigured) return NextResponse.json({ error: "unconfigured" }, { status: 503 });

  const sb = createClient(SUPABASE_URL, SUPABASE_ANON_KEY, { auth: { persistSession: false, autoRefreshToken: false } });
  // team_aliases is anon-readable (1,674 rows, already normalised). PostgREST caps a page at 1,000 rows, so read two ranges.
  const aliasPage = (from: number, to: number) => sb.from("team_aliases").select("team_id,alias").order("alias").order("team_id").range(from, to);
  // neutral-site flags (slate_games has no such column): read in the same Promise.all (the whole season has only ~70 neutral games), so a failure only costs the "Neutral" tag
  const [t, g, a1, a2, nr] = await Promise.all([
    sb.from("teams").select("id,name").eq("league", sport.league).order("name").limit(2000),
    sb.from("slate_games").select("game_id,game_date,home,away,tipoff,status,final_at").eq("sport", sport.key)
      .gte("game_date", chicagoDate(new Date(Date.now() - 24 * 3600 * 1000))).order("game_date", { ascending: true }).order("tipoff", { ascending: true, nullsFirst: false }).limit(2000),
    aliasPage(0, 999),
    aliasPage(1000, 1999),
    // never rejects: any failure (including the timeout) becomes an error value, so it cannot take the route down
    Promise.resolve(sb.from("games").select("id").eq("neutral", true).limit(2000).abortSignal(AbortSignal.timeout(2500)))
      .catch((e: unknown) => ({ data: null, error: { code: "abort", message: String(e) } })),
  ]);
  if (t.error) {
    console.error(`[supabase] teams: ${t.error.code ?? ""} ${t.error.message}`);
    return NextResponse.json({ error: "teams" }, { status: 502 });
  }
  // A missing schedule only hides the "Next game" buttons; names still search.
  if (g.error) console.error(`[supabase] slate_games: ${g.error.code ?? ""} ${g.error.message}`);

  const now = new Date();
  // If either alias page fails, send no DB aliases at all: the client then uses the local alias map.
  const aliasRows = a1.error || a2.error ? null : [...(a1.data ?? []), ...(a2.data ?? [])];
  if (!aliasRows) console.error(`[supabase] team_aliases: ${(a1.error ?? a2.error)?.message}`);
  const aliasesById = new Map<number, string[]>();
  for (const r of aliasRows ?? []) {
    const id = Number(r.team_id);
    const list = aliasesById.get(id) ?? [];
    list.push(String(r.alias));
    aliasesById.set(id, list);
  }

  const neutral = new Set<number>();
  if (nr.error) console.error(`[supabase] games.neutral: ${nr.error.code ?? ""} ${nr.error.message}`);
  else for (const x of nr.data ?? []) neutral.add(Number(x.id));

  const next = new Map<string, TeamNextGame>();
  for (const r of g.error ? [] : g.data ?? []) {
    // Only games that can still be watched. `status` flips to final in the morning run, so also drop anything
    // with a final_at stamp or a tip time more than ~3h ago (yesterday's rows are read so a late tip survives midnight) (see isUpcoming).
    if (["final", "postponed", "canceled"].includes(String(r.status)) || r.final_at) continue;
    const base = { gameId: Number(r.game_id), date: String(r.game_date), tipoff: r.tipoff ? String(r.tipoff) : null, neutral: neutral.has(Number(r.game_id)) };
    if (!isUpcoming(base, now)) continue;
    const home = String(r.home), away = String(r.away);
    if (!next.has(home)) next.set(home, { ...base, opponent: away, home: true });
    if (!next.has(away)) next.set(away, { ...base, opponent: home, home: false });
  }
  const teams: SearchTeam[] = (t.data ?? []).map((r) => {
    const name = String(r.name);
    const team: SearchTeam = { id: Number(r.id), name, slug: teamSlug(name), next: next.get(name) ?? null };
    if (aliasRows) team.aliases = aliasesById.get(team.id) ?? [];
    return team;
  });
  return NextResponse.json({ teams }, { headers: { "Cache-Control": CACHE } });
}
