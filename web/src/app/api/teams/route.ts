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
  const [t, g] = await Promise.all([
    sb.from("teams").select("id,name").eq("league", sport.league).order("name").limit(2000),
    sb.from("slate_games").select("game_id,game_date,home,away,tipoff,status,final_at").eq("sport", sport.key)
      .gte("game_date", chicagoDate()).order("game_date", { ascending: true }).order("tipoff", { ascending: true, nullsFirst: false }).limit(2000),
  ]);
  if (t.error) {
    console.error(`[supabase] teams: ${t.error.code ?? ""} ${t.error.message}`);
    return NextResponse.json({ error: "teams" }, { status: 502 });
  }
  // A missing schedule only hides the "Next game" buttons; names still search.
  if (g.error) console.error(`[supabase] slate_games: ${g.error.code ?? ""} ${g.error.message}`);

  const now = new Date();
  const next = new Map<string, TeamNextGame>();
  for (const r of g.error ? [] : g.data ?? []) {
    // Only games that can still be watched. `status` flips to final in the morning run, so also drop anything
    // with a final_at stamp, a past date, or a tip time more than ~3h ago (see isUpcoming).
    if (["final", "postponed", "canceled"].includes(String(r.status)) || r.final_at) continue;
    const base = { gameId: Number(r.game_id), date: String(r.game_date), tipoff: r.tipoff ? String(r.tipoff) : null };
    if (!isUpcoming(base, now)) continue;
    const home = String(r.home), away = String(r.away);
    if (!next.has(home)) next.set(home, { ...base, opponent: away, home: true });
    if (!next.has(away)) next.set(away, { ...base, opponent: home, home: false });
  }
  const teams: SearchTeam[] = (t.data ?? []).map((r) => {
    const name = String(r.name);
    return { id: Number(r.id), name, slug: teamSlug(name), next: next.get(name) ?? null };
  });
  return NextResponse.json({ teams }, { headers: { "Cache-Control": CACHE } });
}
