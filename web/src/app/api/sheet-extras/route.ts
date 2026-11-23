/** Anon-key extras for the game sheet: profiles, our rankings, recent finals
 *  from team_game_log, and official-pick results only. Never uses the service-role key. */
import { createClient } from "@supabase/supabase-js";
import { NextResponse } from "next/server";
import { SUPABASE_ANON_KEY, SUPABASE_URL, supabaseConfigured } from "@/lib/supabase/env";
import { DEFAULT_SPORT, getSport } from "@/lib/sports";
import { parseTagList, visibleTags } from "@/lib/archetypes";
import { TEAM_RANKING_COLUMNS, parseTeamRanking } from "@/lib/teamPage";
import type { OfficialPickResult, SheetRecentGame, SheetTeamExtras } from "@/lib/sheetExtras";
import { officialPicksFromLedger } from "@/lib/sheetExtras";

export const dynamic = "force-dynamic";

const RECENT_N = 5;

type Row = Record<string, unknown>;
const num = (v: unknown): number | null => (v == null || v === "" ? null : Number.isFinite(Number(v)) ? Number(v) : null);
const str = (v: unknown): string => (v == null ? "" : String(v));

function nightsBetween(earlier: string, later: string): number | null {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(earlier) || !/^\d{4}-\d{2}-\d{2}$/.test(later)) return null;
  const a = Date.parse(earlier + "T12:00:00Z");
  const b = Date.parse(later + "T12:00:00Z");
  if (!Number.isFinite(a) || !Number.isFinite(b) || b <= a) return null;
  return Math.round((b - a) / 86_400_000) - 1;
}

export async function GET(req: Request) {
  const url = new URL(req.url);
  const sport = getSport(url.searchParams.get("sport") ?? DEFAULT_SPORT);
  const awayName = (url.searchParams.get("away") ?? "").trim();
  const homeName = (url.searchParams.get("home") ?? "").trim();
  const gameDate = (url.searchParams.get("date") ?? "").trim();
  if (!sport || sport.status !== "live") return NextResponse.json({ error: "sport" }, { status: 404 });
  if (!awayName || !homeName) return NextResponse.json({ error: "teams" }, { status: 400 });
  if (gameDate && !/^\d{4}-\d{2}-\d{2}$/.test(gameDate)) return NextResponse.json({ error: "date" }, { status: 400 });
  if (!supabaseConfigured) return NextResponse.json({ error: "unconfigured" }, { status: 503 });

  const sb = createClient(SUPABASE_URL, SUPABASE_ANON_KEY, { auth: { persistSession: false, autoRefreshToken: false } });

  const [awayT, homeT] = await Promise.all([
    sb.from("teams").select("id,name").eq("league", sport.league).eq("name", awayName).limit(1),
    sb.from("teams").select("id,name").eq("league", sport.league).eq("name", homeName).limit(1),
  ]);
  if (awayT.error) return NextResponse.json({ error: "teams" }, { status: 502 });
  if (homeT.error) return NextResponse.json({ error: "teams" }, { status: 502 });

  // Fall back to the profile view if teams missed a name.
  let awayId = awayT.data?.[0] ? Number(awayT.data[0].id) : null;
  let homeId = homeT.data?.[0] ? Number(homeT.data[0].id) : null;
  let awayResolved = awayT.data?.[0] ? str(awayT.data[0].name) : awayName;
  let homeResolved = homeT.data?.[0] ? str(homeT.data[0].name) : homeName;
  if (awayId == null || homeId == null) {
    const p = await sb.from("team_profile_latest").select("team_id,team_name").eq("league", sport.league).limit(500);
    if (!p.error) {
      if (awayId == null) {
        const hit = (p.data ?? []).find((r) => str(r.team_name) === awayName);
        if (hit) { awayId = Number(hit.team_id); awayResolved = str(hit.team_name); }
      }
      if (homeId == null) {
        const hit = (p.data ?? []).find((r) => str(r.team_name) === homeName);
        if (hit) { homeId = Number(hit.team_id); homeResolved = str(hit.team_name); }
      }
    }
  }
  const ids = [awayId, homeId].filter((n): n is number => n != null);
  if (ids.length === 0) return NextResponse.json({ away: null, home: null });

  const dateFilter = gameDate || "9999-12-31";
  // One query per team, generous cap: a combined 80-row cap used to truncate later in the season.
  const logQueries = ids.map((id) =>
    sb.from("team_game_log")
      .select("team_id,game_id,game_date,tipoff,status,neutral,opponent,venue,team_score,opp_score,vs_di")
      .eq("team_id", id)
      .eq("league", sport.league)
      .eq("status", "final")
      .lt("game_date", dateFilter)
      .order("game_date", { ascending: false })
      .order("tipoff", { ascending: false })
      .limit(400),
  );
  const [profiles, records, ranks, ...logResults] = await Promise.all([
    sb.from("team_profile_latest").select("team_id,team_name,games_played,tentative,adj_oe,adj_de,tempo,tempo_pctile,efg_pctile,d_efg_pctile,to_pct_pctile,d_to_pct_pctile,or_pct_pctile,opp_or_pct_pctile,ftr_pctile,d_ftr_pctile,tags,tags_all").in("team_id", ids),
    sb.from("team_model_record").select("team_id,official_wins,official_losses,official_pushes,official_ats_wins,official_ats_losses,official_ou_wins,official_ou_losses").in("team_id", ids),
    Promise.all(ids.map((id) => sb.from("ranking_snapshots").select(TEAM_RANKING_COLUMNS).eq("team_id", id).eq("league", sport.league).order("week_of", { ascending: false }).limit(1))),
    ...logQueries,
  ]);

  if (profiles.error) console.error(`[supabase] team_profile_latest: ${profiles.error.message}`);
  if (records.error) console.error(`[supabase] team_model_record: ${records.error.message}`);

  const rankById = new Map<number, ReturnType<typeof parseTeamRanking>>();
  ranks.forEach((r, i) => {
    if (r.error) console.error(`[supabase] ranking_snapshots: ${r.error.message}`);
    const row = r.data?.[0] as Row | undefined;
    rankById.set(ids[i], parseTeamRanking(row ?? null));
  });

  const logRows: Row[] = [];
  for (const r of logResults) {
    if (r.error) console.error(`[supabase] team_game_log: ${r.error.message}`);
    else logRows.push(...((r.data ?? []) as Row[]));
  }
  const gameIds = [...new Set(logRows.map((g) => Number(g.game_id)).filter(Number.isFinite))];
  const officialByGame = new Map<number, OfficialPickResult[]>();
  if (gameIds.length) {
    // Query official rows only; officialPicksFromLedger is the same guard the UI relies on.
    const led = await sb.from("ledger").select("game_id,pick,result,tier").eq("sport", sport.key).eq("tier", "official").in("game_id", gameIds);
    if (led.error) console.error(`[supabase] ledger: ${led.error.message}`);
    else {
      const byGame = new Map<number, { tier?: string | null; pick?: string | null; result?: string | null }[]>();
      for (const row of led.data ?? []) {
        const gid = Number(row.game_id);
        const list = byGame.get(gid) ?? [];
        list.push({ tier: row.tier ? str(row.tier) : null, pick: row.pick ? str(row.pick) : null, result: row.result ? str(row.result) : null });
        byGame.set(gid, list);
      }
      for (const [gid, rows] of byGame) officialByGame.set(gid, officialPicksFromLedger(rows));
    }
  }

  function build(teamId: number | null, name: string): SheetTeamExtras | null {
    if (teamId == null) return null;
    const p = (profiles.data ?? []).find((r) => Number(r.team_id) === teamId) as Row | undefined;
    const rec = (records.data ?? []).find((r) => Number(r.team_id) === teamId) as Row | undefined;
    const ranking = rankById.get(teamId) ?? null;
    const mine = logRows
      .filter((g) => Number(g.team_id) === teamId)
      .sort((a, b) => {
        const da = str(a.game_date), db = str(b.game_date);
        if (da !== db) return db.localeCompare(da);
        return str(b.tipoff).localeCompare(str(a.tipoff));
      });
    let log_wins = 0, log_losses = 0, di_wins = 0, di_losses = 0;
    const recents: SheetRecentGame[] = [];
    let lastFinal: string | null = null;
    for (const g of mine) {
      const vsDi = g.vs_di === true;
      const neutral = g.neutral === true;
      const venue = str(g.venue);
      const opponent = str(g.opponent);
      const team_score = num(g.team_score);
      const opp_score = num(g.opp_score);
      const hasScore = team_score != null && opp_score != null;
      let result: SheetRecentGame["result"] = null;
      if (hasScore) {
        result = team_score > opp_score ? "W" : team_score < opp_score ? "L" : "T";
        if (result === "W") {
          log_wins += 1;
          if (vsDi) di_wins += 1;
        } else if (result === "L") {
          log_losses += 1;
          if (vsDi) di_losses += 1;
        }
      }
      const date = str(g.game_date);
      if (!lastFinal) lastFinal = date;
      const gid = num(g.game_id);
      if (recents.length < RECENT_N) {
        recents.push({
          game_id: gid,
          date,
          opponent,
          location: neutral ? "neutral" : venue === "home" ? "home" : "away",
          team_score,
          opp_score,
          result,
          vs_di: vsDi,
          // Non-D-I games never show a CZarp chip, even if a ledger row existed.
          official: vsDi && gid != null ? (officialByGame.get(gid) ?? []) : [],
        });
      }
    }
    const tags = visibleTags(parseTagList(p?.tags_all ?? p?.tags ?? [], p?.tentative === true));
    return {
      team_id: teamId,
      name,
      games_played: num(p?.games_played),
      tentative: p?.tentative === true,
      adj_oe: num(p?.adj_oe),
      adj_de: num(p?.adj_de),
      tempo: num(p?.tempo),
      tempo_pctile: num(p?.tempo_pctile),
      efg_pctile: num(p?.efg_pctile),
      d_efg_pctile: num(p?.d_efg_pctile),
      to_pct_pctile: num(p?.to_pct_pctile),
      d_to_pct_pctile: num(p?.d_to_pct_pctile),
      or_pct_pctile: num(p?.or_pct_pctile),
      opp_or_pct_pctile: num(p?.opp_or_pct_pctile),
      ftr_pctile: num(p?.ftr_pctile),
      d_ftr_pctile: num(p?.d_ftr_pctile),
      tags,
      rank: ranking?.rank ?? null,
      prev_rank: ranking?.prev_rank ?? null,
      rating: ranking?.rating ?? null,
      di_wins,
      di_losses,
      rank_week_of: ranking?.week_of ?? null,
      log_wins,
      log_losses,
      log_games: mine.length,
      official_wins: num(rec?.official_wins) ?? 0,
      official_losses: num(rec?.official_losses) ?? 0,
      official_pushes: num(rec?.official_pushes) ?? 0,
      rest_days: lastFinal && gameDate ? nightsBetween(lastFinal, gameDate) : null,
      recent: recents,
    };
  }

  return NextResponse.json({
    away: build(awayId, awayResolved),
    home: build(homeId, homeResolved),
  });
}
