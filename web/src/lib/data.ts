// Live data loaders for Server Components. Reads CZarp's Supabase views with the anon key
// (all picks are public; session cookies only matter for user-owned data).
// Every loader returns a tagged result so pages can render a clear empty state instead of
// silently falling back to example numbers.
import { createSupabaseServerClient } from "./supabase/server";
import type { Sport } from "./sports";
import type { Archetypes, GameInsight, LedgerRow, SlateGame, TierRecord, ValueBet } from "./types";
import { portfolioRisk, computedLineEdge, type PortfolioRisk } from "./portfolio";
import { parseTagList, visibleCopy, visibleNotes, visibleTags, type ArchetypeTag } from "./archetypes";
import { teamSlug, type ModelGrade, type TeamModelRecord, type TeamProfile } from "./teams";
import {
  RANKING_SNAPSHOT_COLUMNS,
  rankingsErrorKind,
  sortRankings,
  withMovement,
  type RankingRow,
  type RankingsData,
} from "./rankings";
import {
  TEAM_RANKING_COLUMNS,
  TEAM_RANKING_TIMEOUT_MS,
  parseTeamRanking,
  type TeamRankingSnapshot,
} from "./teamPage";
import { isPickTier, type Market, type MarketSource, type PickTier } from "./labels";
import { wilsonCi, SMALL_SAMPLE_N } from "./stats";
import { newCoachLock, type NewCoachLock } from "./coach";
import { parseVerdict } from "./verdict";
import { newestByGame, pickLinesAsOf } from "./linesAsOf";
import { parseMarketPrices } from "./kelly";
import { applyOfficialRecord, ledgerToOfficialRecord } from "./officialPick";

export type Loaded<T> =
  | { state: "live"; data: T }
  | { state: "unconfigured" }
  | { state: "error"; reason?: "unreachable" | "unavailable" };

type Row = Record<string, unknown>;

const num = (v: unknown): number | null => (v == null || v === "" ? null : Number.isFinite(Number(v)) ? Number(v) : null);
const str = (v: unknown): string => (v == null ? "" : String(v));
const strs = (v: unknown): string[] => (Array.isArray(v) ? v.filter((x) => typeof x === "string") : []);
const strArr = (v: unknown): string[] | null => (v == null ? null : Array.isArray(v) ? v.map((x) => String(x)).filter(Boolean) : null);
const textOrNull = (v: unknown): string | null => (typeof v === "string" && v.trim() ? v.trim() : null);

/** YYYY-MM-DD in America/Chicago (game dates are stored as CT local dates). */
export function todayCT(now = new Date()) {
  return new Intl.DateTimeFormat("en-CA", { timeZone: "America/Chicago", year: "numeric", month: "2-digit", day: "2-digit" }).format(now);
}
export function shiftDate(d: string, days: number) {
  const t = new Date(d + "T12:00:00Z");
  t.setUTCDate(t.getUTCDate() + days);
  return t.toISOString().slice(0, 10);
}
export const isIsoDate = (d: unknown): d is string => typeof d === "string" && /^\d{4}-\d{2}-\d{2}$/.test(d) && !Number.isNaN(Date.parse(d));

function toArchetypes(v: unknown): Archetypes {
  const a = (v && typeof v === "object" ? v : {}) as Row;
  const homeTent = Boolean(a.home_tentative);
  const awayTent = Boolean(a.away_tentative);
  const home = parseTagList(a.home, homeTent);
  const away = parseTagList(a.away, awayTent);
  return {
    home,
    away,
    homeTop: home,
    awayTop: away,
    home_tentative: homeTent,
    away_tentative: awayTent,
    tentative: Boolean(a.tentative),
    // Hidden-tag names (Soft Glass, Open Arc) never leave the server.
    notes: visibleNotes(strs(a.notes)),
  };
}

function toSlateGame(r: Row): SlateGame {
  return {
    sport: str(r.sport) as SlateGame["sport"],
    game_date: str(r.game_date),
    game_id: Number(r.game_id),
    home: str(r.home),
    away: str(r.away),
    tipoff: r.tipoff ? str(r.tipoff) : null,
    game: str(r.game) || `${str(r.away)} @ ${str(r.home)}`,
    neutral: r.neutral === true,
    // present on select('*'); a missing column just leaves this null — the slate still loads
    tv_rating: num(r.tv_rating),
    proj_home: num(r.proj_home),
    proj_away: num(r.proj_away),
    proj_spread: num(r.proj_spread),
    proj_total: num(r.proj_total),
    home_win_prob: num(r.home_win_prob),
    home_cover_prob: num(r.home_cover_prob),
    archetypes: toArchetypes(r.archetypes),
    market_spread: num(r.market_spread),
    market_total: num(r.market_total),
    market_source: (r.market_source ? str(r.market_source).toLowerCase() : null) as MarketSource | null,
    reason: visibleCopy(typeof r.reason === "string" ? r.reason : "") || null,
    proj_possessions: num(r.proj_possessions),
    upset_alert: r.upset_alert === true,
    upset_reason: visibleCopy(typeof r.upset_reason === "string" ? r.upset_reason : "") || null,
    status: r.status ? str(r.status) : null,
    final_at: r.final_at ? str(r.final_at) : null,
    final_at_estimated: r.final_at_estimated === true,
    skip_reasons_spread: strArr(r.skip_reasons_spread),
    skip_reasons_total: strArr(r.skip_reasons_total),
    skip_reason_spread: textOrNull(r.skip_reason_spread),
    skip_reason_total: textOrNull(r.skip_reason_total),
    // Stored verdict/edge/lean columns: present once live, null on older rows. Never computed here.
    edge_now_pct_spread: num(r.edge_now_pct_spread),
    edge_vs_open_pct_spread: num(r.edge_vs_open_pct_spread),
    edge_now_pct_total: num(r.edge_now_pct_total),
    edge_vs_open_pct_total: num(r.edge_vs_open_pct_total),
    verdict_spread: parseVerdict(r.verdict_spread),
    verdict_total: parseVerdict(r.verdict_total),
    lean_spread: r.lean_spread == null ? null : r.lean_spread === true,
    lean_total: r.lean_total == null ? null : r.lean_total === true,
    // present once the pipeline adds it; absent today, which just leaves this null
    market_fetched_at: typeof r.market_fetched_at === "string" ? r.market_fetched_at : null,
    edges_as_of: typeof r.edges_as_of === "string" ? r.edges_as_of : null,
  };
}

function toValueBet(r: Row): ValueBet {
  const fair = num(r.fair_at_post);
  const line = num(r.line_at_post) ?? num(r.line_or_price) ?? num(r.posted_line);
  const edge = num(r.edge_at_post_pct) ?? num(r.edge) ?? num(r.line_edge_pct);
  const postedAt = r.posted_at ? str(r.posted_at) : r.logged_at ? str(r.logged_at) : null;
  return {
    sport: str(r.sport) as ValueBet["sport"],
    game_id: Number(r.game_id),
    market: str(r.market) as Market,
    side: str(r.side || r.bet),
    model_prob: num(r.model_prob),
    market_prob: num(r.market_prob) ?? 0,
    prob_edge: num(r.prob_edge) ?? 0,
    line_edge_pct: edge ?? 0,
    kelly_quarter_units: num(r.kelly_quarter_units),
    pick_tier: isPickTier(r.tier) ? r.tier : "official",
    logged_at: postedAt,
    posted_line: line,
    posted_odds: num(r.price) ?? num(r.posted_odds),
    pick_hash: r.pick_hash ? str(r.pick_hash) : null,
    pick_id: r.pick_id ? str(r.pick_id) : null,
    fair_at_post: fair,
    line_at_post: line,
    total_at_post: num(r.total_at_post),
    open_line: num(r.open_line),
    edge_at_post_pct: edge,
    edge_vs_open_pct: num(r.edge_vs_open_pct),
    posted_at: postedAt,
  };
}

function applyLocked(v: ValueBet, r: Row) {
  if (r.pick_id) v.pick_id = str(r.pick_id);
  v.fair_at_post = num(r.fair_at_post);
  v.line_at_post = num(r.line_at_post) ?? v.line_at_post;
  v.total_at_post = num(r.total_at_post);
  v.open_line = num(r.open_line);
  v.edge_at_post_pct = num(r.edge_at_post_pct) ?? v.edge_at_post_pct;
  v.edge_vs_open_pct = num(r.edge_vs_open_pct);
  if (num(r.line_at_post) != null) v.posted_line = num(r.line_at_post);
  if (num(r.price) != null) v.posted_odds = num(r.price);
  if (r.posted_at) {
    v.posted_at = str(r.posted_at);
    if (!v.logged_at) v.logged_at = str(r.posted_at);
  }
  if (isPickTier(r.tier)) v.pick_tier = r.tier;
  if (num(r.edge_at_post_pct) != null) v.line_edge_pct = num(r.edge_at_post_pct)!;
}

function lockedMatches(r: Row, v: ValueBet) {
  if (r.pick_id && v.pick_id && str(r.pick_id) === v.pick_id) return true;
  if (v.game_id !== Number(r.game_id) || v.market !== str(r.market)) return false;
  const bet = str(r.bet), side = str(r.side);
  return v.side === bet || v.side === side || (side && v.side.startsWith(side + " "));
}

async function mergeOfficialLocked(sb: SB, sport: Sport, ids: number[], all: ValueBet[]) {
  const lk = await sb.from("official_picks_locked").select("*").in("game_id", ids);
  if (lk.error) {
    console.error(`[supabase] official_picks_locked: ${lk.error.code ?? ""} ${lk.error.message}`);
    return all;
  }
  for (const r of (lk.data ?? []) as Row[]) {
    const hit = all.find((v) => lockedMatches(r, v));
    if (hit) applyLocked(hit, r);
    else all.push(toValueBet({ ...r, sport: sport.key, side: r.bet || r.side }));
  }
  return all;
}

/** Official ledger stake/result/P/L onto official ValueBets. Tracked rows are ignored.
 *  A failed read leaves bets unchanged so the slate/hero/team page still render. */
async function attachOfficialRecord(sb: SB, sport: Sport, ids: number[], bets: ValueBet[]): Promise<ValueBet[]> {
  if (!ids.length || !bets.length) return bets;
  const q = await sb.from("ledger").select("game_id,pick,stake_units,result,profit_units,clv,logged_at,tier,pick_hash").eq("sport", sport.key).eq("tier", "official").in("game_id", ids);
  if (q.error) {
    console.error(`[supabase] ledger official record: ${q.error.message}`);
    return bets;
  }
  const rows = [];
  for (const r of (q.data ?? []) as Row[]) {
    const rec = ledgerToOfficialRecord({
      game_id: Number(r.game_id),
      pick: r.pick != null ? str(r.pick) : "",
      stake_units: num(r.stake_units),
      result: (() => { const x = str(r.result); return x === "W" || x === "L" || x === "P" ? x : null; })(),
      profit_units: num(r.profit_units),
      clv: num(r.clv),
      logged_at: r.logged_at ? str(r.logged_at) : null,
      pick_hash: r.pick_hash ? str(r.pick_hash) : null,
      pick_tier: isPickTier(r.tier) ? r.tier : "official",
    });
    if (rec) rows.push(rec);
  }
  return applyOfficialRecord(bets, rows);
}

function toLedgerRow(r: Row): LedgerRow {
  const res = str(r.result);
  return {
    sport: str(r.sport) as LedgerRow["sport"],
    game_date: str(r.game_date),
    game_id: Number(r.game_id),
    pick: str(r.pick),
    line: num(r.line),
    odds: num(r.odds),
    stake_units: num(r.stake_units),
    result: res === "W" || res === "L" || res === "P" ? res : null,
    profit_units: num(r.profit_units),
    is_paper: r.is_paper !== false,
    pick_tier: isPickTier(r.tier) ? r.tier : "official",
    flat_units: num(r.flat_units),
    clv: num(r.clv),
    created_at: r.created_at ? str(r.created_at) : null,
    logged_at: r.logged_at ? str(r.logged_at) : null,
    source: str(r.source),
    pick_hash: r.pick_hash ? str(r.pick_hash) : null,
  };
}

// ---- /[sport]/slate ---------------------------------------------------------
export type SlateData = {
  date: string;
  games: SlateGame[];
  /** official plays (tier = 'official') */
  bets: ValueBet[];
  /** paper-only 1u tracking picks (tier = 'track'), never bet */
  tracked: ValueBet[];
  /** nearest dates with games, offered when the requested date is empty */
  prevDate: string | null;
  nextDate: string | null;
  /** Newest games.updated_at / projection / line timestamp — used for live/in-progress freshness only. */
  updatedAt: string | null;
  /** Every game is completed (final or graded). */
  finished: boolean;
  /** Slate team name → team_aliases spellings (normalised), so the slate search matches aliases on first paint.
   *  null when the read failed; the board then falls back to the /api/teams index. */
  aliases?: Record<string, string[]> | null;
  /** Empty slate only: does the games table list anything for this date? null = not checked / read failed. */
  hasScheduledGames?: boolean | null;
  /** max(final_at) on a finished slate; null if none of the games have a stamp. */
  finalAt: string | null;
  finalAtEstimated: boolean;
};

export async function loadSlate(sport: Sport, date: string): Promise<Loaded<SlateData>> {
  const sb = await createSupabaseServerClient();
  if (!sb) return { state: "unconfigured" };
  return loadSlateFrom(sb, sport, date);
}

/** The slate loader on an injected client (loadSlate wraps it), so tests can simulate failing reads. */
export async function loadSlateFrom(sb: SB, sport: Sport, date: string): Promise<Loaded<SlateData>> {

  const g = await sb.from("slate_games").select("*").eq("sport", sport.key).eq("game_date", date)
    .order("tipoff", { ascending: true, nullsFirst: false }).order("game_id");
  if (g.error) return fail("slate_games", g.error);
  const games = (g.data ?? []).map(toSlateGame);

  let bets: ValueBet[] = [];
  let tracked: ValueBet[] = [];
  let prevDate: string | null = null;
  let nextDate: string | null = null;
  let updatedAt: string | null = null;
  let finished = false;
  let finalAt: string | null = null;
  let finalAtEstimated = false;
  let hasScheduledGames: boolean | null = null;
  let aliases: Record<string, string[]> | null = null;
  if (games.length) {
    const b = await sb.from("value_bets").select("*").eq("sport", sport.key).in("game_id", games.map((x) => x.game_id));
    if (b.error) return fail("value_bets", b.error);
    const ids = games.map((x) => x.game_id);
    const all = await attachOfficialRecord(sb, sport, ids, await mergeOfficialLocked(sb, sport, ids, (b.data ?? []).map(toValueBet)));
    all.sort((a, z) => z.line_edge_pct - a.line_edge_pct);
    bets = all.filter((x) => x.pick_tier === "official");
    // neutralIds() is its own read in the same Promise.all: if it fails, only titles fall back to "@".
    // The status query below is unchanged, so finals, scores and completed never depend on it.
    const [st, gr, pr, ml, neutral] = await Promise.all([
      sb.from("games").select("id,status,updated_at,final_at,final_at_estimated,home,away,home_team_id,away_team_id,home_score,away_score,external_id").in("id", ids),
      sb.from("ledger").select("game_id,result").in("game_id", ids).not("result", "is", null),
      sb.from("projections").select("game_id,model_version,spread,total,over_prob,created_at").in("game_id", ids).order("created_at", { ascending: false }),
      sb.from("market_lines").select("game_id,fetched_at").in("game_id", ids).eq("source", "vegas"),
      neutralIds(sb, ids),
    ]);
    for (const x of games) x.neutral = neutral.has(x.game_id);
    const done = new Set<number>();
    for (const r of st.data ?? []) if (str(r.status) === "final") done.add(Number(r.id));
    for (const r of gr.data ?? []) done.add(Number(r.game_id));
    const byId = new Map((st.data ?? []).map((r) => [Number(r.id), r as Row]));
    for (const x of games) {
      x.completed = done.has(x.game_id);
      const row = byId.get(x.game_id);
      if (!row) continue;
      if (row.status) x.status = str(row.status);
      if (row.final_at) x.final_at = str(row.final_at);
      if (row.final_at_estimated === true) x.final_at_estimated = true;
      x.home_score = num(row.home_score);
      x.away_score = num(row.away_score);
      x.external_id = row.external_id != null && str(row.external_id) ? str(row.external_id) : null;
    }
    finished = games.every((x) => x.completed);
    if (finished) {
      let bestMs = -Infinity;
      for (const x of games) {
        const ms = x.final_at ? Date.parse(x.final_at) : NaN;
        if (!Number.isFinite(ms) || ms < bestMs) continue;
        bestMs = ms;
        finalAt = x.final_at ?? null;
        finalAtEstimated = x.final_at_estimated === true;
      }
    } else {
      const stamps: number[] = [];
      const bump = (v: unknown) => { const t = v ? Date.parse(str(v)) : NaN; if (Number.isFinite(t)) stamps.push(t); };
      for (const r of st.data ?? []) bump(r.updated_at);
      for (const r of pr.data ?? []) bump(r.created_at);
      for (const r of ml.data ?? []) bump(r.fetched_at);
      if (stamps.length) updatedAt = new Date(Math.max(...stamps)).toISOString();
    }
    attachRawEdges(games, (pr.data ?? []) as Row[]);
    await attachLinesAsOf(sb, games);
    await attachMarketPrices(sb, games);
    // Pause chips read the team's CURRENT profile, so only show them on today's and future slates.
    // Past slates keep the reason that was stored with that day's projection instead.
    if (date >= todayCT()) await attachNewCoaches(sb, games, st.data ?? []);
    await attachTeamTags(sb, games, st.data ?? []);
    await attachEspnTeamIds(sb, games, (st.data ?? []) as Row[]);
    aliases = await loadSlateAliases(sb, games, (st.data ?? []) as Row[]);
    const ins = await loadInsights(sb, ids);
    for (const x of games) {
      x.insight = ins.get(x.game_id);
      if (x.insight?.neutral) x.neutral = true;
    }
    tracked = all.filter((x) => x.pick_tier === "track");
  } else {
    const [n, p, sched] = await Promise.all([
      sb.from("slate_games").select("game_date").eq("sport", sport.key).gt("game_date", date).order("game_date").limit(1),
      sb.from("slate_games").select("game_date").eq("sport", sport.key).lt("game_date", date).order("game_date", { ascending: false }).limit(1),
      // Tells "no games today" apart from "today's slate hasn't posted yet". A failed read stays null.
      Promise.resolve(sb.from("games").select("id").eq("league", sport.league).eq("date", date).limit(1))
        .then((r) => (r.error ? null : (r.data ?? []).length > 0), () => null),
    ]);
    hasScheduledGames = sched;
    nextDate = n.data?.[0] ? str(n.data[0].game_date) : null;
    prevDate = p.data?.[0] ? str(p.data[0].game_date) : null;
  }
  return { state: "live", data: { date, games, bets, tracked, prevDate, nextDate, updatedAt, finished, finalAt, finalAtEstimated, hasScheduledGames, aliases } };
}


const V2 = "czarp-v2";
const V1_TOTALS = "czarp-v1-totals";

/** Latest projection row per game + model_version. */
function latestByModel(rows: Row[]) {
  const m = new Map<string, Row>();
  for (const r of rows) {
    const k = `${r.game_id}:${r.model_version}`;
    if (!m.has(k)) m.set(k, r);
  }
  return m;
}

/** Attach raw |fair − price| ÷ Vegas total for sides (v2) and totals (v1 and v2). */
function attachRawEdges(games: SlateGame[], projRows: Row[]) {
  const latest = latestByModel(projRows);
  for (const g of games) {
    const v2 = latest.get(`${g.game_id}:${V2}`);
    const v1 = latest.get(`${g.game_id}:${V1_TOTALS}`);
    const spread = num(v2?.spread) ?? g.proj_spread;
    const totalV2 = num(v2?.total) ?? g.proj_total;
    const totalV1 = num(v1?.total);
    // Model names and the second-look total stay on the server; only CZarp's official total ships.
    g.czarp_total = totalV1;
    g.czarp_over_prob = num(v1?.over_prob);
    const vegasT = g.market_total ?? totalV2 ?? totalV1;
    const spreadE = computedLineEdge(spread, g.market_spread, vegasT);
    const totV2E = computedLineEdge(totalV2, g.market_total, g.market_total ?? vegasT);
    const totV1E = computedLineEdge(totalV1, g.market_total, g.market_total ?? vegasT);
    const cands: { pct: number; market: "spread" | "total" }[] = [];
    if (spreadE != null) cands.push({ pct: spreadE, market: "spread" });
    if (totV1E != null) cands.push({ pct: totV1E, market: "total" });
    if (totV2E != null) cands.push({ pct: totV2E, market: "total" });
    const best = cands.sort((a, z) => z.pct - a.pct)[0];
    g.rawEdge = best?.pct ?? null;
    g.rawEdgeMarket = best?.market ?? null;
  }
}


async function attachNewCoaches(sb: SB, games: SlateGame[], gameRows: Row[]) {
  const idByGame = new Map<number, { home?: number | null; away?: number | null; homeName?: string; awayName?: string }>();
  for (const r of gameRows) {
    idByGame.set(Number(r.id), { home: num(r.home_team_id), away: num(r.away_team_id), homeName: str(r.home), awayName: str(r.away) });
  }
  const tids = new Set<number>();
  const names = new Set<string>();
  for (const g of games) {
    const ids = idByGame.get(g.game_id);
    if (ids?.home) tids.add(ids.home);
    else names.add(g.home);
    if (ids?.away) tids.add(ids.away);
    else names.add(g.away);
  }
  const profiles: Row[] = [];
  try {
    if (tids.size) {
      const q = await sb.from("team_profile_latest").select("team_id,team_name,new_coach,new_coach_games,games_played,lock_sides_games,lock_totals_games,lock_sides_remaining,lock_totals_remaining").in("team_id", [...tids]);
      if (!q.error) profiles.push(...((q.data ?? []) as Row[]));
    }
    if (names.size) {
      const q = await sb.from("team_profile_latest").select("team_id,team_name,new_coach,new_coach_games,games_played,lock_sides_games,lock_totals_games,lock_sides_remaining,lock_totals_remaining").in("team_name", [...names]);
      if (!q.error) profiles.push(...((q.data ?? []) as Row[]));
    }
  } catch { /* badges just stay hidden */ }
  const byId = new Map<number, Row>();
  const byName = new Map<string, Row>();
  for (const r of profiles) {
    byId.set(Number(r.team_id), r);
    byName.set(str(r.team_name).toLowerCase(), r);
  }
  const lockFor = (name: string, tid?: number | null): NewCoachLock | null => {
    const r = (tid != null ? byId.get(tid) : null) ?? byName.get(name.toLowerCase());
    if (!r) return null;
    return newCoachLock(str(r.team_name) || name, {
      new_coach: r.new_coach === true,
      new_coach_games: num(r.new_coach_games),
      games_played: num(r.games_played),
      lock_sides_games: num(r.lock_sides_games),
      lock_totals_games: num(r.lock_totals_games),
      lock_sides_remaining: num(r.lock_sides_remaining),
      lock_totals_remaining: num(r.lock_totals_remaining),
    });
  };
  for (const g of games) {
    const ids = idByGame.get(g.game_id);
    const locks = [lockFor(g.away, ids?.away), lockFor(g.home, ids?.home)].filter((x): x is NewCoachLock => x != null);
    g.coachLocks = locks;
  }
}


/** Replace each team's capped slate tags with every fired public tag from team_profile_latest.tags_all.
 *  The stored top-2 list is kept as homeTop/awayTop for the 320px compact card. Failures are non-fatal. */

/** teams.external_ids.espn for each slate side, when stored. Failures are non-fatal; name matching still works. */
async function attachEspnTeamIds(sb: SB, games: SlateGame[], gameRows: Row[]) {
  const idsByGame = new Map<number, { home: number | null; away: number | null }>();
  const teamIds = new Set<number>();
  for (const r of gameRows) {
    const home = num(r.home_team_id);
    const away = num(r.away_team_id);
    idsByGame.set(Number(r.id), { home, away });
    if (home) teamIds.add(home);
    if (away) teamIds.add(away);
  }
  if (!teamIds.size) return;
  try {
    const q = await sb.from("teams").select("id,external_ids").in("id", [...teamIds]).limit(500);
    if (q.error || !q.data) return;
    const espnByTeam = new Map<number, string>();
    for (const r of q.data as Row[]) {
      const ext = r.external_ids;
      const bag = ext && typeof ext === "object" && !Array.isArray(ext) ? ext as Record<string, unknown> : null;
      const raw = bag?.espn ?? bag?.ESPN;
      if (raw == null || raw === "") continue;
      const id = String(raw).trim();
      if (!id) continue;
      espnByTeam.set(Number(r.id), id);
    }
    for (const g of games) {
      const ids = idsByGame.get(g.game_id);
      if (!ids) continue;
      g.home_espn_id = ids.home != null ? espnByTeam.get(ids.home) ?? null : null;
      g.away_espn_id = ids.away != null ? espnByTeam.get(ids.away) ?? null : null;
    }
  } catch {
    /* leave ids unset */
  }
}

/** team_aliases for the teams on this slate, keyed by the slate's team name. Never throws; null on failure. */
async function loadSlateAliases(sb: SB, games: SlateGame[], gameRows: Row[]): Promise<Record<string, string[]> | null> {
  const nameById = new Map<number, Set<string>>();
  const add = (id: number | null, name: string) => {
    if (!id || !name) return;
    const set = nameById.get(id) ?? new Set<string>();
    set.add(name);
    nameById.set(id, set);
  };
  const rowById = new Map(gameRows.map((r) => [Number(r.id), r]));
  for (const g of games) {
    const r = rowById.get(g.game_id);
    if (!r) continue;
    add(num(r.home_team_id), g.home);
    add(num(r.away_team_id), g.away);
  }
  if (!nameById.size) return {};
  try {
    const q = await sb.from("team_aliases").select("team_id,alias").in("team_id", [...nameById.keys()]).limit(5000);
    if (q.error) return null;
    const out: Record<string, string[]> = {};
    for (const r of (q.data ?? []) as Row[]) {
      const alias = str(r.alias);
      if (!alias) continue;
      for (const name of nameById.get(Number(r.team_id)) ?? []) (out[name] ??= []).push(alias);
    }
    return out;
  } catch {
    return null;
  }
}

async function attachTeamTags(sb: SB, games: SlateGame[], gameRows: Row[]) {
  const idByGame = new Map<number, { home?: number | null; away?: number | null }>();
  for (const r of gameRows) {
    idByGame.set(Number(r.id), { home: num(r.home_team_id), away: num(r.away_team_id) });
  }
  const tids = new Set<number>();
  const names = new Set<string>();
  for (const g of games) {
    const ids = idByGame.get(g.game_id);
    if (ids?.home) tids.add(ids.home);
    else names.add(g.home);
    if (ids?.away) tids.add(ids.away);
    else names.add(g.away);
  }
  const profiles: Row[] = [];
  try {
    if (tids.size) {
      const q = await sb.from("team_profile_latest").select("team_id,team_name,tags,tags_all,tentative").in("team_id", [...tids]);
      if (!q.error) profiles.push(...((q.data ?? []) as Row[]));
    }
    if (names.size) {
      const q = await sb.from("team_profile_latest").select("team_id,team_name,tags,tags_all,tentative").in("team_name", [...names]);
      if (!q.error) profiles.push(...((q.data ?? []) as Row[]));
    }
  } catch { /* cards keep the stored (capped) tags */ }
  const byId = new Map<number, Row>();
  const byName = new Map<string, Row>();
  for (const r of profiles) {
    byId.set(Number(r.team_id), r);
    byName.set(str(r.team_name).toLowerCase(), r);
  }
  const apply = (g: SlateGame, side: "home" | "away", name: string, tid?: number | null) => {
    const r = (tid != null ? byId.get(tid) : null) ?? byName.get(name.toLowerCase());
    if (!r) return;
    const tent = r.tentative === true || (side === "home" ? g.archetypes.home_tentative : g.archetypes.away_tentative);
    const storedTop = visibleTags(parseTagList(r.tags ?? (side === "home" ? g.archetypes.homeTop ?? g.archetypes.home : g.archetypes.awayTop ?? g.archetypes.away), tent));
    const all = visibleTags(parseTagList(r.tags_all ?? r.tags ?? (side === "home" ? g.archetypes.home : g.archetypes.away), tent));
    if (side === "home") {
      if (storedTop.length) g.archetypes.homeTop = storedTop;
      if (all.length) g.archetypes.home = all;
    } else {
      if (storedTop.length) g.archetypes.awayTop = storedTop;
      if (all.length) g.archetypes.away = all;
    }
  };
  for (const g of games) {
    const ids = idByGame.get(g.game_id);
    apply(g, "away", g.away, ids?.away);
    apply(g, "home", g.home, ids?.home);
  }
}


type SB = NonNullable<Awaited<ReturnType<typeof createSupabaseServerClient>>>;

/** Stamp each game with the real "as of" time of the line its ratings use (lib/linesAsOf.ts).
 *  One read per source for the whole slate. slate_games.edges_as_of / market_fetched_at come on the
 *  select('*') row once the pipeline adds them; projections.edges_as_of may not exist yet: that read
 *  then errors and is ignored, so this upgrades by itself once the columns appear. Never throws. */
export async function attachLinesAsOf(sb: SB, games: SlateGame[]): Promise<void> {
  if (!games.length) return;
  const ids = games.map((g) => g.game_id);
  type Res = { data: unknown[] | null; error: unknown } | null;
  const safe = async (q: () => PromiseLike<Res>): Promise<Res> => { try { return await q(); } catch { return null; } };
  const [ea, ml] = await Promise.all([
    games.every((g) => g.edges_as_of) ? Promise.resolve(null) : safe(() => sb.from("projections").select("game_id,model_version,created_at,edges_as_of").in("game_id", ids)
      .eq("model_version", V2).order("created_at", { ascending: false })),
    games.some((g) => !g.market_fetched_at)
      ? safe(() => sb.from("market_lines").select("game_id,fetched_at").in("game_id", ids).eq("source", "vegas").eq("line_type", "current"))
      : Promise.resolve(null),
  ]);
  // edges_as_of of the NEWEST default-model (czarp-v2) row per game, the row slate_games reads.
  // A 7 AM row leaves it null, so that game falls through to the next source.
  const newestRow = new Map<number, Row>();
  for (const r of ea && !ea.error ? ((ea.data ?? []) as Row[]) : []) {
    const id = Number(r.game_id);
    const cur = newestRow.get(id);
    if (!cur || Date.parse(str(r.created_at)) > Date.parse(str(cur.created_at))) newestRow.set(id, r);
  }
  const lines = ml && !ml.error ? newestByGame((ml.data ?? []) as Row[], "fetched_at") : new Map<number, string>();
  for (const g of games) {
    const p = pickLinesAsOf({
      edgesAsOf: g.edges_as_of ?? newestRow.get(g.game_id)?.edges_as_of,
      marketFetchedAt: g.market_fetched_at,
      marketLinesFetchedAt: lines.get(g.game_id),
    });
    g.lines_as_of = p?.at ?? null;
    g.lines_as_of_source = p?.source ?? null;
  }
}

/** Stamp current DraftKings American odds from market_lines.prices onto each game.
 *  Used only to word a kelly_too_small hold when the price is what kills the 0.1u floor.
 *  A missing or unreadable prices blob leaves the fields null (generic hold wording). */
export async function attachMarketPrices(sb: SB, games: SlateGame[]): Promise<void> {
  if (!games.length) return;
  const ids = games.map((g) => g.game_id);
  let rows: Row[] = [];
  try {
    const r = await sb.from("market_lines").select("game_id,prices,fetched_at").in("game_id", ids).eq("source", "vegas").eq("line_type", "current");
    if (!r.error) rows = (r.data ?? []) as Row[];
  } catch { /* odds stay null; hold copy stays generic */ }
  const newest = new Map<number, Row>();
  for (const r of rows) {
    const id = Number(r.game_id);
    const cur = newest.get(id);
    if (!cur || Date.parse(str(r.fetched_at)) > Date.parse(str(cur.fetched_at))) newest.set(id, r);
  }
  for (const g of games) {
    const prices = parseMarketPrices(newest.get(g.game_id)?.prices);
    g.market_over_price = prices.over;
    g.market_under_price = prices.under;
    g.market_home_spread_price = prices.homeSpread;
    g.market_away_spread_price = prices.awaySpread;
  }
}

/** Which of these game ids are neutral-site games (games.neutral). slate_games has no such column. Never throws:
 *  on any failure every game reads as a normal home game. */
export const NEUTRAL_TIMEOUT_MS = 2500;

export async function neutralIds(sb: SB, ids: number[]): Promise<Set<number>> {
  const out = new Set<number>();
  if (!ids.length) return out;
  try {
    // optional read: a short timeout keeps a hung connection from holding the page
    const r = await sb.from("games").select("id").eq("neutral", true).in("id", ids).abortSignal(AbortSignal.timeout(NEUTRAL_TIMEOUT_MS));
    if (r.error) console.error(`[supabase] games.neutral: ${r.error.code ?? ""} ${r.error.message}`);
    for (const x of r.data ?? []) out.add(Number(x.id));
  } catch { /* titles fall back to '@' */ }
  return out;
}

/** Per-game sheet detail. `select *` on projections so reason / proj_possessions are picked up
 *  automatically once CZarp adds them, without breaking before then. Never throws. */
async function loadInsights(sb: SB, ids: number[]): Promise<Map<number, GameInsight>> {
  const out = new Map<number, GameInsight>();
  try {
    const [cfg, pr, ml, gm] = await Promise.all([
      sb.from("model_configs").select("model_version").eq("is_default", true),
      sb.from("projections").select("*").in("game_id", ids).order("created_at", { ascending: false }),
      sb.from("market_lines").select("game_id,source,line_type,spread,total,fetched_at").in("game_id", ids).eq("source", "vegas").order("fetched_at", { ascending: false }),
      sb.from("games").select("id,neutral").in("id", ids),
    ]);
    const defaults = new Set((cfg.data ?? []).map((r) => str(r.model_version)));
    const proj = new Map<number, Row>();
    for (const r of (pr.data ?? []) as Row[]) {
      const id = Number(r.game_id);
      const cur = proj.get(id);
      // newest row wins, but a default-model row beats a non-default one
      if (!cur || (!defaults.has(str(cur.model_version)) && defaults.has(str(r.model_version)))) proj.set(id, r);
    }
    const lines = new Map<string, Row>(); // newest snapshot per game + line_type
    for (const r of (ml.data ?? []) as Row[]) {
      const k = `${r.game_id}:${r.line_type}`;
      if (!lines.has(k)) lines.set(k, r);
    }
    const neutral = new Map((gm.data ?? []).map((r) => [Number(r.id), r.neutral === true]));
    for (const id of ids) {
      const p = proj.get(id) ?? {};
      const o = lines.get(`${id}:open`), c = lines.get(`${id}:current`);
      out.set(id, {
        proj_home: num(p.proj_home), proj_away: num(p.proj_away),
        home_win_prob: num(p.home_win_prob), home_cover_prob: num(p.home_cover_prob), over_prob: num(p.over_prob),
        neutral: neutral.get(id) ?? false,
        open_spread: num(o?.spread), cur_spread: num(c?.spread), open_total: num(o?.total), cur_total: num(c?.total),
        reason: visibleCopy(typeof p.reason === "string" ? p.reason : "") || null,
        proj_possessions: num(p.proj_possessions),
        upset_alert: p.upset_alert === true,
        upset_reason: visibleCopy(typeof p.upset_reason === "string" ? p.upset_reason : "") || null,
      });
    }
  } catch { /* sheet just shows less */ }
  return out;
}

// ---- landing hero -----------------------------------------------------------
export type Hero =
  | { kind: "today"; plays: { game: SlateGame; bet: ValueBet }[] }
  | { kind: "graded"; when: "yesterday" | "latest"; row: LedgerRow; game: string | null; neutral: boolean };

/** Every official play today (highest posted edge first); else yesterday's top official pick with its result;
 *  else the latest graded official pick. Tracked picks are never used. */
export async function loadHero(sport: Sport): Promise<Loaded<Hero | null>> {
  const sb = await createSupabaseServerClient();
  if (!sb) return { state: "unconfigured" };
  const today = todayCT();
  const g = await sb.from("slate_games").select("*").eq("sport", sport.key).eq("game_date", today);
  if (g.error) return fail("slate_games", g.error);
  const games = (g.data ?? []).map(toSlateGame);
  if (games.length) {
    const ids = games.map((x) => x.game_id);
    // titles only: read alongside value_bets, and a failure just leaves "@" titles
    const [b, neutral] = await Promise.all([sb.from("value_bets").select("*").eq("sport", sport.key).in("game_id", ids), neutralIds(sb, ids)]);
    for (const x of games) x.neutral = neutral.has(x.game_id);
    if (b.error) return fail("value_bets", b.error);
    // Same merge as the slate, so every time-stamped official play shows (not just the top value_bets row).
    const all = await attachOfficialRecord(sb, sport, ids, await mergeOfficialLocked(sb, sport, ids, (b.data ?? []).map(toValueBet)));
    const plays = all
      .filter((x) => x.pick_tier === "official")
      .sort((a, z) => (z.edge_at_post_pct ?? z.line_edge_pct) - (a.edge_at_post_pct ?? a.line_edge_pct))
      .map((bet) => ({ bet, game: games.find((x) => x.game_id === bet.game_id) }))
      .filter((p): p is { bet: ValueBet; game: SlateGame } => p.game != null);
    if (plays.length) return { state: "live", data: { kind: "today", plays } };
  }
  const pick = async (q: PromiseLike<{ data: Row[] | null; error: { message: string } | null }>, when: "yesterday" | "latest") => {
    const r = await q;
    const row = r.data?.[0] ? toLedgerRow(r.data[0]) : null;
    if (!row) return null;
    const n = await sb.from("slate_games").select("game").eq("game_id", row.game_id).limit(1);
    const neutral = (await neutralIds(sb, [row.game_id])).has(row.game_id);
    return { kind: "graded" as const, when, row, game: n.data?.[0] ? str(n.data[0].game) : null, neutral };
  };
  const y = await pick(sb.from("ledger").select("*").eq("sport", sport.key).eq("tier", "official")
    .eq("game_date", shiftDate(today, -1)).order("stake_units", { ascending: false, nullsFirst: false }).limit(1), "yesterday");
  if (y) return { state: "live", data: y };
  const l = await pick(sb.from("ledger").select("*").eq("sport", sport.key).eq("tier", "official")
    .not("result", "is", null).order("game_date", { ascending: false }).order("stake_units", { ascending: false, nullsFirst: false }).limit(1), "latest");
  return { state: "live", data: l };
}

// ---- /[sport]/results -------------------------------------------------------
export const RANGES = { season: { label: "Season", days: null }, "30d": { label: "Last 30d", days: 30 }, "7d": { label: "Last 7d", days: 7 } } as const;
export type RangeKey = keyof typeof RANGES;

export type ResultsData = {
  tier: PickTier;
  ledger: LedgerRow[];
  gameNames: Record<number, string>;
  /** game ids (by games.neutral) whose title should read 'vs' with a Neutral chip */
  neutralGames: Record<number, true>;
  tipoffs: Record<number, string>;
  /** per-market season record for the selected tier (tier_record, latest season) */
  byMarket: TierRecord[];
  season: number | null;
  summary: {
    record: string; wins: number; losses: number; pushes: number; pending: number; graded: number;
    winPct: number | null; roi: number | null; units: number; flatUnits: number;
    avgClv: number | null; beatClose: number; clvN: number;
    /** 95% Wilson CI on win rate (wins / (wins+losses)); null if no decided bets */
    winCi: { lo: number; hi: number } | null;
    smallSample: boolean;
  };
  /** per-market ¼-Kelly P/L, stake, and ROI rolled up from the ledger (same data as the headline record).
   *  tier_record.units / roi are the flat 1u book and are not used for the large ROI. */
  byMarketUnits: Record<string, MarketUnits>;
  risk: PortfolioRisk;
  cumulativeUnits: number[];
};

const LEDGER_LIMIT = 1000;
const NAME_LOOKUP_LIMIT = 200;

/** Units for a ledger row: official = ¼-Kelly profit_units; tracked = flat 1u result. */
export const rowUnits = (r: LedgerRow) => (r.pick_tier === "track" ? r.flat_units ?? r.profit_units : r.profit_units);

export type MarketUnits = { kelly: number; flat: number; staked: number; roi: number | null };

/** Infer market from a ledger pick label (ledger view has no market column). */
export function marketFromPick(pick: string): Market {
  const s = pick.toLowerCase();
  if (s.startsWith("under") || s.startsWith("over")) return "total";
  if (/\bml\b/.test(s) || s.includes("moneyline")) return "moneyline";
  return "spread";
}

/** Stake-sized (¼-Kelly) P/L, staked units, and ROI per market, from the same ledger the headline uses. */
export function rollupMarketUnits(ledger: LedgerRow[], tier: PickTier): Record<string, MarketUnits> {
  const out: Record<string, MarketUnits> = {};
  for (const r of ledger) {
    if (r.result == null) continue;
    const mk = marketFromPick(r.pick);
    const slot = out[mk] ?? (out[mk] = { kelly: 0, flat: 0, staked: 0, roi: null });
    slot.kelly += rowUnits(r) ?? 0;
    slot.flat += r.flat_units ?? 0;
    slot.staked += tier === "track" ? 1 : r.stake_units ?? 0;
  }
  for (const slot of Object.values(out)) {
    slot.kelly = Math.round(slot.kelly * 100) / 100;
    slot.flat = Math.round(slot.flat * 100) / 100;
    slot.staked = Math.round(slot.staked * 100) / 100;
    slot.roi = slot.staked ? slot.kelly / slot.staked : null;
  }
  return out;
}

export async function loadResults(sport: Sport, range: RangeKey, tier: PickTier, opts: { titles?: boolean } = {}): Promise<Loaded<ResultsData>> {
  const sb = await createSupabaseServerClient();
  if (!sb) return { state: "unconfigured" };

  let lq = sb.from("ledger").select("*").eq("sport", sport.key).eq("tier", tier);
  const days = RANGES[range].days;
  if (days) lq = lq.gte("game_date", shiftDate(todayCT(), -days));
  const [l, tr] = await Promise.all([
    lq.order("game_date", { ascending: false }).order("game_id", { ascending: false }).limit(LEDGER_LIMIT),
    sb.from("tier_record").select("*").eq("league", sport.league).eq("tier", tier),
  ]);
  if (l.error) return fail("ledger", l.error);
  if (tr.error) return fail("tier_record", tr.error);

  const ledger = (l.data ?? []).map(toLedgerRow);

  const ids = [...new Set(ledger.slice(0, NAME_LOOKUP_LIMIT).map((r) => r.game_id))];
  const gameNames: Record<number, string> = {};
  const neutralGames: Record<number, true> = {};
  const tipoffs: Record<number, string> = {};
  if (ids.length) {
    const [n, neutral] = await Promise.all([sb.from("slate_games").select("game_id, game, tipoff").in("game_id", ids), opts.titles === false ? Promise.resolve(new Set<number>()) : neutralIds(sb, ids)]);
    for (const id of neutral) neutralGames[id] = true;
    for (const r of n.data ?? []) {
      gameNames[Number(r.game_id)] = str(r.game);
      if (r.tipoff) tipoffs[Number(r.game_id)] = str(r.tipoff);
    }
  }

  const recs = (tr.data ?? []) as Row[];
  const season = recs.reduce<number | null>((m, r) => (num(r.season) != null && (m == null || num(r.season)! > m) ? num(r.season) : m), null);
  const order: Record<string, number> = { spread: 0, total: 1, moneyline: 2 };
  const byMarket: TierRecord[] = recs
    .filter((r) => season == null || num(r.season) === season)
    .map((r) => ({
      league: str(r.league), season: num(r.season) ?? 0, tier, market: str(r.market) as Market,
      wins: num(r.wins) ?? 0, losses: num(r.losses) ?? 0, pushes: num(r.pushes) ?? 0, pending: num(r.pending) ?? 0,
      units: num(r.units) ?? 0, roi: num(r.roi), avg_clv: num(r.avg_clv), beat_close: num(r.beat_close) ?? 0, pnl_dollars: num(r.pnl_dollars),
    }))
    .sort((a, z) => (order[a.market] ?? 9) - (order[z.market] ?? 9));

  // KPIs from the ledger (respects range and tier). Official units = ¼-Kelly profit_units.
  let wins = 0, losses = 0, pushes = 0, pending = 0, units = 0, flatUnits = 0, staked = 0, clvS = 0, clvN = 0, beatClose = 0;
  for (const r of ledger) {
    if (r.result == null) { pending++; continue; }
    if (r.result === "W") wins++; else if (r.result === "L") losses++; else pushes++;
    const k = rowUnits(r) ?? 0;
    const f = r.flat_units ?? 0;
    units += k;
    flatUnits += f;
    staked += tier === "track" ? 1 : r.stake_units ?? 0;
    if (r.clv != null) { clvS += r.clv; clvN++; if (r.clv > 0) beatClose++; }
  }
  const byMarketUnits = rollupMarketUnits(ledger, tier);
  const decided = wins + losses;
  const winCi = wilsonCi(wins, decided);
  const smallSample = decided > 0 && decided < SMALL_SAMPLE_N;

  const cumulativeUnits = [0];
  for (const r of [...ledger].reverse()) {
    if (r.result == null) continue;
    cumulativeUnits.push(Math.round((cumulativeUnits[cumulativeUnits.length - 1] + (rowUnits(r) ?? 0)) * 100) / 100);
  }

  return {
    state: "live",
    data: {
      tier, ledger, gameNames, neutralGames, tipoffs, byMarket, season, cumulativeUnits, byMarketUnits,
      risk: portfolioRisk(ledger),
      summary: {
        record: `${wins}-${losses}-${pushes}`, wins, losses, pushes, pending, graded: wins + losses + pushes,
        winPct: wins + losses ? wins / (wins + losses) : null,
        roi: staked ? units / staked : null,
        units: Math.round(units * 100) / 100,
        flatUnits: Math.round(flatUnits * 100) / 100,
        avgClv: clvN ? clvS / clvN : null,
        beatClose, clvN, winCi, smallSample,
      },
    },
  };
}

function fail(view: string, error: { message: string; code?: string }): { state: "error" } {
  console.error(`[supabase] ${view}: ${error.code ?? ""} ${error.message}`);
  return { state: "error" };
}

// ---- /[sport]/team/[slug] -------------------------------------------------

export type TeamPageData = {
  profile: TeamProfile | null;
  record: TeamModelRecord | null;
  grades: ModelGrade[];
  next: (SlateGame & { officialBets: ValueBet[] }) | null;
  tags: ArchetypeTag[];
  /** Latest weekly snapshot for this team; null when missing or the read fails. */
  ranking: TeamRankingSnapshot | null;
};

function toProfile(r: Row): TeamProfile {
  return {
    team_id: Number(r.team_id),
    team_name: str(r.team_name),
    season: num(r.season) ?? 0,
    as_of: r.as_of ? str(r.as_of) : null,
    games_played: num(r.games_played),
    tentative: r.tentative === true,
    new_coach: r.new_coach === true,
    new_coach_games: num(r.new_coach_games),
    lock_sides_games: num(r.lock_sides_games),
    lock_totals_games: num(r.lock_totals_games),
    lock_sides_remaining: num(r.lock_sides_remaining),
    lock_totals_remaining: num(r.lock_totals_remaining),
    efg_pctile: num(r.efg_pctile),
    d_efg_pctile: num(r.d_efg_pctile),
    to_pct_pctile: num(r.to_pct_pctile),
    d_to_pct_pctile: num(r.d_to_pct_pctile),
    or_pct_pctile: num(r.or_pct_pctile),
    opp_or_pct_pctile: num(r.opp_or_pct_pctile),
    ftr_pctile: num(r.ftr_pctile),
    d_ftr_pctile: num(r.d_ftr_pctile),
    tempo_pctile: num(r.tempo_pctile),
    fg3a_rate_pctile: num(r.fg3a_rate_pctile),
    fg3_pct_pctile: num(r.fg3_pct_pctile),
    opp_fg3_pct_pctile: num(r.opp_fg3_pct_pctile),
    ft_pct_pctile: num(r.ft_pct_pctile),
    adj_oe: num(r.adj_oe),
    adj_de: num(r.adj_de),
    hca: num(r.hca),
    tags: r.tags,
    tags_all: r.tags_all,
  };
}

function toRecord(r: Row): TeamModelRecord {
  const n0 = (v: unknown) => num(v) ?? 0;
  return {
    team_id: Number(r.team_id),
    team_name: str(r.team_name),
    season: n0(r.season),
    model_ats_wins: n0(r.model_ats_wins),
    model_ats_losses: n0(r.model_ats_losses),
    model_ats_pushes: n0(r.model_ats_pushes),
    model_ou_wins: n0(r.model_ou_wins),
    model_ou_losses: n0(r.model_ou_losses),
    model_ou_pushes: n0(r.model_ou_pushes),
    official_wins: n0(r.official_wins),
    official_losses: n0(r.official_losses),
    official_pushes: n0(r.official_pushes),
    official_ats_wins: n0(r.official_ats_wins),
    official_ats_losses: n0(r.official_ats_losses),
    official_ou_wins: n0(r.official_ou_wins),
    official_ou_losses: n0(r.official_ou_losses),
  };
}

function toGrade(r: Row): ModelGrade {
  return {
    game_id: Number(r.game_id),
    game_date: str(r.game_date),
    home: str(r.home),
    away: str(r.away),
    home_team_id: num(r.home_team_id),
    away_team_id: num(r.away_team_id),
    home_score: num(r.home_score),
    away_score: num(r.away_score),
    status: r.status ? str(r.status) : null,
    final_at: r.final_at ? str(r.final_at) : null,
    final_at_estimated: r.final_at_estimated === true,
    model_spread: num(r.model_spread),
    model_total: num(r.model_total),
    vegas_spread: num(r.vegas_spread),
    vegas_total: num(r.vegas_total),
    model_ats_side: r.model_ats_side ? str(r.model_ats_side) : null,
    model_ou_side: r.model_ou_side ? str(r.model_ou_side) : null,
    model_ats_result: r.model_ats_result ? str(r.model_ats_result) : null,
    model_ou_result: r.model_ou_result ? str(r.model_ou_result) : null,
    actual_ats_cover: r.actual_ats_cover ? str(r.actual_ats_cover) : null,
    actual_ou: r.actual_ou ? str(r.actual_ou) : null,
  };
}

/** Latest ranking_snapshots row for a team. Fail-safe: errors and empty results return null. */
export async function loadTeamRanking(sb: SB, teamId: number, league: string): Promise<TeamRankingSnapshot | null> {
  try {
    const r = await sb
      .from("ranking_snapshots")
      .select(TEAM_RANKING_COLUMNS)
      .eq("team_id", teamId)
      .eq("league", league)
      .order("week_of", { ascending: false })
      .limit(1)
      .abortSignal(AbortSignal.timeout(TEAM_RANKING_TIMEOUT_MS));
    if (r.error || !r.data?.[0]) return null;
    return parseTeamRanking(r.data[0] as Row);
  } catch {
    return null;
  }
}

/** Resolve team by numeric id or name slug; load profile, record, grades, next slate game. */
export async function loadTeam(sport: Sport, slug: string): Promise<Loaded<TeamPageData | null>> {
  const sb = await createSupabaseServerClient();
  if (!sb) return { state: "unconfigured" };

  const isId = /^\d+$/.test(slug);
  let teamId: number | null = isId ? Number(slug) : null;
  let teamName: string | null = null;

  if (teamId != null) {
    const t = await sb.from("teams").select("id,name").eq("id", teamId).eq("league", sport.league).limit(1);
    if (t.error) return fail("teams", t.error);
    if (t.data?.[0]) teamName = str(t.data[0].name);
  } else {
    // Match slug against teams.name (league-scoped). Fetch candidates — names are unique enough per league.
    const t = await sb.from("teams").select("id,name").eq("league", sport.league).limit(500);
    if (t.error) return fail("teams", t.error);
    const hit = (t.data ?? []).find((r) => teamSlug(str(r.name)) === slug);
    if (hit) { teamId = Number(hit.id); teamName = str(hit.name); }
  }

  if (teamId == null || !teamName) {
    // Fall back: profile view may have a name the teams table missed
    const p = await sb.from("team_profile_latest").select("*").eq("league", sport.league).limit(500);
    if (p.error) return fail("team_profile_latest", p.error);
    const hit = (p.data ?? []).find((r) => (isId ? Number(r.team_id) === Number(slug) : teamSlug(str(r.team_name)) === slug));
    if (!hit) return { state: "live", data: null };
    teamId = Number(hit.team_id);
    teamName = str(hit.team_name);
  }

  const [pr, rec, gr, nx, ranking] = await Promise.all([
    sb.from("team_profile_latest").select("*").eq("team_id", teamId).limit(1),
    sb.from("team_model_record").select("*").eq("team_id", teamId).limit(1),
    sb.from("model_grades").select("*").or(`home_team_id.eq.${teamId},away_team_id.eq.${teamId}`)
      .order("game_date", { ascending: false }).limit(40),
    // Filter by name in JS — PostgREST .or() breaks on apostrophes (St. John's).
    sb.from("slate_games").select("*").eq("sport", sport.key).gte("game_date", todayCT())
      .order("game_date", { ascending: true }).order("tipoff", { ascending: true, nullsFirst: false }).limit(120),
    loadTeamRanking(sb, teamId, sport.league),
  ]);
  if (pr.error) return fail("team_profile_latest", pr.error);
  if (rec.error) return fail("team_model_record", rec.error);
  if (gr.error) return fail("model_grades", gr.error);
  if (nx.error) return fail("slate_games", nx.error);

  const profile = pr.data?.[0] ? toProfile(pr.data[0] as Row) : null;
  const record = rec.data?.[0] ? toRecord(rec.data[0] as Row) : null;
  const grades = (gr.data ?? []).map((r) => toGrade(r as Row));
  const nextRowRaw = (nx.data ?? []).find((r) => str(r.home) === teamName || str(r.away) === teamName);
  // games.neutral for the game log and the next game (neither source view carries it)
  // started now, awaited after the next-game reads below, so it adds no round trip; failure only affects titles
  const neutralP = neutralIds(sb, [...grades.map((x) => x.game_id), ...(nextRowRaw ? [Number(nextRowRaw.game_id)] : [])]);
  const tags = visibleTags(parseTagList(profile?.tags_all ?? profile?.tags ?? [], profile?.tentative ?? false));

  let next: TeamPageData["next"] = null;
  const nextRow = (nx.data ?? []).find((r) => str(r.home) === teamName || str(r.away) === teamName);
  if (nextRow) {
    const g = toSlateGame(nextRow as Row);
    let officialBets: ValueBet[] = [];
    try {
      const b = await sb.from("value_bets").select("*").eq("sport", sport.key).eq("game_id", g.game_id);
      if (!b.error) {
        officialBets = (await attachOfficialRecord(sb, sport, [g.game_id], await mergeOfficialLocked(sb, sport, [g.game_id], (b.data ?? []).map((r) => toValueBet(r as Row)))))
          .filter((x) => x.pick_tier === "official");
      }
    } catch { /* team page still renders without official rows */ }
    try {
      // Official totals model for the next-game Total FV (slate_games.proj_total is the v2 team sum).
      const v1 = await sb.from("projections").select("total,over_prob").eq("game_id", g.game_id)
        .eq("model_version", V1_TOTALS).order("created_at", { ascending: false }).limit(1);
      if (!v1.error && v1.data?.[0]) {
        g.czarp_total = num(v1.data[0].total);
        g.czarp_over_prob = num(v1.data[0].over_prob);
      }
    } catch { /* Total FV is hidden without v1 */ }
    g.neutral = (await neutralP).has(g.game_id);
    // Same pause chips as the slate card / game sheet (name lookup is enough if game rows have no ids).
    await attachNewCoaches(sb, [g], []);
    await attachLinesAsOf(sb, [g]);
    await attachMarketPrices(sb, [g]);
    next = { ...g, officialBets };
  }
  const neutral = await neutralP;
  for (const x of grades) x.neutral = neutral.has(x.game_id);

  return { state: "live", data: { profile, record, grades, next, tags, ranking } };
}


// ---- /[sport]/rankings -----------------------------------------------------

export async function loadRankings(sport: Sport): Promise<Loaded<RankingsData>> {
  const sb = await createSupabaseServerClient();
  return loadRankingsFrom(sb, sport);
}

function teamNameOf(r: Row): string {
  const emb = r.teams;
  if (emb && typeof emb === "object" && !Array.isArray(emb) && (emb as Row).name != null) {
    return str((emb as Row).name);
  }
  if (Array.isArray(emb) && emb[0] && (emb[0] as Row).name != null) return str((emb[0] as Row).name);
  return r.team_name != null ? str(r.team_name) : "";
}

/** Latest official poll for this league, plus movement vs the previous week_of.
 *  A failed or missing prior-week read never fails the page: we fall back to
 *  stored prev_rank, then to a dash. 0 rows is a live empty payload, not an error. */
function rankingsFail(error: { message: string; code?: string }): { state: "error"; reason: "unreachable" | "unavailable" } {
  fail("ranking_snapshots", error);
  return { state: "error", reason: rankingsErrorKind(error) };
}

export async function loadRankingsFrom(sb: SB | null, sport: Sport): Promise<Loaded<RankingsData>> {
  if (!sb) return { state: "unconfigured" };
  try {
  const latest = await sb
    .from("ranking_snapshots")
    .select("week_of,as_of")
    .eq("league", sport.league)
    .order("week_of", { ascending: false })
    .order("as_of", { ascending: false })
    .limit(1);
  if (latest.error) return rankingsFail(latest.error);
  const head = (latest.data ?? [])[0] as Row | undefined;
  const weekOf = head?.week_of ? str(head.week_of) : "";
  if (!weekOf) {
    return { state: "live", data: { week_of: "", as_of: "", season: null, rows: [] } };
  }

  const snap = await sb
    .from("ranking_snapshots")
    .select(`${RANKING_SNAPSHOT_COLUMNS},teams(name)`)
    .eq("league", sport.league)
    .eq("week_of", weekOf)
    .order("rank")
    .range(0, 999);
  if (snap.error) return rankingsFail(snap.error);

  const raw = (snap.data ?? []) as Row[];
  const asOf = raw[0]?.as_of ? str(raw[0].as_of) : (head?.as_of ? str(head.as_of) : "");
  const season = raw[0] ? num(raw[0].season) : null;

  let previous: { team_id: number; rank: number }[] | null | undefined = undefined;
  try {
    const prevWeek = await sb
      .from("ranking_snapshots")
      .select("week_of")
      .eq("league", sport.league)
      .lt("week_of", weekOf)
      .order("week_of", { ascending: false })
      .limit(1);
    if (prevWeek.error) {
      previous = undefined;
    } else {
      const pw = (prevWeek.data ?? [])[0] as Row | undefined;
      if (!pw?.week_of) {
        previous = [];
      } else {
        const prevRows = await sb
          .from("ranking_snapshots")
          .select("team_id,rank")
          .eq("league", sport.league)
          .eq("week_of", str(pw.week_of))
          .range(0, 999);
        previous = prevRows.error
          ? undefined
          : ((prevRows.data ?? []) as Row[])
              .map((r) => ({ team_id: Number(r.team_id), rank: Number(r.rank) }))
              .filter((r) => Number.isFinite(r.team_id) && Number.isFinite(r.rank));
      }
    }
  } catch {
    previous = undefined;
  }

  const current = raw
    .map((r) => {
      const team = teamNameOf(r).trim();
      const team_id = Number(r.team_id);
      const rank = Number(r.rank);
      const rating = num(r.rating);
      if (!team || !Number.isFinite(team_id) || !Number.isFinite(rank) || rank < 1 || rating == null) return null;
      const prev = num(r.prev_rank);
      return {
        team_id,
        team,
        rank,
        prev_rank: prev != null && prev >= 1 ? prev : null,
        rating,
        di_wins: num(r.di_wins),
        di_losses: num(r.di_losses),
      };
    })
    .filter((r): r is NonNullable<typeof r> => r != null);

  const rows: RankingRow[] = sortRankings(withMovement(current, previous));
  return { state: "live", data: { week_of: weekOf, as_of: asOf, season, rows } };
  } catch (e) {
    const message = e instanceof Error ? e.message : "fetch failed";
    return rankingsFail({ message });
  }
}
