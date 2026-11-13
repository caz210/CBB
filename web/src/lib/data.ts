// Live data loaders for Server Components. Reads CZarp's Supabase views with the anon key
// (all picks are public; session cookies only matter for user-owned data).
// Every loader returns a tagged result so pages can render a clear empty state instead of
// silently falling back to example numbers.
import { createSupabaseServerClient } from "./supabase/server";
import type { Sport } from "./sports";
import type { Archetypes, GameInsight, LedgerRow, SlateGame, TierRecord, ValueBet } from "./types";
import { portfolioRisk, computedLineEdge, type PortfolioRisk } from "./portfolio";
import { parseTagList, type ArchetypeTag } from "./archetypes";
import { teamSlug, type ModelGrade, type TeamModelRecord, type TeamProfile } from "./teams";
import { isPickTier, type Market, type MarketSource, type PickTier } from "./labels";
import { wilsonCi, SMALL_SAMPLE_N } from "./stats";
import { newCoachLock, type NewCoachLock } from "./coach";
import { parseVerdict } from "./verdict";

export type Loaded<T> =
  | { state: "live"; data: T }
  | { state: "unconfigured" }
  | { state: "error" };

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
  return {
    home: parseTagList(a.home, homeTent),
    away: parseTagList(a.away, awayTent),
    home_tentative: homeTent,
    away_tentative: awayTent,
    tentative: Boolean(a.tentative),
    notes: strs(a.notes),
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
    reason: typeof r.reason === "string" && r.reason.trim() ? r.reason.trim() : null,
    proj_possessions: num(r.proj_possessions),
    upset_alert: r.upset_alert === true,
    upset_reason: typeof r.upset_reason === "string" && r.upset_reason.trim() ? r.upset_reason.trim() : null,
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
    model_version: r.model_name ? str(r.model_name) : r.model_version ? str(r.model_version) : null,
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
  if (r.model_name) v.model_version = str(r.model_name);
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
  /** max(final_at) on a finished slate; null if none of the games have a stamp. */
  finalAt: string | null;
  finalAtEstimated: boolean;
};

export async function loadSlate(sport: Sport, date: string): Promise<Loaded<SlateData>> {
  const sb = await createSupabaseServerClient();
  if (!sb) return { state: "unconfigured" };

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
  if (games.length) {
    const b = await sb.from("value_bets").select("*").eq("sport", sport.key).in("game_id", games.map((x) => x.game_id));
    if (b.error) return fail("value_bets", b.error);
    const ids = games.map((x) => x.game_id);
    const all = await mergeOfficialLocked(sb, sport, ids, (b.data ?? []).map(toValueBet));
    all.sort((a, z) => z.line_edge_pct - a.line_edge_pct);
    bets = all.filter((x) => x.pick_tier === "official");
    const [st, gr, pr, ml] = await Promise.all([
      sb.from("games").select("id,status,updated_at,final_at,final_at_estimated,home,away,home_team_id,away_team_id,home_score,away_score").in("id", ids),
      sb.from("ledger").select("game_id,result").in("game_id", ids).not("result", "is", null),
      sb.from("projections").select("game_id,model_version,spread,total,over_prob,created_at").in("game_id", ids).order("created_at", { ascending: false }),
      sb.from("market_lines").select("game_id,fetched_at").in("game_id", ids).eq("source", "vegas"),
    ]);
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
    // Pause chips read the team's CURRENT profile, so only show them on today's and future slates.
    // Past slates keep the reason that was stored with that day's projection instead.
    if (date >= todayCT()) await attachNewCoaches(sb, games, st.data ?? []);
    const ins = await loadInsights(sb, ids);
    for (const x of games) x.insight = ins.get(x.game_id);
    tracked = all.filter((x) => x.pick_tier === "track");
  } else {
    const [n, p] = await Promise.all([
      sb.from("slate_games").select("game_date").eq("sport", sport.key).gt("game_date", date).order("game_date").limit(1),
      sb.from("slate_games").select("game_date").eq("sport", sport.key).lt("game_date", date).order("game_date", { ascending: false }).limit(1),
    ]);
    nextDate = n.data?.[0] ? str(n.data[0].game_date) : null;
    prevDate = p.data?.[0] ? str(p.data[0].game_date) : null;
  }
  return { state: "live", data: { date, games, bets, tracked, prevDate, nextDate, updatedAt, finished, finalAt, finalAtEstimated } };
}


const V2 = "czarp-v2";
const V1_TOTALS = "czarp-v1-totals";

function modelLabel(v: string) {
  if (v === V1_TOTALS) return "czarp-v1-totals";
  if (v === V2) return "czarp-v2";
  return v;
}

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
    g.proj_total_v2 = totalV2;
    g.proj_total_v1 = totalV1;
    g.over_prob_v1 = num(v1?.over_prob);
    const vegasT = g.market_total ?? totalV2 ?? totalV1;
    const spreadE = computedLineEdge(spread, g.market_spread, vegasT);
    const totV2E = computedLineEdge(totalV2, g.market_total, g.market_total ?? vegasT);
    const totV1E = computedLineEdge(totalV1, g.market_total, g.market_total ?? vegasT);
    const cands: { pct: number; market: "spread" | "total"; model: string }[] = [];
    if (spreadE != null) cands.push({ pct: spreadE, market: "spread", model: V2 });
    if (totV1E != null) cands.push({ pct: totV1E, market: "total", model: V1_TOTALS });
    if (totV2E != null) cands.push({ pct: totV2E, market: "total", model: V2 });
    const best = cands.sort((a, z) => z.pct - a.pct)[0];
    g.rawEdge = best?.pct ?? null;
    g.rawEdgeMarket = best?.market ?? null;
    g.rawEdgeModel = best ? modelLabel(best.model) : null;
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

type SB = NonNullable<Awaited<ReturnType<typeof createSupabaseServerClient>>>;

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
        reason: typeof p.reason === "string" && p.reason.trim() ? p.reason.trim() : null,
        proj_possessions: num(p.proj_possessions),
        upset_alert: p.upset_alert === true,
        upset_reason: typeof p.upset_reason === "string" && p.upset_reason.trim() ? String(p.upset_reason).trim() : null,
      });
    }
  } catch { /* sheet just shows less */ }
  return out;
}

// ---- landing hero -----------------------------------------------------------
export type Hero =
  | { kind: "today"; plays: { game: SlateGame; bet: ValueBet }[] }
  | { kind: "graded"; when: "yesterday" | "latest"; row: LedgerRow; game: string | null };

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
    const b = await sb.from("value_bets").select("*").eq("sport", sport.key).in("game_id", ids);
    if (b.error) return fail("value_bets", b.error);
    // Same merge as the slate, so every time-stamped official play shows (not just the top value_bets row).
    const all = await mergeOfficialLocked(sb, sport, ids, (b.data ?? []).map(toValueBet));
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
    return { kind: "graded" as const, when, row, game: n.data?.[0] ? str(n.data[0].game) : null };
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
  /** per-market ¼-Kelly units rolled up from the ledger (tier_record.units is flat 1u) */
  byMarketUnits: Record<string, { kelly: number; flat: number }>;
  risk: PortfolioRisk;
  cumulativeUnits: number[];
};

const LEDGER_LIMIT = 1000;
const NAME_LOOKUP_LIMIT = 200;

/** Units for a ledger row: official = ¼-Kelly profit_units; tracked = flat 1u result. */
export const rowUnits = (r: LedgerRow) => (r.pick_tier === "track" ? r.flat_units ?? r.profit_units : r.profit_units);

/** Infer market from a ledger pick label (ledger view has no market column). */
export function marketFromPick(pick: string): Market {
  const s = pick.toLowerCase();
  if (s.startsWith("under") || s.startsWith("over")) return "total";
  if (/ml/.test(s) || s.includes("moneyline")) return "moneyline";
  return "spread";
}

export async function loadResults(sport: Sport, range: RangeKey, tier: PickTier): Promise<Loaded<ResultsData>> {
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
  const tipoffs: Record<number, string> = {};
  if (ids.length) {
    const n = await sb.from("slate_games").select("game_id, game, tipoff").in("game_id", ids);
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
  const byMarketUnits: ResultsData["byMarketUnits"] = {};
  for (const r of ledger) {
    if (r.result == null) { pending++; continue; }
    if (r.result === "W") wins++; else if (r.result === "L") losses++; else pushes++;
    const k = rowUnits(r) ?? 0;
    const f = r.flat_units ?? 0;
    units += k;
    flatUnits += f;
    staked += tier === "track" ? 1 : r.stake_units ?? 0;
    if (r.clv != null) { clvS += r.clv; clvN++; if (r.clv > 0) beatClose++; }
    const mk = marketFromPick(r.pick);
    const slot = byMarketUnits[mk] ?? (byMarketUnits[mk] = { kelly: 0, flat: 0 });
    slot.kelly += k;
    slot.flat += f;
  }
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
      tier, ledger, gameNames, tipoffs, byMarket, season, cumulativeUnits, byMarketUnits,
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

  const [pr, rec, gr, nx] = await Promise.all([
    sb.from("team_profile_latest").select("*").eq("team_id", teamId).limit(1),
    sb.from("team_model_record").select("*").eq("team_id", teamId).limit(1),
    sb.from("model_grades").select("*").or(`home_team_id.eq.${teamId},away_team_id.eq.${teamId}`)
      .order("game_date", { ascending: false }).limit(40),
    // Filter by name in JS — PostgREST .or() breaks on apostrophes (St. John's).
    sb.from("slate_games").select("*").eq("sport", sport.key).gte("game_date", todayCT())
      .order("game_date", { ascending: true }).order("tipoff", { ascending: true, nullsFirst: false }).limit(120),
  ]);
  if (pr.error) return fail("team_profile_latest", pr.error);
  if (rec.error) return fail("team_model_record", rec.error);
  if (gr.error) return fail("model_grades", gr.error);
  if (nx.error) return fail("slate_games", nx.error);

  const profile = pr.data?.[0] ? toProfile(pr.data[0] as Row) : null;
  const record = rec.data?.[0] ? toRecord(rec.data[0] as Row) : null;
  const grades = (gr.data ?? []).map((r) => toGrade(r as Row));
  const tags = parseTagList(profile?.tags ?? [], profile?.tentative ?? false);

  let next: TeamPageData["next"] = null;
  const nextRow = (nx.data ?? []).find((r) => str(r.home) === teamName || str(r.away) === teamName);
  if (nextRow) {
    const g = toSlateGame(nextRow as Row);
    let officialBets: ValueBet[] = [];
    try {
      const b = await sb.from("value_bets").select("*").eq("sport", sport.key).eq("game_id", g.game_id);
      if (!b.error) {
        officialBets = (await mergeOfficialLocked(sb, sport, [g.game_id], (b.data ?? []).map((r) => toValueBet(r as Row))))
          .filter((x) => x.pick_tier === "official");
      }
    } catch { /* team page still renders without official rows */ }
    try {
      // Official totals model for the next-game Total FV (slate_games.proj_total is the v2 team sum).
      const v1 = await sb.from("projections").select("total,over_prob").eq("game_id", g.game_id)
        .eq("model_version", V1_TOTALS).order("created_at", { ascending: false }).limit(1);
      if (!v1.error && v1.data?.[0]) {
        g.proj_total_v1 = num(v1.data[0].total);
        g.over_prob_v1 = num(v1.data[0].over_prob);
      }
    } catch { /* Total FV is hidden without v1 */ }
    next = { ...g, officialBets };
  }

  return { state: "live", data: { profile, record, grades, next, tags } };
}
