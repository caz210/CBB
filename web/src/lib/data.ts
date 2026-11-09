// Live data loaders for Server Components. Reads CZarp's Supabase views with the anon key
// (all picks are public; session cookies only matter for user-owned data).
// Every loader returns a tagged result so pages can render a clear empty state instead of
// silently falling back to example numbers.
import { createSupabaseServerClient } from "./supabase/server";
import type { Sport } from "./sports";
import type { Archetypes, GameInsight, LedgerRow, SlateGame, TierRecord, ValueBet } from "./types";
import { portfolioRisk, type PortfolioRisk } from "./portfolio";
import { parseTagList } from "./archetypes";
import { isPickTier, type Market, type MarketSource, type PickTier } from "./labels";

export type Loaded<T> =
  | { state: "live"; data: T }
  | { state: "unconfigured" }
  | { state: "error" };

type Row = Record<string, unknown>;

const num = (v: unknown): number | null => (v == null || v === "" ? null : Number.isFinite(Number(v)) ? Number(v) : null);
const str = (v: unknown): string => (v == null ? "" : String(v));
const strs = (v: unknown): string[] => (Array.isArray(v) ? v.filter((x) => typeof x === "string") : []);

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
  };
}

function toValueBet(r: Row): ValueBet {
  return {
    sport: str(r.sport) as ValueBet["sport"],
    game_id: Number(r.game_id),
    market: str(r.market) as Market,
    side: str(r.side),
    model_prob: num(r.model_prob),
    market_prob: num(r.market_prob) ?? 0,
    prob_edge: num(r.prob_edge) ?? 0,
    line_edge_pct: num(r.line_edge_pct) ?? 0,
    kelly_quarter_units: num(r.kelly_quarter_units),
    pick_tier: isPickTier(r.tier) ? r.tier : "official",
    logged_at: r.logged_at ? str(r.logged_at) : null,
  };
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
  if (games.length) {
    const b = await sb.from("value_bets").select("*").eq("sport", sport.key).in("game_id", games.map((x) => x.game_id));
    if (b.error) return fail("value_bets", b.error);
    const all = (b.data ?? []).map(toValueBet).sort((a, z) => z.line_edge_pct - a.line_edge_pct);
    bets = all.filter((x) => x.pick_tier === "official");
    // Completed = games.status 'final' or any graded pick in the ledger. Best-effort: a failure here
    // just means nothing is marked completed.
    const ids = games.map((x) => x.game_id);
    const [st, gr] = await Promise.all([
      sb.from("games").select("id,status").in("id", ids),
      sb.from("ledger").select("game_id,result").in("game_id", ids).not("result", "is", null),
    ]);
    const done = new Set<number>();
    for (const r of st.data ?? []) if (str(r.status) === "final") done.add(Number(r.id));
    for (const r of gr.data ?? []) done.add(Number(r.game_id));
    for (const x of games) x.completed = done.has(x.game_id);
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
  return { state: "live", data: { date, games, bets, tracked, prevDate, nextDate } };
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
  | { kind: "today"; game: SlateGame; bet: ValueBet }
  | { kind: "graded"; when: "yesterday" | "latest"; row: LedgerRow; game: string | null };

/** Top official play today (highest line edge); else yesterday's top official pick with its result;
 *  else the latest graded official pick. Tracked picks are never used. */
export async function loadHero(sport: Sport): Promise<Loaded<Hero | null>> {
  const sb = await createSupabaseServerClient();
  if (!sb) return { state: "unconfigured" };
  const today = todayCT();
  const g = await sb.from("slate_games").select("*").eq("sport", sport.key).eq("game_date", today);
  if (g.error) return fail("slate_games", g.error);
  const games = (g.data ?? []).map(toSlateGame);
  if (games.length) {
    const b = await sb.from("value_bets").select("*").eq("sport", sport.key).eq("tier", "official")
      .in("game_id", games.map((x) => x.game_id)).order("line_edge_pct", { ascending: false }).limit(1);
    if (b.error) return fail("value_bets", b.error);
    const bet = b.data?.[0] ? toValueBet(b.data[0]) : null;
    const game = bet && games.find((x) => x.game_id === bet.game_id);
    if (bet && game) return { state: "live", data: { kind: "today", game, bet } };
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
    winPct: number | null; roi: number | null; units: number; avgClv: number | null;
    beatClose: number; clvN: number;
  };
  risk: PortfolioRisk;
  cumulativeUnits: number[];
};

const LEDGER_LIMIT = 1000;
const NAME_LOOKUP_LIMIT = 200;

/** Units for a ledger row: official = ¼-Kelly profit_units; tracked = flat 1u result. */
export const rowUnits = (r: LedgerRow) => (r.pick_tier === "track" ? r.flat_units ?? r.profit_units : r.profit_units);

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

  // KPIs from the ledger (respects range and tier)
  let wins = 0, losses = 0, pushes = 0, pending = 0, units = 0, staked = 0, clvS = 0, clvN = 0, beatClose = 0;
  for (const r of ledger) {
    if (r.result == null) { pending++; continue; }
    if (r.result === "W") wins++; else if (r.result === "L") losses++; else pushes++;
    units += rowUnits(r) ?? 0;
    staked += tier === "track" ? 1 : r.stake_units ?? 0;
    if (r.clv != null) { clvS += r.clv; clvN++; if (r.clv > 0) beatClose++; }
  }

  const cumulativeUnits = [0];
  for (const r of [...ledger].reverse()) {
    if (r.result == null) continue;
    cumulativeUnits.push(Math.round((cumulativeUnits[cumulativeUnits.length - 1] + (rowUnits(r) ?? 0)) * 100) / 100);
  }

  return {
    state: "live",
    data: {
      tier, ledger, gameNames, tipoffs, byMarket, season, cumulativeUnits,
      risk: portfolioRisk(ledger),
      summary: {
        record: `${wins}-${losses}-${pushes}`, wins, losses, pushes, pending, graded: wins + losses + pushes,
        winPct: wins + losses ? wins / (wins + losses) : null,
        roi: staked ? units / staked : null,
        units: Math.round(units * 100) / 100,
        avgClv: clvN ? clvS / clvN : null,
        beatClose, clvN,
      },
    },
  };
}

function fail(view: string, error: { message: string; code?: string }): { state: "error" } {
  console.error(`[supabase] ${view}: ${error.code ?? ""} ${error.message}`);
  return { state: "error" };
}
