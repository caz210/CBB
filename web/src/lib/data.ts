// Live data loaders for Server Components. Reads CZarp's Supabase views with the anon key
// (+ the visitor's session cookies, so RLS decides what each tier sees).
// Every loader returns a tagged result so pages can render a clear empty state instead of
// silently falling back to example numbers.
import { createSupabaseServerClient } from "./supabase/server";
import type { Sport } from "./sports";
import type { Archetypes, LedgerRow, PickRecord, SlateGame, ValueBet } from "./types";
import type { ArchetypeName } from "./archetypes";
import type { Market, MarketSource } from "./labels";

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
  return {
    home: strs(a.home) as ArchetypeName[],
    away: strs(a.away) as ArchetypeName[],
    home_tentative: Boolean(a.home_tentative),
    away_tentative: Boolean(a.away_tentative),
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
  };
}

// ---- /[sport]/slate ---------------------------------------------------------
export type SlateData = {
  date: string;
  games: SlateGame[];
  bets: ValueBet[];
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
  let prevDate: string | null = null;
  let nextDate: string | null = null;
  if (games.length) {
    const b = await sb.from("value_bets").select("*").eq("sport", sport.key).in("game_id", games.map((x) => x.game_id));
    if (b.error) return fail("value_bets", b.error);
    bets = (b.data ?? []).map(toValueBet).sort((a, z) => z.line_edge_pct - a.line_edge_pct);
  } else {
    const [n, p] = await Promise.all([
      sb.from("slate_games").select("game_date").eq("sport", sport.key).gt("game_date", date).order("game_date").limit(1),
      sb.from("slate_games").select("game_date").eq("sport", sport.key).lt("game_date", date).order("game_date", { ascending: false }).limit(1),
    ]);
    nextDate = n.data?.[0] ? str(n.data[0].game_date) : null;
    prevDate = p.data?.[0] ? str(p.data[0].game_date) : null;
  }
  return { state: "live", data: { date, games, bets, prevDate, nextDate } };
}

// ---- /[sport]/results -------------------------------------------------------
export const RANGES = { season: { label: "Season", days: null }, "30d": { label: "Last 30d", days: 30 }, "7d": { label: "Last 7d", days: 7 } } as const;
export type RangeKey = keyof typeof RANGES;

export type ResultsData = {
  ledger: LedgerRow[];
  gameNames: Record<number, string>;
  /** per-market season record (pick_record, latest season, all model versions combined) */
  byMarket: PickRecord[];
  season: number | null;
  summary: {
    record: string; wins: number; losses: number; pushes: number; pending: number; graded: number;
    winPct: number | null; roi: number | null; units: number; avgClv: number | null;
  };
  cumulativeUnits: number[];
};

const LEDGER_LIMIT = 1000;
const NAME_LOOKUP_LIMIT = 200;

export async function loadResults(sport: Sport, range: RangeKey): Promise<Loaded<ResultsData>> {
  const sb = await createSupabaseServerClient();
  if (!sb) return { state: "unconfigured" };

  let lq = sb.from("ledger").select("*").eq("sport", sport.key);
  const days = RANGES[range].days;
  if (days) lq = lq.gte("game_date", shiftDate(todayCT(), -days));
  const [l, pr] = await Promise.all([
    lq.order("game_date", { ascending: false }).order("game_id", { ascending: false }).limit(LEDGER_LIMIT),
    sb.from("pick_record").select("*").eq("league", sport.league),
  ]);
  if (l.error) return fail("ledger", l.error);
  if (pr.error) return fail("pick_record", pr.error);

  const ledger = (l.data ?? []).map(toLedgerRow);

  // game names for the rows we show
  const ids = [...new Set(ledger.slice(0, NAME_LOOKUP_LIMIT).map((r) => r.game_id))];
  const gameNames: Record<number, string> = {};
  if (ids.length) {
    const n = await sb.from("slate_games").select("game_id, game").in("game_id", ids);
    for (const r of n.data ?? []) gameNames[Number(r.game_id)] = str(r.game);
  }

  // pick_record: latest season, combine model versions per market
  const recs = (pr.data ?? []) as Row[];
  const season = recs.reduce<number | null>((m, r) => (num(r.season) != null && (m == null || num(r.season)! > m) ? num(r.season) : m), null);
  const byMarketMap = new Map<string, PickRecord & { clvW: number; clvN: number }>();
  for (const r of recs) {
    if (season != null && num(r.season) !== season) continue;
    const k = str(r.market);
    const cur = byMarketMap.get(k) ?? {
      league: str(r.league), season: season ?? 0, model_version: "all", market: k as Market,
      wins: 0, losses: 0, pushes: 0, pending: 0, staked: 0, pnl: 0, avg_clv: null, clvW: 0, clvN: 0,
    };
    const w = num(r.wins) ?? 0, lo = num(r.losses) ?? 0, pu = num(r.pushes) ?? 0;
    cur.wins += w; cur.losses += lo; cur.pushes += pu; cur.pending += num(r.pending) ?? 0;
    cur.staked += num(r.staked) ?? 0; cur.pnl += num(r.pnl) ?? 0;
    const clv = num(r.avg_clv);
    if (clv != null && w + lo + pu > 0) { cur.clvW += clv * (w + lo + pu); cur.clvN += w + lo + pu; }
    byMarketMap.set(k, cur);
  }
  const order: Record<string, number> = { spread: 0, total: 1, moneyline: 2 };
  const byMarket: PickRecord[] = [...byMarketMap.values()]
    .map(({ clvW, clvN, ...r }) => ({ ...r, avg_clv: clvN ? clvW / clvN : null }))
    .sort((a, z) => (order[a.market] ?? 9) - (order[z.market] ?? 9));

  // KPIs from the ledger (respects the selected range); units = 1% of bankroll at pick time
  let wins = 0, losses = 0, pushes = 0, pending = 0, units = 0, staked = 0;
  for (const r of ledger) {
    if (r.result == null) { pending++; continue; }
    if (r.result === "W") wins++; else if (r.result === "L") losses++; else pushes++;
    units += r.profit_units ?? 0;
    staked += r.stake_units ?? 0;
  }
  const clvAll = byMarket.reduce((a, r) => (r.avg_clv != null ? { s: a.s + r.avg_clv * (r.wins + r.losses + r.pushes), n: a.n + r.wins + r.losses + r.pushes } : a), { s: 0, n: 0 });

  const cumulativeUnits = [0];
  for (const r of [...ledger].reverse()) {
    if (r.result == null) continue;
    cumulativeUnits.push(Math.round((cumulativeUnits[cumulativeUnits.length - 1] + (r.profit_units ?? 0)) * 100) / 100);
  }

  return {
    state: "live",
    data: {
      ledger, gameNames, byMarket, season, cumulativeUnits,
      summary: {
        record: `${wins}-${losses}-${pushes}`, wins, losses, pushes, pending, graded: wins + losses + pushes,
        winPct: wins + losses ? wins / (wins + losses) : null,
        roi: staked ? units / staked : null,
        units: Math.round(units * 100) / 100,
        avgClv: clvAll.n ? clvAll.s / clvAll.n : null,
      },
    },
  };
}

function fail(view: string, error: { message: string; code?: string }): { state: "error" } {
  console.error(`[supabase] ${view}: ${error.code ?? ""} ${error.message}`);
  return { state: "error" };
}
