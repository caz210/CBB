// ⚠️ EXAMPLE DATA ONLY: fictional teams, made-up numbers. No KenPom stats, no real CZarp output.
// Used by the landing-page preview and My Models, and as typed sample rows. The Slate and Results
// pages read the live Supabase views instead (src/lib/data.ts) and never fall back to these numbers.
import type { Tier } from "./labels";
import type {
  Archetypes, LedgerRow, ModelFeature, PickRecord, Profile, SlateGame, UserModel, ValueBet,
} from "./types";
export type { Archetypes, LedgerRow, ModelFeature, PickRecord, Profile, SlateGame, UserModel, ValueBet };

/** Saved-model limit per tier (enforced server-side too; see README). */
export const MODEL_LIMIT: Record<Tier, number> = { free: 1, pro: Infinity, syndicate: Infinity };

// ===========================================================================
const D = "2026-11-02";
const t = (hhmmCT: string) => `${D}T${hhmmCT}:00-06:00`;
const game = (away: string, home: string) => `${away} @ ${home}`;

export const SLATE_DATE = D;

export const EXAMPLE_SLATE: SlateGame[] = [
  {
    sport: "cbb", game_date: D, game_id: 900101, away: "Ridgeview State", home: "Coastal Pines",
    game: game("Ridgeview State", "Coastal Pines"), tipoff: t("18:00"),
    proj_away: 74.6, proj_home: 71.2, proj_spread: 3.4, proj_total: 145.8,
    home_win_prob: 0.38, home_cover_prob: 0.44,
    archetypes: { away: ["Addicted to 3s", "Don't Blink"], home: ["Lock Down", "Open Arc"], away_tentative: false, home_tentative: true, tentative: true,
      notes: ["Addicted to 3s vs Open Arc: exploit"] },
    market_spread: 1.5, market_total: 147.5, market_source: "vegas",
  },
  {
    sport: "cbb", game_date: D, game_id: 900102, away: "Northgate Tech", home: "Harbor University",
    game: game("Northgate Tech", "Harbor University"), tipoff: t("18:30"),
    proj_away: 61.8, proj_home: 76.4, proj_spread: -14.6, proj_total: 138.2,
    home_win_prob: 0.9, home_cover_prob: 0.51,
    archetypes: { away: ["Careless", "Sieve"], home: ["Lock Down", "Turnover Machine"], away_tentative: false, home_tentative: false, tentative: false,
      notes: ["Turnover Machine vs Careless"] },
    market_spread: -14.5, market_total: 136.5, market_source: "vegas",
  },
  {
    sport: "cbb", game_date: D, game_id: 900103, away: "Mesa Valley", home: "St. Aldric",
    game: game("Mesa Valley", "St. Aldric"), tipoff: t("19:00"),
    proj_away: 70.3, proj_home: 69.1, proj_spread: 1.2, proj_total: 139.4,
    home_win_prob: 0.46, home_cover_prob: 0.53,
    archetypes: { away: ["Board Crashers", "Free Throw Merchants"], home: ["Addicted to 3s", "Glass Leak"], away_tentative: true, home_tentative: false, tentative: true,
      notes: ["Board Crashers vs Glass Leak: exploit"] },
    market_spread: 2.5, market_total: 134.5, market_source: "vegas",
  },
  {
    sport: "cbb", game_date: D, game_id: 900104, away: "Lakeshore A&M", home: "Granite Falls",
    game: game("Lakeshore A&M", "Granite Falls"), tipoff: t("20:00"),
    proj_away: 72.0, proj_home: 73.4, proj_spread: -1.4, proj_total: 145.4,
    home_win_prob: 0.55, home_cover_prob: 0.61,
    archetypes: { away: ["Arc Wall", "No Rebounds for You"], home: ["Don't Blink", "Passive D"], away_tentative: false, home_tentative: false, tentative: false, notes: [] },
    market_spread: 2.5, market_total: 141.5, market_source: "vegas",
  },
  {
    sport: "cbb", game_date: D, game_id: 900105, away: "Desert Sun", home: "Pacific Crest",
    game: game("Desert Sun", "Pacific Crest"), tipoff: t("21:00"),
    proj_away: 64.9, proj_home: 74.1, proj_spread: -9.2, proj_total: 139.0,
    home_win_prob: 0.8, home_cover_prob: 0.48,
    archetypes: { away: ["Board Crashers"], home: ["Addicted to 3s", "Lock Down"], away_tentative: true, home_tentative: false, tentative: true, notes: [] },
    market_spread: -9.5, market_total: 140.0, market_source: "vegas",
  },
];

export const EXAMPLE_VALUE_BETS: ValueBet[] = [
  { sport: "cbb", game_id: 900101, market: "spread", side: "Ridgeview State -1.5", model_prob: 0.565, market_prob: 0.5238, prob_edge: 0.0412, line_edge_pct: 0.0130, kelly_quarter_units: 2.16 },
  { sport: "cbb", game_id: 900103, market: "total", side: "Over 134.5", model_prob: 0.57, market_prob: 0.5098, prob_edge: 0.0602, line_edge_pct: 0.0364, kelly_quarter_units: 2.5 },
  { sport: "cbb", game_id: 900104, market: "moneyline", side: "Granite Falls ML", model_prob: 0.55, market_prob: 0.4444, prob_edge: 0.1056, line_edge_pct: 0.0276, kelly_quarter_units: 2.5 },
  { sport: "cbb", game_id: 900104, market: "spread", side: "Granite Falls +2.5", model_prob: 0.61, market_prob: 0.5238, prob_edge: 0.0862, line_edge_pct: 0.0276, kelly_quarter_units: 2.5 },
];

// Past games referenced by the ledger (stands in for a join to `games`).
export const EXAMPLE_GAME_NAMES: Record<number, string> = {
  900001: "Ridgeview State @ Coastal Pines",
  900002: "Mesa Valley @ Harbor University",
  900003: "Granite Falls @ St. Aldric",
  900004: "Northgate Tech @ Lakeshore A&M",
  900005: "Pacific Crest @ Desert Sun",
  900006: "Coastal Pines @ Northgate Tech",
  900007: "Harbor University @ Granite Falls",
  900008: "St. Aldric @ Mesa Valley",
  ...Object.fromEntries(EXAMPLE_SLATE.map((g) => [g.game_id, g.game])),
};

export const EXAMPLE_LEDGER: LedgerRow[] = [
  { sport: "cbb", game_date: "2026-11-02", game_id: 900101, pick: "Ridgeview State -1.5", line: -1.5, odds: -110, stake_units: 2.16, result: null, profit_units: null, is_paper: true },
  { sport: "cbb", game_date: "2026-11-01", game_id: 900001, pick: "Ridgeview State -3.5", line: -3.5, odds: -110, stake_units: 1.2, result: "W", profit_units: 1.09, is_paper: true },
  { sport: "cbb", game_date: "2026-11-01", game_id: 900002, pick: "Under 139.5", line: 139.5, odds: -108, stake_units: 0.8, result: "L", profit_units: -0.8, is_paper: true },
  { sport: "cbb", game_date: "2026-11-01", game_id: 900003, pick: "St. Aldric +6.5", line: 6.5, odds: -110, stake_units: 1.0, result: "W", profit_units: 0.91, is_paper: true },
  { sport: "cbb", game_date: "2026-10-31", game_id: 900004, pick: "Lakeshore A&M ML", line: 140, odds: 140, stake_units: 0.6, result: "L", profit_units: -0.6, is_paper: true },
  { sport: "cbb", game_date: "2026-10-31", game_id: 900005, pick: "Over 144.5", line: 144.5, odds: -105, stake_units: 0.9, result: "W", profit_units: 0.86, is_paper: true },
  { sport: "cbb", game_date: "2026-10-31", game_id: 900006, pick: "Coastal Pines -2", line: -2, odds: -110, stake_units: 1.1, result: "P", profit_units: 0, is_paper: true },
  { sport: "cbb", game_date: "2026-10-30", game_id: 900007, pick: "Harbor University -7.5", line: -7.5, odds: -110, stake_units: 1.3, result: "L", profit_units: -1.3, is_paper: true },
  { sport: "cbb", game_date: "2026-10-30", game_id: 900008, pick: "Mesa Valley -4", line: -4, odds: -112, stake_units: 0.7, result: "W", profit_units: 0.63, is_paper: true },
];

export const EXAMPLE_PICK_RECORD: PickRecord[] = [
  { league: "ncaam", season: 2027, model_version: "czarp-v2", market: "spread", wins: 24, losses: 21, pushes: 2, pending: 2, staked: 52.4, pnl: 1.0, avg_clv: 0.4 },
  { league: "ncaam", season: 2027, model_version: "czarp-v2", market: "total", wins: 12, losses: 13, pushes: 0, pending: 1, staked: 24.1, pnl: -0.82, avg_clv: -0.1 },
  { league: "ncaam", season: 2027, model_version: "czarp-v2", market: "moneyline", wins: 5, losses: 4, pushes: 0, pending: 1, staked: 8.9, pnl: 0.72, avg_clv: 0.2 },
];

/** Season summary derived from pick_record (computed client-side). */
export function summarize(rows: PickRecord[]) {
  const w = rows.reduce((a, r) => a + r.wins, 0);
  const l = rows.reduce((a, r) => a + r.losses, 0);
  const p = rows.reduce((a, r) => a + r.pushes, 0);
  const staked = rows.reduce((a, r) => a + r.staked, 0);
  const pnl = rows.reduce((a, r) => a + r.pnl, 0);
  return { record: `${w}-${l}-${p}`, winPct: w / (w + l), roi: pnl / staked, units: pnl, graded: w + l + p };
}

export const EXAMPLE_CUMULATIVE_UNITS = [0, 0.6, -0.4, -1.1, -0.3, 0.8, 0.2, -0.5, 0.4, 1.6, 1.1, 0.3, 1.0, 0.9];
export const EXAMPLE_CLV_BEAT_PCT = 0.54;

// ---- My Models -------------------------------------------------------------
export const BASELINE_FEATURES: ModelFeature[] = [
  { name: "adj_margin", label: "Adjusted efficiency margin (CZarp)", kind: "rating", weight: 0.45, enabled: true },
  { name: "pace", label: "Tempo / pace", kind: "style", weight: 0.1, enabled: true },
  { name: "three_rate", label: "3PA rate", kind: "style", weight: 0.08, enabled: true },
  { name: "experience", label: "Roster experience", kind: "context", weight: 0.07, enabled: true },
  { name: "home_court", label: "Home-court edge", kind: "context", weight: 0.12, enabled: true },
  { name: "archetype_matchups", label: "Archetype matchups", kind: "style", weight: 0.08, enabled: false },
  { name: "rest", label: "Rest days", kind: "context", weight: 0.05, enabled: false },
];

export const EXAMPLE_USER_MODELS: UserModel[] = [
  {
    id: "7d1e0c1a-0000-4000-8000-000000000001", user_id: "example-user", sport: "cbb", name: "Pace & Space v1",
    features: BASELINE_FEATURES.map((f) => (f.name === "pace" ? { ...f, weight: 0.2 } : f)),
    weights: { kelly_fraction: 0.25 }, created_at: "2026-10-28T20:15:00-05:00",
  },
];
