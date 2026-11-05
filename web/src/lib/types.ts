// Row shapes of CZarp's Supabase views (/workspace/CBB/db/schema.sql):
//   slate_games, value_bets, ledger, pick_record, profiles, user_models.
// Shared by the live loaders (src/lib/data.ts) and the example fallback (example-data.ts).
import type { SportKey } from "./sports";
import type { ArchetypeName } from "./archetypes";
import type { Market, MarketSource, PickTier, Tier } from "./labels";

// ---- view: slate_games -----------------------------------------------------
export type Archetypes = {
  home: ArchetypeName[];
  away: ArchetypeName[];
  home_tentative: boolean;
  away_tentative: boolean;
  tentative: boolean;
  notes: string[];
};

export type SlateGame = {
  sport: SportKey;
  game_date: string; // date (America/Chicago)
  game_id: number;
  home: string;
  away: string;
  tipoff: string | null; // timestamptz
  game: string; // 'Away @ Home'
  // null when the default model has no projection for the game yet
  proj_home: number | null;
  proj_away: number | null;
  proj_spread: number | null; // home line notation: -5.5 = home favored by 5.5
  proj_total: number | null;
  home_win_prob: number | null;
  home_cover_prob: number | null;
  archetypes: Archetypes;
  market_spread: number | null;
  market_total: number | null;
  market_source: MarketSource | null;
  /** game is final, or a pick on it has been graded */
  completed?: boolean;
  /** extra detail for the game sheet; fields are null when not published */
  insight?: GameInsight;
};
export type GameInsight = {
  proj_home: number | null;
  proj_away: number | null;
  home_win_prob: number | null;
  home_cover_prob: number | null;
  over_prob: number | null;
  neutral: boolean;
  open_spread: number | null; // Vegas, home-line notation
  cur_spread: number | null;
  open_total: number | null;
  cur_total: number | null;
  reason: string | null; // projections.reason (may not exist yet)
  proj_possessions: number | null; // projections.proj_possessions (may not exist yet)
};

// ---- view: value_bets ------------------------------------------------------
export type ValueBet = {
  sport: SportKey;
  game_id: number;
  market: Market;
  side: string;
  model_prob: number | null;
  market_prob: number; // implied by price taken (incl. vig)
  prob_edge: number; // model_prob - market_prob
  line_edge_pct: number; // |CZarp line - Vegas line| / Vegas line; drives VALUE/EDGE/HOT tiers
  kelly_quarter_units: number | null; // 1u = 1% of bankroll
  pick_tier?: PickTier; // 'official' (staked) or 'track' (paper-only 1u)
  logged_at?: string | null; // timestamptz the pick was posted
};

// ---- view: ledger ----------------------------------------------------------
export type LedgerRow = {
  sport: SportKey;
  game_date: string;
  game_id: number;
  pick: string;
  line: number | null;
  odds: number | null;
  stake_units: number | null;
  result: "W" | "L" | "P" | null; // null = pending
  profit_units: number | null;
  is_paper: boolean;
  pick_tier?: PickTier;
  flat_units?: number | null; // 1u flat-stake result
  clv?: number | null; // closing line value, points
  created_at?: string | null;
  logged_at?: string | null; // posted at (immutable)
  source?: string;
  pick_hash?: string | null;
};

// ---- view: pick_record (aggregated per market) ------------------------------
export type PickRecord = {
  league: string;
  season: number;
  model_version: string;
  market: Market;
  wins: number;
  losses: number;
  pushes: number;
  pending: number;
  staked: number;
  pnl: number;
  avg_clv: number | null;
};

// ---- view: tier_record (per league/season/tier/market) ---------------------
export type TierRecord = {
  league: string; season: number; tier: PickTier; market: Market;
  wins: number; losses: number; pushes: number; pending: number;
  units: number; roi: number | null; avg_clv: number | null; beat_close: number; pnl_dollars: number | null;
};

// ---- tables: profiles / user_models ---------------------------------------
export type Profile = { user_id: string; tier: Tier; stripe_customer_id: string | null };
export type ModelFeature = { name: string; label: string; kind: "rating" | "style" | "context"; weight: number; enabled: boolean };
export type UserModel = {
  id: string;
  user_id: string;
  sport: SportKey;
  name: string;
  features: ModelFeature[];
  weights: Record<string, number>;
  created_at: string;
};
