// Row shapes of CZarp's Supabase views (/workspace/CBB/db/schema.sql):
//   slate_games, value_bets, ledger, pick_record, profiles, user_models.
// Shared by the live loaders (src/lib/data.ts) and the example fallback (example-data.ts).
import type { SportKey } from "./sports";
import type { ArchetypeName } from "./archetypes";
import type { Market, MarketSource, Tier } from "./labels";

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
  line_edge_pct: number; // |model - line| / market total; 0.08 = 8%
  kelly_quarter_units: number | null; // 1u = 1% of bankroll
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
