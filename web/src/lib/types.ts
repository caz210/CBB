// Row shapes of CZarp's Supabase views (/workspace/CBB/db/schema.sql):
//   slate_games, value_bets, ledger, pick_record, profiles, user_models.
// Shared by the live loaders (src/lib/data.ts) and the example fallback (example-data.ts).
import type { SportKey } from "./sports";
import type { ArchetypeTag } from "./archetypes";
import type { Market, MarketSource, PickTier, Tier } from "./labels";

// ---- view: slate_games -----------------------------------------------------
export type Archetypes = {
  home: ArchetypeTag[];
  away: ArchetypeTag[];
  /** Stored top-2 (pipeline extremity), used to pick the compact slate chips. */
  homeTop?: ArchetypeTag[];
  awayTop?: ArchetypeTag[];
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
  game: string; // 'Away @ Home' as stored; render titles with gameTitle() so neutral games read 'vs'
  /** games.neutral. Missing or null means a normal home game. */
  neutral?: boolean;
  /** slate_games.tv_rating, 0–100. Null/missing means unknown, not 0. */
  tv_rating?: number | null;
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
  reason?: string | null;
  proj_possessions?: number | null;
  upset_alert?: boolean;
  upset_reason?: string | null;
  /** game is final, or a pick on it has been graded */
  completed?: boolean;
  status?: string | null;
  /** When the game went final (timestamptz). */
  final_at?: string | null;
  /** True when final_at was inferred rather than reported. */
  final_at_estimated?: boolean;
  /** Final (or live) score from games.home_score / away_score; null until ESPN reports it. */
  home_score?: number | null;
  away_score?: number | null;
  /** games.external_id — the ESPN event id. Used only to prefill the Live Line clock. */
  external_id?: string | null;
  /** extra detail for the game sheet; fields are null when not published */
  insight?: GameInsight;
  /** Raw |fair − price| ÷ Vegas total (not official-pick sort). */
  rawEdge?: number | null;
  rawEdgeMarket?: Market | null;
  /** CZarp's official total (the totals model). Every total number on a card comes from here.
   *  Model names and the team-score-sum total are not shipped to the client. */
  czarp_total?: number | null;
  /** Over probability from the same official totals model (insight.over_prob is the default model's). */
  czarp_over_prob?: number | null;
  coachLocks?: import("./coach").NewCoachLock[];
  skip_reasons_spread?: string[] | null;
  skip_reasons_total?: string[] | null;
  skip_reason_spread?: string | null;
  skip_reason_total?: string | null;
  /** Stored on slate_games (fractions, like edge_at_post_pct). Absent until the columns are live. */
  edge_now_pct_spread?: number | null;
  edge_vs_open_pct_spread?: number | null;
  edge_now_pct_total?: number | null;
  edge_vs_open_pct_total?: number | null;
  verdict_spread?: "official" | "low_value" | null;
  verdict_total?: "official" | "low_value" | null;
  /** Stored booleans from the pipeline. The ladder (lib/verdicts.ts) reads the stored edges instead. */
  lean_spread?: boolean | null;
  lean_total?: boolean | null;
  /** slate_games.market_fetched_at, once the pipeline adds it (czarp-v2). */
  market_fetched_at?: string | null;
  /** slate_games.edges_as_of (coalesce of projections.edges_as_of / created_at), once the pipeline adds it. */
  edges_as_of?: string | null;
  /** Real stored stamp for the line the ladder ratings use (lib/linesAsOf.ts); null → omit "as of". */
  lines_as_of?: string | null;
  lines_as_of_source?: import("./linesAsOf").LinesAsOfSource | null;
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
  reason: string | null;
  proj_possessions: number | null;
  upset_alert?: boolean;
  upset_reason?: string | null;
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
  line_edge_pct: number; // |fair − price| ÷ Vegas total (fraction); the headline Edge %
  kelly_quarter_units: number | null; // 1u = 1% of bankroll
  pick_tier?: PickTier; // 'official' (staked) or 'track' (paper-only 1u)
  logged_at?: string | null; // timestamptz the pick was posted
  /** Market line at post (picks.line_or_price). */
  posted_line?: number | null;
  /** American odds at post (picks.price). */
  posted_odds?: number | null;
  pick_hash?: string | null;
  pick_id?: string | null;
  /** official_picks_locked / value_bets locked post-time fields. */
  fair_at_post?: number | null;
  line_at_post?: number | null;
  total_at_post?: number | null;
  open_line?: number | null;
  /** Fraction (0.0173 = 1.73%), not a percent integer. */
  edge_at_post_pct?: number | null;
  edge_vs_open_pct?: number | null;
  posted_at?: string | null;
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
