/** URL slug for a team display name. Stable enough for /[sport]/team/[slug]. */
export function teamSlug(name: string) {
  return name
    .toLowerCase()
    .replace(/&/g, "and")
    .replace(/\./g, "")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

export function teamHref(sportKey: string, name: string) {
  return `/${sportKey}/team/${teamSlug(name)}`;
}

export type TeamProfile = {
  team_id: number;
  team_name: string;
  season: number;
  as_of: string | null;
  games_played: number | null;
  tentative: boolean;
  new_coach: boolean;
  new_coach_games: number | null;
  lock_sides_games: number | null;
  lock_totals_games: number | null;
  lock_sides_remaining: number | null;
  lock_totals_remaining: number | null;
  // percentiles only for four factors + style
  efg_pctile: number | null;
  d_efg_pctile: number | null;
  to_pct_pctile: number | null;
  d_to_pct_pctile: number | null;
  or_pct_pctile: number | null;
  opp_or_pct_pctile: number | null;
  ftr_pctile: number | null;
  d_ftr_pctile: number | null;
  tempo_pctile: number | null;
  fg3a_rate_pctile: number | null;
  fg3_pct_pctile: number | null;
  opp_fg3_pct_pctile: number | null;
  ft_pct_pctile: number | null;
  // efficiency (pts/100) — not KenPom ranks; no percentile columns exist
  adj_oe: number | null;
  adj_de: number | null;
  hca: number | null;
  tags: unknown;
  tags_all: unknown;
};

export type TeamModelRecord = {
  team_id: number;
  team_name: string;
  season: number;
  model_ats_wins: number;
  model_ats_losses: number;
  model_ats_pushes: number;
  model_ou_wins: number;
  model_ou_losses: number;
  model_ou_pushes: number;
  official_wins: number;
  official_losses: number;
  official_pushes: number;
  official_ats_wins: number;
  official_ats_losses: number;
  official_ou_wins: number;
  official_ou_losses: number;
};

export type ModelGrade = {
  game_id: number;
  game_date: string;
  home: string;
  away: string;
  home_team_id: number | null;
  away_team_id: number | null;
  home_score: number | null;
  away_score: number | null;
  status: string | null;
  final_at: string | null;
  final_at_estimated: boolean;
  model_spread: number | null;
  model_total: number | null;
  vegas_spread: number | null;
  vegas_total: number | null;
  model_ats_side: string | null;
  model_ou_side: string | null;
  model_ats_result: string | null;
  model_ou_result: string | null;
  actual_ats_cover: string | null;
  actual_ou: string | null;
};
