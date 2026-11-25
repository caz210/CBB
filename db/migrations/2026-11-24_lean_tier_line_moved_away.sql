-- CZarp LEAN tier for sides + LINE MOVED AWAY flag (approved by Christian 2026-11-24, 8:55 PM CT).
-- Analysis: czarp-notes/sides-analysis-2026-11-24.md. Code: daily_run.py (LEAN_*, line_moved_away), czarp/supabase_writer.py.
--
-- 1. picks.tier gains 'lean' = CZarp Leans: sides with 2.5% <= edge vs the 7 AM line < 4%, same side as at the open,
--    DK line not moved away from CZarp since the open, plus NEW COACH-paused sides that would otherwise be official or
--    lean. Flat 1u (units_pnl), kelly_stake 0, NEVER part of the official record or the paper bankroll.
-- 2. picks.new_coach (boolean, default false): true on a lean pick held by the NEW COACH pause.
--    Note: this column is NOT added to picks_guard()/picks_immutable (this migration does not touch the trigger).
--    The marker is also sealed in the CSV ledger's hash chain (rule text ends in '+new coach').
-- 3. projections.line_moved_away / line_moved_away_pts: display flag, never a play. True when CZarp's side gap vs the
--    current DK spread is >= 4 pts and the spread moved >= 1.5 pts AWAY from CZarp's side since the open
--    (line_moved_away_pts = picked-side spread now − at open; > 0 = away, < 0 = toward). Written by the 7 AM run and by
--    the intraday refresh whenever the spread inputs change.
-- 4. slate_games: the same 39 columns (CREATE OR REPLACE, no DROP; grants kept) + line_moved_away, line_moved_away_pts
--    appended at the END.
-- 5. New views: lean_picks (one row per lean pick) and lean_record (W-L-P + flat units per season).
--
-- How every official-record view stays official-only (checked against prod 2026-11-24):
--   slate_games.verdict_*, team_model_record, team_game_log, model_grades -> pk.tier = 'official' (unchanged)
--   official_picks_locked, value_bets, ledger -> carry pk.tier; readers must filter tier = 'official'
--   tier_record, pick_record -> GROUP BY tier, so lean is its own row and never mixes into official
--   (the site reads tier='official' via the ledger view and maps tier client-side: see the UI spec; until the site
--    knows 'lean', the pipeline does NOT mirror lean picks — CZARP_SYNC_LEAN stays off).
--
-- Additive only. Does not touch the picks_immutable trigger / picks_guard(), the hash chain, existing pick rows
-- (ADD COLUMN ... DEFAULT is a catalog change: no row is updated and no trigger fires) or official_picks_locked.
-- Requires 2026-11-21_intraday_line_refresh.sql and 2026-11-22_team_game_log.sql (the 39-column slate_games).
-- Apply only after Scantron approves. Rollout: apply this -> deploy the site change that reads 'lean' -> set
-- CZARP_SYNC_LEAN=1 for the 7 AM run. Until this is applied the pipeline writes projections without the two flag
-- columns (supabase_writer.upsert_projections fallback) and keeps lean picks local.
begin;

alter table picks drop constraint if exists picks_tier_chk;
alter table picks add constraint picks_tier_chk check (tier in ('official','track','lean'));

alter table picks add column if not exists new_coach boolean not null default false;
comment on column picks.new_coach is
  'Lean tier only: true when the side was held by the NEW COACH pause (would otherwise be official or lean). Not covered by picks_guard.';

alter table projections
  add column if not exists line_moved_away boolean,
  add column if not exists line_moved_away_pts numeric(4,1);
comment on column projections.line_moved_away is
  'Display flag, never a play: CZarp side gap vs the current DK spread >= 4 pts AND the spread moved >= 1.5 pts away from CZarp since the open. NULL without a model margin or open/current spread.';
comment on column projections.line_moved_away_pts is
  'Picked-side DK spread now minus at open, in points (> 0 = moved away from CZarp, < 0 = toward).';

create or replace view slate_games with (security_invoker = true) as
select s.code                       as sport,
       g.date                       as game_date,
       g.id                         as game_id,
       g.home, g.away,
       g.tipoff,
       g.away || ' @ ' || g.home    as game,
       p.proj_home, p.proj_away,
       p.spread                     as proj_spread,
       p.total                      as proj_total,
       p.home_win_prob, p.home_cover_prob,
       p.archetype_tags             as archetypes,
       p.reason,
       p.proj_possessions,
       p.upset_alert,
       p.upset_reason,
       p.skip_reasons_spread,
       p.skip_reasons_total,
       p.skip_reason_spread,
       p.skip_reason_total,
       p.edge_now_pct_spread,
       p.edge_vs_open_pct_spread,
       p.edge_now_pct_total,
       p.edge_vs_open_pct_total,
       p.tv_rating,
       case when off_sp.game_id is not null then 'official' else 'low_value' end as verdict_spread,
       case when off_tot.game_id is not null then 'official' else 'low_value' end as verdict_total,
       (off_sp.game_id is null
          and p.edge_now_pct_spread is not null and p.edge_now_pct_spread >= 0.04
          and p.edge_vs_open_pct_spread is not null and p.edge_vs_open_pct_spread < 0.04) as lean_spread,
       (off_tot.game_id is null
          and p.edge_now_pct_total is not null and p.edge_now_pct_total >= 0.03
          and p.edge_vs_open_pct_total is not null and p.edge_vs_open_pct_total < 0.03) as lean_total,
       m.spread                     as market_spread,
       m.total                      as market_total,
       m.source                     as market_source,
       g.status,
       g.final_at,
       g.final_at_estimated,
       -- appended (2026-11-21 intraday refresh / PR #65); existing columns keep their names, types and order
       coalesce(p.edges_as_of, p.created_at) as edges_as_of,
       m.fetched_at                 as market_fetched_at,
       -- appended (2026-11-24 lean tier / line moved away); existing 39 columns keep their names, types and order
       p.line_moved_away,
       p.line_moved_away_pts
from games g
join sports s on s.id = g.sport_id
left join lateral (
  select pr.* from projections pr
  join model_configs mc on mc.model_version = pr.model_version and mc.is_default and mc.league = g.league
  where pr.game_id = g.id order by pr.created_at desc limit 1) p on true
left join lateral (
  select ml.* from market_lines ml
  where ml.game_id = g.id and ml.source = 'vegas' and ml.line_type = 'current'
  order by ml.fetched_at desc limit 1) m on true
left join lateral (
  select pk.game_id from official_picks_locked pk
  where pk.game_id = g.id and pk.market = 'spread' and pk.tier = 'official'
  limit 1) off_sp on true
left join lateral (
  select pk.game_id from official_picks_locked pk
  where pk.game_id = g.id and pk.market = 'total' and pk.tier = 'official'
  limit 1) off_tot on true
where p.game_id is not null
   or exists (select 1 from market_lines ml where ml.game_id = g.id);;

grant select on slate_games to anon, authenticated;

-- One row per CZarp Lean pick (never official). The site's Lean list / card label reads this.
create or replace view lean_picks with (security_invoker = true) as
select s.code                                    as sport,
       g.date                                    as game_date,
       pk.game_id,
       g.tipoff,
       g.home, g.away,
       pk.pick_key                               as pick_id,
       pk.bet                                    as pick,           -- e.g. 'Pittsburgh -2.5'
       coalesce(pk.line_at_post, pk.line_or_price) as line,
       pk.price                                  as odds,
       pk.edge                                   as edge_pct,       -- |CZarp − line| / total at post (fraction)
       pk.edge_vs_open                           as edge_vs_open_pct,
       pk.fair_at_post                           as czarp_line,     -- CZarp's number for the picked side
       pk.open_line,
       pk.new_coach,
       case pk.result when 'win' then 'W' when 'loss' then 'L' when 'push' then 'P' when 'void' then 'V' end as result,
       pk.units_pnl                              as units,          -- flat 1u result (null = pending)
       pk.clv,
       pk.created_at,
       pk.pick_hash
from picks pk
join games g  on g.id = pk.game_id
join sports s on s.id = pk.sport_id
where pk.tier = 'lean';

-- The separate CZarp Lean record (flat 1u). Never combined with the official record.
create or replace view lean_record with (security_invoker = true) as
select pk.league, pk.season,
       count(*) filter (where pk.result = 'win')  as wins,
       count(*) filter (where pk.result = 'loss') as losses,
       count(*) filter (where pk.result = 'push') as pushes,
       count(*) filter (where pk.result is null)  as pending,
       round(coalesce(sum(pk.units_pnl), 0), 2)   as units,
       round(sum(pk.units_pnl) / nullif(count(*) filter (where pk.result in ('win','loss','push')), 0), 4) as roi,
       round(avg(pk.clv), 2)                      as avg_clv,
       count(*) filter (where pk.new_coach)       as new_coach_picks,
       round(coalesce(sum(pk.units_pnl) filter (where pk.new_coach), 0), 2) as new_coach_units
from picks pk
where pk.tier = 'lean' and pk.result is distinct from 'void'
group by pk.league, pk.season;

comment on view lean_picks is 'CZarp Leans (tier=lean): flat 1u, not official, not staked. Never part of the official record.';
comment on view lean_record is 'Season record of CZarp Leans in flat units. Separate from the official record (tier_record / ledger tier=official).';

revoke all on lean_picks, lean_record from anon, authenticated;
grant select on lean_picks, lean_record to anon, authenticated;

commit;
