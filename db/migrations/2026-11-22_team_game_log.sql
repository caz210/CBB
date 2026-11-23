-- Team game log: D-I games already in public.games, plus regular-season
-- results against non-D-I opponents (never projected, never on the slate).
-- The site game log and rest-day calc should read team_game_log, not only
-- model_grades (model_grades stays D-I vs D-I with a CZarp projection).
--
-- Requires PR #65 (2026-11-21_intraday_line_refresh.sql) to be applied first:
-- projections.edges_as_of and the 39-column slate_games must already exist.
-- This migration then CREATE OR REPLACE-s slate_games as a strict superset
-- of #65 (same 39 columns, same order) and excludes only games that have
-- neither a default-model projection nor any market_lines row. The Nov 1
-- openers have neither, so they stay off the slate and do not create a
-- Nov 1 slate day; a game with lines but a failed projection still appears.
-- Those openers still appear in team_game_log.
-- CREATE OR REPLACE (no DROP): an ordering mistake fails loudly and grants
-- are kept.
--
-- Does not touch picks, the ledger, or the hash chain.
-- One transaction: a mid-failure under psql -f rolls back completely
-- (no half-created table sitting under default grants).
-- Apply only after Scantron approves. The backfill script is not run here.

begin;

create table if not exists non_di_games (
    id              bigint generated always as identity primary key,
    sport_id        smallint not null references sports(id),
    league          text not null references leagues(code),
    season          int not null,
    date            date not null,                  -- America/Chicago game date
    home            text not null,
    away            text not null,
    home_team_id    bigint references teams(id),    -- null when that side is not D-I
    away_team_id    bigint references teams(id),
    tipoff          timestamptz,
    neutral         boolean not null default false,
    external_id     text not null,                  -- ESPN event id
    external_ids    jsonb not null default '{}'::jsonb,
    status          text not null default 'final'
                    check (status in ('scheduled','in_progress','final','postponed','canceled')),
    home_score      int,
    away_score      int,
    final_at        timestamptz,
    updated_at      timestamptz not null default now(),
    unique (league, external_id),
    check (num_nonnulls(home_team_id, away_team_id) = 1)
);
create index if not exists non_di_games_league_date_idx on non_di_games (league, date);
create index if not exists non_di_games_home_team_idx on non_di_games (home_team_id, date);
create index if not exists non_di_games_away_team_idx on non_di_games (away_team_id, date);

comment on table non_di_games is
  'Regular-season games against non-D-I opponents. Not projected and not on the D-I slate. Used by team_game_log for the team sheet Recent list and rest days.';

alter table non_di_games enable row level security;
drop policy if exists non_di_games_read on non_di_games;
create policy non_di_games_read on non_di_games
  for select to anon, authenticated using (true);

revoke all on table non_di_games from anon, authenticated;
grant select on table non_di_games to anon, authenticated;
revoke all on sequence non_di_games_id_seq from anon, authenticated;

drop view if exists team_game_log;
create view team_game_log with (security_invoker = true) as
with di as (
  select
    t.id as team_id,
    t.name as team_name,
    g.league,
    g.season,
    g.id as game_id,
    g.external_id,
    g.date as game_date,
    g.tipoff,
    g.final_at,
    g.status,
    g.neutral,
    case when t.id = g.home_team_id then g.away else g.home end as opponent,
    case
      when g.neutral then 'neutral'
      when t.id = g.home_team_id then 'home'
      else 'away'
    end as venue,
    case when t.id = g.home_team_id then g.home_score else g.away_score end as team_score,
    case when t.id = g.home_team_id then g.away_score else g.home_score end as opp_score,
    true as vs_di,
    mg.model_spread,
    mg.model_total,
    mg.vegas_spread,
    mg.vegas_total,
    mg.model_ats_side,
    mg.model_ou_side,
    mg.model_ats_result,
    mg.model_ou_result,
    mg.actual_ats_cover,
    mg.actual_ou
  from games g
  join teams t on t.id in (g.home_team_id, g.away_team_id)
  left join model_grades mg on mg.game_id = g.id
),
nondi as (
  select
    t.id as team_id,
    t.name as team_name,
    n.league,
    n.season,
    null::bigint as game_id,
    n.external_id,
    n.date as game_date,
    n.tipoff,
    n.final_at,
    n.status,
    n.neutral,
    case when t.id = n.home_team_id then n.away else n.home end as opponent,
    case
      when n.neutral then 'neutral'
      when t.id = n.home_team_id then 'home'
      else 'away'
    end as venue,
    case when t.id = n.home_team_id then n.home_score else n.away_score end as team_score,
    case when t.id = n.home_team_id then n.away_score else n.home_score end as opp_score,
    false as vs_di,
    null::numeric as model_spread,
    null::numeric as model_total,
    null::numeric as vegas_spread,
    null::numeric as vegas_total,
    null::text as model_ats_side,
    null::text as model_ou_side,
    null::text as model_ats_result,
    null::text as model_ou_result,
    null::text as actual_ats_cover,
    null::text as actual_ou
  from non_di_games n
  join teams t on t.id in (n.home_team_id, n.away_team_id)
)
select * from di
union all
select * from nondi;

comment on view team_game_log is
  'Per-team game log for the site Recent list: every stored D-I game (with CZarp grades when present) plus non-D-I results. Rest days and log completeness should use this view, not model_grades alone.';

grant select on team_game_log to anon, authenticated;


-- #65's 39-column slate_games, plus a filter: keep the row when a
-- default-model projection exists OR any market_lines row exists.
-- CREATE OR REPLACE (no DROP) keeps grants and fails if column order drifts.
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
       m.fetched_at                 as market_fetched_at
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
   or exists (select 1 from market_lines ml where ml.game_id = g.id);

grant select on slate_games to anon, authenticated;

commit;
