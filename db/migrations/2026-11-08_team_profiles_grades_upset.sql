-- Team profiles, projection reason/possessions/upset alert, model grades (additive).
-- Does NOT touch picks, picks_immutable, ledger hash chain, or official/TRACK rules.

-- ── projections: overdue + upset fields ─────────────────────────────────────
alter table projections add column if not exists reason text;
alter table projections add column if not exists proj_possessions numeric(6,2);
alter table projections add column if not exists upset_alert boolean not null default false;
alter table projections add column if not exists upset_reason text;

-- ── team_profiles: one row per team per as_of date (stamped by daily run) ───
create table if not exists team_profiles (
    id              bigint generated always as identity primary key,
    team_id         bigint not null references teams(id) on delete cascade,
    sport_id        smallint not null references sports(id),
    league          text not null references leagues(code),
    season          int not null,
    as_of           date not null,                 -- America/Chicago date of the stamp
    games_played    int,
    tentative       boolean not null default true,
    -- Four Factors + extras (values). Percentiles are 0–100, 100 = better for that side.
    efg             numeric(6,2), efg_pctile numeric(5,1),
    d_efg           numeric(6,2), d_efg_pctile numeric(5,1),
    to_pct          numeric(6,2), to_pct_pctile numeric(5,1),
    d_to_pct        numeric(6,2), d_to_pct_pctile numeric(5,1),
    or_pct          numeric(6,2), or_pct_pctile numeric(5,1),
    opp_or_pct      numeric(6,2), opp_or_pct_pctile numeric(5,1),
    ftr             numeric(6,2), ftr_pctile numeric(5,1),
    d_ftr           numeric(6,2), d_ftr_pctile numeric(5,1),
    tempo           numeric(6,2), tempo_pctile numeric(5,1),
    fg3a_rate       numeric(6,2), fg3a_rate_pctile numeric(5,1),
    fg3_pct         numeric(6,2), fg3_pct_pctile numeric(5,1),
    opp_fg3_pct     numeric(6,2), opp_fg3_pct_pctile numeric(5,1),
    ft_pct          numeric(6,2), ft_pct_pctile numeric(5,1),
    -- efficiency header (no KenPom ranks stored)
    adj_oe          numeric(6,2),
    adj_de          numeric(6,2),
    hca             numeric(5,2),                  -- KenPom per-team home-court points
    -- tags: [{"name","kind","tentative"?}]  kind in strength|weakness|style  (top-2 chips)
    tags            jsonb not null default '[]'::jsonb,
    tags_all        jsonb not null default '[]'::jsonb,  -- every fired tag that day
    new_coach       boolean not null default false,
    new_coach_games int,
    created_at      timestamptz not null default now(),
    unique (team_id, as_of)
);
create index if not exists team_profiles_as_of_idx on team_profiles (league, as_of desc);
create index if not exists team_profiles_team_idx on team_profiles (team_id, as_of desc);

alter table team_profiles enable row level security;
drop policy if exists team_profiles_read on team_profiles;
create policy team_profiles_read on team_profiles for select to anon, authenticated using (true);
revoke insert, update, delete on team_profiles from anon, authenticated;
grant select on team_profiles to anon, authenticated;

-- latest profile per team (handy for the team page)
drop view if exists team_profile_latest;
create view team_profile_latest with (security_invoker = true) as
select distinct on (tp.team_id)
       t.name as team_name, tp.*
from team_profiles tp
join teams t on t.id = tp.team_id
order by tp.team_id, tp.as_of desc;

grant select on team_profile_latest to anon, authenticated;

-- ── model_grades: every final projected game graded vs Vegas (not just picks) ─
-- One row per (game, model_version). Built as a view over games + latest projection + lines.
drop view if exists model_grades;
create view model_grades with (security_invoker = true) as
with latest_proj as (
  select distinct on (pr.game_id, pr.model_version) pr.*
  from projections pr
  where pr.model_version = 'czarp-v2'
  order by pr.game_id, pr.model_version, pr.created_at desc
),
vegas as (
  select ml.game_id,
         max(ml.spread) filter (where ml.line_type = 'open')    as open_spread,
         max(ml.total)  filter (where ml.line_type = 'open')    as open_total,
         max(ml.spread) filter (where ml.line_type = 'current') as current_spread,
         max(ml.total)  filter (where ml.line_type = 'current') as current_total,
         max(ml.spread) filter (where ml.line_type = 'close')   as close_spread,
         max(ml.total)  filter (where ml.line_type = 'close')   as close_total
  from market_lines ml
  where ml.source = 'vegas'
  group by ml.game_id
)
select g.id as game_id,
       g.league, g.season, g.date as game_date,
       g.home, g.away, g.home_team_id, g.away_team_id,
       g.home_score, g.away_score,
       g.status,
       p.model_version,
       p.spread as model_spread,          -- home line notation
       p.total  as model_total,
       p.proj_home, p.proj_away,
       p.proj_possessions,
       p.upset_alert, p.upset_reason,
       p.reason,
       coalesce(v.close_spread, v.current_spread, v.open_spread) as vegas_spread,
       coalesce(v.close_total,  v.current_total,  v.open_total)  as vegas_total,
       v.open_spread, v.open_total,
       v.current_spread, v.current_total,
       v.close_spread, v.close_total,
       -- model side of the Vegas spread: which team does the model prefer vs the market line?
       case
         when coalesce(v.close_spread, v.current_spread, v.open_spread) is null then null
         when ( - p.spread )  -- model home margin
              + coalesce(v.close_spread, v.current_spread, v.open_spread) > 0 then 'home'
         when ( - p.spread )
              + coalesce(v.close_spread, v.current_spread, v.open_spread) < 0 then 'away'
         else 'push'
       end as model_ats_side,
       case
         when coalesce(v.close_total, v.current_total, v.open_total) is null then null
         when p.total > coalesce(v.close_total, v.current_total, v.open_total) then 'over'
         when p.total < coalesce(v.close_total, v.current_total, v.open_total) then 'under'
         else 'push'
       end as model_ou_side,
       -- grade once final
       case when g.status = 'final' and g.home_score is not null and g.away_score is not null
                 and coalesce(v.close_spread, v.current_spread, v.open_spread) is not null then
         case
           when (g.home_score - g.away_score)
                + coalesce(v.close_spread, v.current_spread, v.open_spread) > 0 then 'home'
           when (g.home_score - g.away_score)
                + coalesce(v.close_spread, v.current_spread, v.open_spread) < 0 then 'away'
           else 'push'
         end
       end as actual_ats_cover,
       case when g.status = 'final' and g.home_score is not null and g.away_score is not null
                 and coalesce(v.close_total, v.current_total, v.open_total) is not null then
         case
           when (g.home_score + g.away_score) > coalesce(v.close_total, v.current_total, v.open_total) then 'over'
           when (g.home_score + g.away_score) < coalesce(v.close_total, v.current_total, v.open_total) then 'under'
           else 'push'
         end
       end as actual_ou,
       case when g.status = 'final' and g.home_score is not null and g.away_score is not null
                 and coalesce(v.close_spread, v.current_spread, v.open_spread) is not null then
         case
           when (
             case
               when ( - p.spread ) + coalesce(v.close_spread, v.current_spread, v.open_spread) > 0 then 'home'
               when ( - p.spread ) + coalesce(v.close_spread, v.current_spread, v.open_spread) < 0 then 'away'
               else 'push'
             end
           ) = 'push' then 'push'
           when (
             case
               when ( - p.spread ) + coalesce(v.close_spread, v.current_spread, v.open_spread) > 0 then 'home'
               when ( - p.spread ) + coalesce(v.close_spread, v.current_spread, v.open_spread) < 0 then 'away'
               else 'push'
             end
           ) = (
             case
               when (g.home_score - g.away_score)
                    + coalesce(v.close_spread, v.current_spread, v.open_spread) > 0 then 'home'
               when (g.home_score - g.away_score)
                    + coalesce(v.close_spread, v.current_spread, v.open_spread) < 0 then 'away'
               else 'push'
             end
           ) then 'win'
           when (
             case
               when (g.home_score - g.away_score)
                    + coalesce(v.close_spread, v.current_spread, v.open_spread) > 0 then 'home'
               when (g.home_score - g.away_score)
                    + coalesce(v.close_spread, v.current_spread, v.open_spread) < 0 then 'away'
               else 'push'
             end
           ) = 'push' then 'push'
           else 'loss'
         end
       end as model_ats_result,
       case when g.status = 'final' and g.home_score is not null and g.away_score is not null
                 and coalesce(v.close_total, v.current_total, v.open_total) is not null then
         case
           when (
             case
               when p.total > coalesce(v.close_total, v.current_total, v.open_total) then 'over'
               when p.total < coalesce(v.close_total, v.current_total, v.open_total) then 'under'
               else 'push'
             end
           ) = 'push' then 'push'
           when (
             case
               when p.total > coalesce(v.close_total, v.current_total, v.open_total) then 'over'
               when p.total < coalesce(v.close_total, v.current_total, v.open_total) then 'under'
               else 'push'
             end
           ) = (
             case
               when (g.home_score + g.away_score) > coalesce(v.close_total, v.current_total, v.open_total) then 'over'
               when (g.home_score + g.away_score) < coalesce(v.close_total, v.current_total, v.open_total) then 'under'
               else 'push'
             end
           ) then 'win'
           when (
             case
               when (g.home_score + g.away_score) > coalesce(v.close_total, v.current_total, v.open_total) then 'over'
               when (g.home_score + g.away_score) < coalesce(v.close_total, v.current_total, v.open_total) then 'under'
               else 'push'
             end
           ) = 'push' then 'push'
           else 'loss'
         end
       end as model_ou_result
from games g
join latest_proj p on p.game_id = g.id
left join vegas v on v.game_id = g.id;

grant select on model_grades to anon, authenticated;

-- Per-team CZarp record on ALL graded games (spreads + totals) + official picks record
drop view if exists team_model_record;
create view team_model_record with (security_invoker = true) as
with sides as (
  -- explode each graded game into home + away team rows for ATS
  select home_team_id as team_id, league, season,
         model_ats_result as ats,
         model_ou_result as ou,
         case when model_ats_side = 'home' then true else false end as model_picked_this_team_ats
  from model_grades
  where status = 'final' and home_team_id is not null
  union all
  select away_team_id, league, season, model_ats_result, model_ou_result,
         case when model_ats_side = 'away' then true else false end
  from model_grades
  where status = 'final' and away_team_id is not null
),
ats_when_picked as (
  select team_id, league, season,
         count(*) filter (where ats = 'win')  as model_ats_wins,
         count(*) filter (where ats = 'loss') as model_ats_losses,
         count(*) filter (where ats = 'push') as model_ats_pushes
  from sides
  where model_picked_this_team_ats and ats is not null
  group by team_id, league, season
),
ou_all as (
  select team_id, league, season,
         count(*) filter (where ou = 'win')  as model_ou_wins,
         count(*) filter (where ou = 'loss') as model_ou_losses,
         count(*) filter (where ou = 'push') as model_ou_pushes
  from sides
  where ou is not null
  group by team_id, league, season
),
official as (
  select t.id as team_id, pk.league, pk.season,
         count(*) filter (where pk.result = 'win')  as official_wins,
         count(*) filter (where pk.result = 'loss') as official_losses,
         count(*) filter (where pk.result = 'push') as official_pushes,
         count(*) filter (where pk.market = 'spread' and pk.result = 'win')  as official_ats_wins,
         count(*) filter (where pk.market = 'spread' and pk.result = 'loss') as official_ats_losses,
         count(*) filter (where pk.market = 'total' and pk.result = 'win')   as official_ou_wins,
         count(*) filter (where pk.market = 'total' and pk.result = 'loss')  as official_ou_losses
  from picks pk
  join games g on g.id = pk.game_id
  join teams t on t.id in (g.home_team_id, g.away_team_id)
    and (
      (pk.market = 'spread' and (
        (pk.bet ilike g.home || ' %' and t.id = g.home_team_id) or
        (pk.bet ilike g.away || ' %' and t.id = g.away_team_id)
      ))
      or pk.market = 'total'  -- attribute totals to both teams via the join
    )
  where pk.tier = 'official' and pk.result is distinct from 'void'
  group by t.id, pk.league, pk.season
)
select t.id as team_id, t.name as team_name,
       coalesce(a.league, o.league, u.league) as league,
       coalesce(a.season, o.season, u.season) as season,
       coalesce(a.model_ats_wins, 0) as model_ats_wins,
       coalesce(a.model_ats_losses, 0) as model_ats_losses,
       coalesce(a.model_ats_pushes, 0) as model_ats_pushes,
       coalesce(u.model_ou_wins, 0) as model_ou_wins,
       coalesce(u.model_ou_losses, 0) as model_ou_losses,
       coalesce(u.model_ou_pushes, 0) as model_ou_pushes,
       coalesce(o.official_wins, 0) as official_wins,
       coalesce(o.official_losses, 0) as official_losses,
       coalesce(o.official_pushes, 0) as official_pushes,
       coalesce(o.official_ats_wins, 0) as official_ats_wins,
       coalesce(o.official_ats_losses, 0) as official_ats_losses,
       coalesce(o.official_ou_wins, 0) as official_ou_wins,
       coalesce(o.official_ou_losses, 0) as official_ou_losses
from teams t
left join ats_when_picked a on a.team_id = t.id
left join ou_all u on u.team_id = t.id and u.league = a.league and u.season = a.season
left join official o on o.team_id = t.id
  and o.league = coalesce(a.league, u.league)
  and o.season = coalesce(a.season, u.season)
where a.team_id is not null or u.team_id is not null or o.team_id is not null;

grant select on team_model_record to anon, authenticated;

-- Refresh slate_games to expose new projection fields
drop view if exists slate_games;
create view slate_games with (security_invoker = true) as
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
       m.spread                     as market_spread,
       m.total                      as market_total,
       m.source                     as market_source
from games g
join sports s on s.id = g.sport_id
left join lateral (
  select pr.* from projections pr
  join model_configs mc on mc.model_version = pr.model_version and mc.is_default and mc.league = g.league
  where pr.game_id = g.id order by pr.created_at desc limit 1) p on true
left join lateral (
  select ml.* from market_lines ml
  where ml.game_id = g.id and ml.source = 'vegas' and ml.line_type = 'current'
  order by ml.fetched_at desc limit 1) m on true;

grant select on slate_games to anon, authenticated;
