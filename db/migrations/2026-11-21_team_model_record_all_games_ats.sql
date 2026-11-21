-- team_model_record: count CZarp's ATS the same way as O/U — every final
-- game with a graded model spread for the team, not only games where
-- CZarp's spread pick was this team. Official-pick columns, column names,
-- types, security_invoker, and SELECT-only grants are unchanged so the
-- team page needs no code change.
-- Does not touch picks, the ledger, or the hash chain.
-- Apply only after Scantron approves.

begin;

create or replace view team_model_record with (security_invoker = true) as
with sides as (
  select home_team_id as team_id, league, season,
         model_ats_result as ats,
         model_ou_result as ou
  from model_grades
  where status = 'final' and home_team_id is not null
  union all
  select away_team_id, league, season, model_ats_result, model_ou_result
  from model_grades
  where status = 'final' and away_team_id is not null
),
ats_all as (
  select team_id, league, season,
         count(*) filter (where ats = 'win')  as model_ats_wins,
         count(*) filter (where ats = 'loss') as model_ats_losses,
         count(*) filter (where ats = 'push') as model_ats_pushes
  from sides
  where ats is not null
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
  select x.team_id, pk.league, pk.season,
         count(*) filter (where pk.result = 'win')  as official_wins,
         count(*) filter (where pk.result = 'loss') as official_losses,
         count(*) filter (where pk.result = 'push') as official_pushes,
         count(*) filter (where pk.market = 'spread' and pk.result = 'win')  as official_ats_wins,
         count(*) filter (where pk.market = 'spread' and pk.result = 'loss') as official_ats_losses,
         count(*) filter (where pk.market = 'total' and pk.result = 'win')   as official_ou_wins,
         count(*) filter (where pk.market = 'total' and pk.result = 'loss')  as official_ou_losses
  from picks pk
  join games g on g.id = pk.game_id
  cross join lateral (
    select g.home_team_id as team_id
    where pk.market = 'total' and g.home_team_id is not null
    union all
    select g.away_team_id
    where pk.market = 'total' and g.away_team_id is not null
    union all
    select g.home_team_id
    where pk.market = 'spread' and g.home_team_id is not null
      and pk.bet ~~* (g.home || ' %')
    union all
    select g.away_team_id
    where pk.market = 'spread' and g.away_team_id is not null
      and pk.bet ~~* (g.away || ' %')
  ) x
  where pk.tier = 'official' and pk.result is distinct from 'void'
    and x.team_id is not null
  group by x.team_id, pk.league, pk.season
),
keys as (
  select team_id, league, season from ats_all
  union
  select team_id, league, season from ou_all
  union
  select team_id, league, season from official
)
select t.id as team_id, t.name as team_name,
       k.league, k.season,
       coalesce(a.model_ats_wins, 0::bigint) as model_ats_wins,
       coalesce(a.model_ats_losses, 0::bigint) as model_ats_losses,
       coalesce(a.model_ats_pushes, 0::bigint) as model_ats_pushes,
       coalesce(u.model_ou_wins, 0::bigint) as model_ou_wins,
       coalesce(u.model_ou_losses, 0::bigint) as model_ou_losses,
       coalesce(u.model_ou_pushes, 0::bigint) as model_ou_pushes,
       coalesce(o.official_wins, 0::bigint) as official_wins,
       coalesce(o.official_losses, 0::bigint) as official_losses,
       coalesce(o.official_pushes, 0::bigint) as official_pushes,
       coalesce(o.official_ats_wins, 0::bigint) as official_ats_wins,
       coalesce(o.official_ats_losses, 0::bigint) as official_ats_losses,
       coalesce(o.official_ou_wins, 0::bigint) as official_ou_wins,
       coalesce(o.official_ou_losses, 0::bigint) as official_ou_losses
from keys k
join teams t on t.id = k.team_id
left join ats_all a on a.team_id = k.team_id and a.league = k.league and a.season = k.season
left join ou_all u on u.team_id = k.team_id and u.league = k.league and u.season = k.season
left join official o on o.team_id = k.team_id and o.league = k.league and o.season = k.season;

comment on view team_model_record is
  'Per-team CZarp record. Model ATS and O/U are CZarp''s sides on every final graded game for the team, not whether the team covered. Official columns count official picks only.';

revoke all on team_model_record from anon, authenticated;
grant select on team_model_record to anon, authenticated;

commit;
