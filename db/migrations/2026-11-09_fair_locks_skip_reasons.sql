-- fair_at_post / line_at_post / total_at_post on picks (nullable, insert-time only);
-- lock lengths + remaining on team_profiles; skip reasons on projections.
-- Does NOT update existing pick rows or the hash chain.

-- ── picks: rename model_line -> fair_at_post (added earlier today, still unused on existing rows)
do $$ begin
  if exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'picks' and column_name = 'model_line'
  ) and not exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'picks' and column_name = 'fair_at_post'
  ) then
    alter table picks rename column model_line to fair_at_post;
  end if;
end $$;
alter table picks add column if not exists fair_at_post numeric(8,2);
alter table picks add column if not exists line_at_post numeric(8,2);
alter table picks add column if not exists total_at_post numeric(8,2);

comment on column picks.fair_at_post is
  'Model fair number at post, same convention as line_or_price (totals: model total; spreads: picked-side line).';
comment on column picks.line_at_post is
  'Vegas line taken at post (same convention as line_or_price).';
comment on column picks.total_at_post is
  'Vegas total at post (current total when the pick was logged); denominator for edge %.';

create or replace function public.picks_guard() returns trigger
language plpgsql set search_path = public as $$
declare
  v_tip timestamptz;
begin
  if tg_op = 'DELETE' then
    if coalesce(current_setting('czarp.allow_pick_delete', true), '') <> 'on' then
      raise exception 'picks are immutable: delete of % refused', old.pick_key;
    end if;
    return old;
  end if;
  if tg_op = 'INSERT' then
    if new.source = 'live' then
      select tipoff into v_tip from games where id = new.game_id;
      if v_tip is not null and new.created_at >= v_tip then
        raise exception 'live pick % created at % is not before tip-off %', new.pick_key, new.created_at, v_tip;
      end if;
    end if;
    if new.logged_at is null then new.logged_at := now(); end if;
    return new;
  end if;
  if (old.pick_key is distinct from new.pick_key)
     or (old.game_id is distinct from new.game_id)
     or (old.market is distinct from new.market)
     or (old.bet is distinct from new.bet)
     or (old.line_or_price is not null and old.line_or_price is distinct from new.line_or_price)
     or (old.price is not null and old.price is distinct from new.price)
     or (old.edge is not null and old.edge is distinct from new.edge)
     or (old.kelly_stake is not null and old.kelly_stake is distinct from new.kelly_stake)
     or (old.model_version is distinct from new.model_version)
     or (old.created_at is distinct from new.created_at)
     or (old.model_prob is not null and old.model_prob is distinct from new.model_prob)
     or (old.bankroll_at_pick is not null and old.bankroll_at_pick is distinct from new.bankroll_at_pick)
     or (old.tier is distinct from new.tier)
     or (old.source is distinct from new.source)
     or (old.is_paper is distinct from new.is_paper)
     or (old.logged_at is not null and old.logged_at is distinct from new.logged_at)
     or (old.pick_hash is not null and old.pick_hash is distinct from new.pick_hash)
     or (old.result is not null and old.result is distinct from new.result)
     or (old.closing_line is not null and old.closing_line is distinct from new.closing_line)
     or (old.clv is not null and old.clv is distinct from new.clv)
     or (old.pnl is not null and old.pnl is distinct from new.pnl)
     or (old.units_pnl is not null and old.units_pnl is distinct from new.units_pnl)
     or (old.graded_at is not null and old.graded_at is distinct from new.graded_at)
     or (old.fair_at_post is not null and old.fair_at_post is distinct from new.fair_at_post)
     or (old.line_at_post is not null and old.line_at_post is distinct from new.line_at_post)
     or (old.total_at_post is not null and old.total_at_post is distinct from new.total_at_post)
     or (old.open_line is not null and old.open_line is distinct from new.open_line)
     or (old.edge_vs_open is not null and old.edge_vs_open is distinct from new.edge_vs_open) then
    raise exception 'picks are immutable: % cannot be changed once published (only null fields can be filled)', old.pick_key;
  end if;
  return new;
end $$;

drop view if exists value_bets;
drop view if exists official_picks_locked;
create view official_picks_locked with (security_invoker = true) as
with raw as (
  select
    pk.pick_key                                        as pick_id,
    pk.game_id,
    pk.market,
    case
      when pk.market = 'total' and pk.bet ilike 'Over %'  then 'Over'
      when pk.market = 'total' and pk.bet ilike 'Under %' then 'Under'
      when pk.market = 'spread' and pk.bet ilike g.home || ' %' then g.home
      when pk.market = 'spread' and pk.bet ilike g.away || ' %' then g.away
      else regexp_replace(pk.bet, '\s+[+-]?[0-9]+(\.[0-9]+)?$', '')
    end                                                as side,
    pk.model_version                                   as model_name,
    pk.bet,
    pk.tier,
    pk.price,
    coalesce(pk.line_at_post, pk.line_or_price)        as line_at_post,
    pk.edge                                            as edge_at_post_pct,
    pk.fair_at_post                                    as fair_stored,
    pk.open_line                                       as open_line_stored,
    pk.edge_vs_open,
    pk.total_at_post                                   as total_stored,
    coalesce(pk.logged_at, pk.created_at)              as posted_at,
    g.home, g.away,
    proj.spread                                        as proj_spread,
    proj.total                                         as proj_total,
    opn.spread                                         as open_spread,
    opn.total                                          as open_total,
    cur.spread                                         as current_spread,
    cur.total                                          as current_total
  from picks pk
  join games g on g.id = pk.game_id
  left join lateral (
    select x.spread, x.total from (
      select pr.spread, pr.total
      from projections pr
      where pr.game_id = pk.game_id
        and pr.model_version = pk.model_version
        and pr.created_at <= coalesce(pk.logged_at, pk.created_at) + interval '5 minutes'
      order by pr.created_at desc
      limit 1
    ) x
    union all
    select y.spread, y.total from (
      select pr.spread, pr.total
      from projections pr
      where pr.game_id = pk.game_id
        and pr.model_version = pk.model_version
      order by pr.created_at asc
      limit 1
    ) y
    limit 1
  ) proj on true
  left join lateral (
    select x.spread, x.total from (
      select ml.spread, ml.total
      from market_lines ml
      where ml.game_id = pk.game_id and ml.source = 'vegas' and ml.line_type = 'open'
        and ml.fetched_at <= coalesce(pk.logged_at, pk.created_at) + interval '5 minutes'
      order by ml.fetched_at desc
      limit 1
    ) x
    union all
    select y.spread, y.total from (
      select ml.spread, ml.total
      from market_lines ml
      where ml.game_id = pk.game_id and ml.source = 'vegas' and ml.line_type = 'open'
      order by ml.fetched_at asc
      limit 1
    ) y
    limit 1
  ) opn on true
  left join lateral (
    select ml.spread, ml.total
    from market_lines ml
    where ml.game_id = pk.game_id and ml.source = 'vegas' and ml.line_type = 'current'
      and ml.fetched_at <= coalesce(pk.logged_at, pk.created_at) + interval '5 minutes'
    order by ml.fetched_at desc
    limit 1
  ) cur on true
  where pk.result is distinct from 'void'
),
base as (
  select raw.*,
         coalesce(
           fair_stored,
           case
             when market = 'total' then proj_total
             when market = 'spread' and side = home then proj_spread
             when market = 'spread' and side = away then - proj_spread
           end
         ) as fair_at_post,
         coalesce(
           open_line_stored,
           case
             when market = 'total' then open_total
             when market = 'spread' and side = home then open_spread
             when market = 'spread' and side = away then - open_spread
           end
         ) as open_line,
         coalesce(total_stored, current_total, case when market = 'total' then line_at_post end, open_total) as total_at_post,
         coalesce(open_total, current_total) as edge_total
  from raw
)
select pick_id, game_id, market, side, model_name,
       fair_at_post,
       line_at_post,
       total_at_post,
       open_line,
       edge_at_post_pct,
       coalesce(
         edge_vs_open,
         case
           when fair_at_post is null or open_line is null
                or edge_total is null or edge_total = 0 then null
           else round(abs(fair_at_post - open_line) / edge_total, 4)
         end
       )                                               as edge_vs_open_pct,
       posted_at,
       bet, tier, price
from base;

grant select on official_picks_locked to anon, authenticated;

create view value_bets with (security_invoker = true) as
select s.code                                     as sport,
       pk.game_id,
       pk.market,
       pk.bet                                     as side,
       pk.model_prob,
       round(case when pk.price < 0 then -pk.price / (-pk.price + 100) else 100 / (pk.price + 100) end, 4)
                                                  as market_prob,
       round(pk.model_prob - case when pk.price < 0 then -pk.price / (-pk.price + 100) else 100 / (pk.price + 100) end, 4)
                                                  as prob_edge,
       pk.edge                                    as line_edge_pct,
       round(pk.kelly_stake / nullif(pk.bankroll_at_pick / 100, 0), 2)
                                                  as kelly_quarter_units,
       pk.tier,
       pk.logged_at                               as logged_at,
       pk.pick_hash,
       lk.pick_id,
       lk.model_name,
       lk.fair_at_post,
       lk.line_at_post,
       lk.total_at_post,
       lk.open_line,
       lk.edge_at_post_pct,
       lk.edge_vs_open_pct,
       lk.posted_at
from picks pk
join games g  on g.id = pk.game_id
join sports s on s.id = pk.sport_id
left join official_picks_locked lk on lk.pick_id = pk.pick_key
where pk.result is null
  and g.date >= (now() at time zone 'America/Chicago')::date;

grant select on value_bets to anon, authenticated;

-- ── team_profiles: lock lengths + remaining (UI must not hardcode 8 / 4)
alter table team_profiles add column if not exists lock_sides_games int;
alter table team_profiles add column if not exists lock_totals_games int;
alter table team_profiles add column if not exists lock_sides_remaining int;
alter table team_profiles add column if not exists lock_totals_remaining int;

comment on column team_profiles.lock_sides_games is
  'NEW COACH side-lock length in games (site default 8, or a per-team override). Present on every row so the UI does not hardcode it.';
comment on column team_profiles.lock_totals_games is
  'NEW COACH totals-lock length in games (site default 4, or a per-team override).';
comment on column team_profiles.lock_sides_remaining is
  'Games left on the side lock; null when new_coach is false.';
comment on column team_profiles.lock_totals_remaining is
  'Games left on the totals lock; null when new_coach is false.';

-- default the lengths on existing rows (flag + remaining are backfilled by the writer)
update team_profiles
   set lock_sides_games  = coalesce(lock_sides_games, 8),
       lock_totals_games = coalesce(lock_totals_games, 4)
 where lock_sides_games is null or lock_totals_games is null;

drop view if exists team_profile_latest;
create view team_profile_latest with (security_invoker = true) as
select distinct on (tp.team_id)
       t.name as team_name, tp.*
from team_profiles tp
join teams t on t.id = tp.team_id
order by tp.team_id, tp.as_of desc;

grant select on team_profile_latest to anon, authenticated;

-- ── projections: why this game is not an official play
alter table projections add column if not exists skip_reasons_spread text[];
alter table projections add column if not exists skip_reasons_total text[];
alter table projections add column if not exists skip_reason_spread text;
alter table projections add column if not exists skip_reason_total text;

comment on column projections.skip_reasons_spread is
  'Machine-readable official-play skip codes for the spread (below_open_threshold, below_current_threshold, line_moved_against, new_coach_lock, kelly_too_small, ...). Empty if it qualifies or there is no side candidate.';
comment on column projections.skip_reasons_total is
  'Same codes for the total.';

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
       p.skip_reasons_spread,
       p.skip_reasons_total,
       p.skip_reason_spread,
       p.skip_reason_total,
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
