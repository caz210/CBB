-- Locked post-time fields for official (and track) pick cards, plus a view that
-- backfills fair/open/edge from projections + market_lines for existing rows.
-- Does NOT update existing pick rows or touch the hash chain.
-- New columns are nullable and set by the writer at INSERT time (set-once in picks_guard).

alter table picks add column if not exists model_line numeric(8,2);
alter table picks add column if not exists open_line numeric(8,2);
alter table picks add column if not exists edge_vs_open numeric(6,4);

comment on column picks.model_line is
  'Fair number at post, same convention as line_or_price (totals: model total; spreads: picked-side line).';
comment on column picks.open_line is
  'Vegas opening line at post, same convention as line_or_price.';
comment on column picks.edge_vs_open is
  'Edge vs the opening line at post: |fair - open| / Vegas total (fraction, same as picks.edge).';

-- Protect the new columns set-once (null may be filled; a non-null value cannot change).
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
  -- UPDATE: every column below is set-once
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
     or (old.model_line is not null and old.model_line is distinct from new.model_line)
     or (old.open_line is not null and old.open_line is distinct from new.open_line)
     or (old.edge_vs_open is not null and old.edge_vs_open is distinct from new.edge_vs_open) then
    raise exception 'picks are immutable: % cannot be changed once published (only null fields can be filled)', old.pick_key;
  end if;
  return new;
end $$;

-- Projection / open line at (or just after) post time, falling back to the earliest row.
-- Used so official cards show the locked model (e.g. czarp-v1-totals 142.0) rather than
-- the latest default-model slate number (czarp-v2 146.2).
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
    pk.line_or_price                                   as line_at_post,
    pk.edge                                            as edge_at_post_pct,
    pk.model_line,
    pk.open_line                                       as open_line_stored,
    pk.edge_vs_open,
    coalesce(pk.logged_at, pk.created_at)              as posted_at,
    g.home, g.away,
    proj.spread                                        as proj_spread,
    proj.total                                         as proj_total,
    opn.spread                                         as open_spread,
    opn.total                                          as open_total,
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
    select ml.total
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
           model_line,
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
         coalesce(open_total, current_total) as edge_total
  from raw
)
select pick_id, game_id, market, side, model_name,
       fair_at_post,
       line_at_post,
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

-- Append locked-post fields to value_bets (existing columns and meaning unchanged).
drop view if exists value_bets;
create view value_bets with (security_invoker = true) as
select s.code                                     as sport,
       pk.game_id,
       pk.market,                                  -- 'spread' | 'total' | 'moneyline'
       pk.bet                                     as side,      -- e.g. 'Tulane -6.5', 'Under 162.5'
       pk.model_prob,
       round(case when pk.price < 0 then -pk.price / (-pk.price + 100) else 100 / (pk.price + 100) end, 4)
                                                  as market_prob,  -- implied by the price taken (incl. vig)
       round(pk.model_prob - case when pk.price < 0 then -pk.price / (-pk.price + 100) else 100 / (pk.price + 100) end, 4)
                                                  as prob_edge,    -- model_prob - market_prob
       pk.edge                                    as line_edge_pct,  -- |model - line| / market total (the betting rule)
       round(pk.kelly_stake / nullif(pk.bankroll_at_pick / 100, 0), 2)
                                                  as kelly_quarter_units,  -- 1u = 1% of bankroll (0 for tier 'track')
       pk.tier,                                   -- 'official' (the play) | 'track' (shadow, paper only, flat 1u)
       pk.logged_at                               as logged_at,            -- when the pick was written (before tip-off)
       pk.pick_hash,                                                       -- tamper-evident hash of the pick
       -- locked post-time numbers (same as official_picks_locked; existing pick rows derived)
       lk.pick_id,
       lk.model_name,
       lk.fair_at_post,
       lk.line_at_post,
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
