-- official_picks_locked becomes official-only (Scantron follow-up to #90/#91, 2026-11-24).
--
-- Why: since the CZarp Lean tier (2026-11-24_lean_tier_line_moved_away.sql) the picks table holds
-- 'official', 'track' and 'lean' rows. official_picks_locked never filtered by tier (it carried pk.tier
-- and left the filter to readers); on prod it already returned 88 official + 2 track rows, and with
-- CZARP_SYNC_LEAN=1 it would start returning lean rows too. Defense in depth: the view named "official"
-- now returns tier = 'official' only.
--
-- Consumers checked (czarp-v2 pipeline + main web/src):
--   slate_games                    -> already filters pk.tier = 'official'; result unchanged.
--   value_bets                     -> LEFT JOINs the lock fields for every tier (pending picks); now joins the new
--                                     all-tiers view picks_locked, so its rows and columns are unchanged.
--   czarp/supabase_writer.fill_pick_locks -> fills NULL lock columns on every pick (incl. track/lean);
--                                     now reads picks_locked (falls back to official_picks_locked before this lands).
--   web data.ts mergeOfficialLocked -> reads official_picks_locked for the official book; track/lean rows were
--                                     only ever kept out of official. Lean cards read lean_picks. No web change needed.
--
-- 1. picks_locked: the exact current official_picks_locked definition (all tiers, same 15 columns).
-- 2. official_picks_locked: CREATE OR REPLACE (same columns, order and types; grants kept) = picks_locked
--    WHERE tier = 'official'.
-- 3. value_bets: CREATE OR REPLACE, identical except the join target (official_picks_locked -> picks_locked).
--
-- Views only. Does not touch picks rows, the picks_immutable trigger / picks_guard(), the hash chain,
-- slate_games, ledger, tier_record or any official number.
begin;

create or replace view picks_locked with (security_invoker = true) as
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

-- security_invoker views: value_bets is read by anon, so anon needs SELECT on its join target
-- (the same rows official_picks_locked exposed to anon before this migration).
revoke all on picks_locked from anon, authenticated;
grant select on picks_locked to anon, authenticated;
comment on view picks_locked is
  'Post-time locked numbers for every non-void pick, all tiers (official, track, lean). Pipeline lock backfill and value_bets read this; the official book reads official_picks_locked.';

create or replace view official_picks_locked with (security_invoker = true) as
select pick_id, game_id, market, side, model_name,
       fair_at_post, line_at_post, total_at_post, open_line,
       edge_at_post_pct, edge_vs_open_pct, posted_at,
       bet, tier, price
from picks_locked
where tier = 'official';

comment on view official_picks_locked is
  'Official picks only (tier = official) with their post-time locked numbers. All tiers: picks_locked.';

create or replace view value_bets with (security_invoker = true) as
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
left join picks_locked lk on lk.pick_id = pk.pick_key   -- all tiers: track/lean rows keep their lock fields
where pk.result is null
  and g.date >= (now() at time zone 'America/Chicago')::date;

grant select on official_picks_locked, value_bets to anon, authenticated;

commit;
