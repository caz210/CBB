-- Track-record tiers + tamper-evident, immutable picks (applied 2026-11-04; also folded into db/schema.sql).
-- tier:      'official' = the plays (quarter-Kelly paper stake) | 'track' = shadow paper sides (flat 1u, kelly_stake 0)
-- source:    'live' = logged by the daily run before tip | 'backfill' = reconstructed later from saved morning snapshots
-- logged_at: when the row was actually written to the ledger (created_at = the line/price timestamp of the pick)
-- units_pnl: flat 1u result at the price taken (win = +payout, loss = -1, push/void = 0) -> comparable across tiers
-- pick_hash: sha256 hash-chain seal from the CSV ledger (daily_run.verify_ledger re-checks it)
alter table picks add column if not exists tier text not null default 'official';
alter table picks add column if not exists source text not null default 'live';
alter table picks add column if not exists logged_at timestamptz;
alter table picks add column if not exists units_pnl numeric(8,4);
alter table picks add column if not exists pick_hash text;
do $$ begin
  alter table picks add constraint picks_tier_chk check (tier in ('official','track'));
exception when duplicate_object then null; end $$;
do $$ begin
  alter table picks add constraint picks_source_chk check (source in ('live','backfill'));
exception when duplicate_object then null; end $$;
create index if not exists picks_tier_idx on picks (league, tier, created_at desc);

-- Immutability: a published pick never changes. Pick fields and grading fields are set-once (null -> value only);
-- deletes are refused (an operator can override in a SQL session with: set local czarp.allow_pick_delete = 'on').
-- A LIVE pick must be created before its game's tip-off.
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
     or (old.graded_at is not null and old.graded_at is distinct from new.graded_at) then
    raise exception 'picks are immutable: % cannot be changed once published (only null fields can be filled)', old.pick_key;
  end if;
  return new;
end $$;
drop trigger if exists picks_immutable on picks;
create trigger picks_immutable before insert or update or delete on picks
  for each row execute function public.picks_guard();

-- record by tier (W-L-P, flat units, ROI per 1u, CLV); the site's public track record
drop view if exists pick_record;
create view pick_record with (security_invoker = true) as
select p.league, p.season, p.model_version, p.market,
       count(*) filter (where p.result = 'win')  as wins,
       count(*) filter (where p.result = 'loss') as losses,
       count(*) filter (where p.result = 'push') as pushes,
       count(*) filter (where p.result is null)  as pending,
       sum(p.kelly_stake) filter (where p.result in ('win','loss','push')) as staked,
       sum(p.pnl)  as pnl,
       round(avg(p.clv), 2) as avg_clv,
       p.tier
from picks p
group by p.league, p.season, p.model_version, p.market, p.tier;

drop view if exists tier_record;
create view tier_record with (security_invoker = true) as
select p.league, p.season, p.tier, p.market,
       count(*) filter (where p.result = 'win')  as wins,
       count(*) filter (where p.result = 'loss') as losses,
       count(*) filter (where p.result = 'push') as pushes,
       count(*) filter (where p.result is null)  as pending,
       round(sum(p.units_pnl), 2)                as units,          -- flat 1u per pick at the price taken
       round(sum(p.units_pnl) / nullif(count(*) filter (where p.result in ('win','loss','push')), 0), 4) as roi,
       round(avg(p.clv), 2)                      as avg_clv,        -- points vs the DK close, + = beat it
       count(*) filter (where p.clv > 0)         as beat_close,
       sum(p.pnl) filter (where p.tier = 'official') as pnl_dollars  -- paper $ (official tier only)
from picks p
where p.result is distinct from 'void'
group by p.league, p.season, p.tier, p.market;

grant select on pick_record, tier_record to anon, authenticated;

-- front-end views: tier + proof columns appended (existing columns unchanged)
drop view if exists value_bets;
drop view if exists ledger;
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
       pk.created_at                              as logged_at             -- pick timestamp (before tip-off)
from picks pk
join games g  on g.id = pk.game_id
join sports s on s.id = pk.sport_id
where pk.result is null
  and g.date >= (now() at time zone 'America/Chicago')::date;

create view ledger with (security_invoker = true) as
select s.code                                          as sport,
       g.date                                          as game_date,
       pk.game_id,
       pk.bet                                          as pick,
       pk.line_or_price                                as line,
       pk.price                                        as odds,
       round(pk.kelly_stake / nullif(pk.bankroll_at_pick / 100, 0), 2) as stake_units,
       case pk.result when 'win' then 'W' when 'loss' then 'L' when 'push' then 'P' end as result,  -- null = pending
       round(pk.pnl / nullif(pk.bankroll_at_pick / 100, 0), 2)         as profit_units,
       pk.is_paper,
       pk.tier,                                        -- 'official' | 'track'
       pk.units_pnl                                    as flat_units,   -- flat 1u result (compare tiers on this)
       pk.clv,                                         -- points vs the DK close
       pk.created_at,                                  -- pick timestamp (live picks: before tip-off, db-enforced)
       pk.source,                                      -- 'live' | 'backfill'
       pk.pick_hash
from picks pk
join games g  on g.id = pk.game_id
join sports s on s.id = pk.sport_id
where pk.result is distinct from 'void';

grant select on value_bets, ledger to anon, authenticated;
