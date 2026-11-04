-- 2026-11-04: site-builder requests
--  * value_bets.logged_at now comes from picks.logged_at (was picks.created_at)
--  * value_bets gains pick_hash; ledger gains logged_at (appended at the end; existing columns unchanged)
-- create or replace keeps existing grants (anon/authenticated select).
-- Note: value_bets keeps the column name logged_at but its source changes; type is timestamptz in both cases.
begin;

create or replace view value_bets with (security_invoker = true) as
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
       pk.pick_hash                                                        -- tamper-evident hash of the pick
from picks pk
join games g  on g.id = pk.game_id
join sports s on s.id = pk.sport_id
where pk.result is null
  and g.date >= (now() at time zone 'America/Chicago')::date;

create or replace view ledger with (security_invoker = true) as
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
       pk.pick_hash,
       pk.logged_at                                    -- when the pick was written to the ledger
from picks pk
join games g  on g.id = pk.game_id
join sports s on s.id = pk.sport_id
where pk.result is distinct from 'void';

commit;
