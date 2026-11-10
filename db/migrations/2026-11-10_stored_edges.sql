-- Per-market stored edges for the public slate cards, plus verdict/lean on
-- slate_games. Additive: nullable columns + view extras only. Does not touch picks.
--
-- Edges are fractions (0.045 = 4.5%), matching picks.edge / edge_at_post_pct,
-- and are written by the pipeline with the same formulas as daily_run.candidates():
--   spread now     = |v2 home margin + current home spread| / current total
--   spread vs open = |v2 home margin + open home spread| / (open total or current total)
--   total now      = |v1 total − current total| / current total
--   total vs open  = |v1 total − open total| / open total

alter table projections
  add column if not exists edge_now_pct_spread numeric(6,4),
  add column if not exists edge_vs_open_pct_spread numeric(6,4),
  add column if not exists edge_now_pct_total numeric(6,4),
  add column if not exists edge_vs_open_pct_total numeric(6,4);

comment on column projections.edge_now_pct_spread is
  'Spread edge vs the current Vegas line, as a fraction (0.045 = 4.5%). Same formula as daily_run.candidates().';
comment on column projections.edge_vs_open_pct_spread is
  'Spread edge vs the opening Vegas line, as a fraction. Same formula as daily_run.candidates().';
comment on column projections.edge_now_pct_total is
  'Total edge vs the current Vegas total, as a fraction. Same formula as daily_run.candidates() (v1 total).';
comment on column projections.edge_vs_open_pct_total is
  'Total edge vs the opening Vegas total, as a fraction. Same formula as daily_run.candidates() (v1 total).';

-- verdict_* is official when an official (non-track) pick exists for that market,
-- otherwise low_value. lean_* is true when there is no official pick, the edge
-- now clears the current-line bar, and the edge vs open is below the open bar.
-- Thresholds match daily_run.RULES: sides 4%, totals 3%.
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
       p.edge_now_pct_spread,
       p.edge_vs_open_pct_spread,
       p.edge_now_pct_total,
       p.edge_vs_open_pct_total,
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
       g.final_at_estimated
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
  limit 1) off_tot on true;

grant select on slate_games to anon, authenticated;
