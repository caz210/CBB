-- Pre-game TV Rating (0–100, one decimal) on projections, plus slate_games.
-- Additive: nullable column + view field only. Does not touch picks, the
-- ledger, official_picks_locked, or the hash chain. Does not depend on
-- Vegas lines.
--
-- Formula (czarp/tv_rating.py), computed by the daily run when projections
-- are saved and backfilled by backfill_tv_rating.py:
--   tv_rating = 100 × quality × (0.5 + 0.5 × closeness) × pace_factor
-- quality is the D-I percentile of the weaker team's high-precision AdjEM;
-- closeness uses the card's CZarp home win probability; pace_factor is a
-- small possessions (else total) lean in [0.85, 1.00].
--
-- No new table grants: projections already has SELECT for anon/authenticated
-- (2026-11-18), and a new column inherits those. Recreating slate_games
-- drops its grants, so SELECT is restored only — the same as other
-- projection columns, with no wider anon privileges.
-- Apply only after Scantron approves; do not rewrite any posted pick.

alter table projections
  add column if not exists tv_rating numeric(5,1);

comment on column projections.tv_rating is
  'Pre-game TV Rating, 0–100 (one decimal). Weaker-team quality × closeness of the CZarp win% × a small pace lean. Null when a team or win probability is missing. Not a pick; does not use Vegas lines.';

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
