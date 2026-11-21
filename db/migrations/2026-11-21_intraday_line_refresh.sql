-- Intraday line refresh (refresh_lines.py): when the stored edges were computed, plus the newest
-- Vegas line's fetch time, on slate_games.
-- Additive: one nullable column + two view fields appended at the END of slate_games (CREATE OR REPLACE
-- keeps every existing column, its type and its order, and keeps the view's grants). Does not touch picks,
-- the picks_immutable trigger, the ledger, official_picks_locked or the hash chain. No data is rewritten.
--
-- projections.edges_as_of: the time edge_now_pct_* / edge_vs_open_pct_* / skip_reasons_* / skip_reason_* on that
--   row were computed against the newest Vegas line. refresh_lines.py (10a/1p/4p/7p CT, or 12p/5p CT on light
--   days) and pretip_check.py write a NEW projections row each time a game's line or edges change, with the
--   7:15 projections copied unchanged and edges_as_of = created_at. Rows written by the 7 AM run leave it NULL,
--   so slate_games falls back to created_at (their edges were computed in that same run).
-- slate_games already reads the NEWEST default-model projections row per game (order by created_at desc
--   limit 1) and the newest vegas 'current' market_lines row (order by fetched_at desc limit 1), so each
--   refresh shows up on the slate with no change to how rows are chosen.
-- slate_games.edges_as_of      = coalesce(projections.edges_as_of, projections.created_at) of that row
-- slate_games.market_fetched_at = market_lines.fetched_at of the newest vegas current row shown as market_*
--
-- No new grants: projections already has SELECT for anon/authenticated (2026-11-18) and a new column inherits
-- it; CREATE OR REPLACE VIEW keeps slate_games' grants (re-granted below anyway, SELECT only).
-- Apply only after Scantron approves, BEFORE the czarp-v2 code that writes edges_as_of runs.

alter table projections
  add column if not exists edges_as_of timestamptz;

comment on column projections.edges_as_of is
  'When this row''s stored edges and skip reasons were computed against the newest Vegas line (intraday refresh / pre-tip fill). NULL on rows from the 7 AM run: use created_at.';

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
       -- appended (2026-11-21 intraday refresh); existing columns keep their names, types and order
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
  limit 1) off_tot on true;

grant select on slate_games to anon, authenticated;
