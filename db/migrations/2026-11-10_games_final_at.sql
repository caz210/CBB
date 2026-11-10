-- games.final_at is when the game actually ended (or tip + 2h if ESPN has
-- no end timestamp). The finals ingest must not use games.updated_at as a
-- finish clock — that column is row-write time.
-- Additive: nullable columns + view extras only. Does not touch picks.

alter table games add column if not exists final_at timestamptz;
alter table games add column if not exists final_at_estimated boolean;

comment on column games.final_at is
  'When the game ended. ESPN End-of-Game wallclock when available, otherwise tipoff + 2 hours. Set once (an estimate may be upgraded to an actual).';
comment on column games.final_at_estimated is
  'True when final_at is tipoff + 2 hours rather than an ESPN end timestamp.';

-- Append status + final_at so the frontend can show Final · {final_at}
-- instead of Final · {updated_at}.
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
  order by ml.fetched_at desc limit 1) m on true;

grant select on slate_games to anon, authenticated;

-- model_grades already exposes g.status; append finish time for the same UI.
create or replace view model_grades with (security_invoker = true) as
 WITH latest_proj AS (
         SELECT DISTINCT ON (pr.game_id, pr.model_version) pr.id,
            pr.game_id,
            pr.sport_id,
            pr.league,
            pr.season,
            pr.model_version,
            pr.proj_home,
            pr.proj_away,
            pr.spread,
            pr.total,
            pr.home_win_prob,
            pr.home_cover_prob,
            pr.over_prob,
            pr.archetype_tags,
            pr.created_at,
            pr.reason,
            pr.proj_possessions,
            pr.upset_alert,
            pr.upset_reason
           FROM projections pr
          WHERE pr.model_version = 'czarp-v2'::text
          ORDER BY pr.game_id, pr.model_version, pr.created_at DESC
        ), vegas AS (
         SELECT ml.game_id,
            max(ml.spread) FILTER (WHERE ml.line_type = 'open'::text) AS open_spread,
            max(ml.total) FILTER (WHERE ml.line_type = 'open'::text) AS open_total,
            max(ml.spread) FILTER (WHERE ml.line_type = 'current'::text) AS current_spread,
            max(ml.total) FILTER (WHERE ml.line_type = 'current'::text) AS current_total,
            max(ml.spread) FILTER (WHERE ml.line_type = 'close'::text) AS close_spread,
            max(ml.total) FILTER (WHERE ml.line_type = 'close'::text) AS close_total
           FROM market_lines ml
          WHERE ml.source = 'vegas'::text
          GROUP BY ml.game_id
        )
 SELECT g.id AS game_id,
    g.league,
    g.season,
    g.date AS game_date,
    g.home,
    g.away,
    g.home_team_id,
    g.away_team_id,
    g.home_score,
    g.away_score,
    g.status,
    p.model_version,
    p.spread AS model_spread,
    p.total AS model_total,
    p.proj_home,
    p.proj_away,
    p.proj_possessions,
    p.upset_alert,
    p.upset_reason,
    p.reason,
    COALESCE(v.close_spread, v.current_spread, v.open_spread) AS vegas_spread,
    COALESCE(v.close_total, v.current_total, v.open_total) AS vegas_total,
    v.open_spread,
    v.open_total,
    v.current_spread,
    v.current_total,
    v.close_spread,
    v.close_total,
        CASE
            WHEN COALESCE(v.close_spread, v.current_spread, v.open_spread) IS NULL THEN NULL::text
            WHEN ((- p.spread) + COALESCE(v.close_spread, v.current_spread, v.open_spread)) > 0::numeric THEN 'home'::text
            WHEN ((- p.spread) + COALESCE(v.close_spread, v.current_spread, v.open_spread)) < 0::numeric THEN 'away'::text
            ELSE 'push'::text
        END AS model_ats_side,
        CASE
            WHEN COALESCE(v.close_total, v.current_total, v.open_total) IS NULL THEN NULL::text
            WHEN p.total > COALESCE(v.close_total, v.current_total, v.open_total) THEN 'over'::text
            WHEN p.total < COALESCE(v.close_total, v.current_total, v.open_total) THEN 'under'::text
            ELSE 'push'::text
        END AS model_ou_side,
        CASE
            WHEN g.status = 'final'::text AND g.home_score IS NOT NULL AND g.away_score IS NOT NULL AND COALESCE(v.close_spread, v.current_spread, v.open_spread) IS NOT NULL THEN
            CASE
                WHEN ((g.home_score - g.away_score)::numeric + COALESCE(v.close_spread, v.current_spread, v.open_spread)) > 0::numeric THEN 'home'::text
                WHEN ((g.home_score - g.away_score)::numeric + COALESCE(v.close_spread, v.current_spread, v.open_spread)) < 0::numeric THEN 'away'::text
                ELSE 'push'::text
            END
            ELSE NULL::text
        END AS actual_ats_cover,
        CASE
            WHEN g.status = 'final'::text AND g.home_score IS NOT NULL AND g.away_score IS NOT NULL AND COALESCE(v.close_total, v.current_total, v.open_total) IS NOT NULL THEN
            CASE
                WHEN (g.home_score + g.away_score)::numeric > COALESCE(v.close_total, v.current_total, v.open_total) THEN 'over'::text
                WHEN (g.home_score + g.away_score)::numeric < COALESCE(v.close_total, v.current_total, v.open_total) THEN 'under'::text
                ELSE 'push'::text
            END
            ELSE NULL::text
        END AS actual_ou,
        CASE
            WHEN g.status = 'final'::text AND g.home_score IS NOT NULL AND g.away_score IS NOT NULL AND COALESCE(v.close_spread, v.current_spread, v.open_spread) IS NOT NULL THEN
            CASE
                WHEN ((- p.spread) + COALESCE(v.close_spread, v.current_spread, v.open_spread)) = 0::numeric THEN 'push'::text
                WHEN sign((- p.spread) + COALESCE(v.close_spread, v.current_spread, v.open_spread)) = sign((g.home_score - g.away_score)::numeric + COALESCE(v.close_spread, v.current_spread, v.open_spread)) AND ((g.home_score - g.away_score)::numeric + COALESCE(v.close_spread, v.current_spread, v.open_spread)) <> 0::numeric THEN 'win'::text
                WHEN ((g.home_score - g.away_score)::numeric + COALESCE(v.close_spread, v.current_spread, v.open_spread)) = 0::numeric THEN 'push'::text
                ELSE 'loss'::text
            END
            ELSE NULL::text
        END AS model_ats_result,
        CASE
            WHEN g.status = 'final'::text AND g.home_score IS NOT NULL AND g.away_score IS NOT NULL AND COALESCE(v.close_total, v.current_total, v.open_total) IS NOT NULL THEN
            CASE
                WHEN p.total = COALESCE(v.close_total, v.current_total, v.open_total) THEN 'push'::text
                WHEN p.total > COALESCE(v.close_total, v.current_total, v.open_total) AND (g.home_score + g.away_score)::numeric > COALESCE(v.close_total, v.current_total, v.open_total) OR p.total < COALESCE(v.close_total, v.current_total, v.open_total) AND (g.home_score + g.away_score)::numeric < COALESCE(v.close_total, v.current_total, v.open_total) THEN 'win'::text
                WHEN (g.home_score + g.away_score)::numeric = COALESCE(v.close_total, v.current_total, v.open_total) THEN 'push'::text
                ELSE 'loss'::text
            END
            ELSE NULL::text
        END AS model_ou_result,
    g.final_at,
    g.final_at_estimated
   FROM games g
     JOIN latest_proj p ON p.game_id = g.id
     LEFT JOIN vegas v ON v.game_id = g.id;

grant select on model_grades to anon, authenticated;
