-- model_grades: grade every game against the LAST Vegas line before tip-off (Christian: "Yes, grade records
-- against the last line before tip-off."). Feeds team_model_record (team pages) and the public model record.
--
-- Before: vegas.current_spread / current_total were max() over EVERY 'current' row of a game, i.e. the
-- highest number of the day, not the last one. With the intraday refresh (refresh_lines.py) and pre-tip fills
-- writing more current rows, that got worse.
-- After:  current_spread / current_total = the newest source='vegas', line_type='current' row by fetched_at
--         whose fetched_at is before games.tipoff (taken per value: the newest pre-tip row that HAS a spread,
--         and the newest that HAS a total). A game with no stored tipoff (TBD) uses its newest current row.
--         Fallback is unchanged: vegas_* = coalesce(close, current, open); with no pre-tip current row the
--         game grades against close (none stored today), then open, exactly as before.
-- open_* and close_* are unchanged (max per game). Every column keeps its name, type (numeric) and order,
-- so CREATE OR REPLACE keeps the view's identity, its options and team_model_record on top of it.
--
-- Read-only dry run against live data (2026-11-21 ~3:15 PM CT, 859 final games): ATS graded 850 -> 850,
-- O/U graded 850 -> 850; 9 ATS and 4 O/U results change, all on 2026-11-10 (the day with several 7 AM
-- re-runs); team_model_record game counts identical for all 365 teams.
--
-- Does not touch picks, the picks_immutable trigger, the ledger, official_picks_locked (already as-of the
-- pick's logged_at) or the hash chain. Official pick results are graded by the pipeline, not by this view.
-- Apply only after Scantron approves.

begin;

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
         -- open / close: unchanged (max per game). current: the NEWEST pre-tip vegas 'current' row by fetched_at,
         -- per value (a row missing a spread or total falls back to the newest pre-tip row that has it).
         -- Pre-tip = fetched_at < games.tipoff; a game with no stored tipoff (TBD) uses its newest row.
         -- No pre-tip current row -> current is NULL, so grading falls back to close, then open, as before.
         SELECT g.id AS game_id,
            oc.open_spread,
            oc.open_total,
            cs.spread::numeric AS current_spread,
            ct.total::numeric AS current_total,
            oc.close_spread,
            oc.close_total
           FROM games g
             LEFT JOIN LATERAL ( SELECT max(ml.spread) FILTER (WHERE ml.line_type = 'open'::text) AS open_spread,
                    max(ml.total) FILTER (WHERE ml.line_type = 'open'::text) AS open_total,
                    max(ml.spread) FILTER (WHERE ml.line_type = 'close'::text) AS close_spread,
                    max(ml.total) FILTER (WHERE ml.line_type = 'close'::text) AS close_total
                   FROM market_lines ml
                  WHERE ml.game_id = g.id AND ml.source = 'vegas'::text) oc ON true
             LEFT JOIN LATERAL ( SELECT ml.spread
                   FROM market_lines ml
                  WHERE ml.game_id = g.id AND ml.source = 'vegas'::text AND ml.line_type = 'current'::text
                    AND ml.spread IS NOT NULL AND (g.tipoff IS NULL OR ml.fetched_at < g.tipoff)
                  ORDER BY ml.fetched_at DESC
                 LIMIT 1) cs ON true
             LEFT JOIN LATERAL ( SELECT ml.total
                   FROM market_lines ml
                  WHERE ml.game_id = g.id AND ml.source = 'vegas'::text AND ml.line_type = 'current'::text
                    AND ml.total IS NOT NULL AND (g.tipoff IS NULL OR ml.fetched_at < g.tipoff)
                  ORDER BY ml.fetched_at DESC
                 LIMIT 1) ct ON true
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

revoke all on model_grades from anon, authenticated;
grant select on model_grades to anon, authenticated;

commit;
