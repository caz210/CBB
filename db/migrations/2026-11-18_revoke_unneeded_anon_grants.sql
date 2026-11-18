-- Revoke write-type privileges that anon / authenticated never needed.
--
-- Why: Supabase's default privileges in `public` hand every new table, view and
-- sequence to anon + authenticated with ALL privileges. Earlier migrations only
-- revoked insert/update/delete on the base tables; TRUNCATE, REFERENCES, TRIGGER
-- and MAINTAIN were left behind, and the views (slate_games, value_bets, ledger,
-- ...) kept INSERT/UPDATE/DELETE/TRUNCATE too. Row level security does not
-- apply to TRUNCATE, so anon holding TRUNCATE on teams / picks / games / ... is a
-- real (if not reachable through PostgREST today) hole.
--
-- What the site actually needs (web/src makes no inserts/updates/deletes):
--   anon           SELECT on the base tables and views the site reads
--   authenticated  the same, plus SELECT on profiles and full CRUD on user_models
--                  (RLS user_models_own limits it to the caller's own rows)
-- The pipeline writes through the service role (SUPABASE_SERVICE_KEY), which is a
-- separate role with its own grants and bypasses RLS, so it is not touched here.
--
-- Safe to re-run. Nothing here changes RLS, policies, triggers or any data.
-- Apply only after Scantron approves; verify with the queries at the bottom.

begin;

-- ── 1. public-read base tables: SELECT only ─────────────────────────────────
revoke all on table
  sports, leagues, teams, games, model_configs, projections, market_lines,
  picks, team_profiles, big_moves
from anon, authenticated;
grant select on table
  sports, leagues, teams, games, model_configs, projections, market_lines,
  picks, team_profiles, big_moves
to anon, authenticated;

-- ── 2. public-read views: SELECT only ───────────────────────────────────────
revoke all on table
  slate_games, value_bets, ledger, pick_record, tier_record,
  official_picks_locked, model_grades, team_model_record,
  team_profile_latest, big_moves_record
from anon, authenticated;
grant select on table
  slate_games, value_bets, ledger, pick_record, tier_record,
  official_picks_locked, model_grades, team_model_record,
  team_profile_latest, big_moves_record
to anon, authenticated;

-- ── 3. account tables ───────────────────────────────────────────────────────
-- profiles: signed-in users read their own row (RLS); tier is written by the
-- service role / handle_new_user() only. anon has no access.
revoke all on table profiles from anon, authenticated;
grant select on table profiles to authenticated;

-- user_models: signed-in users manage their own rows (RLS user_models_own).
revoke all on table user_models from anon, authenticated;
grant select, insert, update, delete on table user_models to authenticated;

-- ── 4. legacy Streamlit tables (bet_results, daily_snapshots, game_results) ──
-- RLS is on with no policies, so anon/authenticated can't read or write rows
-- today. Only drop the privileges RLS does not gate (TRUNCATE etc.). The
-- SELECT/INSERT/UPDATE/DELETE grants are left in place on purpose: removing them
-- would only matter if the old Streamlit app used the anon key, which we can't
-- tell from here. To lock these down fully, run
--   revoke all on bet_results, daily_snapshots, game_results from anon, authenticated;
-- once the Streamlit app is confirmed to use the service key (or is retired).
revoke truncate, references, trigger, maintain on table
  bet_results, daily_snapshots, game_results
from anon, authenticated;

-- ── 5. sequences: no client role should advance an id sequence ──────────────
revoke all on all sequences in schema public from anon, authenticated;

-- ── 6. future objects: stop new tables from being born writable by clients ───
-- (SELECT stays in the defaults, so new read tables/views still work; a table
-- that clients must write to needs an explicit grant, as user_models has.)
alter default privileges for role postgres in schema public
  revoke insert, update, delete, truncate, references, trigger, maintain
  on tables from anon, authenticated;
alter default privileges for role postgres in schema public
  revoke all on sequences from anon, authenticated;

commit;

-- ── verify (run after applying; expect zero rows from the first query) ──────
-- select c.relname, r.rolname, a.privilege_type
-- from pg_class c
-- join pg_namespace n on n.oid = c.relnamespace and n.nspname = 'public'
-- cross join lateral aclexplode(c.relacl) a
-- join pg_roles r on r.oid = a.grantee
-- where c.relkind in ('r','v','p','m') and r.rolname in ('anon','authenticated')
--   and a.privilege_type <> 'SELECT'
--   and not (c.relname = 'user_models' and r.rolname = 'authenticated')
--   and c.relname not in ('bet_results','daily_snapshots','game_results');
