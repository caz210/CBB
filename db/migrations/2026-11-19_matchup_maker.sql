-- Matchup maker backend: private per-team inputs + constants, public.project_matchup() RPC,
-- and the public team_aliases search table (seeded by 2026-11-19_team_aliases_seed.sql).
--
-- Follows 2026-11-18_revoke_unneeded_anon_grants.sql: client roles get SELECT only where intended
-- (team_aliases), nothing at all on the private tables, and EXECUTE on the one function. No grant
-- that migration closed is reopened; no existing table, view, policy, trigger or row is touched.
-- Mirrored in db/schema.sql. Safe to re-run. Apply only after Scantron approves; then run the
-- alias seed migration, then the daily run (or scripts/publish_matchup_inputs.py) to fill inputs.

begin;

-- ── matchup maker: private inputs + public.project_matchup() ────────────────
-- Pick any two D-I teams and a venue; get CZarp's scores, total, spread and win %.
-- Inputs are written by the pipeline (daily_run.py -> czarp/matchup.py, service role) after the
-- morning projections. They hold model inputs (incl. licensed ratings), so they are PRIVATE:
-- RLS on, no policies, no anon/authenticated grants. Clients only get projections, through the
-- SECURITY DEFINER function below. Doubles (float8) on purpose: the function must reproduce the
-- Python model to the last bit so the CZarp total's 0.5 rounding matches the daily run exactly.

create table if not exists public.matchup_inputs (
    team_id     bigint primary key references public.teams(id) on delete cascade,
    season      int not null,                 -- season END year (2026-27 = 2027)
    oe          float8 not null,              -- AdjOE
    de          float8 not null,              -- AdjDE
    tempo       float8 not null,              -- AdjTempo
    kp_pct      float8 not null,              -- (n_teams - KenPom rank) / n_teams
    unit_v2     float8,                       -- v2 unit score (height/exp/bench); null = no unit adj.
    unit_v1     float8 not null,              -- v1 unit score
    hca         float8 not null,              -- team home-court value (KenPom hca.php; table mean if missing)
    to_pct      float8 not null,              -- offense TO%
    d_to_pct    float8 not null,              -- defense TO% forced
    or_pct      float8 not null,              -- offense OR%
    opp_or_pct  float8 not null,              -- opponent OR% allowed
    as_of       timestamptz not null          -- pipeline run that wrote the row
);

create table if not exists public.matchup_constants (
    league                text primary key references public.leagues(code),
    season                int not null,
    model_version         text not null,
    -- v2 (model.py project_game): averages + calibration + weights
    avg_tempo             float8 not null,
    avg_eff               float8 not null,
    total_intercept       float8 not null,
    total_slope           float8 not null,
    margin_scale          float8 not null,    -- efficiency_margin_scale
    w_efficiency          float8 not null,
    qg_coef               float8 not null,    -- 0 when the quality-gap feature is off
    w_quality_gap         float8 not null,
    hca_scale             float8 not null,
    w_hca                 float8 not null,
    hca_split             float8 not null,
    unit_scale            float8 not null,
    w_unit                float8 not null,
    unit_close_threshold  float8 not null,
    unit_close_mult       float8 not null,
    unit_neutral_mult     float8 not null,
    unit_late_factor      float8 not null,    -- late-season unit multiplier for the as_of date
    margin_sd             float8 not null,    -- win % = Phi(margin / margin_sd)
    -- v1 totals model (czarp/legacy_v1.py)
    v1_avg_tempo          float8 not null,
    v1_avg_to             float8 not null,
    v1_avg_dto            float8 not null,
    v1_avg_or             float8 not null,
    v1_avg_dor            float8 not null,
    v1_hca                float8 not null,    -- 3.5 (±1.75)
    v1_unit_weight        float8 not null,    -- 0.25
    as_of                 timestamptz not null
);

alter table public.matchup_inputs enable row level security;
alter table public.matchup_constants enable row level security;
revoke all on table public.matchup_inputs, public.matchup_constants from public, anon, authenticated;
grant select, insert, update, delete on table public.matchup_inputs, public.matchup_constants to service_role;

-- team_aliases: search spellings -> team (anon-readable). alias is normalised like the site's
-- search box (web/src/lib/teamSearch.ts normalize): lowercase, accents/punctuation dropped,
-- "A&M" -> "am", "&"/"and" dropped, single spaces. One alias may point at several teams
-- ("miami", "a&m", "wildcats"), so the key is (alias, team_id).
create table if not exists public.team_aliases (
    alias    text not null check (alias <> '' and alias = lower(alias)),
    team_id  bigint not null references public.teams(id) on delete cascade,
    source   text not null default 'manual',
    primary key (alias, team_id)
);
create index if not exists team_aliases_team_idx on public.team_aliases (team_id);
create index if not exists team_aliases_prefix_idx on public.team_aliases (alias text_pattern_ops);
alter table public.team_aliases enable row level security;
drop policy if exists team_aliases_read on public.team_aliases;
create policy team_aliases_read on public.team_aliases for select to anon, authenticated using (true);
revoke all on table public.team_aliases from public, anon, authenticated;
grant select on table public.team_aliases to anon, authenticated;
grant select, insert, update, delete on table public.team_aliases to service_role;

-- result row of project_matchup (a composite, so the function's arguments can keep the
-- natural names team_a / team_b / venue and PostgREST returns one JSON object)
do $$ begin
  if to_regtype('public.matchup_projection') is null then
    create type public.matchup_projection as (
        team_a     bigint,
        team_b     bigint,
        venue      text,
        as_of      timestamptz,
        score_a    float8,
        score_b    float8,
        total      float8,
        spread_a   float8,
        win_pct_a  float8,
        win_pct_b  float8
    );
  end if;
end $$;

-- Twin of czarp/matchup.py project(); same operation order (keep them in sync: the daily run's
-- parity check compares that Python twin with the model every morning, and
-- tests/test_matchup.py compares this function with it).
--   total    = v1 totals model: each team rounded to 0.5 (round() on float8 is half-to-even,
--              like Python), then summed  -> the CZarp total the daily run uses
--   margin   = v2 margin (efficiency, calibration, unit score, per-team home court)
--   score_a  = (total + margin_a) / 2, score_b = (total - margin_a) / 2
--   spread_a = -margin_a (betting notation), win_pct_a = Phi(margin_a / margin_sd)
-- The daily run projects (away, home): a home team is team2, and at a neutral site team_a takes
-- the listed-away (team1) role. Full precision is returned; round only for display.
create or replace function public.project_matchup(team_a bigint, team_b bigint, venue text default 'neutral')
returns public.matchup_projection
language plpgsql stable security definer
set search_path = ''
as $$
declare
  c   public.matchup_constants%rowtype;
  a   public.matchup_inputs%rowtype;
  b   public.matchup_inputs%rowtype;
  t1  public.matchup_inputs%rowtype;
  t2  public.matchup_inputs%rowtype;
  v   text := project_matchup.venue;
  t1_home boolean;                       -- null = neutral site
  a_is_t1 boolean;
  -- v2
  pace float8; p1 float8; p2 float8; b1 float8; b2 float8; bt float8; bm float8;
  s1 float8; s2 float8; qg float8; hv float8; h1 float8; h2 float8; pre float8; mult float8;
  u1 float8; u2 float8; m1 float8;
  -- v1
  a1 float8; a2 float8; avg float8; pace1 float8; ppp1 float8; ppp2 float8; raw float8;
  to1 float8; to2 float8; rb1 float8; rb2 float8; poss1 float8; poss2 float8; c1 float8; c2 float8;
  tot float8; margin_a float8; win_a float8;
  r public.matchup_projection;
begin
  if project_matchup.team_a is null or project_matchup.team_b is null then
    raise exception 'team_a and team_b are required' using errcode = '22023';
  end if;
  if v is null or v not in ('neutral', 'a_home', 'b_home') then
    raise exception 'venue must be ''neutral'', ''a_home'' or ''b_home'' (got %)', coalesce(quote_literal(v), 'null')
      using errcode = '22023';
  end if;
  if project_matchup.team_a = project_matchup.team_b then
    raise exception 'team_a and team_b must be different teams (both are %)', project_matchup.team_a
      using errcode = '22023';
  end if;
  select * into a from public.matchup_inputs mi where mi.team_id = project_matchup.team_a;
  if not found then
    raise exception 'unknown team id % (no current-season matchup inputs)', project_matchup.team_a
      using errcode = '22023', hint = 'use a teams.id for a current D-I team';
  end if;
  select * into b from public.matchup_inputs mi where mi.team_id = project_matchup.team_b;
  if not found then
    raise exception 'unknown team id % (no current-season matchup inputs)', project_matchup.team_b
      using errcode = '22023', hint = 'use a teams.id for a current D-I team';
  end if;
  select mc.* into c from public.matchup_constants mc
   join public.teams t on t.league = mc.league where t.id = project_matchup.team_a;
  if not found then
    raise exception 'matchup constants are not loaded yet' using errcode = '55000';
  end if;

  if v = 'a_home' then t1 := b; t2 := a; t1_home := false; a_is_t1 := false;
  elsif v = 'b_home' then t1 := a; t2 := b; t1_home := false; a_is_t1 := true;
  else t1 := a; t2 := b; t1_home := null; a_is_t1 := true;
  end if;

  -- ── v2 margin (model.py project_game) ──
  pace := t1.tempo * t2.tempo / c.avg_tempo;
  p1 := t1.oe * t2.de / c.avg_eff / 100::float8;
  p2 := t2.oe * t1.de / c.avg_eff / 100::float8;
  b1 := p1 * pace;  b2 := p2 * pace;
  bt := c.total_intercept + c.total_slope * (b1 + b2);
  bm := c.margin_scale * (b1 - b2) * c.w_efficiency;
  s1 := (bt + bm) / 2::float8;  s2 := (bt - bm) / 2::float8;
  qg := c.qg_coef * (t1.kp_pct - t2.kp_pct) * 10::float8 * c.w_quality_gap;
  s1 := s1 + qg / 2::float8;  s2 := s2 - qg / 2::float8;
  if t1_home is null then
    h1 := 0; h2 := 0;
  else
    hv := (case when t1_home then t1.hca else t2.hca end) * c.hca_scale * c.w_hca;
    if t1_home then h1 := hv * c.hca_split; h2 := -hv * c.hca_split;
    else h1 := -hv * c.hca_split; h2 := hv * c.hca_split;
    end if;
  end if;
  pre := (s1 + h1) - (s2 + h2);
  mult := 1;
  if abs(pre) <= c.unit_close_threshold then mult := mult * c.unit_close_mult; end if;
  if t1_home is null then mult := mult * c.unit_neutral_mult; end if;
  mult := mult * c.unit_late_factor;
  if t1.unit_v2 is null or t2.unit_v2 is null or t1.unit_v2 = 'NaN'::float8 or t2.unit_v2 = 'NaN'::float8 then
    u1 := 0; u2 := 0;
  else
    u1 := (t1.unit_v2 - t2.unit_v2) * c.unit_scale * mult * c.w_unit;
    u2 := -u1;
  end if;
  s1 := s1 + (u1 + h1);  s2 := s2 + (u2 + h2);
  m1 := s1 - s2;

  -- ── v1 total (czarp/legacy_v1.py project_game) ──
  a1 := (t2.kp_pct - t1.kp_pct) * 0.5::float8;
  a2 := (t1.kp_pct - t2.kp_pct) * 0.5::float8;
  avg := c.v1_avg_tempo;
  pace1 := avg + (t1.tempo - avg) + (t2.tempo - avg);
  ppp1 := ((t1.oe + t2.de) / 2::float8) / 100::float8;
  ppp2 := ((t2.oe + t1.de) / 2::float8) / 100::float8;
  raw := ((c.v1_avg_to - t1.to_pct) + (c.v1_avg_dto - t2.d_to_pct)) / 2::float8;  to1 := raw + abs(raw) * a1;
  raw := ((c.v1_avg_to - t2.to_pct) + (c.v1_avg_dto - t1.d_to_pct)) / 2::float8;  to2 := raw + abs(raw) * a2;
  raw := ((t2.opp_or_pct - c.v1_avg_dor) + (t1.or_pct - c.v1_avg_or)) / 2::float8; rb1 := raw + abs(raw) * a1;
  raw := ((t1.opp_or_pct - c.v1_avg_dor) + (t2.or_pct - c.v1_avg_or)) / 2::float8; rb2 := raw + abs(raw) * a2;
  poss1 := pace1 + (pace1 * (rb1 * 0.01::float8) + pace1 * (to1 * 0.01::float8));
  poss2 := pace1 + (pace1 * (rb2 * 0.01::float8) + pace1 * (to2 * 0.01::float8));
  c1 := ppp1 * poss1;  c2 := ppp2 * poss2;
  c1 := c1 + (t1.unit_v1 - t2.unit_v1) * c.v1_unit_weight;
  c2 := c2 + (t2.unit_v1 - t1.unit_v1) * c.v1_unit_weight;
  if t1_home is not null then
    if t1_home then c1 := c1 + c.v1_hca * 0.5::float8; c2 := c2 - c.v1_hca * 0.5::float8;
    else c1 := c1 - c.v1_hca * 0.5::float8; c2 := c2 + c.v1_hca * 0.5::float8;
    end if;
  end if;
  -- legacy mround: round(x / 0.5) * 0.5, half-to-even (pg round(float8) uses rint)
  tot := pg_catalog.round(c1 / 0.5::float8) * 0.5::float8 + pg_catalog.round(c2 / 0.5::float8) * 0.5::float8;

  margin_a := case when a_is_t1 then m1 else -m1 end;
  win_a := 0.5::float8 * (1::float8 + pg_catalog.erf((margin_a / c.margin_sd) / pg_catalog.sqrt(2::float8)));

  r.team_a := project_matchup.team_a;
  r.team_b := project_matchup.team_b;
  r.venue := v;
  r.as_of := least(a.as_of, b.as_of, c.as_of);
  r.score_a := (tot + margin_a) / 2::float8;
  r.score_b := (tot - margin_a) / 2::float8;
  r.total := tot;
  r.spread_a := -margin_a;
  r.win_pct_a := win_a;
  r.win_pct_b := 1::float8 - win_a;
  return r;
end $$;

revoke all on function public.project_matchup(bigint, bigint, text) from public;
grant execute on function public.project_matchup(bigint, bigint, text) to anon, authenticated, service_role;

commit;

-- PostgREST: pick up the new function/tables now (Supabase also does this on DDL)
notify pgrst, 'reload schema';

-- ── verify (run after applying) ──────────────────────────────────────────────
-- 1. client roles: SELECT on team_aliases only; nothing on the private tables (expect 2 rows:
--    team_aliases/anon SELECT, team_aliases/authenticated SELECT)
-- select c.relname, r.rolname, a.privilege_type
-- from pg_class c join pg_namespace n on n.oid = c.relnamespace and n.nspname = 'public'
-- cross join lateral aclexplode(c.relacl) a join pg_roles r on r.oid = a.grantee
-- where c.relname in ('matchup_inputs','matchup_constants','team_aliases') and r.rolname in ('anon','authenticated')
-- order by 1, 2, 3;
-- 2. function: security definer, search_path='' and anon may execute (expect t, {search_path=""}, t)
-- select p.prosecdef, p.proconfig, has_function_privilege('anon', p.oid, 'execute')
-- from pg_proc p where p.oid = 'public.project_matchup(bigint,bigint,text)'::regprocedure;
