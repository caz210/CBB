-- CZarp outputs schema (Supabase / Postgres 15+) — multi-sport.
-- Stores model OUTPUTS only: games, projections, market prices, picks, model configs.
-- No raw KenPom (or other licensed) team statistics are exposed: the matchup maker's per-team inputs
-- (matchup_inputs) are private (RLS, no client grants) and reach clients only as projections via project_matchup().
-- The CSV ledger (data/ledger/ledger.csv) stays the source of truth; daily_run.py mirrors
-- into these tables when SUPABASE_URL + SUPABASE_SERVICE_KEY are set.
-- Security: RLS on every table. Model outputs are read-only for anon/authenticated and written only
-- by the service role (bypasses RLS); picks are public to everyone (anon + authenticated) as soon as they are written.
-- profiles/user_models are per-user (auth.uid()). Requires Supabase auth (auth.users, auth.uid()).
-- Front-end views: slate_games, value_bets, ledger. Idempotent: safe to re-run.

-- ── reference tables ────────────────────────────────────────────────────────
create table if not exists sports (
    id          smallint generated always as identity primary key,
    code        text not null unique,             -- 'cbb', 'nba', 'wnba', ...
    name        text not null
);

create table if not exists leagues (
    code        text primary key,                 -- 'ncaam' (NCAA D-I men), 'nba', 'wnba'
    sport_id    smallint not null references sports(id),
    name        text not null
);

create table if not exists teams (
    id            bigint generated always as identity primary key,
    sport_id      smallint not null references sports(id),
    league        text not null references leagues(code),
    season        int,                            -- last season seen (teams persist across seasons)
    name          text not null,                  -- canonical name (KenPom-style for CBB)
    external_ids  jsonb not null default '{}'::jsonb,   -- {"espn": "2005", "kalshi": "Air Force", ...}
    unique (league, name)
);

-- ── games ───────────────────────────────────────────────────────────────────
create table if not exists games (
    id            bigint generated always as identity primary key,
    sport_id      smallint not null references sports(id),
    league        text not null references leagues(code),
    season        int not null,                   -- season END year (2026-27 CBB season = 2027)
    date          date not null,                  -- local (America/Chicago) game date
    home          text not null,
    away          text not null,
    home_team_id  bigint references teams(id),
    away_team_id  bigint references teams(id),
    tipoff        timestamptz,
    neutral       boolean not null default false,
    external_id   text,                           -- ESPN event id
    external_ids  jsonb not null default '{}'::jsonb,
    status        text not null default 'scheduled' check (status in ('scheduled','in_progress','final','postponed','canceled')),
    home_score    int,
    away_score    int,
    updated_at    timestamptz not null default now(),
    unique (league, external_id)
);
create index if not exists games_league_date_idx on games (league, date);

-- ── model configs (feature list + weights; custom models are just new rows) ─
create table if not exists model_configs (
    model_version text primary key,               -- 'czarp-v2', 'czarp-v1-totals', 'my-custom-1'
    sport_id      smallint not null references sports(id),
    league        text not null references leagues(code),
    season        int,                            -- null = not season-specific
    description   text,
    features      jsonb not null,                 -- [{"name","kind","weight","enabled",...}]
    weights       jsonb not null,                 -- all numeric coefficients (calibration, sd, kelly, ...)
    is_default    boolean not null default false, -- the model shown on the public slate for its league
    created_at    timestamptz not null default now(),
    updated_at    timestamptz not null default now()
);
alter table model_configs add column if not exists is_default boolean not null default false;
create unique index if not exists model_configs_one_default on model_configs (league) where is_default;

-- ── projections (one row per game x model x run) ────────────────────────────
create table if not exists projections (
    id               bigint generated always as identity primary key,
    game_id          bigint not null references games(id) on delete cascade,
    sport_id         smallint not null references sports(id),
    league           text not null references leagues(code),
    season           int not null,
    model_version    text not null references model_configs(model_version),
    proj_home        numeric(6,2),
    proj_away        numeric(6,2),
    spread           numeric(5,2),                -- home line notation: -5.5 = home favored by 5.5
    total            numeric(6,2),
    home_win_prob    numeric(5,4),
    home_cover_prob  numeric(5,4),                -- vs the market spread at projection time
    over_prob        numeric(5,4),                -- vs the market total at projection time
    archetype_tags   jsonb not null default '{}'::jsonb,  -- {"home": [...], "away": [...], "notes": [...]}
    created_at       timestamptz not null default now(),
    unique (game_id, model_version, created_at)
);
create index if not exists projections_game_idx on projections (game_id, model_version, created_at desc);

-- ── market lines (timestamped price snapshots) ──────────────────────────────
create table if not exists market_lines (
    id          bigint generated always as identity primary key,
    game_id     bigint not null references games(id) on delete cascade,
    source      text not null check (source in ('vegas','kalshi','robinhood')),
    book        text,                             -- 'DraftKings', 'FanDuel', ... (vegas only)
    line_type   text not null default 'current' check (line_type in ('open','current','close')),
    spread      numeric(5,2),                     -- home line notation
    total       numeric(6,2),
    ml_home     numeric(8,2),                     -- American odds (vegas) or YES price in $ (kalshi/robinhood)
    ml_away     numeric(8,2),
    prices      jsonb not null default '{}'::jsonb,   -- spread/total juice, or the Kalshi ladder
    fetched_at  timestamptz not null default now(),
    unique (game_id, source, book, line_type, fetched_at)
);
create index if not exists market_lines_game_idx on market_lines (game_id, source, fetched_at desc);

-- ── picks (paper bets; mirrors the CSV ledger) ──────────────────────────────
create table if not exists picks (
    id             bigint generated always as identity primary key,
    pick_key       text not null unique,          -- ledger pick_id, e.g. '401812345-total'
    game_id        bigint not null references games(id) on delete cascade,
    sport_id       smallint not null references sports(id),
    league         text not null references leagues(code),
    season         int not null,
    market         text not null check (market in ('spread','total','moneyline')),
    bet            text not null,                 -- 'Tulane -6.5', 'Under 162.5'
    line_or_price  numeric(8,2),                  -- spread/total line, or ML price
    price          numeric(8,2),                  -- American odds taken
    edge           numeric(6,4),                  -- edge% = |model - line| / market total
    kelly_stake    numeric(10,2),                 -- paper $ on the notional bankroll
    model_version  text not null references model_configs(model_version),
    created_at     timestamptz not null default now(),
    result         text check (result in ('win','loss','push','void')),
    closing_line   numeric(8,2),
    clv            numeric(6,2),                  -- points, positive = beat the close
    pnl            numeric(10,2),
    graded_at      timestamptz,
    model_prob       numeric(5,4),                -- hit-rate estimate used for Kelly (conservative, per rule)
    bankroll_at_pick numeric(12,2),               -- paper bankroll when placed; 1 unit = 1% of it
    is_paper         boolean not null default true
);
alter table picks add column if not exists model_prob numeric(5,4);
alter table picks add column if not exists bankroll_at_pick numeric(12,2);
alter table picks add column if not exists is_paper boolean not null default true;
create index if not exists picks_league_created_idx on picks (league, created_at desc);

-- ── tiers, immutability, record views (db/migrations/2026-11-04_pick_tiers_and_immutability.sql) ──
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

revoke all on pick_record, tier_record from anon, authenticated;
grant select on pick_record, tier_record to anon, authenticated;


-- ── accounts: profiles + user_models (Supabase auth) ────────────────────────
create table if not exists profiles (
    user_id             uuid primary key references auth.users(id) on delete cascade,
    tier                text not null default 'free' check (tier in ('free','pro','syndicate')),
    stripe_customer_id  text unique,
    created_at          timestamptz not null default now(),
    updated_at          timestamptz not null default now()
);

create table if not exists user_models (
    id          uuid primary key default gen_random_uuid(),
    user_id     uuid not null default auth.uid() references auth.users(id) on delete cascade,
    sport       text not null references sports(code),
    name        text not null,
    features    jsonb not null default '[]'::jsonb,   -- same shape as model_configs.features
    weights     jsonb not null default '{}'::jsonb,
    created_at  timestamptz not null default now(),
    updated_at  timestamptz not null default now(),
    unique (user_id, sport, name)
);

-- plan limit: free = 1 saved model, pro/syndicate = unlimited. Tier is read from profiles
-- (set manually for comped friends, or by the billing webhook). A downgrade keeps existing rows
-- but blocks new inserts. Rows can never be moved to another user.
create or replace function public.enforce_user_model_limit() returns trigger
language plpgsql security definer set search_path = public as $$
declare
  v_tier  text;
  v_count int;
begin
  if tg_op = 'UPDATE' then
    if new.user_id is distinct from old.user_id then
      raise exception 'Saved models cannot be reassigned to another user'
        using errcode = '42501';
    end if;
    return new;
  end if;
  -- serialize concurrent inserts for the same user so two requests can't both slip under the limit
  perform pg_advisory_xact_lock(hashtextextended('user_models:' || new.user_id::text, 0));
  select tier into v_tier from public.profiles where user_id = new.user_id;
  if coalesce(v_tier, 'free') = 'free' then
    select count(*) into v_count from public.user_models where user_id = new.user_id;
    if v_count >= 1 then
      raise exception 'Free plan allows 1 saved model; upgrade to Pro'
        using errcode = 'P0001', hint = 'Delete your existing model or upgrade to Pro for unlimited models.';
    end if;
  end if;
  return new;
end $$;
drop trigger if exists user_models_limit on user_models;
create trigger user_models_limit before insert or update of user_id on user_models
  for each row execute function public.enforce_user_model_limit();

-- new auth user -> free profile (tier/stripe id are changed only by the service role, e.g. a Stripe webhook)
create or replace function public.handle_new_user() returns trigger
language plpgsql security definer set search_path = public as $$
begin
  insert into public.profiles (user_id) values (new.id) on conflict (user_id) do nothing;
  return new;
end $$;
drop trigger if exists on_auth_user_created on auth.users;
create trigger on_auth_user_created after insert on auth.users
  for each row execute function public.handle_new_user();

-- paid tier check, kept for future tier gating; picks_read no longer uses it (security definer so it can read profiles under RLS)
create or replace function public.is_pro() returns boolean
language sql stable security definer set search_path = public as $$
  select exists (select 1 from public.profiles p where p.user_id = auth.uid() and p.tier in ('pro','syndicate'));
$$;
grant execute on function public.is_pro() to anon, authenticated;

-- ── RLS ─────────────────────────────────────────────────────────────────────
-- Outputs: public read-only; all writes via the service role (bypasses RLS).
do $$
declare t text;
begin
  foreach t in array array['sports','leagues','teams','games','model_configs','projections','market_lines','picks'] loop
    execute format('alter table %I enable row level security', t);
    execute format('drop policy if exists %I on %I', t || '_read', t);
    execute format('revoke all on %I from anon, authenticated', t);
    execute format('grant select on %I to anon, authenticated', t);
    if t <> 'picks' then
      execute format('create policy %I on %I for select to anon, authenticated using (true)', t || '_read', t);
    end if;
  end loop;
end $$;
-- picks (value bets): public to everyone, matching the live picks_read policy (no tip-off or tier gate).
create policy picks_read on picks for select to anon, authenticated using (true);

-- profiles: read your own row only; no client writes (tier is set server-side)
alter table profiles enable row level security;
drop policy if exists profiles_read_own on profiles;
create policy profiles_read_own on profiles for select to authenticated using (user_id = auth.uid());
revoke all on profiles from anon;
revoke insert, update, delete on profiles from authenticated;
grant select on profiles to authenticated;

-- user_models: full CRUD on your own rows only
alter table user_models enable row level security;
drop policy if exists user_models_own on user_models;
create policy user_models_own on user_models for all to authenticated
  using (user_id = auth.uid()) with check (user_id = auth.uid());
revoke all on user_models from anon;
grant select, insert, update, delete on user_models to authenticated;

-- ── front-end read views (names/columns match czarp-web src/lib/supabase/README.md) ─
-- security_invoker: the caller's RLS applies (so value_bets/ledger inherit the picks tier gate).
-- dropped first so column renames/additions apply cleanly on re-run
drop view if exists slate_games;
drop view if exists value_bets;
drop view if exists ledger;

create view slate_games with (security_invoker = true) as
select s.code                       as sport,
       g.date                       as game_date,
       g.id                         as game_id,
       g.home, g.away,
       g.tipoff,                                          -- timestamptz
       g.away || ' @ ' || g.home    as game,              -- 'Away @ Home'
       p.proj_home, p.proj_away,
       p.spread                     as proj_spread,       -- home line notation
       p.total                      as proj_total,
       p.home_win_prob, p.home_cover_prob,
       p.archetype_tags             as archetypes,        -- {"home": [tag names], "away": [...], "home_tentative": bool, "away_tentative": bool, "tentative": bool, "notes": [...]}
       m.spread                     as market_spread,     -- home line, negative = home favored
       m.total                      as market_total,
       m.source                     as market_source      -- 'vegas' (book in market_lines.book)
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
       pk.logged_at                               as logged_at,            -- when the pick was written (before tip-off)
       pk.pick_hash                                                        -- tamper-evident hash of the pick
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
       pk.pick_hash,
       pk.logged_at                                    -- when the pick was written to the ledger
from picks pk
join games g  on g.id = pk.game_id
join sports s on s.id = pk.sport_id
where pk.result is distinct from 'void';

revoke all on slate_games, value_bets, ledger, pick_record, tier_record from anon, authenticated;
grant select on slate_games, value_bets, ledger, pick_record, tier_record to anon, authenticated;
-- (To restrict reads to signed-in users only, change "to anon, authenticated" to "to authenticated".)

-- ── matchup maker: private inputs + public.project_matchup() ────────────────
-- (mirrors db/migrations/2026-11-19_matchup_maker.sql; aliases are seeded by
--  db/migrations/2026-11-19_team_aliases_seed.sql, generated by scripts/build_team_aliases.py)
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
-- ("a&m", "wildcats", "sdsu"), so the key is (alias, team_id).
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

-- ── full-precision core: PRIVATE ──────────────────────────────────────────────
-- Full float8 outputs would let anyone solve for the private, licensed per-team inputs (e.g. two
-- calls give 1.109*(hca_a + hca_b) to the last bit). So the exact math lives in a schema no client
-- role can use, and the public RPC below returns display-rounded numbers only. Parity checks run
-- against this function (as the owner) and against the Python twin, before any rounding.
create schema if not exists matchup_private;
revoke all on schema matchup_private from public, anon, authenticated, service_role;

drop function if exists public.project_matchup(bigint, bigint, text);
drop function if exists matchup_private.project_matchup_full(bigint, bigint, text);
drop type if exists public.matchup_projection;
drop type if exists matchup_private.matchup_full;
create type matchup_private.matchup_full as (
    team_a     bigint,
    team_b     bigint,
    venue      text,
    as_of      timestamptz,
    total      float8,       -- CZarp total (already a multiple of 0.5)
    margin_a   float8,       -- team_a margin, full precision
    win_pct_a  float8        -- full precision
);

-- Twin of czarp/matchup.py project(); same operation order (keep them in sync: the daily run's
-- parity check compares that Python twin with the model every morning, and
-- tests/test_matchup.py compares this function with it).
--   total     = v1 totals model: each team rounded to 0.5 (round() on float8 is half-to-even,
--               like Python), then summed  -> the CZarp total the daily run uses
--   margin_a  = v2 margin (efficiency, calibration, unit score, per-team home court)
--   win_pct_a = Phi(margin_a / margin_sd)
-- The daily run projects (away, home): a home team is team2, and at a neutral site team_a takes
-- the listed-away (team1) role.
create or replace function matchup_private.project_matchup_full(team_a bigint, team_b bigint, venue text default 'neutral')
returns matchup_private.matchup_full
language plpgsql stable
set search_path = ''
as $$
declare
  c   public.matchup_constants%rowtype;
  a   public.matchup_inputs%rowtype;
  b   public.matchup_inputs%rowtype;
  t1  public.matchup_inputs%rowtype;
  t2  public.matchup_inputs%rowtype;
  v   text := project_matchup_full.venue;
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
  r matchup_private.matchup_full;
begin
  if project_matchup_full.team_a is null or project_matchup_full.team_b is null then
    raise exception 'team_a and team_b are required' using errcode = '22023';
  end if;
  if v is null or v not in ('neutral', 'a_home', 'b_home') then
    raise exception 'venue must be ''neutral'', ''a_home'' or ''b_home'' (got %)',
      coalesce(quote_literal(left(v, 20)) || case when length(v) > 20 then '...' else '' end, 'null')
      using errcode = '22023';
  end if;
  if project_matchup_full.team_a = project_matchup_full.team_b then
    raise exception 'team_a and team_b must be different teams (both are %)', project_matchup_full.team_a
      using errcode = '22023';
  end if;
  select * into a from public.matchup_inputs mi where mi.team_id = project_matchup_full.team_a;
  if not found then
    raise exception 'unknown team id % (no current-season matchup inputs)', project_matchup_full.team_a
      using errcode = '22023', hint = 'use a teams.id for a current D-I team';
  end if;
  select * into b from public.matchup_inputs mi where mi.team_id = project_matchup_full.team_b;
  if not found then
    raise exception 'unknown team id % (no current-season matchup inputs)', project_matchup_full.team_b
      using errcode = '22023', hint = 'use a teams.id for a current D-I team';
  end if;
  select mc.* into c from public.matchup_constants mc
   join public.teams t on t.league = mc.league where t.id = project_matchup_full.team_a;
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

  r.team_a := project_matchup_full.team_a;
  r.team_b := project_matchup_full.team_b;
  r.venue := v;
  r.as_of := least(a.as_of, b.as_of, c.as_of);
  r.total := tot;
  r.margin_a := margin_a;
  r.win_pct_a := win_a;
  return r;
end $$;

revoke all on function matchup_private.project_matchup_full(bigint, bigint, text) from public, anon, authenticated, service_role;

-- ── public RPC: display-rounded ──────────────────────────────────────────────
-- Rounding rule (czarp/matchup.py public_projection() is the Python twin):
--   total     multiple of 0.5 (the CZarp total, unchanged)
--   spread_a  0.1: rint(-margin_a * 10) / 10 (half-to-even, same as Python round())
--   score_a   (total - spread_a) / 2 and score_b = (total + spread_a) / 2, exact decimals computed
--             after rounding, so score_a + score_b = total and score_a - score_b = -spread_a always.
--             Scores are on a 0.1 step except when the total's and the spread's tenths don't pair
--             up (total 145.5 with spread -1.0): then both sit 0.05 off it (72.25 / 73.25). That
--             0.05 is the only adjustment; the total and the spread are never moved.
--   win_pct   0.001: rint(win_a * 1000) / 1000; win_pct_b = 1 - win_pct_a (sums to exactly 1)
-- Returns one composite (PostgREST: one JSON object). numeric, so JSON shows 72.25, not 72.2500001.
create type public.matchup_projection as (
    team_a     bigint,
    team_b     bigint,
    venue      text,
    as_of      timestamptz,
    score_a    numeric,
    score_b    numeric,
    total      numeric,
    spread_a   numeric,
    win_pct_a  numeric,
    win_pct_b  numeric
);

create or replace function public.project_matchup(team_a bigint, team_b bigint, venue text default 'neutral')
returns public.matchup_projection
language plpgsql stable security definer
set search_path = ''
as $$
declare
  f   matchup_private.matchup_full;
  r   public.matchup_projection;
  sp  numeric;
  w   numeric;
  t   numeric;
begin
  -- validation (null / same team / unknown id / bad venue) happens in the core and raises 22023
  f  := matchup_private.project_matchup_full(project_matchup.team_a, project_matchup.team_b, project_matchup.venue);
  t  := f.total::numeric;                                                    -- exact: k * 0.5
  sp := pg_catalog.round(pg_catalog.round(-f.margin_a * 10::float8)::numeric / 10, 1);
  w  := pg_catalog.round(pg_catalog.round(f.win_pct_a * 1000::float8)::numeric / 1000, 3);
  r.team_a    := f.team_a;
  r.team_b    := f.team_b;
  r.venue     := f.venue;
  r.as_of     := f.as_of;
  r.total     := pg_catalog.round(t, 1);
  r.spread_a  := sp;
  r.score_a   := pg_catalog.round((t - sp) / 2, 2);                          -- exact: k * 0.05
  r.score_b   := pg_catalog.round((t + sp) / 2, 2);
  r.win_pct_a := w;
  r.win_pct_b := 1 - w;
  return r;
end $$;

revoke all on function public.project_matchup(bigint, bigint, text) from public;
grant execute on function public.project_matchup(bigint, bigint, text) to anon, authenticated, service_role;

-- ── seed: CBB ───────────────────────────────────────────────────────────────
insert into sports (code, name) values ('cbb', 'College Basketball') on conflict (code) do nothing;
insert into leagues (code, sport_id, name)
select 'ncaam', id, 'NCAA Division I Men''s Basketball' from sports where code = 'cbb'
on conflict (code) do nothing;
-- Later, no migration needed:
-- insert into sports (code, name) values ('nba','NBA'), ('wnba','WNBA') on conflict (code) do nothing;
-- insert into leagues (code, sport_id, name) select 'nba',  id, 'National Basketball Association'   from sports where code='nba'  on conflict do nothing;
-- insert into leagues (code, sport_id, name) select 'wnba', id, 'Women''s National Basketball Association' from sports where code='wnba' on conflict do nothing;
