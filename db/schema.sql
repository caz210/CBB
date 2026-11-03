-- CZarp outputs schema (Supabase / Postgres 15+) — multi-sport.
-- Stores model OUTPUTS only: games, projections, market prices, picks, model configs.
-- No raw KenPom (or other licensed) team statistics are stored here.
-- The CSV ledger (data/ledger/ledger.csv) stays the source of truth; daily_run.py mirrors
-- into these tables when SUPABASE_URL + SUPABASE_SERVICE_KEY are set.
-- Security: RLS on every table. Model outputs are read-only for anon/authenticated and written only
-- by the service role (bypasses RLS); picks are hidden until tip-off unless profiles.tier is pro/syndicate.
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

-- ── convenience view (security_invoker keeps the caller's RLS) ──────────────
create or replace view pick_record with (security_invoker = true) as
select p.league, p.season, p.model_version, p.market,
       count(*) filter (where p.result = 'win')  as wins,
       count(*) filter (where p.result = 'loss') as losses,
       count(*) filter (where p.result = 'push') as pushes,
       count(*) filter (where p.result is null)  as pending,
       sum(p.kelly_stake) filter (where p.result in ('win','loss','push')) as staked,
       sum(p.pnl)  as pnl,
       round(avg(p.clv), 2) as avg_clv
from picks p
group by 1, 2, 3, 4;

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

-- paid tier check, used by the picks policy (security definer so it can read profiles under RLS)
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
    execute format('revoke insert, update, delete on %I from anon, authenticated', t);
    execute format('grant select on %I to anon, authenticated', t);
    if t <> 'picks' then
      execute format('create policy %I on %I for select to anon, authenticated using (true)', t || '_read', t);
    end if;
  end loop;
end $$;
-- picks (value bets): everyone sees a pick once its game has tipped; Pro/Syndicate see it immediately.
create policy picks_read on picks for select to anon, authenticated using (
  public.is_pro()
  or exists (select 1 from games g where g.id = picks.game_id and g.tipoff is not null and g.tipoff <= now())
);

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
                                                  as kelly_quarter_units   -- 1u = 1% of bankroll
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
       pk.is_paper
from picks pk
join games g  on g.id = pk.game_id
join sports s on s.id = pk.sport_id
where pk.result is distinct from 'void';

grant select on slate_games, value_bets, ledger, pick_record to anon, authenticated;
-- (To restrict reads to signed-in users only, change "to anon, authenticated" to "to authenticated".)

-- ── seed: CBB ───────────────────────────────────────────────────────────────
insert into sports (code, name) values ('cbb', 'College Basketball') on conflict (code) do nothing;
insert into leagues (code, sport_id, name)
select 'ncaam', id, 'NCAA Division I Men''s Basketball' from sports where code = 'cbb'
on conflict (code) do nothing;
-- Later, no migration needed:
-- insert into sports (code, name) values ('nba','NBA'), ('wnba','WNBA') on conflict (code) do nothing;
-- insert into leagues (code, sport_id, name) select 'nba',  id, 'National Basketball Association'   from sports where code='nba'  on conflict do nothing;
-- insert into leagues (code, sport_id, name) select 'wnba', id, 'Women''s National Basketball Association' from sports where code='wnba' on conflict do nothing;
