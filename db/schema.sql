-- CZarp outputs schema (Supabase / Postgres 15+) — multi-sport.
-- Stores model OUTPUTS only: games, projections, market prices, picks, model configs.
-- No raw KenPom (or other licensed) team statistics are stored here.
-- The CSV ledger (data/ledger/ledger.csv) stays the source of truth; daily_run.py mirrors
-- into these tables when SUPABASE_URL + SUPABASE_SERVICE_KEY are set.
-- Security: RLS on every table; anon/authenticated get read-only SELECT policies;
-- writes come only from the service role (which bypasses RLS). Idempotent: safe to re-run.

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
    created_at    timestamptz not null default now(),
    updated_at    timestamptz not null default now()
);

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
    graded_at      timestamptz
);
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

-- ── RLS: public read-only, service-role writes ──────────────────────────────
do $$
declare t text;
begin
  foreach t in array array['sports','leagues','teams','games','model_configs','projections','market_lines','picks'] loop
    execute format('alter table %I enable row level security', t);
    execute format('drop policy if exists %I on %I', t || '_read', t);
    execute format('create policy %I on %I for select to anon, authenticated using (true)', t || '_read', t);
    execute format('grant select on %I to anon, authenticated', t);
  end loop;
end $$;
grant select on pick_record to anon, authenticated;
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
