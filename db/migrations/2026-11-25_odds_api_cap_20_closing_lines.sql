-- Odds API cap raised from 5 to 20 paid calls per America/Chicago day (Christian, 24 Nov 2026: "raise the cap to
-- about 20 calls a day"; The Odds API 20K plan = 20,000 credits a month, ~3 credits a call) + pre-tip closing lines.
--
-- 1. odds_api_refresh_claims: the inline CHECK (n between 1 and 4) from 2026-11-21_intraday_line_refresh.sql is
--    replaced by odds_api_refresh_claims_n_range CHECK (n between 1 and 6), exactly refresh_lines.py's
--    REFRESH_PAID_CALLS_PER_DAY (6), so the database caps the refresh job at its real max (Scantron's ask on #86).
--    Existing rows (n <= 3) satisfy it.
-- 2. closing_line_snapshots (new, insert-only from pretip_check.py): the spread/total for games with an open official
--    or lean play, captured in the last CLOSE_WINDOW_MIN (30) minutes before tip, from ESPN/DK (free) and from
--    The Odds API (one 'pretip' call shared by the whole pretip run). One row per (game, source, capture time);
--    the closing line of a game = its newest row with captured_at < games.tipoff. pick_ids = the ledger pick ids it
--    was captured for. Lines are home-perspective, like market_lines.spread.
--    Service role only (RLS on, no policies, anon/authenticated revoked). Not shown to customers.
--
-- Does NOT touch picks, the picks_immutable trigger, the pick hash chain, or any view.
-- Apply after Scantron approves; the code is safe either way (pretip_check skips the snapshot when the table is
-- missing; refresh_lines only inserts n=1..6, and with the old CHECK it simply stops at n=4).

begin;

do $$
declare c record;
begin
  for c in select conname from pg_constraint
           where conrelid = 'public.odds_api_refresh_claims'::regclass and contype = 'c'
  loop
    execute format('alter table public.odds_api_refresh_claims drop constraint %I', c.conname);
  end loop;
end $$;

alter table odds_api_refresh_claims
  add constraint odds_api_refresh_claims_n_range check (n between 1 and 6);

create table if not exists closing_line_snapshots (
  id                 bigint generated always as identity primary key,
  game_id            bigint      not null references games(id) on delete cascade,
  event_id           text,                                   -- ESPN event id (games.external_id)
  game_date          date        not null,                   -- America/Chicago game date
  source             text        not null check (source in ('espn', 'oddsapi')),
  book               text,                                   -- 'DraftKings', 'fanduel', ...
  home_spread        numeric(5,2),                           -- home line notation
  total              numeric(6,2),
  home_spread_price  numeric(8,2),
  away_spread_price  numeric(8,2),
  over_price         numeric(8,2),
  under_price        numeric(8,2),
  tipoff             timestamptz,
  captured_at        timestamptz not null default now(),
  minutes_to_tip     numeric(6,1),
  pick_ids           text[]      not null default '{}',
  unique (game_id, source, captured_at),
  check (home_spread is not null or total is not null)
);

create index if not exists closing_line_snapshots_game_idx
  on closing_line_snapshots (game_id, source, captured_at desc);

alter table closing_line_snapshots enable row level security;
revoke all on closing_line_snapshots from anon, authenticated;

comment on table closing_line_snapshots is
  'Pre-tip line snapshots (pretip_check.py) for games with open official/lean plays; closing line = newest row before tipoff. Insert-only. Service role only.';

commit;
