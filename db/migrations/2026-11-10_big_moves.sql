-- BIG MOVE tracked record (info only).
-- One row per game / market / date where the DK line moved a lot from open to the
-- current line at report time: total >= 4 pts or spread >= 3 pts, either direction.
-- Paper/info only: never creates picks or bets; does not touch picks, picks_immutable
-- or the ledger hash chain.
--
-- Grading (daily run, czarp/big_moves.py) is against the CURRENT line at report time:
--   landed_with_move  = the final went past current_line in the direction the line moved
--   landed_with_czarp = CZarp's lean (czarp_side) beat current_line
-- A push (final exactly on the line) leaves the bool NULL with result_text 'push'.
-- Backfilled rows (source = 'backfill') use the stored open + morning-current lines;
-- the close is unavailable for those (close_available = false).

create table if not exists big_moves (
    id                bigint generated always as identity primary key,
    league            text not null default 'ncaam',
    game_id           bigint not null references games(id),
    game_date         date not null,
    market            text not null check (market in ('spread', 'total')),
    open_line         numeric(6,2),            -- DK open (home spread notation for spreads)
    current_line      numeric(6,2),            -- DK current at report time
    close_line        numeric(6,2),            -- DK close, filled at grading (null if unavailable)
    close_available   boolean,                 -- false = close not available (backfill / feed gap)
    move              numeric(6,2) not null,   -- current - open (signed; spreads in home notation)
    move_size         numeric(6,2) not null,   -- |move|
    direction         text not null,           -- 'up' / 'down' (totals), 'toward <team>' (spreads)
    move_side         text,                    -- 'Over' / 'Under' / team the line moved toward
    czarp_line        numeric(6,2),            -- CZarp total, or CZarp home spread
    czarp_side        text,                    -- CZarp lean vs current: 'Over' / 'Under' / team
    against_czarp     boolean,                 -- move went away from CZarp's lean
    official_pick     boolean not null default false,
    skipped           boolean not null default true,  -- not an official play
    skip_reason       text,
    final_home        int,
    final_away        int,
    final_value       numeric(6,1),            -- final total, or home margin
    result_text       text,                    -- e.g. 'Over 167.5: final 171' / 'push' / 'void'
    landed_with_move  boolean,
    landed_with_czarp boolean,
    note              text,
    source            text not null default 'live' check (source in ('live', 'backfill')),
    reported_at       timestamptz not null default now(),
    graded_at         timestamptz,
    unique (game_id, market, game_date)
);
create index if not exists big_moves_date_idx on big_moves (game_date);

alter table big_moves enable row level security;
drop policy if exists big_moves_read on big_moves;
create policy big_moves_read on big_moves for select to anon, authenticated using (true);
grant select on big_moves to anon, authenticated;

comment on table big_moves is
  'Info-only BIG MOVE tracker (total >= 4 / spread >= 3 pts open -> current). Never a pick or bet.';

create or replace view big_moves_record with (security_invoker = true) as
with g as (
  select market, against_czarp, landed_with_move, landed_with_czarp, result_text
  from big_moves where graded_at is not null and result_text is distinct from 'void'
), s as (
  select 'all'::text as scope, against_czarp, landed_with_move, landed_with_czarp, result_text from g
  union all
  select market, against_czarp, landed_with_move, landed_with_czarp, result_text from g
)
select scope,
       count(*)                                                            as graded,
       count(*) filter (where landed_with_move)                            as with_move_w,
       count(*) filter (where landed_with_move = false)                    as with_move_l,
       count(*) filter (where landed_with_move is null)                    as with_move_p,
       count(*) filter (where landed_with_czarp)                           as with_czarp_w,
       count(*) filter (where landed_with_czarp = false)                   as with_czarp_l,
       count(*) filter (where against_czarp and landed_with_czarp)         as against_czarp_w,
       count(*) filter (where against_czarp and landed_with_czarp = false) as against_czarp_l
from s
group by scope;

grant select on big_moves_record to anon, authenticated;
