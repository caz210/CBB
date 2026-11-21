-- Weekly CZarp rankings: one row per team per official poll week (Monday, America/Chicago).
-- Rating is the model's projected margin vs a league-average D-I team on a
-- neutral floor (same formula as public.project_matchup / czarp.matchup).
-- Additive; does not touch picks, the ledger, or the hash chain.
--
-- Grants: SELECT only for anon/authenticated, same pattern as team_profiles.
-- No wider default privileges. Apply only after Scantron approves.
-- Unique key is (league, week_of, team_id) so a week can hold only one poll.

create table if not exists ranking_snapshots (
    id              bigint generated always as identity primary key,
    as_of           date not null,                 -- America/Chicago date the snapshot was published
    week_of         date not null,                 -- Monday of the official poll week
    season          int not null,
    league          text not null references leagues(code),
    team_id         bigint not null references teams(id) on delete restrict,
    rank            int not null check (rank >= 1),
    prev_rank       int,                           -- rank in the previous official week; null = unranked / first poll
    rating          numeric(6,2) not null,         -- projected margin vs an average D-I team, neutral floor
    di_wins         int,                           -- W from games we have (D-I vs D-I only); this is the public record
    di_losses       int,
    kp_wins         int,                           -- W-L from the morning ratings file (includes non-D1 games)
    kp_losses       int,
    games_played    int,
    created_at      timestamptz not null default now(),
    unique (league, week_of, team_id)
);
create index if not exists ranking_snapshots_as_of_idx
  on ranking_snapshots (league, as_of desc, rank);
create index if not exists ranking_snapshots_week_idx
  on ranking_snapshots (league, week_of desc, rank);
create index if not exists ranking_snapshots_team_idx
  on ranking_snapshots (team_id, as_of desc);

-- If an earlier draft of this migration created unique (as_of, team_id), replace it.
do $$
begin
  if to_regclass('public.ranking_snapshots') is null then
    return;
  end if;
  if exists (
    select 1 from pg_constraint
    where conrelid = 'public.ranking_snapshots'::regclass
      and contype = 'u'
      and pg_get_constraintdef(oid) = 'UNIQUE (as_of, team_id)'
  ) then
    alter table ranking_snapshots drop constraint ranking_snapshots_as_of_team_id_key;
  end if;
  if not exists (
    select 1 from pg_constraint
    where conrelid = 'public.ranking_snapshots'::regclass
      and contype = 'u'
      and pg_get_constraintdef(oid) = 'UNIQUE (league, week_of, team_id)'
  ) then
    alter table ranking_snapshots
      add constraint ranking_snapshots_league_week_of_team_id_key unique (league, week_of, team_id);
  end if;
  -- An earlier draft used ON DELETE CASCADE, which would wipe poll history if a team was removed.
  if exists (
    select 1
      from pg_constraint c
      join pg_attribute a on a.attrelid = c.conrelid and a.attnum = any (c.conkey)
     where c.conrelid = 'public.ranking_snapshots'::regclass
       and c.contype = 'f'
       and a.attname = 'team_id'
       and pg_get_constraintdef(c.oid) ilike '%on delete cascade%'
  ) then
    alter table ranking_snapshots drop constraint ranking_snapshots_team_id_fkey;
    alter table ranking_snapshots
      add constraint ranking_snapshots_team_id_fkey
      foreign key (team_id) references teams(id) on delete restrict;
  end if;
end $$;

comment on table ranking_snapshots is
  'Official weekly CZarp rankings (Monday 7 AM CT). rating = projected margin vs an average D-I team on a neutral floor.';
comment on column ranking_snapshots.rating is
  'CZarp projected margin (points) vs a league-average D-I opponent on a neutral floor. Higher is better.';
comment on column ranking_snapshots.week_of is
  'Monday of the official poll week. A league+week has at most one snapshot.';
comment on column ranking_snapshots.as_of is
  'America/Chicago date the snapshot was published (Monday, or later in the week on a catch-up run).';
comment on column ranking_snapshots.di_wins is
  'Wins in games we store (D-I vs D-I only). This is the public record on the rankings page.';
comment on column ranking_snapshots.kp_wins is
  'Wins from the morning ratings file, including non-D1 games.';

alter table ranking_snapshots enable row level security;
drop policy if exists ranking_snapshots_read on ranking_snapshots;
create policy ranking_snapshots_read on ranking_snapshots
  for select to anon, authenticated using (true);

revoke all on table ranking_snapshots from anon, authenticated;
grant select on table ranking_snapshots to anon, authenticated;
revoke all on sequence ranking_snapshots_id_seq from anon, authenticated;
