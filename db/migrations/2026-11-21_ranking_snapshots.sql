-- Weekly CZarp rankings: one row per team per official poll date (Monday, America/Chicago).
-- Rating is the model's projected margin vs a league-average D-I team on a
-- neutral floor (same formula as public.project_matchup / czarp.matchup).
-- Additive; does not touch picks, the ledger, or the hash chain.
--
-- Grants: SELECT only for anon/authenticated, same pattern as team_profiles.
-- No wider default privileges. Apply only after Scantron approves.

create table if not exists ranking_snapshots (
    id              bigint generated always as identity primary key,
    as_of           date not null,                 -- America/Chicago date of the official poll (Monday)
    week_of         date not null,                 -- same as as_of when published on Monday
    season          int not null,
    league          text not null references leagues(code),
    team_id         bigint not null references teams(id) on delete cascade,
    rank            int not null check (rank >= 1),
    prev_rank       int,                           -- rank in the previous official snapshot; null = unranked / first poll
    rating          numeric(6,2) not null,         -- projected margin vs an average D-I team, neutral floor
    di_wins         int,                           -- W from games we have (D-I vs D-I only)
    di_losses       int,
    kp_wins         int,                           -- KenPom W-L that morning (includes non-D1); preferred public record
    kp_losses       int,
    games_played    int,
    created_at      timestamptz not null default now(),
    unique (as_of, team_id)
);
create index if not exists ranking_snapshots_as_of_idx
  on ranking_snapshots (league, as_of desc, rank);
create index if not exists ranking_snapshots_team_idx
  on ranking_snapshots (team_id, as_of desc);

comment on table ranking_snapshots is
  'Official weekly CZarp rankings (Monday 7 AM CT). rating = projected margin vs an average D-I team on a neutral floor.';
comment on column ranking_snapshots.rating is
  'CZarp projected margin (points) vs a league-average D-I opponent on a neutral floor. Higher is better.';
comment on column ranking_snapshots.kp_wins is
  'KenPom wins as of that morning, including non-D1 games. Preferred for the public record.';
comment on column ranking_snapshots.di_wins is
  'Wins in games we store (D-I vs D-I only). Will trail KenPom by 1–3 games for many teams.';

alter table ranking_snapshots enable row level security;
drop policy if exists ranking_snapshots_read on ranking_snapshots;
create policy ranking_snapshots_read on ranking_snapshots
  for select to anon, authenticated using (true);

revoke all on table ranking_snapshots from anon, authenticated;
grant select on table ranking_snapshots to anon, authenticated;
