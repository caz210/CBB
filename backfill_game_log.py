"""Dry-run (default): show the writes that would complete the team game log.

Root cause: the D-I slate writer only stores games where both sides map to a
ratings name, and the public log reads model_grades (games ⨝ projections).
team_profiles.games_played is the ratings W+L, which includes non-D-I games,
so the log ran short. Two D-I openers on 2026-11-01 (before slate tracking
started) were also never stored, and one Nov 2 D-I game (Wright St. vs Air
Force) was stored with null team_ids so it did not attach to either log.

This script does not change picks, the ledger, or the hash chain, and it
does not change team_profiles.games_played (that count is correct).
Pass --apply to:

  * upsert completed D-I vs non-D-I games into public.non_di_games
  * insert any completed D-I vs D-I finals missing from public.games
    (no projections or market lines)
  * fill games.home_team_id / away_team_id when the name matches a team

Re-run after Scantron approves; do not apply from this report alone.

    python3 backfill_game_log.py [--start 2026-11-01] [--end YYYY-MM-DD] [--apply]
"""
from __future__ import annotations

import argparse
from datetime import date, datetime
from zoneinfo import ZoneInfo

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

from czarp.sync_game_log import sync_game_log
from czarp.supabase_writer import Client


class DryRunClient:
    """Records upserts and patches; never talks to the network for writes."""

    def __init__(self, inner: Client):
        self.inner = inner
        self.upserts: dict[str, list[dict]] = {}
        self.patches: list[dict] = []

    def get(self, table, **params):
        return self.inner.get(table, **params)

    def upsert(self, table, rows, on_conflict=None, returning=True, ignore_duplicates=False):
        self.upserts.setdefault(table, []).extend(rows)
        return []

    def patch(self, table, row, **params):
        rec = dict(row)
        rec["_table"] = table
        rec["_filters"] = dict(params)
        self.patches.append(rec)


def main():
    today = datetime.now(ZoneInfo("America/Chicago")).date()
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start", default="2026-11-01")
    ap.add_argument("--end", default=today.isoformat())
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    start, end = date.fromisoformat(a.start), date.fromisoformat(a.end)
    if end < start:
        raise SystemExit("end date is before start date")
    lookback = (end - start).days
    real = Client()
    client = real if a.apply else DryRunClient(real)
    stats = sync_game_log(lookback=lookback, day=end, client=client)
    mode = "APPLY" if a.apply else "DRY RUN"
    print(f"{mode} {start} .. {end}  ({lookback} lookback day(s))")
    print(f"  non_di_games: {stats['non_di_new']} new, {stats['non_di_unchanged']} unchanged, "
          f"{stats['non_di_updated']} updated")
    print(f"  games inserts (missing D-I finals): {stats['di_inserted']}")
    print(f"  games team_id patches: {stats['ids_filled']}")
    if stats.get("ids_unavailable"):
        print("  non-D-I writes skipped: ESPN teams list was unavailable")
    if not a.apply and isinstance(client, DryRunClient):
        for table, rows in client.upserts.items():
            print(f"  would upsert {len(rows)} row(s) into {table}")
            for r in rows[:8]:
                print(f"    {r.get('date')} {r.get('away')} @ {r.get('home')} "
                      f"eid={r.get('external_id')} {r.get('away_score')}-{r.get('home_score')}")
            if len(rows) > 8:
                print(f"    ... {len(rows) - 8} more")
        if client.patches:
            print(f"  would patch {len(client.patches)} games row(s) (team_ids):")
            for p in client.patches:
                print(f"    {p.get('_filters')} -> home_team_id={p.get('home_team_id')} "
                      f"away_team_id={p.get('away_team_id')}")
        print("No writes were made. Pass --apply to write, after review.")


if __name__ == "__main__":
    main()
