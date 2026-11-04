"""Edit the per-team NEW COACH lock flags in config/coach_locks.csv (git-tracked).

    python3 coach_locks.py list                       # every flagged team this season
    python3 coach_locks.py on  "LSU" --coach "Will Wade" --note "McMahon fired" --source URL
    python3 coach_locks.py off "Rutgers" --note "acting HC, same program"
    python3 coach_locks.py on  "Butler" --sides 6 --totals 2   # custom lock lengths (0 = no lock that market)
    python3 coach_locks.py audit                      # KenPom auto-detected vs the file

Y = locked out of value plays until it has played the lock length; N = never locked. Teams not in the
file fall back to KenPom auto-detection. Commit the CSV after editing so the change is versioned.
Team names are KenPom names (e.g. "Saint Mary's", "N.C. State", "Kansas St.").
"""
from __future__ import annotations

import argparse
import sys
from datetime import date

import pandas as pd

from czarp import coach as C


def _season(s):
    if s:
        return int(s)
    from czarp.season import season_for_date
    return season_for_date(date.today())


def set_flag(team: str, on: bool, season: int, path: str | None = None, coach=None, prev=None,
             sides=None, totals=None, note=None, source=None) -> pd.DataFrame:
    path = path or C.LOCKS_FILE
    d = C.load_coach_locks(None, path)
    m = (pd.to_numeric(d.season, errors="coerce") == season) & (d.team.str.strip().str.lower() == team.strip().lower())
    if not m.any():
        d = pd.concat([d, pd.DataFrame([{c: "" for c in C.LOCK_COLUMNS} | {"season": str(season), "team": team}])],
                      ignore_index=True)
        m = d.index == d.index[-1]
    d.loc[m, "new_coach"] = "Y" if on else "N"
    for col, v in (("coach", coach), ("prev_coach", prev), ("lock_sides", sides), ("lock_totals", totals),
                   ("note", note), ("source", source)):
        if v is not None:
            d.loc[m, col] = str(v)
    d = d.sort_values(["season", "team"], key=lambda s: s.astype(str).str.lower())
    d.to_csv(path, index=False)
    return d


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("cmd", choices=["list", "on", "off", "audit"])
    ap.add_argument("team", nargs="?")
    ap.add_argument("--season", help="KenPom season (2027 = 2026-27); default = current")
    ap.add_argument("--file", default=None, help="flag file (default config/coach_locks.csv)")
    ap.add_argument("--coach"); ap.add_argument("--prev")
    ap.add_argument("--sides", help="side lock games for this team (blank=default, 0=no lock)")
    ap.add_argument("--totals", help="total lock games for this team (blank=default, 0=no lock)")
    ap.add_argument("--note"); ap.add_argument("--source")
    a = ap.parse_args(argv)
    season = _season(a.season)
    if a.cmd in ("on", "off"):
        if not a.team:
            ap.error("team required")
        set_flag(a.team, a.cmd == "on", season, a.file, a.coach, a.prev, a.sides, a.totals, a.note, a.source)
        print(f"{a.team}: new_coach={'Y' if a.cmd == 'on' else 'N'} ({season}) -> {a.file or C.LOCKS_FILE}. "
              "Commit the file to keep the change.")
        return 0
    d = C.load_coach_locks(season, a.file)
    if a.cmd == "list":
        for _, r in d.iterrows():
            extra = "".join(f" {k}={r[k]}" for k in ("lock_sides", "lock_totals") if str(r[k]).strip())
            print(f"{r.new_coach}  {r.team:<24} {r.prev_coach} -> {r.coach}{extra}  | {r.note}")
        print(f"{(d.new_coach.str.upper() == 'Y').sum()} locked (Y), {(d.new_coach.str.upper() == 'N').sum()} cleared (N)")
        return 0
    auto = set(C.detect_new_coaches(season, locks_path="/nonexistent"))
    final = set(C.detect_new_coaches(season, locks_path=a.file))
    listed = set(d.team)
    print("auto-detected but not in file:", sorted(auto - listed) or "none")
    print("cleared by file (N):", sorted(auto - final) or "none")
    print("added by file (not auto):", sorted(final - auto) or "none")
    print(f"final new-coach teams: {len(final)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
