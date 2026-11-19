"""Publish matchup-maker inputs (matchup_inputs + matchup_constants) without a full daily run.

The daily run does this automatically after projections. Use this for the first fill right after
the migration is applied, or to refresh by hand. Uses the cached KenPom CSVs in data/ (no refresh)
and the service role (SUPABASE_URL + SUPABASE_SERVICE_KEY from the environment; never printed).

  python scripts/publish_matchup_inputs.py --dry-run          # build + parity self-check only
  python scripts/publish_matchup_inputs.py                     # upsert to Supabase
  python scripts/publish_matchup_inputs.py --sql-out f.sql --team-ids ids.json   # SQL for a scratch db
"""
from __future__ import annotations

import argparse
import contextlib
import io
import json
import os
import random
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
os.chdir(ROOT)


def load():
    import model as M
    import daily_run as D
    from czarp import matchup as MU
    with contextlib.redirect_stdout(io.StringIO()):
        data = M.load_data()
    data["config"] = M.load_config()
    today = datetime.now(ZoneInfo("America/Chicago")).date().isoformat()
    v1 = D.v1_frames(M._context(data, data["config"])["tt"])
    rows, consts = MU.build_inputs(data, data["config"], as_of=today, v1data=v1)
    return data, v1, rows, consts, today


def self_check(data, v1, rows, consts, today, n=200) -> int:
    """Compare project() with model.project_game / legacy v1 on random pairs, every venue."""
    import model as M
    import czarp.legacy_v1 as V1
    from czarp import matchup as MU
    names = sorted(rows); rnd = random.Random(7); bad = 0
    for _ in range(n):
        a, b = rnd.sample(names, 2)
        for venue in MU.VENUES:
            t1, t2 = (b, a) if venue == "a_home" else (a, b)
            flag = None if venue == "neutral" else False
            r = M.project_game(t1, t2, flag, data, game_date=today)
            r1 = V1.project_game(t1, t2, flag, v1)
            p = MU.project(rows[a], rows[b], venue, consts)
            m_a = r["spread_raw"] if venue != "a_home" else -r["spread_raw"]
            if abs(p["margin_a"] - m_a) > MU.PARITY_TOL or p["total"] != r1["total"]:
                bad += 1
    return bad


def sql_literal(v):
    if v is None:
        return "null"
    if isinstance(v, str):
        return "'" + v.replace("'", "''") + "'"
    return repr(float(v))          # repr round-trips a double exactly


def main(argv=None) -> int:
    from czarp import matchup as MU
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--sql-out")
    ap.add_argument("--team-ids", help="JSON {kenpom name: teams.id} for --sql-out")
    a = ap.parse_args(argv)
    data, v1, rows, consts, today = load()
    bad = self_check(data, v1, rows, consts, today)
    print(f"built inputs for {len(rows)} teams; self-check mismatches: {bad}; unsupported: {consts['unsupported'] or 'none'}")
    if bad or consts["unsupported"]:
        print("!!! the matchup formulas do not match the model — not publishing"); return 1
    now = datetime.now(ZoneInfo("America/Chicago")).isoformat(timespec="seconds")
    from czarp.supabase_writer import _season
    season = _season(today)
    if a.sql_out:
        ids = json.load(open(a.team_ids))
        with open(a.sql_out, "w") as f:
            cols = ("team_id", "season") + MU.INPUT_COLS + ("as_of",)
            f.write(f"insert into public.matchup_inputs ({', '.join(cols)}) values\n")
            f.write(",\n".join("(" + ", ".join([str(ids[n]), str(season)] + [sql_literal(rows[n][k]) for k in MU.INPUT_COLS]
                                                 + [sql_literal(now)]) + ")" for n in sorted(rows)))
            f.write("\non conflict (team_id) do update set " + ", ".join(f"{c} = excluded.{c}" for c in cols[1:]) + ";\n")
            ccols = ("league", "season", "model_version") + MU.CONST_COLS + ("as_of",)
            vals = [sql_literal(MU.LEAGUE), str(season), sql_literal(consts["model_version"])] + \
                   [sql_literal(consts[k]) for k in MU.CONST_COLS] + [sql_literal(now)]
            f.write(f"insert into public.matchup_constants ({', '.join(ccols)}) values ({', '.join(vals)})\n"
                    "on conflict (league) do update set " + ", ".join(f"{c} = excluded.{c}" for c in ccols[1:]) + ";\n")
        print(f"wrote {a.sql_out}")
        return 0
    if a.dry_run:
        return 0
    from czarp.supabase_writer import Client
    n = MU.write_supabase(Client(), rows, consts, season, now)
    print(f"upserted matchup_inputs for {n} teams + matchup_constants ({MU.LEAGUE}, season {season})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
