"""Build db/migrations/2026-11-19_team_aliases_seed.sql (search aliases -> KenPom team names).

Sources (all checked against the current KenPom team list; anything that doesn't resolve to a
real team is dropped):
  kenpom       the canonical name itself
  odds_map     team_map_master.csv  odds_name -> kenpom_name
  espn_map     backtest._ESPN_TO_KP / _ESPN_TO_KP_MULTI
  model_map    model.TEAM_NAME_MAP and czarp/legacy_v1.TEAM_NAME_MAP (either side may be the KenPom name)
  espn         ESPN team list (db/seed/espn_teams.csv): location, display name, short name,
               nickname, abbreviation, mascot — matched to KenPom with backtest.NameMatcher
  web          web/src/lib/teamAliases.ts common short names (from the main branch)
  variant      "St." written out ("Michigan State", "Saint Mary's")
Aliases are normalised exactly like the site's search box (czarp.matchup.normalize_alias).

  python scripts/build_team_aliases.py [--refresh-espn] [--web-aliases path/to/teamAliases.ts]
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
import pandas as pd  # noqa: E402

from backtest import _ESPN_TO_KP, _ESPN_TO_KP_MULTI, NameMatcher  # noqa: E402
from czarp.matchup import normalize_alias  # noqa: E402

ESPN_CSV = os.path.join(ROOT, "db", "seed", "espn_teams.csv")
OUT = os.path.join(ROOT, "db", "migrations", "2026-11-19_team_aliases_seed.sql")
ESPN_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/teams?limit=1000"
PRIORITY = ["kenpom", "variant", "web", "model_map", "espn_map", "odds_map", "espn"]


def refresh_espn() -> None:
    import requests
    d = requests.get(ESPN_URL, timeout=30).json()
    ts = [t["team"] for t in d["sports"][0]["leagues"][0]["teams"]]
    with open(ESPN_CSV, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["espn_id", "location", "name", "nickname", "abbreviation", "display_name", "short_display_name"])
        for t in sorted(ts, key=lambda t: int(t["id"])):
            w.writerow([t["id"], t["location"], t.get("name", ""), t.get("nickname", ""), t.get("abbreviation", ""),
                        t["displayName"], t["shortDisplayName"]])


def web_aliases(path: str | None) -> dict[str, list[str]]:
    if path:
        src = open(path).read()
    else:
        try:
            src = subprocess.run(["git", "show", "origin/main:web/src/lib/teamAliases.ts"], cwd=ROOT,
                                 capture_output=True, text=True, check=True).stdout
        except Exception:
            return {}
    out = {}
    for m in re.finditer(r'^\s*"([^"]+)":\s*\[([^\]]*)\]', src, re.M):
        out[m.group(1)] = re.findall(r'"([^"]+)"', m.group(2))
    return out


def st_variants(name: str) -> list[str]:
    toks = normalize_alias(name).split(" ")
    if "st" not in toks:
        return []
    out = []
    for i, t in enumerate(toks):
        if t != "st":
            continue
        lead, trail = i == 0, i == len(toks) - 1
        for word in (["saint"] if lead and not trail else ["state"] if trail and not lead else ["state", "saint"]):
            out.append(" ".join(toks[:i] + [word] + toks[i + 1:]))
    return out


def build(kp_names: list[str], web: dict) -> list[tuple[str, str, str]]:
    kp = set(kp_names)
    lower = {n.lower(): n for n in kp_names}
    match = NameMatcher(kp_names)
    pairs: dict[tuple[str, str], str] = {}

    def add(alias, team, source):
        team = lower.get(str(team).lower())
        a = normalize_alias(alias or "")
        if not team or not a:
            return
        key = (a, team)
        if key not in pairs or PRIORITY.index(source) < PRIORITY.index(pairs[key]):
            pairs[key] = source

    for n in kp_names:
        add(n, n, "kenpom")
        for v in st_variants(n):
            add(v, n, "variant")
    tm = pd.read_csv(os.path.join(ROOT, "team_map_master.csv"))
    for o, k in zip(tm["odds_name"], tm["kenpom_name"]):
        add(o, k, "odds_map")
    for e, k in _ESPN_TO_KP.items():
        add(e, match(k) or k, "espn_map")
    for e, ks in _ESPN_TO_KP_MULTI.items():
        for k in ks:
            if k in kp:
                add(e, k, "espn_map")
    import model as M
    import czarp.legacy_v1 as V1
    for mp in (M.TEAM_NAME_MAP, V1.TEAM_NAME_MAP):
        for k, v in mp.items():
            if k in kp and v not in kp:
                add(v, k, "model_map")
            elif v in kp and k not in kp:
                add(k, v, "model_map")
    if os.path.exists(ESPN_CSV):
        for r in csv.DictReader(open(ESPN_CSV)):
            team = match(r["location"]) or match(r["short_display_name"]) or match(r["nickname"])
            if not team:
                print(f"  ESPN team not matched to KenPom: {r['location']}", file=sys.stderr)
                continue
            for col in ("location", "display_name", "short_display_name", "nickname", "abbreviation", "name"):
                add(r[col], team, "espn")
    for team, al in web.items():
        for a in al:
            add(a, team, "web")
    return sorted((a, t, s) for (a, t), s in pairs.items())


def q(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--refresh-espn", action="store_true")
    ap.add_argument("--web-aliases")
    ap.add_argument("--ratings", default=os.path.join(ROOT, "data", "ratings.csv"))
    a = ap.parse_args(argv)
    if a.refresh_espn:
        refresh_espn()
    kp_names = sorted(pd.read_csv(a.ratings)["TeamName"].astype(str))
    rows = build(kp_names, web_aliases(a.web_aliases))
    with open(OUT, "w") as f:
        f.write("-- GENERATED by scripts/build_team_aliases.py — do not edit by hand; re-run the script.\n"
                "-- Seeds public.team_aliases (search spellings -> team). Idempotent: rows are matched to\n"
                "-- public.teams by KenPom name (league ncaam); names not in teams are skipped. Safe to re-run;\n"
                "-- it only inserts missing (alias, team_id) pairs and never deletes.\n"
                f"-- {len(rows)} aliases for {len({t for _, t, _ in rows})} teams.\n\n")
        f.write("begin;\n\ninsert into public.team_aliases (alias, team_id, source)\n"
                "select v.alias, t.id, v.source\nfrom (values\n")
        f.write(",\n".join(f"  ({q(al)}, {q(tm)}, {q(src)})" for al, tm, src in rows))
        f.write("\n) as v(alias, team_name, source)\n"
                "join public.teams t on t.league = 'ncaam' and t.name = v.team_name\n"
                "on conflict (alias, team_id) do nothing;\n\ncommit;\n")
    from collections import Counter
    print(f"wrote {OUT}: {len(rows)} aliases, {len({t for _, t, _ in rows})} teams, "
          f"by source {dict(Counter(s for *_, s in rows))}")
    amb = Counter(a for a, _, _ in rows)
    print(f"  aliases shared by 2+ teams: {sum(v > 1 for v in amb.values())} (e.g. "
          + ", ".join(a for a, v in amb.most_common(6)) + ")")
    return 0


if __name__ == "__main__":
    sys.exit(main())
