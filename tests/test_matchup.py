"""Matchup maker: Python twin vs the model, parity guard, v1 alias fix, alias normalisation,
and (when MATCHUP_PG_DSN points at a db with the migrations applied) the SQL function itself."""
import math
import os
import random
import shutil
import subprocess

import pandas as pd
import pytest

import daily_run as D
import model as M
import czarp.legacy_v1 as V1
from czarp import matchup as MU
from tests.conftest import make_league

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _league_with_alias_teams():
    lg = make_league(n=40, seed=11)
    ren = {"Team37": "Queens", "Team38": "LIU", "Team39": "SIUE"}
    for k in ("ratings", "four_factors", "height", "misc"):
        lg[k] = lg[k].assign(TeamName=lg[k]["TeamName"].replace(ren))
    lg["config"] = M.load_config()
    return lg


@pytest.fixture(scope="module")
def built():
    data = _league_with_alias_teams()
    tt = M._context(data, data["config"])["tt"]
    v1 = D.v1_frames(tt)
    rows, consts = MU.build_inputs(data, data["config"], as_of="2026-11-18", v1data=v1)
    return data, v1, rows, consts


def _model(data, v1, a, b, venue):
    t1, t2 = (b, a) if venue == "a_home" else (a, b)
    flag = None if venue == "neutral" else False
    r = M.project_game(t1, t2, flag, data, game_date="2026-11-18")
    r1 = V1.project_game(t1, t2, flag, v1)
    m_a = r["spread_raw"] if venue != "a_home" else -r["spread_raw"]
    w_a = r["team1_win_prob"] if venue != "a_home" else r["team2_win_prob"]
    return m_a, w_a, r1["total"]


def test_supported_config_has_no_gaps():
    assert MU.unsupported_features(M.load_config()) == []


def test_python_twin_matches_model_every_venue(built):
    data, v1, rows, consts = built
    names = sorted(rows)
    rnd = random.Random(3)
    pairs = {tuple(rnd.sample(names, 2)) for _ in range(150)} | {("Queens", "LIU"), ("SIUE", "Team00"), ("Team01", "Queens")}
    for a, b in sorted(pairs):
        for venue in MU.VENUES:
            m_a, w_a, v1_total = _model(data, v1, a, b, venue)
            p = MU.project(rows[a], rows[b], venue, consts)
            assert abs(p["margin_a"] - m_a) <= 1e-9
            assert abs(p["win_pct_a"] - w_a) <= 1e-12
            assert p["total"] == v1_total                     # exact, incl. the 0.5 rounding
            assert p["score_a"] + p["score_b"] == pytest.approx(p["total"], abs=1e-9)
            assert p["score_a"] - p["score_b"] == pytest.approx(-p["spread_a"], abs=1e-9)
            assert p["win_pct_a"] + p["win_pct_b"] == pytest.approx(1.0)


def test_venue_symmetry(built):
    _, _, rows, consts = built
    x = MU.project(rows["Team02"], rows["Team05"], "a_home", consts)
    y = MU.project(rows["Team05"], rows["Team02"], "b_home", consts)
    assert x["margin_a"] == pytest.approx(-y["margin_a"], abs=1e-9) and x["total"] == y["total"]
    with pytest.raises(ValueError):
        MU.project(rows["Team02"], rows["Team05"], "home", consts)


def test_round_half_is_half_even():
    assert MU.round_half(70.25) == 70.0 and MU.round_half(70.75) == 71.0 and MU.round_half(70.26) == 70.5


# ── v1 alias bug (Queens / LIU / SIUE silently fell back to the v2 total) ─────
def test_v1_alias_fix_resolves_every_team(built):
    data, v1, rows, _ = built
    tt = M._context(data, data["config"])["tt"]
    assert D.v1_name_check(v1, tt.index.astype(str)) == []
    for n in ("Queens", "LIU", "SIUE"):
        assert n in rows
        r1 = V1.project_game(n, "Team00", False, v1)          # used to raise "Team not found"
        assert math.isfinite(r1["total"])


def test_v1_alias_bug_is_reported_without_fix(built, monkeypatch):
    data, _, _, _ = built
    tt = M._context(data, data["config"])["tt"]
    monkeypatch.setattr(D, "_v1_renames", lambda names: {})
    bad = dict(D.v1_name_check(D.v1_frames(tt), tt.index.astype(str)))
    assert set(bad) == {"Queens", "LIU", "SIUE"}


def test_project_all_logs_v1_failures(built, monkeypatch):
    data, _, _, _ = built
    logs = []
    monkeypatch.setattr(D, "log", logs.append)
    monkeypatch.setattr(V1, "project_game", lambda *a, **k: (_ for _ in ()).throw(ValueError("boom")))
    games = [{"home": "Team00", "away": "Queens", "neutral": False, "game_date": "2026-11-18"}]
    D.project_all(games, data)
    assert "v1_total" not in games[0]
    assert any("WARNING v1 total unavailable for Queens @ Team00" in x for x in logs)


# ── parity guard ──────────────────────────────────────────────────────────────
def test_parity_check_passes_on_project_all(built):
    data, _, rows, consts = built
    games = [{"home": "Team00", "away": "Queens", "neutral": False, "game_date": "2026-11-18"},
             {"home": "LIU", "away": "Team03", "neutral": True, "game_date": "2026-11-18"},
             {"home": "Team04", "away": "SIUE", "neutral": False, "game_date": "2026-11-18"}]
    D.project_all(games, data)
    res = MU.parity_check(games, rows, consts)
    assert res["checked"] == 3 and not res["mismatches"] and not res["missing"]
    assert res["max_margin_diff"] <= MU.PARITY_TOL


def test_parity_check_flags_a_drift(built):
    data, _, rows, consts = built
    games = [{"home": "Team00", "away": "Team01", "neutral": False, "game_date": "2026-11-18"}]
    D.project_all(games, data)
    games[0]["v2_home_margin"] += 0.01
    res = MU.parity_check(games, rows, consts)
    assert len(res["mismatches"]) == 1
    assert any("MATCHUP PARITY FAILED" in x for x in MU.parity_log_lines(res))


def test_matchup_parity_never_raises(monkeypatch):
    logs = []
    monkeypatch.setattr(D, "log", logs.append)
    assert D.matchup_parity([], {"config": None}, "2026-11-18") is None
    assert any("MATCHUP PARITY CHECK ERRORED" in x for x in logs)


# ── alias normalisation = the site's search normalize() ───────────────────────
@pytest.mark.parametrize("raw,norm", [
    ("Texas A&M", "texas am"), ("St. John's", "st johns"), ("Hawai'i", "hawaii"), ("San José State", "san jose state"),
    ("Arkansas-Pine Bluff", "arkansas pine bluff"), ("William & Mary", "william mary"), ("  UNC  ", "unc"),
])
def test_normalize_alias(raw, norm):
    assert MU.normalize_alias(raw) == norm


def test_alias_seed_covers_every_team_and_is_normalised():
    path = os.path.join(ROOT, "db", "migrations", "2026-11-19_team_aliases_seed.sql")
    rows = [ln.strip() for ln in open(path) if ln.strip().startswith("('")]
    assert len(rows) > 1000
    aliases = [r.split("', '")[0][2:].replace("''", "'") for r in rows]
    assert all(a == MU.normalize_alias(a) for a in aliases)
    for must in ("queens", "liu", "siue", "unc", "uconn", "ole miss"):
        assert must in aliases


# ── the SQL function itself (needs a Postgres 16+ db with the migrations applied) ──
DSN = os.getenv("MATCHUP_PG_DSN")


def _q(v):
    return "null" if v is None else repr(float(v))


@pytest.mark.skipif(not DSN or not shutil.which("psql"), reason="set MATCHUP_PG_DSN to a scratch db with the migrations applied")
def test_sql_function_matches_python_twin(built):
    data, v1, rows, consts = built
    names = sorted(rows)
    ids = {n: 900000 + i for i, n in enumerate(names)}
    cols = MU.INPUT_COLS
    rnd = random.Random(5)
    cases = sorted({tuple(rnd.sample(names, 2)) for _ in range(120)})
    sql = ["begin;", "delete from public.matchup_constants where league = 'ncaam';",
           "insert into public.teams (id, sport_id, league, season, name) overriding system value values " +
           ", ".join(f"({ids[n]}, (select id from public.sports where code='cbb'), 'ncaam', 2027, 'zz_test_{n}')" for n in names) + ";",
           f"insert into public.matchup_inputs (team_id, season, {', '.join(cols)}, as_of) values " +
           ", ".join(f"({ids[n]}, 2027, {', '.join(_q(rows[n][c]) for c in cols)}, now())" for n in names) + ";",
           f"insert into public.matchup_constants (league, season, model_version, {', '.join(MU.CONST_COLS)}, as_of) values "
           f"('ncaam', 2027, 'test', {', '.join(_q(consts[c]) for c in MU.CONST_COLS)}, now());",
           "select a, b, v, (p).total, (p).spread_a, (p).win_pct_a, (p).score_a, (p).score_b from (values " +
           ", ".join(f"({ids[a]}, {ids[b]}, '{v}')" for a, b in cases for v in MU.VENUES) +
           ") x(a, b, v) cross join lateral (select public.project_matchup(x.a, x.b, x.v) p) y;",
           "rollback;"]
    out = subprocess.run(["psql", DSN, "-X", "-q", "-A", "-t", "-F", ",", "-v", "ON_ERROR_STOP=1"],
                         input="\n".join(sql), capture_output=True, text=True, check=True).stdout
    back = {v: k for k, v in ids.items()}
    lines = [ln for ln in out.splitlines() if ln.count(",") == 7]
    assert len(lines) == len(cases) * 3
    for ln in lines:
        a, b, v, total, spread_a, win_a, sa, sb = ln.split(",")
        p = MU.project(rows[back[int(a)]], rows[back[int(b)]], v, consts)
        assert float(total) == p["total"]
        assert abs(float(spread_a) - p["spread_a"]) <= 1e-9
        assert abs(float(win_a) - p["win_pct_a"]) <= 1e-12
        assert abs(float(sa) + float(sb) - p["total"]) <= 1e-9


class _FakeClient:
    def __init__(self, teams, existing):
        self.teams, self.existing, self.upserts, self.deleted = teams, existing, {}, []

    def get(self, table, **params):
        return self.teams if table == "teams" else [{"team_id": t} for t in self.existing]

    def upsert(self, table, rows, on_conflict=None, returning=True, ignore_duplicates=False):
        self.upserts[table] = (rows, on_conflict)
        return []

    def delete(self, table, **params):
        self.deleted.append((table, params))


def test_write_supabase_payloads(built):
    _, _, rows, consts = built
    names = sorted(rows)
    c = _FakeClient([{"id": i + 1, "name": n} for i, n in enumerate(names[:-1])], existing=[1, 2, 999])
    logs = []
    n = MU.write_supabase(c, rows, consts, 2027, "2026-11-18T07:15:00-06:00", log=logs.append)
    assert n == len(names) - 1 and any("not in teams table" in x for x in logs)
    inp, key = c.upserts["matchup_inputs"]
    assert key == "team_id" and set(inp[0]) == {"team_id", "season", "as_of", *MU.INPUT_COLS}
    (crow,), ckey = c.upserts["matchup_constants"]
    assert ckey == "league" and crow["league"] == "ncaam" and set(MU.CONST_COLS) <= set(crow)
    assert c.deleted == [("matchup_inputs", {"team_id": "in.(999)"})]       # stale team removed
