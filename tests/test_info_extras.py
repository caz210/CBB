"""Points so far + high-total Unders tracker: info only, never picks/verdicts/ledger."""
from datetime import date, datetime

import pandas as pd

import daily_run as dr
from czarp import htu_tracker as HTU
from czarp import season_form as SF

NOW = datetime(2026, 11, 12, 7, 0, tzinfo=dr.CENTRAL)
D1 = {"Boise State": "Boise St.", "Oakland": "Oakland", "Alabama": "Alabama", "Utah Tech": "Utah Tech"}


def match(n):
    return D1.get(n)


def _fin(eid, day, home, away, hs, as_):
    return dict(event_id=eid, game_date=day, home=home, away=away, home_score=hs, away_score=as_, completed=True)


FINALS = [
    _fin("1", "2026-11-03", "Boise State", "Utah Tech", 100, 80),      # 180
    _fin("2", "2026-11-05", "Boise State", "College of Idaho", 132, 50),  # non-D-I opponent: excluded
    _fin("3", "2026-11-06", "Boise State", "Oakland", 95, 91),         # 186 (both teams)
    _fin("4", "2026-11-07", "Alabama", "Oakland", 109, 60),            # 169
    _fin("5", "2026-11-12", "Alabama", "Utah Tech", 90, 70),           # same day/after: excluded
]


def _g(**kw):
    g = dict(event_id="9", game_date="2026-11-11", kp_home="Boise St.", kp_away="Oakland",
             open_total=162.5, current_total=161.5, v1_total=152.0, state="pre", tip_ct="2026-11-11 20:00")
    g.update(kw)
    return g


def test_points_so_far_excludes_non_d1_and_future_games():
    logs = SF.team_totals(FINALS, match)
    p = SF.points_so_far(_g(), logs)
    # Boise: (180+186)/2 = 183 ; Oakland: (186+169)/2 = 177.5 ; combined 180.25
    assert p["n_home"] == 2 and p["n_away"] == 2
    assert p["points"] == 180.2 or p["points"] == 180.3
    assert p["vs_line"] == round(p["points"] - 161.5, 1)
    assert p["czarp_side"] == "Under" and p["agrees"] is False
    line = SF.fmt_line(p)
    assert line.startswith("Points so far: 180.")
    assert "above line, against CZarp Under" in line and "D-I games" in line and "info only" in line


def test_points_so_far_skips_when_a_team_has_no_games():
    logs = SF.team_totals(FINALS, match)
    assert SF.points_so_far(_g(kp_away="Nobody"), logs) is None
    assert SF.fmt_line(None) is None and SF.fmt_cell(None) == "—"


def test_chat_block_shows_points_line_only_when_present():
    r = dict(g=dict(_g(), open_home_spread=-17.5, current_home_spread=-16.5, v2_home_margin=22.0, neutral=False),
             market="total", side="Under", qualifies=True, edge_pct=0.059, edge_pct_vs_open=0.065, thr=0.03,
             reasons=[], tier=None)
    assert not any(x.startswith("Points so far") for x in dr.format_chat_edge_block(r))
    r["pts_so_far"] = SF.points_so_far(r["g"], SF.team_totals(FINALS, match))
    block = dr.format_chat_edge_block(r)
    assert block[-1].startswith("Points so far: 180.")
    assert block[-2].startswith("Edge: Under")      # verdict line unchanged
    assert "✅ Official play" in block[-2]


def test_load_finals_caches_completed_days(tmp_path):
    calls = []

    def fetch(d):
        calls.append(d)
        return [dict(event_id=str(d.day), game_date=d.isoformat(), home="Oakland", away="Alabama",
                     home_score=70, away_score=60, completed=True)]
    out = SF.load_finals("2026-11-04", fetch, cache_dir=str(tmp_path))
    assert len(out) == 3 and len(calls) == 3           # Nov 1-3
    out2 = SF.load_finals("2026-11-04", fetch, cache_dir=str(tmp_path))
    assert len(out2) == 3 and len(calls) == 3          # served from cache


def test_htu_qualify_log_grade_record(tmp_path):
    games = [
        _g(event_id="a"),                                            # 161.5 posted, CZ 152 → gap 9.5 ✔
        _g(event_id="b", current_total=154.5, v1_total=140.0),       # line < 155 ✘
        _g(event_id="c", current_total=160.0, v1_total=157.0),       # gap 3 ✘
        _g(event_id="d", current_total=160.0, v1_total=165.0),       # CZ over ✘
        _g(event_id="e", current_total=158.0, v1_total=154.0, open_total=156.0),  # gap 4 exactly ✔
        _g(event_id="f", skip="non-D1 / unmatched team"),            # skipped game ✘
        _g(event_id="g", state="in"),                                # already started ✘ (live)
    ]
    df, n = HTU.log_games(HTU.read(str(tmp_path / "x.csv")), games, NOW)
    assert n == 2 and set(df.event_id) == {"a", "e"}
    df, n2 = HTU.log_games(df, [_g(event_id="a", current_total=170.0)], NOW)   # first log wins
    assert n2 == 0 and float(df.loc[df.event_id == "a", "posted_line"].iloc[0]) == 161.5
    fin = [dict(event_id="a", home_score=111, away_score=79, completed=True),
           dict(event_id="e", home_score=78, away_score=79, completed=True)]   # 157: win vs 158, loss vs 156
    df, g = HTU.grade(df, fin, NOW)
    assert g == 2
    rec = HTU.record(df)
    assert rec["posted"] == (1, 1, 0) and rec["open"] == (0, 2, 0) and rec["pending"] == 0
    line = HTU.record_line(df)
    assert "1-1 (50%) vs posted" in line and "0-2 (0%) vs open" in line and "info only" in line
    p = tmp_path / "x.csv"
    HTU.write(df, str(p))
    assert len(HTU.read(str(p))) == 2


def test_htu_never_touches_ledger_and_summary_line(tmp_path, monkeypatch):
    monkeypatch.setattr(HTU, "PATH", str(tmp_path / "htu.csv"))
    monkeypatch.setattr(dr, "write_ledger", lambda *a, **k: (_ for _ in ()).throw(AssertionError("no ledger write")))
    line = dr.update_htu_tracker([_g(event_id="a")], [], NOW, write=True)
    assert "0-0 vs posted" in line and "1 pending" in line
    assert (tmp_path / "htu.csv").exists()
    # dry/resync: nothing saved
    monkeypatch.setattr(HTU, "PATH", str(tmp_path / "htu2.csv"))
    dr.update_htu_tracker([_g(event_id="a")], [], NOW, write=False)
    assert not (tmp_path / "htu2.csv").exists()
