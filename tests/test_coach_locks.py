import pandas as pd

import coach_locks as CL
import daily_run as dr
from czarp import coach as C
from tests.test_daily_run import _game, _now


def _setup(tmp_path):
    (tmp_path / "prior").mkdir()
    pd.DataFrame({"TeamName": ["A", "B", "C"], "Coach": ["New A", "Same B", "New C"],
                  "Wins": [1, 0, 0], "Losses": [0, 0, 0]}).to_csv(tmp_path / "ratings.csv", index=False)
    pd.DataFrame({"TeamName": ["A", "B", "C"], "Coach": ["Old A", "Same B", "Old C"]}).to_csv(
        tmp_path / "prior" / "ratings.csv", index=False)


def test_flag_file_overrides_auto_detection(tmp_path):
    _setup(tmp_path)
    f = tmp_path / "locks.csv"
    pd.DataFrame([
        {"season": 2027, "team": "A", "new_coach": "N", "note": "acting HC"},          # false positive cleared
        {"season": 2027, "team": "D", "new_coach": "Y", "coach": "Z", "lock_sides": "6", "lock_totals": "0"},  # miss added
        {"season": 2026, "team": "B", "new_coach": "Y"},                                 # other season ignored
    ]).to_csv(f, index=False)
    nc = C.detect_new_coaches(2027, str(tmp_path), locks_path=str(f))
    assert set(nc) == {"C", "D"}
    assert nc["D"]["lock_sides"] == 6 and nc["D"]["lock_totals"] == 0 and nc["D"]["source"] == "config/coach_locks.csv"
    assert "lock_sides" not in nc["C"]                                                  # auto entry untouched


def test_lock_len_parsing():
    assert C._lock_len("") is None and C._lock_len("Y") is None and C._lock_len(float("nan")) is None
    assert C._lock_len("N") == 0 and C._lock_len("0") == 0 and C._lock_len("5") == 5


def test_per_team_lock_lengths(monkeypatch):
    monkeypatch.setattr(dr, "COACH_LOCK_GAMES", None)
    monkeypatch.setattr(dr, "COACH_LOCK_SIDES", 8)
    monkeypatch.setattr(dr, "COACH_LOCK_TOTALS", 4)
    g = _game(v1_total=150.0, coach_home={"team": "H", "games": 3, "lock_sides": None, "lock_totals": 0})
    c = {x["market"]: x for x in dr.candidates([g], _now())}
    assert not c["spread"]["rule_ok"] and "Paused until game 8 (new coach, 3 played)" in " ".join(c["spread"]["reasons"])
    assert c["total"]["rule_ok"]                                                        # totals lock off for H
    g["coach_home"]["lock_sides"] = 2
    assert {x["market"]: x for x in dr.candidates([g], _now())}["spread"]["rule_ok"]   # 3 >= custom 2
    monkeypatch.setattr(dr, "COACH_LOCK_SIDES", 0)                                      # global off beats per-team
    g["coach_home"].update(lock_sides=20, games=0)
    assert {x["market"]: x for x in dr.candidates([g], _now())}["spread"]["rule_ok"]


def test_cli_force_on_off(monkeypatch, tmp_path):
    _setup(tmp_path)
    monkeypatch.setattr(dr, "COACH_FORCE_ON", {"B"})
    monkeypatch.setattr(dr, "COACH_FORCE_OFF", {"A"})
    games = [{"kp_home": "A", "kp_away": "B"}, {"kp_home": "C", "kp_away": "X"}]
    nc = dr.tag_new_coaches(games, "2026-11-04", str(tmp_path))
    assert "A" not in nc and "B" in nc and "C" in nc
    assert "coach_home" not in games[0] and games[0]["coach_away"]["team"] == "B"


def test_coach_locks_cli_edits_file(tmp_path):
    f = str(tmp_path / "locks.csv")
    CL.set_flag("LSU", True, 2027, f, coach="Will Wade", source="http://x")
    CL.set_flag("Rutgers", False, 2027, f, note="acting")
    CL.set_flag("lsu", False, 2027, f)                                                  # case-insensitive update
    d = C.load_coach_locks(2027, f)
    assert len(d) == 2 and d.set_index("team").loc["LSU", "new_coach"] == "N"
    assert d.set_index("team").loc["LSU", "coach"] == "Will Wade"


def test_repo_flag_file_is_valid():
    d = C.load_coach_locks(None)
    assert len(d) and set(d.new_coach.str.upper()) <= {"Y", "N"}
    assert not d.duplicated(["season", "team"]).any()
    assert (d.source.str.startswith("http")).all()
    s27 = d[d.season == "2027"].set_index("team")
    assert s27.loc["LSU", "new_coach"] == "Y" and s27.loc["Rutgers", "new_coach"] == "N"


def test_enrich_coach_profile_defaults_and_overrides():
    from czarp.coach import enrich_coach_profile
    nc = {"Charlotte": {"lock_sides": None, "lock_totals": None},
          "Custom": {"games": 3, "lock_sides": 6, "lock_totals": 0}}
    out = enrich_coach_profile(nc, played={"Charlotte": 2})
    assert out["Charlotte"]["games"] == 2
    assert out["Charlotte"]["lock_sides"] == 8 and out["Charlotte"]["lock_totals"] == 4
    assert out["Charlotte"]["lock_sides_remaining"] == 6 and out["Charlotte"]["lock_totals_remaining"] == 2
    assert out["Custom"]["lock_sides"] == 6 and out["Custom"]["lock_totals"] == 0
    assert out["Custom"]["lock_sides_remaining"] == 3 and out["Custom"]["lock_totals_remaining"] == 0
