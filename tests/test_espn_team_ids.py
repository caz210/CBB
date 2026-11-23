"""scripts/build_espn_team_ids.py: derive teams.external_ids.espn from ESPN event competitor pairs."""
import importlib.util
import os

_spec = importlib.util.spec_from_file_location(
    "build_espn_team_ids", os.path.join(os.path.dirname(__file__), "..", "scripts", "build_espn_team_ids.py"))
B = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(B)


def ev(home, away, neutral=False):
    return {"neutral": neutral, "comp": [{"id": home, "home_away": "home", "name": ""},
                                         {"id": away, "home_away": "away", "name": ""}]}


TEAMS = [{"id": 1, "name": "Appalachian St."}, {"id": 2, "name": "St. Bonaventure"},
         {"id": 3, "name": "North Texas"}, {"id": 4, "name": "Lonely"}, {"id": 5, "name": "No Games"}]


def g(gid, eid, h, a, neutral=False):
    return {"id": gid, "date": "2026-11-20", "external_id": eid, "home_team_id": h, "away_team_id": a, "neutral": neutral}


def test_intersection_and_elimination():
    events = {"e1": ev("2026", "111"), "e2": ev("179", "2026", neutral=True), "e3": ev("999", "179")}
    games = [g(1, "e1", 1, 3), g(2, "e2", 1, 2, neutral=True), g(3, "e3", 4, 2)]
    r = B.derive_espn_ids(TEAMS, games, events)
    assert r["unique"] == {1: "2026", 2: "179", 3: "111", 4: "999"}
    assert r["ambiguous"] == {}
    assert r["unmatched"] == [5]
    assert B.check_consistent(r, games, events) == []


def test_neutral_flip_does_not_matter():
    # ESPN lists our away team as home at a neutral site; intersection is orientation-free.
    events = {"e1": ev("179", "2026", neutral=True), "e2": ev("2026", "111")}
    games = [g(1, "e1", 1, 2, neutral=True), g(2, "e2", 1, 3)]
    r = B.derive_espn_ids(TEAMS, games, events)
    assert r["unique"][1] == "2026" and r["unique"][2] == "179" and r["unique"][3] == "111"


def test_single_neutral_game_pair_stays_ambiguous():
    events = {"e1": ev("179", "2026", neutral=True)}
    r = B.derive_espn_ids(TEAMS, [g(1, "e1", 1, 2, neutral=True)], events)
    assert r["unique"] == {}
    assert r["ambiguous"] == {1: ["179", "2026"], 2: ["179", "2026"]}


def test_single_true_home_game_uses_home_away():
    events = {"e1": ev("2026", "179")}
    r = B.derive_espn_ids(TEAMS, [g(1, "e1", 1, 2)], events)
    assert r["unique"] == {1: "2026", 2: "179"}


def test_sql_is_idempotent_and_name_guarded():
    r = {"unique": {1: "2026", 2: "179"}, "ambiguous": {}, "unmatched": []}
    sql = B.render_sql(r, {1: "Appalachian St.", 2: "St. Bonaventure"})
    assert "(1, 'Appalachian St.', '2026')" in sql
    assert "coalesce(t.external_ids->>'espn', '') = ''" in sql
    assert "t.name = v.name" in sql
    assert sql.strip().endswith("commit;")


def test_scoreboard_parse():
    payload = {"events": [{"id": "401927397", "competitions": [{"neutralSite": False, "competitors": [
        {"homeAway": "home", "team": {"id": "2026", "displayName": "App State Mountaineers"}},
        {"homeAway": "away", "team": {"id": "179", "displayName": "St. Bonaventure Bonnies"}}]}]}]}
    out = B.events_from_scoreboard(payload)
    assert out["401927397"]["comp"][0] == {"id": "2026", "home_away": "home", "name": "App State Mountaineers"}
