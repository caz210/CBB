import backfill_slate_day as B


def _g(eid="1", **kw):
    g = dict(event_id=eid, game_date="2026-11-02", tip_ct="2026-11-02 18:00", kp_home="Wright St.", kp_away="Air Force",
             neutral=False, state="post", v2_home_margin=9.685, v2_total=150.35, v2_home_win_prob=0.80,
             v1_total=149.5, v1_home_margin=8.5, open_home_spread=-9.5, current_home_spread=-10.5,
             open_total=156.5, current_total=162.5, archetype_tags={}, snapshot_ct="2026-11-02T07:42:21-06:00")
    g.update(kw)
    g["tip_utc"] = B.pd.Timestamp("2000-01-01", tz="UTC").to_pydatetime()
    return g


def test_build_rows_insert_only_and_edges_match_ledger():
    stamp = "2026-11-02T07:42:21-06:00"
    games = [_g("1"), _g("2", kp_home="Brown", kp_away="Yale")]
    new, proj, lines, missing = B.build_rows(
        games, sport_id=1, team_ids={"Wright St.": 10, "Air Force": 11, "Brown": 12},
        existing_games={"1": 20}, games_with_lines={20}, existing_proj={(20, "czarp-v2", stamp)}, stamp=stamp)
    assert [g["external_id"] for g in new] == ["2"]          # existing game untouched
    assert missing == ["Yale"] and new[0]["away_team_id"] is None
    pr = proj({"1": 20, "2": 21})
    assert (20, "czarp-v2") not in {(p["game_id"], p["model_version"]) for p in pr}   # existing projection kept
    v1 = next(p for p in pr if p["game_id"] == 20 and p["model_version"] == "czarp-v1-totals")
    assert round(v1["edge_now_pct_total"], 4) == 0.08 and round(v1["edge_vs_open_pct_total"], 4) == 0.0447
    assert all("skip_reason_total" not in p for p in pr)
    ln = lines({"1": 20, "2": 21})
    assert {l["game_id"] for l in ln} == {21} and {l["line_type"] for l in ln} == {"open", "current"}
