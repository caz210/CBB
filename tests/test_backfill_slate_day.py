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


def test_backfill_team_scores_match_czarp_total_and_spread():
    stamp = "2026-11-02T07:42:21-06:00"
    _, proj, _, _ = B.build_rows(
        [_g("1"), _g("2", neutral=True, v1_total=float("nan"))], sport_id=1,
        team_ids={"Wright St.": 10, "Air Force": 11}, existing_games={}, games_with_lines=set(),
        existing_proj=set(), stamp=stamp)
    pr = {(p["game_id"], p["model_version"]): p for p in proj({"1": 20, "2": 21})}
    a = pr[(20, "czarp-v2")]
    assert round((a["proj_home"] + a["proj_away"]) * 100) == 14950            # v1 total 149.5
    assert round((a["proj_home"] - a["proj_away"]) * 100) == round(-a["spread"] * 100) == 970
    b = pr[(21, "czarp-v2")]                                                 # no v1 total -> own total
    assert round((b["proj_home"] + b["proj_away"]) * 100) == round(b["total"] * 100)


def test_backfill_team_scores_plan_pairs_same_run_v1_and_skips_matches():
    import backfill_team_scores as T
    games = [dict(id=1689, date="2026-11-21", home="Mercer", away="Charleston Southern"),
             dict(id=7, date="2026-11-22", home="H", away="A")]
    ts, ts0 = "2026-11-21T13:13:26+00:00", "2026-11-20T13:12:00+00:00"
    proj = [dict(id=1, game_id=1689, model_version="czarp-v2", created_at=ts, proj_home="84.80", proj_away="75.00",
                 spread="-9.80", total="159.80"),
            dict(id=2, game_id=1689, model_version="czarp-v1-totals", created_at=ts, total="165.00"),
            dict(id=3, game_id=1689, model_version="czarp-v1-totals", created_at=ts0, total="160.00"),
            # already reconciled -> skipped
            dict(id=4, game_id=7, model_version="czarp-v2", created_at=ts, proj_home="72.25", proj_away="73.25",
                 spread="1.00", total="150.00"),
            dict(id=5, game_id=7, model_version="czarp-v1-totals", created_at=ts, total="145.50")]
    ups = T.plan(games, proj)
    assert [u["id"] for u in ups] == [1]
    assert ups[0]["new"] == (87.4, 77.6) and ups[0]["czarp_total"] == 165.0
    assert all(set(u) >= {"id", "new"} for u in ups)
