"""Paste-ready daily chat report: per-game Top 10 line blocks."""
from datetime import datetime, timezone

import pandas as pd
import daily_run as dr


def _game(**kw):
    g = dict(event_id="9", state="pre", tip_utc=datetime(2030, 1, 1, tzinfo=timezone.utc),
             tip_ct="2029-12-31 18:00", game_date="2029-12-31", kp_home="H", kp_away="A",
             neutral=False, open_home_spread=-5.5, current_home_spread=-3.5,
             open_total=160.0, current_total=161.0, v2_home_margin=4.5, v1_total=155.0)
    g.update(kw)
    return g


def _now():
    return pd.Timestamp("2029-12-31 08:00", tz=dr.CENTRAL).to_pydatetime()


def _row(g=None, market="total", side="Under", qualifies=False, reasons=None, **kw):
    g = g or _game()
    # edge defaults: a low-value total with some current edge
    c = dict(g=g, market=market, side=side, qualifies=qualifies,
             edge_pct=0.063, edge_pct_vs_open=0.039, thr=0.03 if market == "total" else 0.04,
             reasons=reasons or [], tier=None, rule_ok=qualifies)
    c.update(kw)
    return c


def test_tip_time_and_neutral_header():
    assert dr._fmt_tip_ct("2026-11-11 19:00") == "7:00 PM CT"
    assert dr._fmt_tip_ct("2026-11-11 10:30") == "10:30 AM CT"
    assert dr._fmt_tip_ct("2026-11-11 00:00") == "12:00 AM CT"
    assert dr._fmt_tip_ct("2026-11-11 12:00") == "12:00 PM CT"
    g = _game(kp_away="Kansas City", kp_home="Florida A&M", neutral=True, tip_ct="2026-11-11 11:00")
    assert dr._chat_game_title(g) == "Kansas City @ Florida A&M (N) · 11:00 AM CT"
    g2 = _game(kp_away="Furman", kp_home="Holy Cross", tip_ct="2026-11-11 18:00")
    assert dr._chat_game_title(g2) == "Furman @ Holy Cross · 6:00 PM CT"


def test_arrows_favorite_and_big_move():
    # totals: higher ⬆️, lower ⬇️, unchanged ➡️; 4+ pts is a big move
    g = _game(open_total=160, current_total=161, open_home_spread=-5.5, current_home_spread=-3.5,
              v2_home_margin=4.5, v1_total=155, kp_home="Team B", kp_away="Team A")
    # home -5.5 at open → Team B is the favorite
    fav, o, c = dr.open_favorite(g)
    assert fav == "Team B" and o == -5.5 and c == -3.5
    block = "\n".join(dr.format_chat_edge_block(_row(g, side="Under")))
    assert "Lines (open → now)" in block
    assert "Total 160 ⬆️ 161" in block
    assert "Team B -5.5 ⬇️ -3.5" in block
    assert " · big move" not in block.split("Total")[1].split("\n")[0]
    assert "CZarp" in block
    assert "Total: 155" in block
    assert "Team B: -4.5" in block
    # both markets always present
    assert block.count("Total") >= 2

    # total jumped 4, favorite laid more (and 3+ pts = big move on the spread)
    g2 = _game(open_total=155.5, current_total=159.5, open_home_spread=-8.5, current_home_spread=-11.5,
               kp_home="H", kp_away="A")
    lines = "\n".join(dr._lines_open_now_block(g2))
    assert "Total 155.5 ⬆️ 159.5 · big move" in lines
    assert "H -8.5 ⬆️ -11.5 · big move" in lines

    # total dropped, spread unchanged
    g3 = _game(open_total=140.5, current_total=136.5, open_home_spread=-10.5, current_home_spread=-10.5)
    lines = "\n".join(dr._lines_open_now_block(g3))
    assert "Total 140.5 ⬇️ 136.5 · big move" in lines
    assert "H -10.5 ➡️ -10.5" in lines
    assert "H -10.5 ➡️ -10.5 · big move" not in lines


def test_away_favorite_and_czarp_can_be_positive():
    # Loyola-shaped: home is the dog at the open (+8.5), away is -8.5
    g = _game(kp_away="Loyola Chicago", kp_home="Air Force",
              open_home_spread=8.5, current_home_spread=10.5,
              v2_home_margin=-8.7, v1_total=149.5,
              open_total=155.5, current_total=159.5, tip_ct="2026-11-11 14:00")
    fav, o, c = dr.open_favorite(g)
    assert fav == "Loyola Chicago" and o == -8.5 and c == -10.5
    tot, sp = dr.czarp_from_favorite(g, fav)
    assert tot == 149.5 and abs(sp - (-8.7)) < 1e-9
    block = "\n".join(dr.format_chat_edge_block(_row(g, side="Under", reasons=["Paused until game 4 (new coach, 3 played)"])))
    assert "Loyola Chicago @ Air Force · 2:00 PM CT" in block
    assert "Loyola Chicago -8.5 ⬆️ -10.5" in block
    assert "Loyola Chicago: -8.7" in block

    # Open favorite becomes an underdog; CZarp can be positive from that team's view
    flip = _game(kp_away="A", kp_home="H", open_home_spread=-1.5, current_home_spread=2.5,
                 v2_home_margin=-1.0, v1_total=150.0)
    fav, o, c = dr.open_favorite(flip)
    assert fav == "H" and o == -1.5 and c == 2.5
    _, sp = dr.czarp_from_favorite(flip, fav)
    assert sp == 1.0  # H is a 1-point underdog in the model
    lines = "\n".join(dr._lines_open_now_block(flip))
    assert "H -1.5 ⬇️ +2.5" in lines
    cz = "\n".join(dr._czarp_block(flip, fav))
    assert "H: +1" in cz


def test_verdicts_official_low_value_and_lean():
    g = _game()
    official = "\n".join(dr.format_chat_edge_block(
        _row(g, side="Under", qualifies=True, edge_pct=0.059, edge_pct_vs_open=0.065, reasons=[])))
    assert "Edge: Under · 5.9% now, 6.5% vs open · ✅ Official play" in official
    # reasons on an official row are not appended
    official_noisy = "\n".join(dr.format_chat_edge_block(
        _row(g, side="Under", qualifies=True, reasons=["should not appear"])))
    assert "should not appear" not in official_noisy

    # vs-open clears the total bar (3.9% >= 3%) so this is Low value, not Lean
    low = _row(g, side="Under", qualifies=False, edge_pct=0.063, edge_pct_vs_open=0.039,
               reasons=["Not a play: market moved 3+ pts away from CZarp since open",
                        "Paused until game 4 (new coach, 3 played)",
                        "Big move: up 4 pts (155.5 → 159.5) — against our lean"])
    text = "\n".join(dr.format_chat_edge_block(low))
    assert "Edge: Under · 6.3% now, 3.9% vs open · Low value: " in text
    assert "Not a play: market moved 3+ pts away from CZarp since open" in text
    assert "Paused until game 4 (new coach, 3 played)" in text
    assert "Big move:" not in text.split("Edge:")[1]  # stripped from the verdict line
    assert "Lean" not in text
    assert not dr.is_lean(low)

    # now clears 3%, vs open does not → Lean (same rule as slate_games.lean_total)
    lean = _row(g, side="Over", qualifies=False, edge_pct=0.055, edge_pct_vs_open=0.025, thr=0.03,
                reasons=["edge vs open below 3%"])
    assert dr.is_lean(lean)
    text = "\n".join(dr.format_chat_edge_block(lean))
    assert "Edge: Over · 5.5% now, 2.5% vs open · Low value (Lean): edge vs open below 3%" in text

    # sides lean: 5.4% now, 3.8% vs open, bar 4%
    slean = _row(g, market="spread", side="Jacksonville St.", qualifies=False,
                 edge_pct=0.054, edge_pct_vs_open=0.038, thr=0.04,
                 reasons=["edge vs open below 4%"])
    assert dr.is_lean(slean)
    text = "\n".join(dr.format_chat_edge_block(slean))
    assert "Low value (Lean): edge vs open below 4%" in text

    # tracked tier
    tr = _row(g, market="spread", side="H", qualifies=False, tier="track",
              edge_pct=0.03, edge_pct_vs_open=0.05, thr=0.04, reasons=[])
    text = "\n".join(dr.format_chat_edge_block(tr))
    assert text.endswith("· Tracked") or " · Tracked" in text.split("Edge:")[1]


def test_top_edges_chat_ranks_blocks():
    rows = [_row(_game(event_id="1"), side="Under"),
            _row(_game(event_id="2", tip_ct="2029-12-31 20:00"), side="Over", qualifies=True,
                 edge_pct=0.059, edge_pct_vs_open=0.065)]
    md = "\n".join(dr.top_edges_chat(rows))
    assert md.startswith("## Top 10 edges")
    assert "1. A @ H · 6:00 PM CT" in md
    assert "2. A @ H · 8:00 PM CT" in md
    assert "No priced D-I games to rank." in "\n".join(dr.top_edges_chat([]))


def _ledger_rows():
    base = dict(created_ct="2029-12-31T08:00:00-06:00", event_id="1", tip_ct="2029-12-31 19:00",
                away="SFA", home="ACU", neutral=False, rule="v1", price=-110, open_line=143.5,
                model_value=148.0, market_value=142.5, edge_pct=0.039, edge_pct_vs_open=0.031,
                kp_margin_home=None, p_est=0.535, bankroll_at_pick=1000, price_source="dk",
                kalshi_ref="", model_version="czarp-v1-totals", graded_ct=None, away_score=None,
                home_score=None, close_line=None, close_price=None, clv_pts=None, result=None,
                pnl=None, pretip_flag=None, source="live", logged_ct="2029-12-31T08:00:00-06:00",
                pnl_units=None, pick_hash="x", total_at_post=142.5, status="open")
    today = {**base, "pick_id": "1-total", "game_date": "2029-12-31", "market": "total",
             "side": "Over", "line": 142.5, "stake": 5.87, "tier": "official"}
    y_off = {**base, "pick_id": "2-total", "game_date": "2029-12-30", "market": "total",
             "side": "Over", "line": 131.5, "stake": 5.92, "tier": "official", "status": "graded",
             "away": "Army", "home": "New Haven", "result": "loss", "away_score": 55, "home_score": 63,
             "pnl": -5.92, "pnl_units": -1.0, "tip_ct": "2029-12-30 18:00"}
    y_tr = {**base, "pick_id": "3-spread-track", "game_date": "2029-12-30", "market": "spread",
            "side": "Wake Forest", "line": -18.5, "stake": 0.0, "tier": "track", "status": "graded",
            "away": "Charleston Southern", "home": "Wake Forest", "result": "loss",
            "away_score": 64, "home_score": 82, "pnl": 0.0, "pnl_units": -1.0,
            "tip_ct": "2029-12-30 18:00"}
    # older official win so the running record is not just yesterday
    older = {**base, "pick_id": "0-total", "game_date": "2029-12-29", "market": "total",
             "side": "Under", "line": 150.5, "stake": 5.0, "tier": "official", "status": "graded",
             "away": "X", "home": "Y", "result": "win", "away_score": 70, "home_score": 80,
             "pnl": 4.55, "pnl_units": 0.9091, "tip_ct": "2029-12-29 18:00"}
    return pd.DataFrame([today, y_off, y_tr, older], columns=dr.LEDGER_COLS)


def test_write_chat_summary_header_and_top10(tmp_path, monkeypatch):
    monkeypatch.setattr(dr, "OUT_DIR", str(tmp_path))
    led = _ledger_rows()
    top = [_row(_game(), side="Under", edge_pct=0.063, edge_pct_vs_open=0.039,
                reasons=["Paused until game 4 (new coach, 3 played)"])]
    path = dr.write_chat_summary("2029-12-31", _now(), [_game()], [], led, [], [], top,
                                 path=str(tmp_path / "2029-12-31.chat.md"))
    text = open(path).read()
    assert text.startswith("# CZarp paper trades — 2029-12-31")
    assert "One official play today, a total." in text
    assert "- SFA @ ACU, Over 142.5, $5.87, 7:00 PM CT." in text
    assert "No tracked sides." in text
    assert "Yesterday (2029-12-30): official 0-1, -$5.92. Army @ New Haven, Over 131.5 lost 55-63." in text
    assert "Tracked Charleston Southern @ Wake Forest, Wake Forest -18.5 lost 64-82 (-1.00u)." in text
    assert "Official record is now 1-1, -0.09u, -4.5% ROI." in text
    assert "## Top 10 edges" in text
    assert "1. A @ H · 6:00 PM CT" in text
    assert "Edge: Under · 6.3% now, 3.9% vs open · Low value: Paused until game 4 (new coach, 3 played)" in text


def test_write_summary_also_writes_chat_sibling(tmp_path, monkeypatch):
    monkeypatch.setattr(dr, "OUT_DIR", str(tmp_path))
    led = pd.DataFrame(columns=dr.LEDGER_COLS)
    p = dr.write_summary("2029-12-31", _now(), [], [], led, [], [], [], path=str(tmp_path / "s.md"))
    assert p.endswith("s.md")
    chat = tmp_path / "s.chat.md"
    assert chat.exists()
    body = chat.read_text()
    assert "No official plays today." in body
    assert "No tracked sides." in body
    assert "Yesterday (2029-12-30): no plays." in body
    assert "No priced D-I games to rank." in body


def test_render_chat_from_snapshot_and_chat_only(tmp_path, monkeypatch):
    # Build a tiny snapshot and confirm --chat-only never grades, flags, or writes the ledger.
    g = _game(event_id="4019", tip_ct="2026-11-11 20:00", game_date="2026-11-11",
              kp_away="Oakland", kp_home="Boise St.",
              open_total=162.5, current_total=161.5, v1_total=152.0,
              open_home_spread=-17.5, current_home_spread=-16.5, v2_home_margin=22.0,
              snapshot_ct="2026-11-11T07:11:39-06:00")
    snap_dir = tmp_path / "snap"
    snap_dir.mkdir()
    # coaches must serialize the way the daily snapshot does
    row = {k: v for k, v in g.items() if k != "tip_utc"}
    row["coach_home"] = None
    row["coach_away"] = None
    pd.DataFrame([row]).to_csv(snap_dir / "2026-11-11_0711.csv", index=False)

    led = pd.DataFrame([{
        **{c: None for c in dr.LEDGER_COLS},
        "pick_id": "4019-total", "game_date": "2026-11-11", "event_id": "4019",
        "tip_ct": "2026-11-11 20:00", "away": "Oakland", "home": "Boise St.",
        "market": "total", "side": "Under", "line": 161.5, "stake": 8.19,
        "price": -108, "status": "open", "tier": "official", "neutral": False,
    }], columns=dr.LEDGER_COLS)

    monkeypatch.setattr(dr, "SNAP_DIR", str(snap_dir))
    monkeypatch.setattr(dr, "OUT_DIR", str(tmp_path))
    monkeypatch.setattr(dr, "read_ledger", lambda: led)
    monkeypatch.setattr(dr, "grade", lambda *a, **k: (_ for _ in ()).throw(AssertionError("no grade")))
    monkeypatch.setattr(dr, "flag_plays", lambda *a, **k: (_ for _ in ()).throw(AssertionError("no picks")))
    monkeypatch.setattr(dr, "write_ledger", lambda *a, **k: (_ for _ in ()).throw(AssertionError("no ledger write")))

    path = dr.render_chat_from_snapshot("2026-11-11")
    text = open(path).read()
    assert path.endswith("2026-11-11.chat.md")
    assert "Oakland @ Boise St. · 8:00 PM CT" in text
    assert "Total 162.5 ⬇️ 161.5" in text
    assert "Boise St. -17.5 ⬇️ -16.5" in text
    assert "Total: 152" in text
    assert "Boise St.: -22" in text
    assert "Edge: Under · 5.9% now, 6.5% vs open · ✅ Official play" in text
    assert "One official play today, a total." in text
    assert "- Oakland @ Boise St., Under 161.5, $8.19, 8:00 PM CT." in text

    # CLI path
    assert dr.main(["--chat-only", "--date", "2026-11-11"]) == 0
    assert (tmp_path / "2026-11-11.chat.md").exists()
