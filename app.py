# -*- coding: utf-8 -*-
"""
app.py  -  CZarp CBB Betting Model  -  Streamlit Dashboard
"""

import os
import pandas as pd
import streamlit as st
from datetime import date, datetime
try:
    from zoneinfo import ZoneInfo          # Python 3.9+
except ImportError:
    from backports.zoneinfo import ZoneInfo  # fallback

st.set_page_config(
    page_title="CZarp CBB Model",
    page_icon="",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Bebas+Neue&family=DM+Sans:wght@300;400;500;600&display=swap');
html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }
.stApp { background: #0a0d14; color: #e8e8e8; }
h1, h2, h3 { font-family: 'Bebas Neue', sans-serif; letter-spacing: 2px; }
[data-testid="metric-container"] { background: #141820; border: 1px solid #1e2535; border-radius: 10px; padding: 14px !important; }
[data-testid="stSidebar"] { background: #0d1018; border-right: 1px solid #1e2535; }
.game-card { background: #141820; border: 1px solid #1e2535; border-radius: 12px; padding: 16px 20px; margin-bottom: 12px; position: relative; }
.game-card:hover { border-color: #f0b429; transition: border-color 0.2s; }
.game-time { font-size: 0.72rem; color: #f0b429; font-weight: 600; letter-spacing: 1px; margin-bottom: 8px; }
.team-row { display: flex; justify-content: space-between; align-items: center; padding: 5px 0; }
.team-name { font-size: 1.05rem; font-weight: 600; }
.team-label { color: #555; font-size: 0.68rem; margin-left: 6px; letter-spacing: 0.5px; }
.team-score { font-family: 'Bebas Neue', sans-serif; font-size: 1.9rem; color: #888; letter-spacing: 1px; min-width: 50px; text-align: right; }
.team-score-winner { color: #f0b429; }
.game-meta { margin-top: 10px; padding-top: 10px; border-top: 1px solid #1e2535; display: flex; flex-wrap: wrap; gap: 16px; }
.meta-item { display: flex; flex-direction: column; gap: 2px; }
.meta-label { font-size: 0.65rem; color: #555; letter-spacing: 0.5px; text-transform: uppercase; }
.meta-val { color: #ddd; font-weight: 600; font-size: 0.85rem; }
.meta-val-hot { color: #f0b429; font-weight: 700; }
.meta-val-differ { color: #e05c5c; font-weight: 700; }
.edge-badge { display: inline-block; padding: 3px 10px; border-radius: 20px; font-size: 0.68rem; font-weight: 700; letter-spacing: 0.5px; }
.edge-hot  { background: #f0b42922; color: #f0b429; border: 1px solid #f0b42955; }
.edge-good { background: #27a14822; color: #5ddc7a; border: 1px solid #27a14855; }
.edge-low  { background: #1e253522; color: #666;    border: 1px solid #1e2535; }
.edge-diff { background: #e05c5c22; color: #e05c5c; border: 1px solid #e05c5c55; }
.divider { border: none; border-top: 1px solid #1e2535; margin: 20px 0; }
.section-title { font-family: 'Bebas Neue', sans-serif; font-size: 1.3rem; letter-spacing: 2px; color: #f0b429; margin: 24px 0 12px 0; }
</style>
""", unsafe_allow_html=True)

try:
    from kenpom_fetcher import fetch_all, fetch_fanmatch, save_data
    from model import load_data, project_game, mround
    from odds_fetcher import fetch_vegas_lines, match_vegas_to_game
    MODULES_OK = True
except ImportError as e:
    MODULES_OK = False
    st.error(f"Import error: {e}")

SEASON = 2026


@st.cache_data(ttl=3600, show_spinner=False)
def get_kenpom_data():
    data = fetch_all(year=SEASON)
    save_data(data)
    return load_data()


@st.cache_data(ttl=900, show_spinner=False)
def get_todays_games(today_str):
    import os, pandas as pd
    # Try live fanmatch API first
    try:
        fm = fetch_fanmatch(today_str)
    except Exception as e:
        # API failed (400 = date not available, past/future) — try saved CSV fallback
        fanmatch_csv = f"data/fanmatch_{today_str}.csv"
        if os.path.exists(fanmatch_csv):
            fm = pd.read_csv(fanmatch_csv)
            st.info(f"Using saved games from {today_str}")
        else:
            st.warning(f"KenPom fanmatch unavailable for {today_str}: {e}")
            return []
    if fm is None or (hasattr(fm, "empty") and fm.empty):
        return []
    games = []
    for _, row in fm.iterrows():
        games.append({
            "team1": row["Home"], "team2": row["Visitor"],
            "team1_is_home": True,
            "kp_home_score": row.get("HomePred"),
            "kp_away_score": row.get("VisitorPred"),
            "kp_home_wp":    row.get("HomeWP"),
            "kp_tempo":      row.get("PredTempo"),
            "game_time":     row.get("GameTime", row.get("Time", None)),
        })
    return games


@st.cache_data(ttl=3600, show_spinner=False)
def get_vegas_lines():
    return fetch_vegas_lines()


@st.cache_data(ttl=3600, show_spinner=False)
def run_base_projections(today_str):
    """KenPom projections only - cached 1hr. Vegas matching done separately so it can refresh."""
    data = get_kenpom_data()
    games = get_todays_games(today_str)
    if not games:
        return []
    results = []
    for game in games:
        try:
            r = project_game(game["team1"], game["team2"], True, data,
                             game_time=game.get("game_time"))
            r["kp_home_score"] = game["kp_home_score"]
            r["kp_away_score"] = game["kp_away_score"]
            r["kp_home_wp"]    = game["kp_home_wp"]
            r["kp_tempo"]      = game["kp_tempo"]
            results.append(r)
        except Exception:
            pass
    return results


def run_projections(today_str):
    """Full projections with Vegas lines. Vegas refreshes every 15 min independently."""
    results = run_base_projections(today_str)
    if not results:
        return []
    try:
        vegas_df = get_vegas_lines()
    except Exception as e:
        print(f"  Vegas lines unavailable: {e}")
        vegas_df = __import__('pandas').DataFrame()
    return [match_vegas_to_game(r, vegas_df) for r in results]


# --- Sidebar ---
with st.sidebar:
    st.markdown("## CZarp CBB MODEL")
    st.markdown(f"**Season:** 2025-26")
    st.markdown("---")
    sort_by = st.selectbox("Sort games by", ["Edge Score", "Total", "Spread (biggest fav)", "Team Name A-Z"], key="sb_sort")
    min_edge = st.slider("Min Edge Score", 0.0, 0.20, 0.0, 0.01, key="sb_edge")
    show_only_vegas  = st.checkbox("Only games with Vegas lines", value=False, key="sb_vegas")
    show_only_differ = st.checkbox("Only SIDES DIFFER games", value=False, key="sb_differ")
    st.markdown("---")

    # Date picker — default to TODAY in Central time (avoids midnight ET rollover issue)
    from datetime import timedelta
    _ct = datetime.now(ZoneInfo("America/Chicago"))
    _today_ct = _ct.date()
    selected_date = st.date_input(
        "Game Date",
        value=_today_ct,
        min_value=_today_ct - timedelta(days=7),
        max_value=_today_ct + timedelta(days=1),
        help="Defaults to Central time — avoids date rollover issues after 11pm CT",
        key="sb_date"
    )
    today = str(selected_date)

    st.markdown("---")
    if st.button("Refresh Data", use_container_width=True, key="sb_refresh"):
        st.cache_data.clear()
        st.rerun()

# --- Header ---
st.markdown("<h1 style='color:#f0b429; margin-bottom:4px;'>CZARP CBB MODEL</h1>", unsafe_allow_html=True)
st.markdown(f"<p style='color:#555; margin-top:0;'>{selected_date.strftime('%A, %B %d, %Y')} &nbsp; 2025-26 Season</p>", unsafe_allow_html=True)

if not MODULES_OK:
    st.stop()

with st.spinner("Loading projections..."):
    try:
        results = run_projections(today)
        if not results:
            st.cache_data.clear()
            results = run_projections(today)
    except Exception as e:
        st.cache_data.clear()
        st.error(f"Error loading {today}: {e}")
        st.stop()

if not results:
    st.warning(f"No games found for {today}. Try a different date or hit Refresh Data.")
    st.stop()

tab1, tab2 = st.tabs([' Daily Projections', ' Simulator'])

with tab1:
    # --- Metrics ---
    games_with_vegas = [r for r in results if r.get("vegas_spread") is not None]
    high_edge  = [r for r in results if (r.get("edge_score") or 0) > 0.07]
    differ     = [r for r in results if r.get("sides_agree") is False]
    avg_total  = round(sum(r["total"] for r in results) / len(results), 1)

    c1,c2,c3,c4,c5 = st.columns(5)
    c1.metric("Games Today", len(results))
    c2.metric("With Vegas Lines", len(games_with_vegas))
    c3.metric("High Edge (>0.07)", len(high_edge))
    c4.metric("Sides Differ", len(differ))
    c5.metric("Avg Total", avg_total)
    st.markdown("<hr class='divider'>", unsafe_allow_html=True)

    # --- Sort & Filter ---
    if sort_by == "Edge Score":
        results = sorted(results, key=lambda r: r.get("edge_score") or 0, reverse=True)
    elif sort_by == "Total":
        results = sorted(results, key=lambda r: r["total"], reverse=True)
    elif sort_by == "Spread (biggest fav)":
        results = sorted(results, key=lambda r: abs(r["spread"]), reverse=True)
    else:
        results = sorted(results, key=lambda r: r["team1"])

    if show_only_vegas:
        results = [r for r in results if r.get("vegas_spread") is not None]
    if min_edge > 0:
        results = [r for r in results if (r.get("edge_score") or 0) >= min_edge]
    if show_only_differ:
        results = [r for r in results if r.get("sides_agree") is False]

    # --- Game Cards ---
    st.markdown("<div class='section-title'>TODAY'S PROJECTIONS</div>", unsafe_allow_html=True)

    if not results:
        st.info("No games match your filters.")
    else:
        for r in results:
            edge     = r.get("edge_score")
            disagree = r.get("sides_agree") is False

            # Badge — edge displayed as percentage (e.g. 9.16%)
            epct = f"{edge*100:.2f}%" if edge else ""
            if disagree and edge and edge > 0.05:
                badge_cls, badge_txt = "edge-diff", f"SIDES DIFFER  {epct}"
            elif edge and edge > 0.08:
                badge_cls, badge_txt = "edge-hot",  f"HOT EDGE  {epct}"
            elif edge and edge > 0.05:
                badge_cls, badge_txt = "edge-good", f"EDGE  {epct}"
            elif edge:
                badge_cls, badge_txt = "edge-low",  f"EDGE {epct}"
            else:
                badge_cls, badge_txt = "edge-low",  "NO LINE"

            # Away on top, Home on bottom
            away_score = r["team2_score"]
            home_score = r["team1_score"]
            away_name  = r["team2"]
            home_name  = r["team1"]
            away_cls = "team-score team-score-winner" if away_score > home_score else "team-score"
            home_cls = "team-score team-score-winner" if home_score > away_score else "team-score"

            # CZarp Spread: "FavTeam -X.0"
            s = r["spread"]  # positive = home favored
            if s > 0:
                czarp_txt = f"{home_name[:16]} {-abs(s):+.1f}"
            elif s < 0:
                czarp_txt = f"{away_name[:16]} {-abs(s):+.1f}"
            else:
                czarp_txt = "EVEN"

            # Vegas Spread: use pre-computed vegas_fav (handles flip detection correctly)
            vs   = r.get("vegas_spread")
            vt   = r.get("vegas_total")
            vfav = r.get("vegas_fav")
            if vs is not None and vfav:
                vtxt  = f"{vfav[:16]} {-abs(vs):+.1f}" if vs != 0 else "EVEN"
                vttxt = f"{vt:.1f}" if vt else "-"
            else:
                vtxt, vttxt = "-", "-"

            swing_txt = f"{r['spread_edge']:+.1f}" if r.get("spread_edge") is not None else "-"
            # Use KenPom time first, fall back to Odds API time (covers late-night games like USC/UCLA)
            gtime = r.get("game_time") or r.get("odds_game_time") or ""
            time_html = f"<div class='game-time'>{gtime}</div>" if gtime else ""
            differ_html = "<span class='meta-val meta-val-differ'>SIDES DIFFER</span>" if disagree else ""
            differ_block = '<div class="meta-item"><span class="meta-label">&nbsp;</span>' + differ_html + '</div>' if disagree else ""

            # Edge/differ shown as meta-item at end of meta row (no more overlap)
            edge_block = f'<div class="meta-item"><span class="meta-label">Edge</span><span class="edge-badge {badge_cls}">{badge_txt}</span></div>'

            parts = [
                f'<div class="game-card">',
                time_html,
                f'<div class="team-row"><span class="team-name">{away_name} <span class="team-label">AWAY</span></span><span class="{away_cls}">{away_score:.1f}</span></div>',
                f'<div class="team-row"><span class="team-name">{home_name} <span class="team-label">HOME</span></span><span class="{home_cls}">{home_score:.1f}</span></div>',
                f'<div class="game-meta">',
                f'<div class="meta-item"><span class="meta-label">CZarp Spread</span><span class="meta-val meta-val-hot">{czarp_txt}</span></div>',
                f'<div class="meta-item"><span class="meta-label">CZarp Total</span><span class="meta-val">{r["total"]:.1f}</span></div>',
                f'<div class="meta-item"><span class="meta-label">Vegas Spread</span><span class="meta-val">{vtxt}</span></div>',
                f'<div class="meta-item"><span class="meta-label">Vegas Total</span><span class="meta-val">{vttxt}</span></div>',
                f'<div class="meta-item"><span class="meta-label">Swing</span><span class="meta-val">{swing_txt}</span></div>',
                edge_block,
                '</div></div>',
            ]
            st.markdown("".join(parts), unsafe_allow_html=True)

    # --- Full Table ---
    st.markdown("<div class='section-title'>FULL TABLE</div>", unsafe_allow_html=True)
    table_rows = []
    for r in results:
        s = r["spread"]
        czarp_t = f"{(r['team1'] if s>0 else r['team2'])[:18]} {-abs(s):+.1f}" if s != 0 else "EVEN"
        vs = r.get("vegas_spread")
        vfav_t = r.get("vegas_fav")
        vtxt_t = f"{vfav_t[:18]} {-abs(vs):+.1f}" if (vs and vs != 0 and vfav_t) else ("EVEN" if vs == 0 else "-")
        table_rows.append({
            "Time":         r.get("game_time") or "",
            "Away":         r["team2"],
            "Home":         r["team1"],
            "Away Score":   r["team2_score"],
            "Home Score":   r["team1_score"],
            "CZarp Spread": czarp_t,
            "CZarp Total":  r["total"],
            "Vegas Spread": vtxt_t,
            "Vegas Total":  r.get("vegas_total") or "",
            "Swing":        r.get("spread_edge") or "",
            "Edge":         round(r.get("edge_score") or 0, 4),
            "Differ":       "YES" if r.get("sides_agree") is False else "",
            "KP Away":      r.get("kp_away_score") or "",
            "KP Home":      r.get("kp_home_score") or "",
        })
    df = pd.DataFrame(table_rows)
    st.dataframe(df, use_container_width=True, hide_index=True)

    st.markdown(f"<div style='margin-top:40px; padding-top:20px; border-top:1px solid #1e2535; font-size:0.75rem; color:#444; text-align:center;'>CZarp CBB Model &nbsp; 2025-26 &nbsp; Last updated {datetime.now().strftime('%I:%M %p')}</div>", unsafe_allow_html=True)

with tab2:
    st.markdown("<div class='section-title'>GAME SIMULATOR</div>", unsafe_allow_html=True)
    st.markdown("<p style='color:#666; font-size:0.85rem; margin-top:-8px;'>Project any matchup — perfect for March Madness bracket research</p>", unsafe_allow_html=True)

    try:
        sim_data = get_kenpom_data()
        team_list = sorted(sim_data["ratings"]["TeamName"].dropna().tolist())
    except Exception as e:
        st.error(f"Could not load team list: {e}")
        team_list = []

    if team_list:
        st.markdown("<hr class='divider'>", unsafe_allow_html=True)
        sc1, sc2, sc3 = st.columns([2, 2, 1])

        with sc1:
            team_a = st.selectbox("Team A", team_list, index=team_list.index("Duke") if "Duke" in team_list else 0, key="sim_team_a")
        with sc2:
            team_b = st.selectbox("Team B", team_list, index=team_list.index("Kentucky") if "Kentucky" in team_list else 1, key="sim_team_b")
        with sc3:
            site = st.radio("Site", ["Neutral", "Team A Home", "Team B Home"], key="sim_site")

        if site == "Neutral":
            team1_is_home = None
            home_label, away_label = "NEUTRAL", "NEUTRAL"
        elif site == "Team A Home":
            team1_is_home = True
            home_label, away_label = "HOME", "AWAY"
        else:
            team1_is_home = False
            home_label, away_label = "AWAY", "HOME"

        run_sim = st.button("  Run Projection", use_container_width=False, key="sim_run")

        if run_sim or "sim_result" in st.session_state:
            if run_sim:
                try:
                    r = project_game(team_a, team_b, team1_is_home, sim_data)
                    st.session_state["sim_result"] = r
                    st.session_state["sim_labels"] = (home_label, away_label, site, team_a, team_b, team1_is_home)
                except Exception as e:
                    st.error(f"Projection error: {e}")
                    if "sim_result" in st.session_state:
                        del st.session_state["sim_result"]

            if "sim_result" in st.session_state:
                r = st.session_state["sim_result"]
                hl, al, sv, ta, tb, t1_is_home = st.session_state["sim_labels"]

                if t1_is_home or t1_is_home is None:
                    # Team A home or neutral: team1 on bottom (home), team2 on top (away)
                    home_name  = r["team1"]; home_score = r["team1_score"]
                    away_name  = r["team2"]; away_score = r["team2_score"]
                else:
                    # Team B home: team2 is actually home → show team2 on bottom
                    home_name  = r["team2"]; home_score = r["team2_score"]
                    away_name  = r["team1"]; away_score = r["team1_score"]

                away_wins = away_score > home_score
                home_wins = home_score > away_score

                # Spread: always express as favored team -X from home team perspective
                s = r["spread"]  # positive = team1 favored
                if t1_is_home is None:
                    fav_name = r["team1"] if s > 0 else r["team2"]
                elif t1_is_home:
                    fav_name = r["team1"] if s > 0 else r["team2"]
                else:
                    fav_name = r["team2"] if s < 0 else r["team1"]  # team2 is home

                czarp_txt = f"{fav_name} {-abs(s):+.1f}" if s != 0 else "EVEN"

                away_sc = "team-score team-score-winner" if away_wins else "team-score"
                home_sc = "team-score team-score-winner" if home_wins else "team-score"

                site_badge = f"<span style='background:#1e2535; color:#888; font-size:0.7rem; padding:2px 8px; border-radius:10px; margin-bottom:8px; display:inline-block;'>{sv.upper()}</span>"

                st.markdown(f"""
                <div class="game-card" style="max-width:520px; margin-top:20px;">
                    {site_badge}
                    <div class="team-row">
                        <span class="team-name">{away_name} <span class="team-label">{al}</span></span>
                        <span class="{away_sc}">{away_score:.1f}</span>
                    </div>
                    <div class="team-row">
                        <span class="team-name">{home_name} <span class="team-label">{hl}</span></span>
                        <span class="{home_sc}">{home_score:.1f}</span>
                    </div>
                    <div class="game-meta">
                        <div class="meta-item"><span class="meta-label">CZarp Spread</span><span class="meta-val meta-val-hot">{czarp_txt}</span></div>
                        <div class="meta-item"><span class="meta-label">CZarp Total</span><span class="meta-val">{r["total"]:.1f}</span></div>
                        <div class="meta-item"><span class="meta-label">KenPom Proj</span><span class="meta-val">{r.get("kp_home_score") or "—"} / {r.get("kp_away_score") or "—"}</span></div>
                    </div>
                </div>
                """, unsafe_allow_html=True)

                # Detail breakdown
                with st.expander("Show full breakdown"):
                    d = r.get("debug", {})
                    # When Team B is home, team2=home and team1=away — swap stat keys
                    if not t1_is_home and t1_is_home is not None:
                        hk, ak = "t2", "t1"   # home stats from t2, away stats from t1
                        h_ppp, a_ppp = r.get("team2_ppp",0), r.get("team1_ppp",0)
                    else:
                        hk, ak = "t1", "t2"   # normal: home=t1, away=t2
                        h_ppp, a_ppp = r.get("team1_ppp",0), r.get("team2_ppp",0)
                    bc1, bc2 = st.columns(2)
                    with bc1:
                        st.markdown(f"**{home_name}**")
                        st.markdown(f"KenPom Rank: **{int(d.get(f'kenpom_rank_{hk}',0))}**")
                        st.markdown(f"AdjOE: **{d.get(f'{hk}_adjoe',0):.1f}** / AdjDE: **{d.get(f'{hk}_adjde',0):.1f}**")
                        st.markdown(f"PPP: **{h_ppp:.4f}**")
                    with bc2:
                        st.markdown(f"**{away_name}**")
                        st.markdown(f"KenPom Rank: **{int(d.get(f'kenpom_rank_{ak}',0))}**")
                        st.markdown(f"AdjOE: **{d.get(f'{ak}_adjoe',0):.1f}** / AdjDE: **{d.get(f'{ak}_adjde',0):.1f}**")
                        st.markdown(f"PPP: **{a_ppp:.4f}**")
                    st.markdown(f"Projected Pace: **{r.get('projected_pace',0):.1f}** | Avg Pace used: **{d.get('avg_pace',0):.1f}**")

st.markdown(f"<div style='margin-top:40px; padding-top:20px; border-top:1px solid #1e2535; font-size:0.75rem; color:#444; text-align:center;'>CZarp CBB Model &nbsp; 2025-26 &nbsp; Last updated {datetime.now().strftime('%I:%M %p')}</div>", unsafe_allow_html=True)
