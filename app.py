"""
app.py
Streamlit web dashboard for the CBB betting model.

Run locally:    streamlit run app.py
Deploy free:    https://streamlit.io/cloud
"""

import os
import sys
import pandas as pd
import streamlit as st
from datetime import date, datetime

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="CBB Model",
    page_icon="🏀",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Bebas+Neue&family=DM+Sans:wght@300;400;500;600&display=swap');

html, body, [class*="css"] {
    font-family: 'DM Sans', sans-serif;
}

/* Dark scoreboard aesthetic */
.stApp {
    background: #0a0d14;
    color: #e8e8e8;
}

h1, h2, h3 {
    font-family: 'Bebas Neue', sans-serif;
    letter-spacing: 2px;
}

/* Metric cards */
[data-testid="metric-container"] {
    background: #141820;
    border: 1px solid #1e2535;
    border-radius: 10px;
    padding: 14px !important;
}

/* Sidebar */
[data-testid="stSidebar"] {
    background: #0d1018;
    border-right: 1px solid #1e2535;
}

/* Tables */
[data-testid="stDataFrame"] {
    border-radius: 10px;
    overflow: hidden;
}

/* Game cards */
.game-card {
    background: #141820;
    border: 1px solid #1e2535;
    border-radius: 12px;
    padding: 18px 22px;
    margin-bottom: 14px;
    position: relative;
}
.game-card:hover {
    border-color: #f0b429;
    transition: border-color 0.2s;
}
.team-row {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 4px 0;
}
.team-name { font-size: 1.05rem; font-weight: 600; }
.team-score {
    font-family: 'Bebas Neue', sans-serif;
    font-size: 1.8rem;
    color: #f0b429;
    letter-spacing: 1px;
}
.team-home { color: #888; font-size: 0.72rem; margin-left: 6px; }
.game-meta {
    margin-top: 10px;
    padding-top: 10px;
    border-top: 1px solid #1e2535;
    display: flex;
    gap: 20px;
    font-size: 0.8rem;
    color: #888;
}
.meta-val { color: #ccc; font-weight: 500; }
.edge-badge {
    position: absolute;
    top: 14px;
    right: 16px;
    padding: 3px 10px;
    border-radius: 20px;
    font-size: 0.72rem;
    font-weight: 700;
    letter-spacing: 0.5px;
}
.edge-hot  { background: #f0b42922; color: #f0b429; border: 1px solid #f0b42955; }
.edge-good { background: #27a14822; color: #5ddc7a; border: 1px solid #27a14855; }
.edge-low  { background: #1e253522; color: #888;    border: 1px solid #1e2535; }
.divider { border: none; border-top: 1px solid #1e2535; margin: 20px 0; }
.section-title {
    font-family: 'Bebas Neue', sans-serif;
    font-size: 1.3rem;
    letter-spacing: 2px;
    color: #f0b429;
    margin: 24px 0 12px 0;
}
.status-dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 6px; }
.status-live { background: #5ddc7a; animation: pulse 1.5s infinite; }
@keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.4} }
</style>
""", unsafe_allow_html=True)


# ── Import model modules ───────────────────────────────────────────────────────
try:
    from kenpom_fetcher import fetch_all, fetch_fanmatch, save_data
    from net_fetcher import fetch_net_rankings
    from model import load_data, project_game
    from odds_fetcher import fetch_vegas_lines, match_vegas_to_game
    MODULES_OK = True
except ImportError as e:
    MODULES_OK = False
    st.error(f"Could not import model modules: {e}. Make sure app.py is in the cbb_model folder.")

SEASON = 2026


# ── Data loading with caching ─────────────────────────────────────────────────

@st.cache_data(ttl=3600, show_spinner=False)  # cache 1 hour
def get_kenpom_data():
    data = fetch_all(year=SEASON)
    save_data(data)
    net = fetch_net_rankings()
    net.to_csv("data/net.csv", index=False)
    return load_data()


@st.cache_data(ttl=900, show_spinner=False)  # cache 15 min
def get_todays_games(today: str):
    fm = fetch_fanmatch(today)
    if fm.empty:
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
        })
    return games


@st.cache_data(ttl=900, show_spinner=False)
def get_vegas_lines():
    return fetch_vegas_lines()


@st.cache_data(ttl=3600, show_spinner=False)
def run_projections(today: str):
    data = get_kenpom_data()
    games = get_todays_games(today)
    if not games:
        return []
    results = []
    for game in games:
        try:
            r = project_game(game["team1"], game["team2"], True, data)
            r["kp_home_score"] = game["kp_home_score"]
            r["kp_away_score"] = game["kp_away_score"]
            r["kp_home_wp"]    = game["kp_home_wp"]
            r["kp_tempo"]      = game["kp_tempo"]
            results.append(r)
        except Exception as e:
            pass  # skip teams not found

    vegas_df = get_vegas_lines()
    results = [match_vegas_to_game(r, vegas_df) for r in results]
    return results


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🏀 CBB MODEL")
    st.markdown(f"**Season:** 2025–26")
    st.markdown(f"**Date:** {date.today().strftime('%b %d, %Y')}")
    st.markdown("---")

    sort_by = st.selectbox(
        "Sort games by",
        ["Edge Score ↓", "Total ↓", "Spread (biggest fav first)", "Team Name A-Z"]
    )

    min_edge = st.slider("Min Edge Score filter", 0.0, 0.20, 0.0, 0.01,
                         help="Filter to games where your model disagrees most with Vegas")

    show_only_vegas = st.checkbox("Only show games with Vegas lines", value=False)

    st.markdown("---")
    if st.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.markdown("""
    <div style='font-size:0.75rem; color:#555; margin-top:20px;'>
    Data sources:<br>
    • KenPom API (ratings, fanmatch)<br>
    • NCAA NET rankings<br>
    • The Odds API (Vegas lines)
    </div>
    """, unsafe_allow_html=True)


# ── Main content ──────────────────────────────────────────────────────────────
st.markdown("<h1 style='color:#f0b429; margin-bottom:4px;'>CBB BETTING MODEL</h1>", unsafe_allow_html=True)
st.markdown(f"<p style='color:#555; margin-top:0;'>{date.today().strftime('%A, %B %d, %Y')} &nbsp;·&nbsp; 2025–26 Season</p>", unsafe_allow_html=True)

if not MODULES_OK:
    st.stop()

today = str(date.today())

# Load data
with st.spinner("Loading projections..."):
    try:
        results = run_projections(today)
    except Exception as e:
        st.error(f"Error loading data: {e}")
        st.stop()

if not results:
    st.warning("No games found for today. Check back later or try refreshing.")
    st.stop()

# ── Summary metrics ───────────────────────────────────────────────────────────
games_with_vegas = [r for r in results if r.get("vegas_spread") is not None]
high_edge = [r for r in results if (r.get("edge_score") or 0) > 0.07]

col1, col2, col3, col4 = st.columns(4)
col1.metric("Games Today", len(results))
col2.metric("With Vegas Lines", len(games_with_vegas))
col3.metric("High Edge Games", len(high_edge), help="Edge score > 0.07")
avg_total = round(sum(r["total"] for r in results) / len(results), 1) if results else 0
col4.metric("Avg Total", avg_total)

st.markdown("<hr class='divider'>", unsafe_allow_html=True)

# ── Sort results ──────────────────────────────────────────────────────────────
if sort_by == "Edge Score ↓":
    results = sorted(results, key=lambda r: r.get("edge_score") or 0, reverse=True)
elif sort_by == "Total ↓":
    results = sorted(results, key=lambda r: r["total"], reverse=True)
elif sort_by == "Spread (biggest fav first)":
    results = sorted(results, key=lambda r: abs(r["spread"]), reverse=True)
else:
    results = sorted(results, key=lambda r: r["team1"])

# Filter
if show_only_vegas:
    results = [r for r in results if r.get("vegas_spread") is not None]
if min_edge > 0:
    results = [r for r in results if (r.get("edge_score") or 0) >= min_edge]

# ── Game cards ────────────────────────────────────────────────────────────────
st.markdown("<div class='section-title'>TODAY'S PROJECTIONS</div>", unsafe_allow_html=True)

if not results:
    st.info("No games match your filters.")
else:
    for r in results:
        edge = r.get("edge_score")
        if edge and edge > 0.08:
            badge_cls = "edge-hot"
            badge_txt = f"🔥 EDGE {edge:.4f}" + (" · SIDES DIFFER" if disagree else "")
        elif edge and edge > 0.05:
            badge_cls = "edge-good"
            badge_txt = f"✓ EDGE {edge:.4f}" + (" · SIDES DIFFER" if disagree else "")
        elif edge:
            badge_cls = "edge-low"
            badge_txt = f"EDGE {edge:.4f}" + (" · SIDES DIFFER" if disagree else "")
        else:
            badge_cls, badge_txt = "edge-low", "NO LINE"

        v_spread_txt = f"{r['vegas_spread']:+.1f}" if r.get("vegas_spread") is not None else "—"
        v_total_txt  = f"{r['vegas_total']:.1f}"   if r.get("vegas_total")  is not None else "—"
        kp_home = f"{r['kp_home_score']:.0f}" if r.get("kp_home_score") else "—"
        kp_away = f"{r['kp_away_score']:.0f}" if r.get("kp_away_score") else "—"
        spread_diff = f"{r['spread_edge']:+.1f}" if r.get("spread_edge") is not None else "—"

        st.markdown(f"""
        <div class="game-card">
            <span class="edge-badge {badge_cls}">{badge_txt}</span>
            <div class="team-row">
                <span class="team-name">{r['team1']} <span class="team-home">HOME</span></span>
                <span class="team-score">{r['team1_score']:.0f}</span>
            </div>
            <div class="team-row">
                <span class="team-name">{r['team2']}</span>
                <span class="team-score">{r['team2_score']:.0f}</span>
            </div>
            <div class="game-meta">
                <span>MY SPREAD <span class="meta-val">{r['spread']:+.1f}</span></span>
                <span>VEGAS <span class="meta-val">{v_spread_txt}</span></span>
                <span>SWING <span class="meta-val">{spread_diff}</span></span>
                <span>MY FAV <span class="meta-val">{my_fav}</span></span>
                <span>VGS FAV <span class="meta-val">{vegas_fav}</span></span>
                <span>MY TOTAL <span class="meta-val">{r['total']:.1f}</span></span>
                <span>VGS TOTAL <span class="meta-val">{v_total_txt}</span></span>
                <span>PACE <span class="meta-val">{r['projected_pace']}</span></span>
            </div>
        </div>
        """, unsafe_allow_html=True)

# ── Summary table ─────────────────────────────────────────────────────────────
st.markdown("<div class='section-title'>FULL TABLE</div>", unsafe_allow_html=True)

table_rows = []
for r in results:
    table_rows.append({
        "Home":         r["team1"],
        "Away":         r["team2"],
        "My Home":      r["team1_score"],
        "My Away":      r["team2_score"],
        "My Spread":    r["spread"],
        "My Total":     r["total"],
        "Vegas Spread": r.get("vegas_spread"),
        "Vegas Total":  r.get("vegas_total"),
        "Spread Diff":  r.get("spread_edge"),
        "Edge Score":   r.get("edge_score"),
        "KP Home":      r.get("kp_home_score"),
        "KP Away":      r.get("kp_away_score"),
        "Pace":         r["projected_pace"],
        "Book":         r.get("source_book", "—"),
    })

df = pd.DataFrame(table_rows)

# Format floats
for col in ["My Home","My Away","My Spread","My Total","Vegas Spread","Vegas Total","Spread Diff","KP Home","KP Away","Pace"]:
    if col in df.columns:
        df[col] = df[col].apply(lambda x: f"{x:+.1f}" if pd.notna(x) and col in ["My Spread","Vegas Spread","Spread Diff"] else (f"{x:.1f}" if pd.notna(x) else "—"))

if "Edge Score" in df.columns:
    df["Edge Score"] = df["Edge Score"].apply(lambda x: f"{x:.4f}" if pd.notna(x) else "—")

st.dataframe(df, use_container_width=True, hide_index=True)

# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown(f"""
<div style='margin-top:40px; padding-top:20px; border-top:1px solid #1e2535;
     font-size:0.75rem; color:#444; text-align:center;'>
CBB Model · 2025–26 Season · Last updated {datetime.now().strftime('%I:%M %p')}
</div>
""", unsafe_allow_html=True)
