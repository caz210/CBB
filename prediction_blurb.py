"""
prediction_blurb.py
Generates a plain-English game prediction sentence from project_game() output.
Also contains renamed Four Factor labels to fix the offense/defense direction confusion.

USAGE IN STREAMLIT:
    from prediction_blurb import generate_prediction_blurb, FOUR_FACTOR_LABELS

    # Inside your game breakdown block, add a new tab:
    tab1, tab2, tab3, tab4 = st.tabs(["Overview", "Four Factors", "Full Breakdown", "🔮 Prediction"])
    with tab4:
        blurb = generate_prediction_blurb(game_result)
        st.markdown(blurb, unsafe_allow_html=True)
"""

# ─────────────────────────────────────────────────────────────────────────────
# FIX: Renamed Four Factor row labels so offense vs defense direction is clear.
# Use these in your breakdown tables instead of the raw API field names.
# ─────────────────────────────────────────────────────────────────────────────
FOUR_FACTOR_LABELS = {
    # Offensive four factors (what THIS team does on offense)
    "TO_Pct":    "Turnover Rate (Offense)",          # How often THEY turn it over (lower = better)
    "OR_Pct":    "Off. Rebound Rate",                # How often THEY grab their own misses (higher = better)
    "FT_Rate":   "Free Throw Rate (Offense)",        # How often THEY get to the line (higher = better)

    # Defensive four factors (what THIS team ALLOWS / FORCES on the other end)
    "DTO_Pct":   "Turnovers Forced on Opponent",     # How often THEY force the opponent to turn it over
    "DOR_Pct":   "Opp. Off. Rebounds Allowed",       # How often the opponent gets their own miss (lower = better)
    "DFT_Rate":  "Free Throws Allowed to Opponent",  # How often THEY foul / send opponent to line (lower = better)
}

# Projected metric labels (for the "★" rows in breakdowns)
PROJECTED_LABELS = {
    "proj_to":  "Projected TO Advantage",   # Positive = opponent turns it over more / we force more
    "proj_reb": "Projected REB Advantage",  # Positive = we get more offensive boards
    "proj_ft":  "Projected FT Advantage",   # Negative = we draw more FTs than we allow
    "proj_poss":"Projected Possessions",    # Full adjusted possession count
}


# ─────────────────────────────────────────────────────────────────────────────
# PREDICTION BLURB GENERATOR
# ─────────────────────────────────────────────────────────────────────────────

def _advantage_label(val: float, threshold: float = 0.0) -> bool:
    """Returns True if val represents a meaningful advantage (above threshold)."""
    return val > threshold


def generate_prediction_blurb(g: dict) -> str:
    """
    Generate a plain-English prediction paragraph from project_game() output.

    Args:
        g: The dict returned by project_game(). Must contain:
           team1, team2, team1_score, team2_score,
           team1_ppp, team2_ppp, location,
           debug: { t1_reb, t2_reb, t1_to, t2_to, t1_ft, t2_ft,
                    t1_poss, t2_poss }

    Returns:
        A markdown-formatted HTML string suitable for st.markdown(..., unsafe_allow_html=True)
    """
    t1      = g["team1"]
    t2      = g["team2"]
    s1      = g["team1_score"]
    s2      = g["team2_score"]
    ppp1    = g["team1_ppp"]
    ppp2    = g["team2_ppp"]
    loc     = g.get("location", "neutral")  # "home", "away", or "neutral"

    d       = g["debug"]
    reb1    = d["t1_reb"]
    reb2    = d["t2_reb"]
    to1     = d["t1_to"]
    to2     = d["t2_to"]
    ft1     = d["t1_ft"]
    ft2     = d["t2_ft"]
    poss1   = d["t1_poss"]
    poss2   = d["t2_poss"]

    spread  = abs(s1 - s2)

    # ── Determine winner / loser ──────────────────────────────────────────────
    if s1 > s2:
        winner, loser = t1, t2
        w_score, l_score = s1, s2
        w_ppp, l_ppp = ppp1, ppp2
        w_reb, l_reb = reb1, reb2
        w_to,  l_to  = to1,  to2
        w_ft,  l_ft  = ft1,  ft2
        w_poss, l_poss = poss1, poss2
        winner_is_t1 = True
    else:
        winner, loser = t2, t1
        w_score, l_score = s2, s1
        w_ppp, l_ppp = ppp2, ppp1
        w_reb, l_reb = reb2, reb1
        w_to,  l_to  = to2,  to1
        w_ft,  l_ft  = ft2,  ft1
        w_poss, l_poss = poss2, poss1
        winner_is_t1 = False

    # ── Location context ─────────────────────────────────────────────────────
    if loc == "home" and winner_is_t1:
        loc_phrase = f"at home"
    elif loc == "away" and not winner_is_t1:
        loc_phrase = f"on the road"
    elif loc == "neutral":
        loc_phrase = f"on a neutral floor"
    else:
        loc_phrase = ""

    # ── Identify the key advantages ───────────────────────────────────────────
    advantages = []

    # 1. Offensive efficiency
    eff_gap = w_ppp - l_ppp
    if eff_gap > 0.005:
        advantages.append("offensive efficiency")

    # 2. Rebounding edge — positive proj_reb means favorable board matchup
    # Winner's rebound value > 0 means THEY have the board advantage
    if w_reb > 0.5:
        advantages.append("an edge on the offensive glass")
    elif w_reb > 0.0:
        advantages.append("a slight rebounding advantage")

    # 3. Turnover edge — positive proj_to means favorable TO matchup for winner
    # Positive = opponent turns it over more / winner forces more
    if w_to > 0.5:
        advantages.append("forcing turnovers and protecting the ball")
    elif w_to > 0.0:
        advantages.append("a slight turnover margin edge")

    # 4. Free throw edge — NEGATIVE proj_ft means drawing more FTs than you foul
    if w_ft < -1.0:
        advantages.append("getting to the free throw line")
    elif w_ft < 0:
        advantages.append("a slight free throw rate advantage")

    # 5. Possession edge
    if w_poss > l_poss + 0.5:
        advantages.append("generating extra possessions")

    # ── Confidence / margin language ──────────────────────────────────────────
    if spread >= 10:
        confidence = "comfortably"
    elif spread >= 6:
        confidence = "by a solid margin"
    elif spread >= 3:
        confidence = "in what should be a competitive game"
    else:
        confidence = "in what figures to be a tight battle"

    # ── Build the sentence ────────────────────────────────────────────────────
    if not advantages:
        advantages = ["a small overall edge in the model"]

    if len(advantages) == 1:
        adv_text = advantages[0]
    elif len(advantages) == 2:
        adv_text = f"{advantages[0]} and {advantages[1]}"
    else:
        adv_text = ", ".join(advantages[:-1]) + f", and {advantages[-1]}"

    loc_suffix = f" {loc_phrase}" if loc_phrase else ""

    headline = (
        f"**{winner}** ({w_score:.0f}) over **{loser}** ({l_score:.0f}){loc_suffix}, "
        f"{confidence}."
    )

    body = (
        f"The model projects **{winner}** to win due to {adv_text}. "
    )

    # ── Supporting detail sentences ───────────────────────────────────────────
    details = []

    if eff_gap > 0.005:
        details.append(
            f"Their offensive efficiency edge (PPP: {w_ppp:.4f} vs {l_ppp:.4f}) "
            f"suggests they can consistently score on this matchup."
        )

    if w_reb > 0.0:
        details.append(
            f"On the boards, {winner} projects to win the rebounding battle "
            f"(REB margin: +{w_reb:.2f}), giving them extra offensive opportunities."
        )

    if w_to > 0.0:
        details.append(
            f"The turnover picture also favors {winner} — "
            f"they project to win the possession battle through ball security and defense "
            f"(TO margin: +{w_to:.2f})."
        )

    if w_ft < -0.5:
        details.append(
            f"{winner} projects to draw more free throws than they allow "
            f"(FT margin: {w_ft:.2f}), which adds easy possessions at the line."
        )

    detail_text = " ".join(details)

    # ── Final score line ──────────────────────────────────────────────────────
    score_line = (
        f"**Projected Final:** {t1} {s1:.0f} – {t2} {s2:.0f} &nbsp;|&nbsp; "
        f"**Spread:** {winner} -{spread:.1f} &nbsp;|&nbsp; "
        f"**Total:** {s1 + s2:.0f}"
    )

    # ── Assemble full output ──────────────────────────────────────────────────
    blurb = f"""
<div style="
    background: linear-gradient(135deg, #0f1923 0%, #1a2a3a 100%);
    border-left: 4px solid #e8a838;
    border-radius: 8px;
    padding: 20px 24px;
    margin: 8px 0;
    font-family: 'Segoe UI', sans-serif;
    color: #e8e8e8;
    line-height: 1.7;
">
    <div style="font-size: 1.1em; font-weight: 600; color: #e8a838; margin-bottom: 8px;">
        🔮 Model Prediction
    </div>
    <div style="font-size: 1.05em; margin-bottom: 12px;">{headline}</div>
    <div style="font-size: 0.95em; color: #c8d8e8; margin-bottom: 10px;">{body}{detail_text}</div>
    <div style="
        font-size: 0.88em;
        color: #a0b8cc;
        border-top: 1px solid #2a3a4a;
        padding-top: 10px;
        margin-top: 10px;
    ">{score_line}</div>
</div>
"""
    return blurb


# ─────────────────────────────────────────────────────────────────────────────
# STREAMLIT INTEGRATION SNIPPET
# Drop this inside your per-game expander / card where you currently render tabs.
# ─────────────────────────────────────────────────────────────────────────────
STREAMLIT_TAB_SNIPPET = '''
# In your game card, replace your existing tab definition with this:

tab_overview, tab_four_factors, tab_breakdown, tab_prediction = st.tabs([
    "📊 Overview",
    "4️⃣ Four Factors",
    "🔬 Full Breakdown",
    "🔮 Prediction",
])

# ... your existing tab_overview and tab_four_factors content ...

with tab_prediction:
    from prediction_blurb import generate_prediction_blurb
    blurb = generate_prediction_blurb(game_result)
    st.markdown(blurb, unsafe_allow_html=True)

# ── Four Factor label fix ──────────────────────────────────────────────
# In tab_four_factors, replace raw field names with human-readable labels.
# Example row rename:
#   OLD:  st.write("DTO_Pct", t1_dto, t2_dto)
#   NEW:  st.write("Turnovers Forced on Opponent", t1_dto, t2_dto)
#
# See FOUR_FACTOR_LABELS dict in prediction_blurb.py for the full mapping.
#
# KEY RULE: Defense stats always describe what YOU DO TO THEM (not your own stats):
#   DTO_Pct  → "Turnovers Forced on Opponent"     (higher = better defense)
#   DOR_Pct  → "Opp. Off. Rebounds Allowed"        (lower = better defense)
#   DFT_Rate → "Free Throws Allowed to Opponent"   (lower = better defense)
'''
