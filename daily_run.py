"""
daily_run.py — fully automated daily PAPER-trading pipeline (no real bets are placed).

    python daily_run.py                    # today (America/Chicago)
    python daily_run.py --date 2026-11-02  # a specific slate
    options: --no-refresh (use cached KenPom CSVs)  --no-odds-api  --no-kalshi  --dry-run (don't write ledger)
             --exclude-line-moves (drop sides whose spread moved >= 2 pts; default flag only)
             --coach-lock-games N (NEW COACH lock length for both markets; 0 = off)
             --coach-lock-totals N / --coach-lock-sides N (per market, defaults 4 / 8)
             --no-coach-lock (lock off for everyone)  --coach-lock TEAM / --coach-unlock TEAM (one run only)
             per-team flags (permanent, git-tracked): config/coach_locks.csv, edit via coach_locks.py
             --skip-side-moves (also skip sides whose spread moved >= 3 pts AGAINST our side; default flag only)

Steps
  (a) refresh KenPom ratings / prior / per-team HCA (cached) and scrape FanMatch (KenPom predictions)
  (b) prices: DraftKings open + current lines from ESPN's free odds feed (primary); The Odds API
      only as a budget-aware fallback (max ODDS_API_MAX_CALLS_PER_DAY calls/day, never below
      ODDS_API_RESERVE credits); Kalshi board prices recorded as a reference
  (c) project every game with v2 (sides) and the original v1 formula (totals)
  (d) flag plays with the rules below; quarter-Kelly paper stakes on a notional bankroll
  (e) append plays to data/ledger/ledger.csv with timestamped prices
  (f) grade earlier open plays from final scores; record closing lines and CLV
  (g) write outputs/daily/YYYY-MM-DD.md, incl. a Top-10 edges table (every game, regardless of threshold)
      and a 'likely news/injury' flag when a side's spread moved >= 2 pts open -> current
  Later in the day, pretip_check.py re-pulls lines for today's open plays and flags moves against us.

Rules (from the walk-forward backtest, Jan 2025 – Apr 2026, see outputs/walkforward/):
  SIDES require the edge to clear the threshold vs the OPENING line AND the current line (same side).
  TOTALS require it vs the OPENING line only (the vs-current check dropped hit rate 54.8% -> 48.3%,
  coach_lock_study.py); the line-move skip below replaces it. No opening line -> no play.
  SIDES  — v2 model-only margin vs the DK line; edge% = |model − line| / DK total >= 4%
           (vs open: n=85, 60.0% ATS, CLV +1.1; vs close it lost — bet early).
  TOTALS — original v1 total vs the DK total; edge% = |model − total| / total >= 3%
           (vs open: n=1248, 54.2%, +3.6% ROI, CLV +1.6; vs close it lost — bet early).
  NEW COACH — a team whose KenPom head coach changed since last season (czarp/coach.py), with
           config/coach_locks.csv as the git-tracked per-team Y/N flag that overrides auto-detection, is locked out of value plays until it has played
           COACH_LOCK_TOTALS (4) games for totals / COACH_LOCK_SIDES (8) for sides (coach_lock_study.py).
  LINE MOVE — a total whose DK line moved >= 3 pts open -> current AWAY from our side is skipped
           (backtest: 24-42, -31% ROI). A side that moved >= 3 against us is flagged only.
  TRACK  — SHADOW tier, paper only, flat 1u, never staked: sides >= 4% vs open with >= 2% still left vs current
           that are not official (all other rules identical). Logged, graded and reported as a separate record so the
           4%/4% and 4%/2% side rules can be compared on a public, timestamped, hash-sealed ledger.
  Stake  — quarter Kelly at the actual price using a conservative hit-rate estimate per rule
           (not the model's own probability), capped at 2.5% of the current paper bankroll.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
import traceback
import warnings
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pandas as pd
import requests

warnings.filterwarnings("ignore", category=UserWarning)
CENTRAL = ZoneInfo("America/Chicago")
ROOT = os.path.dirname(os.path.abspath(__file__))
os.chdir(ROOT)
sys.path.insert(0, ROOT)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

LEDGER_DIR = os.path.join("data", "ledger")
LEDGER = os.path.join(LEDGER_DIR, "ledger.csv")
USAGE = os.path.join(LEDGER_DIR, "odds_api_usage.json")
SNAP_DIR = os.path.join(LEDGER_DIR, "snapshots")
OUT_DIR = os.path.join("outputs", "daily")

BANKROLL0 = 1000.0
KELLY_MULT = 0.25
MAX_STAKE = 0.025
MIN_STAKE = 1.00   # skip plays whose Kelly stake rounds to ~0 at the posted price
RULES = {
    # market: (model, threshold on edge%, conservative hit-rate estimate used for Kelly)
    "spread": ("v2", 0.04, 0.545),
    "total": ("v1", 0.03, 0.535),
}
V1_VERSION = "czarp-v1-totals"     # original v1 formula, used for totals
# "likely news/injury" flag: the DK spread moved >= LINE_MOVE_PTS from open to current.
# Backtest (line_move_study.py, outputs/walkforward/line_move_study.txt): flagged side picks did BETTER
# than unflagged (sides vs open 22-5 vs 26-22), so the flag is shown but does not exclude plays by
# default. --exclude-line-moves (or LINE_MOVE_EXCLUDE = True) turns the filter on.
LINE_MOVE_PTS = 2.0
LINE_MOVE_EXCLUDE = False
# A total that moved >= TOTAL_MOVE_SKIP_PTS open->current AWAY from our side (market disagrees) is skipped.
# coach_lock_study.py (2024-25 + 2025-26, bets at the open): totals >= 3% vs open that then moved >= 3 pts
# against us went 24-42 (-30.6% ROI); 2-2.5 pts against went 26-41. Sides: only 1 such pick in two seasons,
# so the side rule is a flag unless SIDE_MOVE_SKIP / --skip-side-moves is set.
TOTAL_MOVE_SKIP_PTS = 3.0
SIDE_MOVE_AGAINST_PTS = 3.0
SIDE_MOVE_SKIP = False
# NEW COACH ("new philosophy") lock: games involving a team with a new head coach are not value plays
# until that team has played COACH_LOCK_TOTALS / COACH_LOCK_SIDES games this season (KenPom W+L). 0 turns the lock off.
COACH_LOCK_GAMES = None          # legacy: if set (int), overrides both per-market lengths
COACH_LOCK_TOTALS = 4           # backtest: totals unlock after ~4 games, sides 6-8
COACH_LOCK_SIDES = 8
# Per-team flags live in config/coach_locks.csv (git-tracked; czarp/coach.py). These per-run CLI overrides
# (--coach-lock TEAM / --coach-unlock TEAM) sit on top of that file for one run only.
COACH_FORCE_ON: set = set()
COACH_FORCE_OFF: set = set()


def coach_lock_n(market: str | None = None) -> int:
    """Lock length for a market ('total' / 'spread'); max of the two when market is None."""
    if COACH_LOCK_GAMES is not None:
        return COACH_LOCK_GAMES
    if market == "total":
        return COACH_LOCK_TOTALS
    if market == "spread":
        return COACH_LOCK_SIDES
    return max(COACH_LOCK_TOTALS, COACH_LOCK_SIDES)
TOP_N_EDGES = 10
# SHADOW tier 'track' (paper only, flat 1u, never staked on the bankroll): a side that clears the threshold vs the
# OPEN (4%) and still has >= TRACK_SIDE_CUR_THR (2%) left vs the current line, but is not an official play. Every
# other rule (same side, not started, NEW COACH lock, move skips) is identical to the official tier, so the two
# records compare exactly one thing: the vs-current bar (4% official vs 2% track). Backtest (analysis/side_move_study.py,
# graded at the price taken): 4%/4% 28-20 (58.3%, +11%); 4% open / 2% left 41-29 (58.6%, +11.8%).
TRACK_SIDE_CUR_THR = 0.02
# The NEW COACH lock applies to the track tier too by default (so the tiers differ ONLY in the vs-current bar).
# True = track also logs coach-locked sides (e.g. LSU -23.5 on 2026-11-03), at the cost of a clean A/B comparison.
TRACK_IGNORE_COACH_LOCK = False
TIERS = ("official", "track")
# Totals PACE FIX (analysis/totals_pace_fix.py -> outputs/walkforward/totals_pace_fix.txt). OFF by default:
# walk-forward by season it did not clearly beat the current totals. Games DO play slower than projected in
# November / when a team has < 8 games / at neutral sites (fit: -0.35 / -0.30 / -0.44 poss), but the v1 TOTAL
# has no Under/Over bias there (actual - v1 ~ 0 in Nov), so the pace correction alone was flat (+6.1% vs +6.0% ROI).
# The market blend T' = (1-w)*T1 + w*OPEN total shrinks every edge (== a higher threshold): better ROI on far fewer
# plays, with fewer total units. --pace-fix turns it on; --pace-fix-w sets w.
PACE_FIX = False
PACE_FIX_BIAS = {"nov": -0.35, "early": -0.30, "neutral": -0.44}   # possessions, pooled 2024-25 + 2025-26 fit
PACE_FIX_EARLY_GAMES = 8
PACE_FIX_MKT_W = 0.2


def pace_fix_total(total, pace, open_total=None, nov=False, min_games=None, neutral=False,
                   bias=None, w=None) -> float:
    """v1 total re-paced: P' = P + bias terms (points per possession held fixed: T' = T·P'/P), then blended
    with the OPEN DK total: T'' = (1−w)·T' + w·open (equivalent to blending the pace with the open-implied pace)."""
    bias = PACE_FIX_BIAS if bias is None else bias
    w = PACE_FIX_MKT_W if w is None else w
    if total is None or not pace:
        return total
    early = min_games is not None and min_games < PACE_FIX_EARLY_GAMES
    p2 = pace + bias.get("nov", 0) * bool(nov) + bias.get("early", 0) * early + bias.get("neutral", 0) * bool(neutral)
    t = total * p2 / pace
    if open_total:
        t = (1 - w) * t + w * open_total
    return t


def apply_pace_fix(games, today: str, data_dir: str = "data") -> int:
    """Replace g['v1_total'] by the pace-fixed total (original kept in v1_total_raw). Returns games changed."""
    try:
        from czarp.coach import games_played
        played = games_played(pd.read_csv(os.path.join(data_dir, "ratings.csv")))
    except Exception:
        played = {}
    n = 0
    for g in games:
        if g.get("skip") or g.get("v1_total") is None or not g.get("v1_pace"):
            continue
        gp = [played.get(g.get(f"kp_{s}")) for s in ("home", "away")]
        mg = min(int(x) for x in gp) if all(x is not None for x in gp) else None
        g["v1_total_raw"] = g["v1_total"]
        g["v1_total"] = pace_fix_total(g["v1_total"], g["v1_pace"], g.get("open_total"), nov=today[5:7] == "11",
                                       min_games=mg, neutral=g.get("neutral"))
        n += 1
    return n


ODDS_API_MAX_CALLS_PER_DAY = 1
ODDS_API_RESERVE = 100
ODDS_API_MIN_MISSING = 0.25     # use the Odds API only if ESPN lacks lines for >= 25% of games

SB_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard"
ODDS_URL = ("https://sports.core.api.espn.com/v2/sports/basketball/leagues/mens-college-basketball/"
            "events/{id}/competitions/{id}/odds")

LEDGER_COLS = [
    "pick_id", "created_ct", "game_date", "event_id", "tip_ct", "away", "home", "neutral",
    "market", "rule", "side", "line", "price", "open_line", "model_value", "market_value",
    "edge_pct", "edge_pct_vs_open", "kp_margin_home", "p_est", "stake", "bankroll_at_pick",
    "price_source", "kalshi_ref", "model_version",
    "status", "graded_ct", "away_score", "home_score", "close_line", "close_price", "clv_pts",
    "result", "pnl", "pretip_flag",
    # track record / proof columns (appended so older ledgers still load)
    "tier", "source", "logged_ct", "pnl_units", "pick_hash",
    "total_at_post",
]
# Fields frozen once a pick is logged (the pick itself). Grading, closing line, CLV and the pre-tip annotation are
# filled in later; nothing else may ever change. pick_hash = sha256(previous pick_hash + these fields) -> a hash chain.
PICK_FIELDS = [
    "pick_id", "created_ct", "game_date", "event_id", "tip_ct", "away", "home", "market", "rule", "side", "line",
    "price", "open_line", "model_value", "edge_pct", "edge_pct_vs_open", "stake", "bankroll_at_pick", "tier", "source",
]

_S = requests.Session()
_S.headers.update({"User-Agent": "czarp-daily/1.0"})
log_lines: list[str] = []


def log(msg: str) -> None:
    print(msg, flush=True)
    log_lines.append(msg)


def now_ct() -> datetime:
    return datetime.now(CENTRAL)


def _num(x):
    try:
        if isinstance(x, str):
            x = x.replace("+", "").strip()
            if x.upper() == "EVEN":
                return 100.0
            if x.upper() in ("PK", "PICK"):
                return 0.0
        v = float(x)
        return None if math.isnan(v) else v
    except (TypeError, ValueError):
        return None


# ── ESPN: schedule, scores, DK lines (free) ───────────────────────────────────
def espn_scoreboard(day: date) -> list[dict]:
    r = _S.get(SB_URL, params={"dates": day.strftime("%Y%m%d"), "groups": 50, "limit": 500}, timeout=20)
    r.raise_for_status()
    rows = []
    for e in r.json().get("events", []):
        c = e["competitions"][0]
        comp = {t["homeAway"]: t for t in c["competitors"]}
        if "home" not in comp or "away" not in comp:
            continue
        st = c.get("status", {}).get("type", {})
        utc = datetime.strptime(e["date"], "%Y-%m-%dT%H:%MZ").replace(tzinfo=timezone.utc)
        rows.append({
            "event_id": str(e["id"]), "tip_utc": utc,
            "time_tbd": c.get("timeValid") is False or st.get("name") == "STATUS_TBD", "tip_ct": utc.astimezone(CENTRAL).strftime("%Y-%m-%d %H:%M"),
            "game_date": utc.astimezone(CENTRAL).strftime("%Y-%m-%d"),
            "home": comp["home"]["team"].get("location"), "away": comp["away"]["team"].get("location"),
            "home_full": comp["home"]["team"].get("displayName"), "away_full": comp["away"]["team"].get("displayName"),
            "home_score": _num(comp["home"].get("score")), "away_score": _num(comp["away"].get("score")),
            "neutral": bool(c.get("neutralSite")), "completed": bool(st.get("completed")),
            "state": st.get("state"),  # pre / in / post
            "status_name": st.get("name"),  # STATUS_FINAL / STATUS_POSTPONED / ...
        })
    return rows


def espn_dk_lines(event_id: str) -> dict:
    """DK lines from ESPN, home perspective: open, current (pre-game) and close."""
    try:
        r = _S.get(ODDS_URL.format(id=event_id), timeout=20)
        if r.status_code != 200:
            return {}
        items = r.json().get("items", [])
    except Exception:
        return {}
    it = next((i for i in items if "draft" in i.get("provider", {}).get("name", "").lower()), None)
    it = it or (items[0] if items else None)
    if not it:
        return {}
    h, a = it.get("homeTeamOdds", {}) or {}, it.get("awayTeamOdds", {}) or {}
    out = {"book": it.get("provider", {}).get("name")}
    for when in ("open", "current", "close"):
        ho, ao, top = h.get(when, {}) or {}, a.get(when, {}) or {}, it.get(when, {}) or {}
        out[f"{when}_home_spread"] = _num((ho.get("pointSpread") or {}).get("american"))
        out[f"{when}_home_spread_price"] = _num((ho.get("spread") or {}).get("american"))
        out[f"{when}_away_spread_price"] = _num((ao.get("spread") or {}).get("american"))
        out[f"{when}_total"] = _num((top.get("total") or {}).get("american"))
        out[f"{when}_over_price"] = _num((top.get("over") or {}).get("american"))
        out[f"{when}_under_price"] = _num((top.get("under") or {}).get("american"))
    if out.get("current_home_spread") is None:
        out["current_home_spread"] = _num(it.get("spread"))
    if out.get("current_total") is None:
        out["current_total"] = _num(it.get("overUnder"))
    # ESPN/DK opening lines are sometimes posted home/away swapped (see walkforward.clean_lines)
    o, c = out.get("open_home_spread"), out.get("current_home_spread")
    if o is not None and c is not None and abs(o + c) < abs(o - c) and abs(o + c) <= 3:
        out["open_home_spread"] = -o
        out["open_home_spread_price"], out["open_away_spread_price"] = (
            out.get("open_away_spread_price"), out.get("open_home_spread_price"))
    if o is not None and c is not None and abs(out["open_home_spread"] - c) > 8:
        out["open_home_spread"] = None
    if out.get("open_total") is not None and out.get("current_total") is not None \
            and abs(out["open_total"] - out["current_total"]) > 12:
        out["open_total"] = None
    return out


# ── The Odds API (budget-aware fallback) ──────────────────────────────────────
def _usage() -> dict:
    try:
        with open(USAGE) as fh:
            return json.load(fh)
    except Exception:
        return {"calls": {}, "remaining": None}


def odds_api_fallback(games: list[dict], today: str, allow: bool) -> int:
    """Fill missing DK lines from The Odds API, at most N calls/day and never below the reserve."""
    missing = [g for g in games if g.get("current_home_spread") is None or g.get("current_total") is None]
    if not games or not missing:
        if games:
            log("  Odds API: not needed (ESPN/DK has lines for every D-I game) — 0 credits used")
        return 0
    frac = len(missing) / len(games)
    if not allow:
        log(f"  Odds API: disabled ({len(missing)} games lack ESPN lines)")
        return 0
    if frac < ODDS_API_MIN_MISSING:
        log(f"  Odds API: skipped — ESPN covers {1 - frac:.0%} of games (saving credits)")
        return 0
    u = _usage()
    used_today = u["calls"].get(today, 0)
    if used_today >= ODDS_API_MAX_CALLS_PER_DAY:
        log(f"  Odds API: skipped — daily budget used ({used_today}/{ODDS_API_MAX_CALLS_PER_DAY})")
        return 0
    if u.get("remaining") is not None and float(u["remaining"]) < ODDS_API_RESERVE:
        log(f"  Odds API: skipped — {u['remaining']} credits left (< reserve {ODDS_API_RESERVE})")
        return 0
    try:
        import odds_fetcher as of
        if not of.ODDS_API_KEY:
            log("  Odds API: no ODDS_API_KEY — skipped")
            return 0
        df = of.fetch_vegas_lines()
        u["calls"][today] = used_today + 1
        u["remaining"] = of._odds_credits_remaining
        u["last_call_ct"] = now_ct().isoformat(timespec="seconds")
        with open(USAGE, "w") as fh:
            json.dump(u, fh, indent=1)
    except Exception as e:
        log(f"  Odds API: failed ({type(e).__name__}: {e})")
        return 0
    if df is None or df.empty:
        return 0
    filled = 0
    for g in missing:
        best, bs = None, 0.0
        for _, r in df.iterrows():
            s = min(of._best_score(g["home_full"], r.vegas_home), of._best_score(g["away_full"], r.vegas_away))
            if s > bs:
                best, bs = r, s
        if best is None or bs < 0.75:
            continue
        if g.get("current_home_spread") is None and pd.notna(best.vegas_spread):
            g["current_home_spread"] = float(best.vegas_spread)
            g["current_home_spread_price"] = best.home_spread_price
            g["current_away_spread_price"] = best.away_spread_price
        if g.get("current_total") is None and pd.notna(best.vegas_total):
            g["current_total"] = float(best.vegas_total)
            g["current_over_price"], g["current_under_price"] = best.over_price, best.under_price
        g["price_source"] = f"oddsapi:{best.source_book}"
        filled += 1
    log(f"  Odds API: filled {filled} of {len(missing)} games (credits left {u.get('remaining')})")
    return filled


# ── KenPom data + projections ────────────────────────────────────────────────
def load_kenpom(today: str, refresh: bool) -> dict:
    if refresh:
        try:
            from kenpom_fetcher import fetch_all, fetch_prior, save_data, SEASON
            save_data(fetch_all(SEASON, today))
            if not os.path.exists("data/prior/ratings.csv"):
                save_data(fetch_prior(SEASON), os.path.join("data", "prior"))
            log("  KenPom ratings refreshed")
        except Exception as e:
            log(f"  KenPom refresh failed ({type(e).__name__}: {e}) — using cached CSVs")
        try:
            from hca_fetcher import load_hca
            load_hca(refresh=not os.path.exists("data/hca.csv"))
        except Exception as e:
            log(f"  HCA refresh skipped ({type(e).__name__}) — using cache/average")
    import model as M
    data = M.load_data()
    data["config"] = M.load_config() if hasattr(M, "load_config") else None
    if data["config"] is None:
        from czarp.config import load_config
        data["config"] = load_config()
    return data


def fanmatch_preds(day: str) -> dict:
    """{frozenset(kp names): (team1, kp_team1_margin)} from the FanMatch page (needs KenPom login)."""
    try:
        from kenpom_scraper import scrape_fanmatch_games
        g = scrape_fanmatch_games(day)
        out = {frozenset([x["team1"], x["team2"]]): (x["team1"], x.get("kp_team1_margin")) for x in g}
        log(f"  FanMatch: {len(g)} games, {sum(v[1] is not None for v in out.values())} with KenPom predictions")
        return out
    except Exception as e:
        log(f"  FanMatch scrape failed ({type(e).__name__}) — continuing without KenPom predictions")
        return {}


def v1_frames(tt: pd.DataFrame) -> dict:
    t = tt.reset_index().rename(columns={"index": "TeamName"})
    if "TeamName" not in t:
        t = t.rename(columns={t.columns[0]: "TeamName"})
    return {
        "ratings": t[["TeamName", "AdjOE", "AdjDE", "AdjTempo", "RankAdjEM"]],
        "four_factors": t[["TeamName"]].assign(TO_Pct=t["TO"], DTO_Pct=t["D_TO"], OR_Pct=t["OR"],
                                               DOR_Pct=t.get("Opp_OR", t.get("D_OR_raw")),
                                               FT_Rate=t["FTR"], DFT_Rate=t["D_FTR"]),
        "height": t[["TeamName", "AvgHgt", "Exp", "Bench"]],
        "net": pd.DataFrame(columns=["TeamName", "Rank"]), "misc": pd.DataFrame(),
    }


TENTATIVE_SUFFIX = " (tentative)"


def archetypes(r: dict) -> dict:
    """Tag chips for the front end: list of {name, kind} (max 2), plus tentative flags/notes.

    `home`/`away` are objects with name+kind (strength|weakness|style). Legacy string lists
    are still accepted as input from the model via team*_tag_objs / team*_tags.
    """
    def objs_from(r, side_objs_key, side_tags_key):
        objs = list(r.get(side_objs_key) or [])
        if objs:
            out, tent = [], False
            for o in objs:
                if isinstance(o, dict):
                    name = o.get("name", "")
                    kind = o.get("kind", "strength")
                    t = bool(o.get("tentative"))
                else:
                    name, kind, t = str(o), "strength", False
                if name.endswith(TENTATIVE_SUFFIX):
                    name = name[:-len(TENTATIVE_SUFFIX)]; t = True
                out.append({"name": name, "kind": kind})
                tent = tent or t
            return out, tent
        # fallback: plain name strings
        tags = list(r.get(side_tags_key) or [])
        out, tent = [], False
        for t in tags:
            name = t
            if name.endswith(TENTATIVE_SUFFIX):
                name = name[:-len(TENTATIVE_SUFFIX)]; tent = True
            out.append({"name": name, "kind": "strength"})
        return out, tent
    away, at = objs_from(r, "team1_tag_objs", "team1_tags")  # team1 = away
    home, ht = objs_from(r, "team2_tag_objs", "team2_tags")
    return {"home": home, "away": away, "home_tentative": ht, "away_tentative": at,
            "tentative": bool(r.get("tags_tentative", False)) or ht or at, "notes": r.get("matchup_notes", [])}


def project_all(games: list[dict], data: dict) -> None:
    import model as M
    import czarp.legacy_v1 as V1
    from czarp import teamstats as TS
    from backtest import NameMatcher
    match = NameMatcher(data["ratings"]["TeamName"].astype(str))
    ctx = None
    try:
        ctx = M._context(data, data["config"])
        v1data = v1_frames(ctx["tt"])
    except Exception as e:
        log(f"  v1 inputs unavailable ({type(e).__name__}: {e}) — totals rule disabled")
        v1data = None
    for g in games:
        h, a = match(g["home"]), match(g["away"])
        g["kp_home"], g["kp_away"] = h, a
        if not h or not a:
            g["skip"] = "non-D1 / unmatched team"
            continue
        try:
            r = M.project_game(a, h, None if g["neutral"] else False, data, game_date=g["game_date"])
            g["v2_home_margin"] = -float(r.get("spread_raw", r["spread"]))
            g["v2_total"] = float(r.get("total_raw", r["total"]))
            g["v2_home_win_prob"] = 1 - float(r.get("team1_win_prob", 0.5))
            g["model_version"] = r.get("model_version", "czarp-v2")
            g["archetype_tags"] = archetypes(r)
            g["proj_possessions"] = float(r.get("pace_raw") or r.get("projected_pace") or 0) or None
            vegas = g.get("current_home_spread")
            if vegas is None:
                vegas = g.get("open_home_spread")
            upset, ureason = False, ""
            if ctx is not None:
                upset, ureason = TS.upset_alert(
                    h, a, ctx["tags"], float(g["v2_home_margin"]),
                    float(vegas) if vegas is not None else None,
                    home_win_prob=g.get("v2_home_win_prob"))
            g["upset_alert"] = bool(upset)
            g["upset_reason"] = ureason or None
            home_names = [x["name"] for x in g["archetype_tags"]["home"]]
            away_names = [x["name"] for x in g["archetype_tags"]["away"]]
            g["reason"] = TS.projection_reason(
                h, a, home_names, away_names, g.get("proj_possessions"),
                matchup_notes=g["archetype_tags"].get("notes"),
                upset=upset, upset_reason=ureason)
        except Exception as e:
            g["skip"] = f"v2 failed: {e}"
            continue
        if v1data is not None:
            try:
                r1 = V1.project_game(a, h, None if g["neutral"] else False, v1data)
                g["v1_home_margin"], g["v1_total"] = -float(r1["spread"]), float(r1["total"])
                g["v1_pace"] = float(r1.get("projected_pace") or 0) or None
            except Exception:
                pass


# ── plays ─────────────────────────────────────────────────────────────────────
def american_to_decimal(a) -> float:
    a = _num(a)
    if a is None or a == 0:
        a = -110.0
    return 1 + (a / 100 if a > 0 else 100 / -a)


def kelly_stake(p: float, price, bankroll: float) -> float:
    b = american_to_decimal(price) - 1
    f = max(0.0, (b * p - (1 - p)) / b) * KELLY_MULT
    return round(min(f, MAX_STAKE) * bankroll, 2)


def kalshi_ref(board, g, market, side_team, line) -> str:
    if not board:
        return ""
    try:
        import czarp.kalshi as kx
        game = kx.find_game(board, g["kp_home"], g["kp_away"])
        if not game:
            return ""
        if market == "total":
            legs = sorted(game.get("total", []), key=lambda l: abs(l["strike"] - line))
            if not legs:
                return ""
            l = legs[0]
            over = side_team == "Over"
            ask = l["yes_ask"] if over else l["no_ask"]
            return f"Over {l['strike']} {'YES' if over else 'NO'} @ {ask}"
        team_legs = [l for l in game.get("spread", []) if kx.team_in([l["team"]], side_team)]
        if team_legs and line < 0:      # our team favored: "team by > |line|" YES
            l = min(team_legs, key=lambda l: abs(l["strike"] - abs(line)))
            return f"{l['team']} by >{l['strike']} YES @ {l['yes_ask']}"
        ml = game.get("moneyline", {})
        k = kx.team_in(list(ml.keys()), side_team)
        return f"{k} ML YES @ {ml[k]['yes_ask']}" if k else ""
    except Exception:
        return ""


def line_move(g) -> float | None:
    """|current − open| DK home spread in points (None if either is missing)."""
    o, c = g.get("open_home_spread"), g.get("current_home_spread")
    return abs(c - o) if o is not None and c is not None else None


def move_against(c) -> float | None:
    """Points the DK line moved open -> current AWAY from our side (market disagreeing with us);
    negative = moved toward our side. Same sign convention as negative CLV at the open."""
    o, cur = c.get("open_line"), c.get("line")
    if o is None or cur is None:
        return None
    if c["market"] == "spread":          # picked-side spread: a bigger number = market likes our side less
        return cur - o
    return (o - cur) if c["side"] == "Over" else (cur - o)


def team_lock_n(x: dict, market=None, lock_games=None) -> int:
    """Lock length for one new-coach entry: the global length (0 = lock off for everyone wins), else the
    team's own lock_sides / lock_totals from config/coach_locks.csv when set."""
    n = coach_lock_n(market) if lock_games is None else lock_games
    if n <= 0 or lock_games is not None:
        return max(0, n)
    keys = {"spread": ["lock_sides"], "total": ["lock_totals"]}.get(market, ["lock_sides", "lock_totals"])
    own = [x.get(k) for k in keys if x.get(k) is not None]
    if not own:
        return n
    if market is None:   # either market: longest of the two (defaults filled in for unset ones)
        vals = [x.get("lock_sides") if x.get("lock_sides") is not None else coach_lock_n("spread"),
                x.get("lock_totals") if x.get("lock_totals") is not None else coach_lock_n("total")]
        return max(vals)
    return int(own[0])


def coach_locks(g, lock_games=None, market=None) -> list[dict]:
    """NEW COACH entries for the two teams in a game (set by tag_new_coaches)."""
    out = []
    for k in ("coach_away", "coach_home"):
        x = g.get(k)
        if x:
            n = team_lock_n(x, market, lock_games)
            rem = max(0, n - int(x.get("games", 0)))
            out.append(dict(x, lock_n=n, remaining=rem, locked=rem > 0 and n > 0))
    return out


def coach_tag(g, market=None) -> str:
    """'NEW COACH Air Force 1/8 (7 left)' style tag for the report ('' if no new-coach team)."""
    parts = []
    for x in coach_locks(g, market=market):
        n = x["lock_n"]
        parts.append(f"NEW COACH {x['team']} {x['games']}/{n} "
                     + (f"({x['remaining']} left)" if x["locked"] else "(unlocked)"))
    return "; ".join(parts)


def tag_new_coaches(games, today: str, data_dir: str = "data") -> dict:
    """Attach coach_home / coach_away to each game. Returns the new-coach map."""
    try:
        from czarp.coach import detect_new_coaches, games_played
        from czarp.season import season_for_date
        nc = detect_new_coaches(season_for_date(date.fromisoformat(today)), data_dir)
        try:
            played = games_played(pd.read_csv(os.path.join(data_dir, "ratings.csv")))
        except Exception:
            played = {}
    except Exception as e:
        log(f"  new-coach detection failed ({type(e).__name__}: {e}) — no NEW COACH lock this run")
        nc, played = {}, {}
    for t in COACH_FORCE_ON:
        nc.setdefault(t, {"coach": None, "prev": None, "source": "--coach-lock"})
    for t in COACH_FORCE_OFF:
        nc.pop(t, None)
    for g in games:
        for side in ("home", "away"):
            t = g.get(f"kp_{side}")
            if t and t in nc:
                g[f"coach_{side}"] = {"team": t, "games": int(played.get(t, 0) or 0), **nc[t]}
    return nc


def candidates(games, now) -> list[dict]:
    """Every side + total candidate on the slate with edge% vs open and vs current, whether it
    passes the rules, and why not. flag_plays() bets the passing ones; the report lists the top 10."""
    out = []
    utc_now = now.astimezone(timezone.utc)
    for g in games:
        if g.get("skip"):
            continue
        started = g["state"] != "pre" or g["tip_utc"] <= utc_now
        T, To = g.get("current_total"), g.get("open_total")
        mv = line_move(g)
        flagged = mv is not None and mv >= LINE_MOVE_PTS
        # sides
        L, Lo = g.get("current_home_spread"), g.get("open_home_spread")
        mdl, thr, p = RULES["spread"]
        hm = g.get(f"{mdl}_home_margin")
        if hm is not None and (L is not None or Lo is not None) and (T or To):
            ref = L if L is not None else Lo           # side is picked vs the current line when we have it
            gap = hm + ref                              # model home margin − market home margin
            home = gap > 0
            edge = abs(hm + L) / T if L is not None and T else None
            eo = abs(hm + Lo) / (To or T) if Lo is not None else None
            same = Lo is None or L is None or ((hm + Lo > 0) == (hm + L > 0))
            sgn = 1 if home else -1
            out.append(dict(g=g, market="spread", side=g["kp_home"] if home else g["kp_away"], home=home,
                            line=(sgn * L) if L is not None else None, open_line=(sgn * Lo) if Lo is not None else None,
                            model_value=sgn * hm, market_value=-(sgn * L) if L is not None else None,
                            edge_pct=edge, edge_pct_vs_open=eo, same_side=same, thr=thr, p_est=p,
                            rule=f"v2 edge%>={thr:.0%}", started=started, line_move=mv, flagged=flagged,
                            price=g.get("current_home_spread_price" if home else "current_away_spread_price") or -110))
        # totals
        mdl, thr, p = RULES["total"]
        mt = g.get(f"{mdl}_total")
        if mt is not None and (T or To):
            ref = T or To
            over = mt > ref
            edge = abs(mt - T) / T if T else None
            eo = abs(mt - To) / To if To else None
            same = not (T and To) or ((mt > To) == (mt > T))
            out.append(dict(g=g, market="total", side="Over" if over else "Under", home=None, line=T, open_line=To,
                            model_value=mt, market_value=T, edge_pct=edge, edge_pct_vs_open=eo, same_side=same,
                            thr=thr, p_est=p, rule=f"v1 edge%>={thr:.0%}", started=started, line_move=mv,
                            flagged=flagged, price=g.get("current_over_price" if over else "current_under_price") or -110))
    for c in out:
        why = []
        if c["started"]:
            why.append("game started")
        if c["edge_pct"] is None:
            why.append("no current line")
        if c["edge_pct_vs_open"] is None:
            why.append("no opening line")
        eo, ec, thr = c["edge_pct_vs_open"], c["edge_pct"], c["thr"]
        n_base = len(why)
        if c["market"] == "total":
            # totals: edge vs OPEN only (vs-current check hurt in the backtest); the move-against skip below
            if eo is not None and eo < thr:
                why.append(f"below {thr:.0%} threshold ({eo:.1%})")
        elif eo is not None and ec is not None and eo >= thr and (ec < thr or not c["same_side"]):
            why.append("edge gone vs current" + ("" if c["same_side"] else " (flipped side)"))
        elif (eo is not None and eo < thr) or (ec is not None and ec < thr):
            low = min(x for x in (eo, ec) if x is not None)
            why.append(f"below {thr:.0%} threshold ({low:.1%})")
        # the line-move flag is about the side's spread; it applies to sides only
        if c["market"] == "spread" and c["flagged"] and LINE_MOVE_EXCLUDE:
            why.append(f"line-move flag: spread moved {c['line_move']:g} pts (likely news/injury)")
        ma = move_against(c)
        c["move_against"] = ma
        if c["market"] == "total" and ma is not None and ma >= TOTAL_MOVE_SKIP_PTS:
            why.append(f"total moved {ma:g} pts against our side since open (market disagrees)")
        if c["market"] == "spread" and ma is not None and ma >= SIDE_MOVE_AGAINST_PTS and SIDE_MOVE_SKIP:
            why.append(f"spread moved {ma:g} pts against our side since open (market disagrees)")
        locks = coach_locks(c["g"], market=c["market"])
        c["coach_tag"] = coach_tag(c["g"], c["market"])
        for x in locks:
            if x["locked"]:
                why.append(f"NEW COACH lock: {x['team']} has played {x['games']} of {x['lock_n']} games "
                           f"({x['remaining']} to unlock)")
        c["reasons"] = why
        c["rule_ok"] = not why
        # SHADOW 'track' tier: sides whose ONLY failing rule is the vs-current bar, with >= TRACK_SIDE_CUR_THR left
        cur_reasons = [r for r in why[n_base:] if r.startswith(("edge gone vs current", "below "))]
        others = [r for r in why if r not in cur_reasons
                  and not (TRACK_IGNORE_COACH_LOCK and r.startswith("NEW COACH lock"))]
        c["track_ok"] = bool(
            c["market"] == "spread" and not c["rule_ok"] and not others and c["same_side"]
            and eo is not None and ec is not None and eo >= thr and ec >= TRACK_SIDE_CUR_THR)
        c["tier"] = "official" if c["rule_ok"] else ("track" if c["track_ok"] else None)
    return out


TRACK_RULE = f"v2 edge%>=4% open, >={TRACK_SIDE_CUR_THR:.0%} current (track)"


def pick_id_for(event_id, market, tier="official") -> str:
    return f"{event_id}-{market}" + ("-track" if tier == "track" else "")


def flag_plays(games, bankroll, board, now, include_track: bool = True, source: str = "live") -> list[dict]:
    """Official plays (quarter-Kelly paper stake) + SHADOW 'track' sides (flat 1u, stake 0 on the bankroll)."""
    ts = now.isoformat(timespec="seconds")
    plays = []
    for c in candidates(games, now):
        tier = c.get("tier")
        if tier is None or (tier == "track" and not include_track):
            continue
        plays.append(dict(market=c["market"], rule=TRACK_RULE if tier == "track" else c["rule"], side=c["side"],
                          line=c["line"], price=c["price"],
                          open_line=c["open_line"], model_value=c["model_value"], market_value=c["market_value"],
                          edge_pct=c["edge_pct"], edge_pct_vs_open=c["edge_pct_vs_open"], p_est=c["p_est"],
                          g=c["g"], ts=ts, tier=tier))
    rows = []
    for pl in sorted(plays, key=lambda x: -x["edge_pct"]):
        g = pl.pop("g")
        if pl["tier"] == "track":
            stake = 0.0                # shadow tier: graded flat 1u, never touches the paper bankroll
        else:
            stake = kelly_stake(pl["p_est"], pl["price"], bankroll)
            if stake < MIN_STAKE:          # price too juiced for the assumed hit rate -> no +EV bet
                continue
        rows.append({
            "pick_id": pick_id_for(g["event_id"], pl["market"], pl["tier"]), "created_ct": pl.pop("ts"),
            "game_date": g["game_date"],
            "event_id": g["event_id"], "tip_ct": g["tip_ct"], "away": g["kp_away"], "home": g["kp_home"],
            "neutral": g["neutral"], **pl, "kp_margin_home": g.get("kp_margin_home"), "stake": stake,
            "bankroll_at_pick": round(bankroll, 2), "price_source": g.get("price_source", "espn:DraftKings"),
            "kalshi_ref": kalshi_ref(board, g, pl["market"], pl["side"], pl["line"]),
            "model_version": g.get("model_version", "czarp-v2") if pl["market"] == "spread" else V1_VERSION,
            "status": "open", "source": source,
            "total_at_post": g.get("current_total"),
        })
    return rows


# ── ledger + grading ──────────────────────────────────────────────────────────
class LedgerImmutableError(RuntimeError):
    """A write would change or remove an already-logged pick."""


def skip_codes(c, kelly_ok: bool = True) -> list[str]:
    """Machine-readable 'why not an official play' codes from a candidates() row."""
    codes = []
    if c.get("started"):
        codes.append("game_started")
    if c.get("edge_pct") is None:
        codes.append("no_current_line")
    if c.get("edge_pct_vs_open") is None:
        codes.append("no_opening_line")
    eo, ec, thr = c.get("edge_pct_vs_open"), c.get("edge_pct"), c.get("thr")
    if c.get("market") == "total":
        if eo is not None and thr is not None and eo < thr:
            codes.append("below_open_threshold")
    else:
        if eo is not None and thr is not None and eo < thr:
            codes.append("below_open_threshold")
        if eo is not None and ec is not None and thr is not None and eo >= thr and (ec < thr or not c.get("same_side", True)):
            codes.append("below_current_threshold")
        elif ec is not None and thr is not None and ec < thr and "below_open_threshold" not in codes:
            codes.append("below_current_threshold")
    ma = c.get("move_against")
    if c.get("market") == "total" and ma is not None and ma >= TOTAL_MOVE_SKIP_PTS:
        codes.append("line_moved_against")
    if c.get("market") == "spread" and ma is not None and ma >= SIDE_MOVE_AGAINST_PTS and SIDE_MOVE_SKIP:
        codes.append("line_moved_against")
    if c.get("market") == "spread" and c.get("flagged") and LINE_MOVE_EXCLUDE:
        codes.append("line_moved_against")
    if any(x.get("locked") for x in coach_locks(c["g"], market=c.get("market"))):
        codes.append("new_coach_lock")
    if not kelly_ok:
        codes.append("kelly_too_small")
    return codes


def attach_skip_reasons(games, now, bankroll: float) -> None:
    """Stamp skip_reasons_{spread,total} (codes) and skip_reason_{spread,total} (human text) on each game."""
    for g in games:
        g.setdefault("skip_reasons_spread", [])
        g.setdefault("skip_reasons_total", [])
        g.setdefault("skip_reason_spread", None)
        g.setdefault("skip_reason_total", None)
    for c in candidates(games, now):
        kelly_ok = True
        if c.get("rule_ok"):
            stake = kelly_stake(c["p_est"], c["price"], bankroll)
            kelly_ok = stake >= MIN_STAKE
        codes = skip_codes(c, kelly_ok=kelly_ok)
        human = list(c.get("reasons") or [])
        if not kelly_ok and "price too juiced" not in " ".join(human):
            human.append("price too juiced for the assumed hit rate (Kelly stake < $1)")
        key = "spread" if c["market"] == "spread" else "total"
        c["g"][f"skip_reasons_{key}"] = codes
        c["g"][f"skip_reason_{key}"] = "; ".join(human) if human else None


_NUM_FIELDS = {"line", "price", "open_line", "model_value", "edge_pct", "edge_pct_vs_open", "stake", "bankroll_at_pick", "total_at_post"}


def _canon(field, v) -> str:
    if v is None or (isinstance(v, float) and math.isnan(v)) or (not isinstance(v, str) and pd.isna(v)):
        return ""
    if field in _NUM_FIELDS:
        try:
            return repr(round(float(v), 6))
        except (TypeError, ValueError):
            return str(v)
    return str(v)


def pick_fingerprint(r) -> str:
    return "|".join(f"{f}={_canon(f, r.get(f) if hasattr(r, 'get') else r[f])}" for f in PICK_FIELDS)


def chain_hash(prev: str, r) -> str:
    import hashlib
    return hashlib.sha256(((prev or "") + "\n" + pick_fingerprint(r)).encode()).hexdigest()


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    for c in LEDGER_COLS:
        if c not in df:
            df[c] = None
    df = df[LEDGER_COLS].copy()
    for c in ("tier", "source", "logged_ct", "pick_hash"):
        df[c] = df[c].astype(object)
    df.loc[df.tier.isna(), "tier"] = "official"          # rows logged before tiers existed are official plays
    df.loc[df.source.isna(), "source"] = "live"
    # rows from before logged_ct existed were written by the same live run that set created_ct
    m = df.logged_ct.isna() & (df.source == "live") & df.created_ct.notna()
    df.loc[m, "logged_ct"] = df.loc[m, "created_ct"]
    # flat 1u result for rows graded before pnl_units existed
    df["pnl_units"] = pd.to_numeric(df["pnl_units"], errors="coerce").astype(object)
    for i in df.index[(df.status == "graded") & df.pnl_units.isna()]:
        r = df.loc[i]
        df.at[i, "pnl_units"] = round((american_to_decimal(r.price) - 1) if r.result == "win"
                                      else (-1.0 if r.result == "loss" else 0.0), 4)
    return df


def read_ledger() -> pd.DataFrame:
    if os.path.exists(LEDGER):
        return _normalize(pd.read_csv(LEDGER, dtype={"event_id": str, "pick_id": str}))
    return pd.DataFrame(columns=LEDGER_COLS)


def seal(df: pd.DataFrame) -> pd.DataFrame:
    """Fill pick_hash for rows that lack one, chaining from the previous row (append-only order)."""
    df["pick_hash"] = df["pick_hash"].astype(object)
    prev = ""
    for i in df.index:
        h = df.at[i, "pick_hash"]
        if not isinstance(h, str) or not h:
            df.at[i, "pick_hash"] = h = chain_hash(prev, df.loc[i])
        prev = h
    return df


def verify_ledger(df: pd.DataFrame) -> list[str]:
    """Problems with the hash chain (empty list = every logged pick is unchanged since it was sealed)."""
    bad, prev = [], ""
    for i in df.index:
        h = df.at[i, "pick_hash"]
        if not isinstance(h, str) or not h:
            bad.append(f"{df.at[i, 'pick_id']}: not sealed")
        elif chain_hash(prev, df.loc[i]) != h:
            bad.append(f"{df.at[i, 'pick_id']}: pick fields changed after logging")
        prev = h if isinstance(h, str) else ""
    return bad


def append_picks(ledger: pd.DataFrame, rows: list[dict], now: datetime) -> pd.DataFrame:
    """Append new picks (never rewrites existing ones). Live picks must be logged before tip-off."""
    if not rows:
        return ledger
    have = set(ledger.pick_id.astype(str))
    have_gm = {(str(e), m) for e, m in zip(ledger.event_id, ledger.market)}
    ts = now.isoformat(timespec="seconds")
    naive = pd.Timestamp(now.astimezone(CENTRAL).replace(tzinfo=None))
    keep = []
    for r in rows:
        if r["pick_id"] in have or (str(r["event_id"]), r["market"]) in have_gm:
            continue                                   # one pick per game+market (any tier); first price kept
        if r.get("source", "live") == "live" and pd.Timestamp(r["tip_ct"]) <= naive:
            raise LedgerImmutableError(f"{r['pick_id']}: live pick logged at/after tip-off ({r['tip_ct']} CT)")
        keep.append(dict(r, logged_ct=ts, tier=r.get("tier", "official"), source=r.get("source", "live")))
        have_gm.add((str(r["event_id"]), r["market"]))
    if not keep:
        return ledger
    new = pd.DataFrame(keep)
    for c in LEDGER_COLS:
        if c not in new:
            new[c] = None
    out = pd.concat([ledger, new[LEDGER_COLS]], ignore_index=True) if len(ledger) else new[LEDGER_COLS].copy()
    return seal(_normalize(out))


def check_immutable(old: pd.DataFrame, new: pd.DataFrame) -> None:
    """Raise if any pick already on disk would be removed or have a frozen field changed."""
    if old is None or old.empty:
        return
    newi = {str(p): r for p, r in zip(new.pick_id, new.to_dict("records"))}
    for r in old.to_dict("records"):
        n = newi.get(str(r["pick_id"]))
        if n is None:
            raise LedgerImmutableError(f"{r['pick_id']}: logged pick would be removed")
        if pick_fingerprint(r) != pick_fingerprint(n):
            raise LedgerImmutableError(f"{r['pick_id']}: frozen pick fields would change")
        if isinstance(r.get("pick_hash"), str) and r["pick_hash"] and n.get("pick_hash") != r["pick_hash"]:
            raise LedgerImmutableError(f"{r['pick_id']}: pick_hash would change")
        if r.get("status") == "graded" and (n.get("status") != "graded" or _canon("x", n.get("result")) != _canon("x", r.get("result"))):
            raise LedgerImmutableError(f"{r['pick_id']}: graded result would change")


def write_ledger(df: pd.DataFrame) -> None:
    os.makedirs(LEDGER_DIR, exist_ok=True)
    df = seal(_normalize(df))
    check_immutable(read_ledger() if os.path.exists(LEDGER) else None, df)
    tmp = LEDGER + ".tmp"
    df[LEDGER_COLS].to_csv(tmp, index=False)
    os.replace(tmp, LEDGER)


def grade(ledger: pd.DataFrame, now: datetime) -> list[int]:
    """Grade open plays whose games are final; record closing line + CLV."""
    for c in ("status", "graded_ct", "away_score", "home_score", "close_line", "close_price", "clv_pts", "result", "pnl",
              "pnl_units"):
        if c not in ledger:
            ledger[c] = None
        ledger[c] = ledger[c].astype(object)
    open_ = ledger[(ledger.status == "open") & (pd.to_datetime(ledger.tip_ct) < pd.Timestamp(now.replace(tzinfo=None)))]
    graded = []
    for d in sorted(open_.game_date.unique()):
        try:
            sb = {g["event_id"]: g for g in espn_scoreboard(date.fromisoformat(d))}
        except Exception as e:
            log(f"  grading {d}: ESPN scoreboard failed ({type(e).__name__}) — will retry next run")
            continue
        for i in open_[open_.game_date == d].index:
            r = ledger.loc[i]
            g = sb.get(str(r.event_id))
            if g is None:
                continue
            if g["state"] == "post" and not g["completed"]:      # postponed / canceled
                ledger.loc[i, ["status", "result", "pnl", "pnl_units", "graded_ct"]] = [
                    "void", "void", 0.0, 0.0, now.isoformat(timespec="seconds")]
                graded.append(i); continue
            if not g["completed"] or g["home_score"] is None:
                continue
            ln = espn_dk_lines(str(r.event_id))
            hs, as_ = g["home_score"], g["away_score"]
            line = float(r.line)
            if r.market == "spread":
                home = r.side == r.home
                diff = (hs - as_) if home else (as_ - hs)
                res = diff + line
                cl = ln.get("close_home_spread")
                close = (cl if home else -cl) if cl is not None else None
                cpx = ln.get("close_home_spread_price" if home else "close_away_spread_price")
                clv = (line - close) if close is not None else None
            else:
                over = r.side == "Over"
                res = (hs + as_ - line) if over else (line - hs - as_)
                close = ln.get("close_total")
                cpx = ln.get("close_over_price" if over else "close_under_price")
                clv = ((close - line) if over else (line - close)) if close is not None else None
            stake = float(r.stake or 0)
            win_u = american_to_decimal(r.price) - 1
            if res > 0:
                result, pnl, u = "win", stake * win_u, win_u
            elif res < 0:
                result, pnl, u = "loss", -stake, -1.0
            else:
                result, pnl, u = "push", 0.0, 0.0
            ledger.loc[i, ["status", "graded_ct", "away_score", "home_score", "close_line", "close_price",
                           "clv_pts", "result", "pnl", "pnl_units"]] = [
                "graded", now.isoformat(timespec="seconds"), as_, hs, close, cpx, clv, result, round(pnl, 2), round(u, 4)]
            graded.append(i)
    return graded


def official(ledger: pd.DataFrame) -> pd.DataFrame:
    """Official-tier rows (rows logged before tiers existed count as official)."""
    if "tier" not in ledger:
        return ledger
    return ledger[ledger.tier.isna() | (ledger.tier == "official")]


def bankroll(ledger: pd.DataFrame) -> float:
    """Paper bankroll: official plays only (the track tier is flat 1u and never staked)."""
    o = official(ledger)
    return BANKROLL0 + pd.to_numeric(o.loc[o.status == "graded", "pnl"], errors="coerce").fillna(0).sum()


# ── top edges (every candidate, regardless of threshold) ─────────────────────
def top_edges(cands, played_ids, n=TOP_N_EDGES) -> list[dict]:
    """The n largest edge% candidates (sides + totals), ranked by edge vs current (vs open if no current)."""
    def key(c):
        return c["edge_pct"] if c["edge_pct"] is not None else (c["edge_pct_vs_open"] or 0.0)
    rows = []
    for c in sorted(cands, key=key, reverse=True)[:n]:
        g = c["g"]
        pid = f"{g['event_id']}-{c['market']}"
        played = pid in played_ids
        reasons = [x for x in c["reasons"] if not (played and x == "game started")]
        if c["rule_ok"] and pid not in played_ids:
            reasons.append("price too juiced for the assumed hit rate (Kelly stake < $1)")
        if c["market"] == "spread" and c["flagged"] and not LINE_MOVE_EXCLUDE:
            reasons.append(f"⚠ spread moved {c['line_move']:g} pts since open (likely news/injury; flag only)")
        ma = c.get("move_against")
        if c["market"] == "spread" and ma is not None and ma >= SIDE_MOVE_AGAINST_PTS and not SIDE_MOVE_SKIP:
            reasons.append(f"⚠ spread moved {ma:g} pts AGAINST our side (flag only)")
        rows.append(dict(c, pick_id=pid, qualifies=pid in played_ids, reasons=reasons, rank_edge=key(c)))
    return rows


def _fmt_line(x, market):
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return "—"
    return f"{x:+g}" if market == "spread" else f"{x:g}"


def top_edges_md(rows) -> list[str]:
    L = [f"## Top {TOP_N_EDGES} edges (all games, sides + totals, regardless of threshold)",
         "_Edge% = |CZarp line − Vegas line| ÷ Vegas total. Spread lines are from the picked side; "
         f"thresholds {RULES['spread'][1]:.0%} sides / {RULES['total'][1]:.0%} totals; sides need it vs open AND current, totals vs open only. "
         f"Line move = DK change open→current (spread for sides, total for totals); a side whose spread moved "
         f"≥{LINE_MOVE_PTS:g} pts is flagged ⚠ as likely news/injury; 'against' = moved away from our side "
         f"(a total ≥{TOTAL_MOVE_SKIP_PTS:g} against is skipped). NEW COACH = head coach changed since last season; "
         f"those games are locked out of value plays until the team has played {coach_lock_n('total')} games (totals) / {coach_lock_n('spread')} (sides)._", ""]
    if not rows:
        return L + ["No priced D-I games to rank.", ""]
    L.append("| # | Tip (CT) | Game | Market | Pick | Vegas open | Vegas current | CZarp line | Edge% vs open | "
             "Edge% vs current | Line move | Tags | Qualifies | Reason if not / notes |")
    L.append("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for i, r in enumerate(rows, 1):
        g, m = r["g"], r["market"]
        czarp = -r["model_value"] if m == "spread" else r["model_value"]
        eo = "—" if r["edge_pct_vs_open"] is None else f"{r['edge_pct_vs_open']:.1%}"
        ec = "—" if r["edge_pct"] is None else f"{r['edge_pct']:.1%}"
        ma = r.get("move_against")
        if ma is None:
            mv = "—"
        else:
            mv = "0" if ma == 0 else f"{abs(ma):g} {'against' if ma > 0 else 'with us'}"
            warn = (m == "spread" and (r["flagged"] or ma >= SIDE_MOVE_AGAINST_PTS)) or \
                (m == "total" and ma >= TOTAL_MOVE_SKIP_PTS)
            mv += " ⚠" if warn else ""
        tags = r.get("coach_tag") or ""
        L.append(f"| {i} | {str(g['tip_ct'])[11:]} | {g['kp_away']} @ {g['kp_home']}{' (N)' if g['neutral'] else ''} | "
                 f"{'Side' if m == 'spread' else 'Total'} | {r['side']} | {_fmt_line(r['open_line'], m)} | "
                 f"{_fmt_line(r['line'], m)} | {_fmt_line(round(czarp, 1), m)} | {eo} | {ec} | {mv} | {tags} | "
                 f"{'yes' if r['qualifies'] else 'no'} | {'; '.join(r['reasons'])} |")
    L.append("")
    return L


# ── summary ──────────────────────────────────────────────────────────────────
def _rec(df: pd.DataFrame) -> str:
    g = df[df.status == "graded"]
    if g.empty:
        return "0-0"
    w, l, p = (g.result == "win").sum(), (g.result == "loss").sum(), (g.result == "push").sum()
    st = pd.to_numeric(g.stake).sum()
    pnl = pd.to_numeric(g.pnl).sum()
    u = g.apply(lambda r: (american_to_decimal(r.price) - 1) if r.result == "win" else (-1 if r.result == "loss" else 0), axis=1)
    clv = pd.to_numeric(g.clv_pts, errors="coerce").mean()
    clv_s = f"{clv:+.2f} pts" if not math.isnan(clv) else "n/a"
    return (f"{w}-{l}" + (f"-{p}" if p else "") + f" ({w / max(1, w + l):.1%}) · P&L ${pnl:+,.2f} on ${st:,.2f} staked "
            f"(ROI {pnl / st if st else 0:+.1%}; flat 1u-per-play {u.sum():+.2f}u) · avg CLV {clv_s}")


def flat_units(r) -> float:
    """Flat 1u result for one graded row at the price taken (pnl_units if present)."""
    u = pd.to_numeric(r.get("pnl_units"), errors="coerce")
    if pd.notna(u):
        return float(u)
    return (american_to_decimal(r.price) - 1) if r.result == "win" else (-1.0 if r.result == "loss" else 0.0)


def tier_record(df: pd.DataFrame) -> dict:
    """W-L-P, flat-1u units, ROI per 1u risked, avg CLV, pending — the public per-tier record."""
    g = df[df.status == "graded"]
    w, l, p = int((g.result == "win").sum()), int((g.result == "loss").sum()), int((g.result == "push").sum())
    units = float(sum(flat_units(r) for _, r in g.iterrows())) if len(g) else 0.0
    n = w + l + p
    clv = pd.to_numeric(g.clv_pts, errors="coerce")
    return {"w": w, "l": l, "p": p, "n": n, "pct": (w / (w + l)) if (w + l) else None, "units": round(units, 2),
            "roi": (units / n) if n else None, "clv": None if clv.dropna().empty else round(float(clv.mean()), 2),
            "beat_close": int((clv > 0).sum()), "clv_n": int(clv.notna().sum()),
            "pending": int((df.status == "open").sum())}


def tier_records(ledger: pd.DataFrame) -> dict:
    t = ledger.tier.fillna("official") if "tier" in ledger else pd.Series("official", index=ledger.index)
    out = {}
    for tier in TIERS:
        d = ledger[t == tier]
        out[tier] = {"all": tier_record(d), "spread": tier_record(d[d.market == "spread"]),
                     "total": tier_record(d[d.market == "total"])}
    return out


def tier_record_md(ledger: pd.DataFrame) -> list[str]:
    recs = tier_records(ledger)
    L = ["## Running record by tier",
         "_OFFICIAL = the plays (sides ≥4% vs open AND current; totals ≥3% vs open), quarter-Kelly paper stakes. "
         f"TRACK = shadow sides ≥4% vs open with ≥{TRACK_SIDE_CUR_THR:.0%} still left vs current that are not official; "
         "paper only, flat 1u, never staked. Units are flat 1u per pick at the price taken; ROI = units ÷ picks graded; "
         "CLV = points beaten vs the DK close. Every pick is logged with a timestamp before tip and sealed with a hash chain._",
         "",
         "| Tier | Market | W-L-P | Win% | Units (1u flat) | ROI | Avg CLV | Beat close | Pending |",
         "|---|---|---|---|---|---|---|---|---|"]
    for tier in TIERS:
        for m in ("all", "spread", "total"):
            if tier == "track" and m == "total":
                continue
            r = recs[tier][m]
            label = {"all": "All", "spread": "Sides", "total": "Totals"}[m]
            pct = "—" if r["pct"] is None else f"{r['pct']:.1%}"
            roi = "—" if r["roi"] is None else f"{r['roi']:+.1%}"
            clv = "—" if r["clv"] is None else f"{r['clv']:+.2f}"
            L.append(f"| {tier.upper()} | {label} | {r['w']}-{r['l']}-{r['p']} | {pct} | {r['units']:+.2f}u | {roi} | "
                     f"{clv} | {r['beat_close']}/{r['clv_n']} | {r['pending']} |")
    L.append("")
    return L


def write_summary(today: str, now: datetime, games, new_rows, ledger, graded_idx, notes, top=None,
                  path: str | None = None) -> str:
    os.makedirs(OUT_DIR, exist_ok=True)
    path = path or os.path.join(OUT_DIR, f"{today}.md")
    L = [f"# CZarp paper trading — {today}",
         f"_Generated {now.strftime('%Y-%m-%d %I:%M %p')} CT. Paper bets only; notional bankroll ${BANKROLL0:,.0f}._", ""]
    d1 = [g for g in games if not g.get("skip")]
    priced = [g for g in d1 if g.get("current_home_spread") is not None or g.get("current_total") is not None]
    L.append(f"**Slate:** {len(games)} games on ESPN, {len(d1)} D-I vs D-I projected, {len(priced)} with DK lines. "
             f"Bankroll now **${bankroll(ledger):,.2f}**.")
    L.append("")
    td = ledger[(ledger.game_date == today) & (ledger.status != "void")]
    today_rows = official(td)
    L.append(f"## Today's plays ({len(today_rows)})")
    if today_rows.empty:
        L.append("No plays met the rules today.")
    else:
        L.append("| Tip (CT) | Game | Bet | Price | Open line | Model (bet side margin / total) | Edge% (vs open) | Stake | Kalshi ref |")
        L.append("|---|---|---|---|---|---|---|---|---|")
        for _, r in today_rows.sort_values("tip_ct").iterrows():
            bet = f"{r.side} {float(r.line):+g}" if r.market == "spread" else f"{r.side} {float(r.line):g}"
            op = "" if pd.isna(r.open_line) else (f"{float(r.open_line):+g}" if r.market == "spread" else f"{float(r.open_line):g}")
            eo = "" if pd.isna(r.edge_pct_vs_open) else f" ({float(r.edge_pct_vs_open):.1%})"
            mv = f"{float(r.model_value):+.1f}" if r.market == "spread" else f"{float(r.model_value):.1f}"
            L.append(f"| {str(r.tip_ct)[11:]} | {r.away} @ {r.home}{' (N)' if str(r.neutral) == 'True' else ''} | {bet} | "
                     f"{int(float(r.price)):+d} | {op} | {mv} | {float(r.edge_pct):.1%}{eo} | ${float(r.stake):.2f} | {r.kalshi_ref or ''} |")
    L.append("")
    track_rows = td[td.tier == "track"] if "tier" in td else td.iloc[0:0]
    L.append(f"## Tracked (not played) ({len(track_rows)})")
    L.append(f"_TRACK tier: sides ≥{RULES['spread'][1]:.0%} vs open with ≥{TRACK_SIDE_CUR_THR:.0%} still left vs current "
             "that missed the official bar. Paper only, flat 1u, NOT bet and not staked on the bankroll; graded every "
             "morning to compare against the official rule._")
    if track_rows.empty:
        L.append("No tracked sides today.")
    else:
        L.append("| Tip (CT) | Game | Pick | Price | Open line | CZarp (pick-side margin) | Edge% vs current (vs open) | Logged (CT) |")
        L.append("|---|---|---|---|---|---|---|---|")
        for _, r in track_rows.sort_values("tip_ct").iterrows():
            op = "" if pd.isna(r.open_line) else f"{float(r.open_line):+g}"
            L.append(f"| {str(r.tip_ct)[11:]} | {r.away} @ {r.home}{' (N)' if str(r.neutral) == 'True' else ''} | "
                     f"{r.side} {float(r.line):+g} | {int(float(r.price)):+d} | {op} | {float(r.model_value):+.1f} | "
                     f"{float(r.edge_pct):.1%} ({float(r.edge_pct_vs_open):.1%}) | {str(r.created_ct)[11:16]} |")
    L.append("")
    L.extend(top_edges_md(top or []))
    yday = (date.fromisoformat(today) - timedelta(days=1)).isoformat()
    yr = ledger[ledger.game_date == yday]
    L.append(f"## Yesterday ({yday}) results")
    if yr.empty:
        L.append("No plays yesterday.")
    else:
        L.append(f"Official: {_rec(official(yr))}")
        tr = yr[yr.tier == "track"] if "tier" in yr else yr.iloc[0:0]
        if len(tr):
            r = tier_record(tr)
            L.append(f"Track (flat 1u, not bet): {r['w']}-{r['l']}" + (f"-{r['p']}" if r["p"] else "") + f", {r['units']:+.2f}u")
        L.append("")
        L.append("| Tier | Game | Bet | Result | Score | Close | CLV | P&L |")
        L.append("|---|---|---|---|---|---|---|---|")
        for _, r in yr.iterrows():
            tier = r.get("tier") if isinstance(r.get("tier"), str) else "official"
            sc = "" if pd.isna(r.home_score) else f"{int(r.away_score)}-{int(r.home_score)}"
            pnl = (f"{float(r.pnl_units):+.2f}u (flat)" if tier == "track" and pd.notna(r.get("pnl_units"))
                   else ("" if pd.isna(r.pnl) else f"${float(r.pnl):+.2f}"))
            L.append(f"| {tier.upper()} | {r.away} @ {r.home} | {r.side} {r.line} | {r.result if pd.notna(r.result) else 'pending'} | {sc} | "
                     f"{'' if pd.isna(r.close_line) else r.close_line} | {'' if pd.isna(r.clv_pts) else f'{float(r.clv_pts):+g}'} | "
                     f"{pnl} |")
    L.append("")
    off = official(ledger)
    L.append("## Running record (official plays, $ on the paper bankroll)")
    L.append(f"- All: {_rec(off)}")
    for m in ("spread", "total"):
        L.append(f"- {m.title()}s: {_rec(off[off.market == m])}")
    pend = (off.status == "open").sum()
    L.append(f"- Open (ungraded) plays: {pend}")
    L.append("")
    L.extend(tier_record_md(ledger))
    bad = verify_ledger(ledger) if "pick_hash" in ledger else []
    L.append(f"_Ledger integrity: {len(ledger)} picks, hash chain " + ("OK" if not bad else f"BROKEN ({'; '.join(bad[:3])})")
             + (f", last hash `{str(ledger.pick_hash.iloc[-1])[:12]}`" if len(ledger) and isinstance(ledger.pick_hash.iloc[-1], str) else "")
             + "._")
    L.append("")
    L.append("## Notes")
    L.append(f"- Rules: sides = v2 model-only vs DK line, edge% = |model − line| / DK total ≥ {RULES['spread'][1]:.0%}; "
             f"totals = original v1 total vs DK total, edge% ≥ {RULES['total'][1]:.0%}. Sides must clear it vs BOTH "
             "the opening and the current line (same side); totals vs the opening line only, plus the line-move skip below. Edges only showed up vs *opening* lines in the backtest "
             "(they lost vs the close), so run this as early as possible.")
    L.append(f"- Line-move flag: a side whose DK spread moved ≥{LINE_MOVE_PTS:g} pts from open to current is marked "
             "'likely news/injury'. " + ("Flagged sides are EXCLUDED from plays." if LINE_MOVE_EXCLUDE else
             "Flag only — the backtest showed flagged side picks did better, not worse (see "
             "outputs/walkforward/line_move_study.txt), so they are not excluded."))
    L.append(f"- Total line-move rule: a total whose DK line moved ≥{TOTAL_MOVE_SKIP_PTS:g} pts from open to current "
             "AWAY from our side is skipped (backtest 24-42). Sides that moved "
             f"≥{SIDE_MOVE_AGAINST_PTS:g} against us are " + ("skipped." if SIDE_MOVE_SKIP else "flagged only (too few cases to test)."))
    nc = sorted({x["team"]: x for g in games for x in coach_locks(g)}.values(), key=lambda x: x["team"])
    locked = [x for x in nc if x["locked"]]
    nt, ns = coach_lock_n("total"), coach_lock_n("spread")
    if max(nt, ns) > 0:
        L.append(f"- NEW COACH lock: games involving a team with a new head coach are not value plays until it has played "
                 f"{nt} games (totals) / {ns} games (sides). On today's slate: "
                 + (", ".join(f"{x['team']} ({x['games']} played, {max(0, nt - x['games'])} left totals / {x['remaining']} sides)" for x in locked)
                    if locked else "none locked") + ".")
    else:
        L.append("- NEW COACH lock: off (tags shown only).")
    L.append("- Stakes: quarter Kelly with a conservative hit-rate estimate "
             f"({RULES['spread'][2]:.1%} sides / {RULES['total'][2]:.1%} totals), capped at 2.5% of bankroll.")
    for n in notes:
        L.append(f"- {n}")
    with open(path, "w") as fh:
        fh.write("\n".join(L) + "\n")
    return path


# ── main ─────────────────────────────────────────────────────────────────────
def main(argv=None) -> int:
    global TRACK_IGNORE_COACH_LOCK, PACE_FIX, PACE_FIX_MKT_W, LINE_MOVE_EXCLUDE, COACH_LOCK_GAMES, COACH_LOCK_TOTALS, COACH_LOCK_SIDES, SIDE_MOVE_SKIP, COACH_FORCE_ON, COACH_FORCE_OFF
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--date")
    ap.add_argument("--no-refresh", action="store_true")
    ap.add_argument("--no-odds-api", action="store_true")
    ap.add_argument("--no-kalshi", action="store_true")
    ap.add_argument("--dry-run", action="store_true",
                    help="don't write ledger/snapshot/Supabase; report goes to outputs/daily/<date>.dryrun.md")
    ap.add_argument("--resync", action="store_true",
                    help="re-project today's slate and refresh Supabase (projections, skip reasons, team_profiles, "
                         "finals, NULL pick locks) WITHOUT grading, logging new picks, or writing the ledger/snapshot; "
                         "report goes to outputs/daily/<date>.resync.md")
    ap.add_argument("--exclude-line-moves", action="store_true",
                    help=f"exclude sides whose spread moved >= {LINE_MOVE_PTS:g} pts open->current (default: flag only)")
    ap.add_argument("--coach-lock-games", type=int, default=None,
                    help="NEW COACH lock for BOTH markets: games a new-coach team must play before its games are value plays (0 = off)")
    ap.add_argument("--coach-lock-totals", type=int, default=None,
                    help=f"NEW COACH lock length for totals (default {COACH_LOCK_TOTALS}; 0 = off)")
    ap.add_argument("--coach-lock-sides", type=int, default=None,
                    help=f"NEW COACH lock length for sides (default {COACH_LOCK_SIDES}; 0 = off)")
    ap.add_argument("--no-coach-lock", action="store_true",
                    help="turn the NEW COACH lock OFF for every team this run (same as --coach-lock-games 0)")
    ap.add_argument("--coach-lock", action="append", default=[], metavar="TEAM",
                    help="lock TEAM (KenPom name) as a new-coach team for this run only (repeatable); "
                         "permanent changes go in config/coach_locks.csv / coach_locks.py")
    ap.add_argument("--coach-unlock", action="append", default=[], metavar="TEAM",
                    help="never lock TEAM this run (repeatable)")
    ap.add_argument("--skip-side-moves", action="store_true",
                    help=f"skip sides whose spread moved >= {SIDE_MOVE_AGAINST_PTS:g} pts against our side (default: flag only)")
    ap.add_argument("--pace-fix", action="store_true", default=None,
                    help=f"totals pace fix: early-season/neutral pace correction + blend with the OPEN total (default {'on' if PACE_FIX else 'off'})")
    ap.add_argument("--no-pace-fix", dest="pace_fix", action="store_false")
    ap.add_argument("--pace-fix-w", type=float, default=None, help=f"market weight for --pace-fix (default {PACE_FIX_MKT_W})")
    ap.add_argument("--track-ignore-coach-lock", action="store_true",
                    help="TRACK tier also logs NEW COACH-locked sides (default: same lock as official plays)")
    ap.add_argument("--config", help="model config JSON (feature list + weights); default czarp_config.json / built-in v2")
    a = ap.parse_args(argv)
    if a.resync and a.dry_run:
        ap.error("--resync and --dry-run are mutually exclusive")
    if a.config:
        os.environ["CZARP_CONFIG"] = a.config
    if a.exclude_line_moves:
        LINE_MOVE_EXCLUDE = True
    if a.coach_lock_games is not None:
        COACH_LOCK_TOTALS = COACH_LOCK_SIDES = max(0, a.coach_lock_games)
    if a.coach_lock_totals is not None:
        COACH_LOCK_TOTALS = max(0, a.coach_lock_totals)
    if a.coach_lock_sides is not None:
        COACH_LOCK_SIDES = max(0, a.coach_lock_sides)
    if a.no_coach_lock:
        COACH_LOCK_TOTALS = COACH_LOCK_SIDES = 0
    COACH_FORCE_ON = set(a.coach_lock or [])
    COACH_FORCE_OFF = set(a.coach_unlock or [])
    if a.skip_side_moves:
        SIDE_MOVE_SKIP = True
    if a.track_ignore_coach_lock:
        TRACK_IGNORE_COACH_LOCK = True
    if a.pace_fix is not None:
        PACE_FIX = a.pace_fix
    if a.pace_fix_w is not None:
        PACE_FIX_MKT_W = a.pace_fix_w
    now = now_ct()
    today = a.date or now.date().isoformat()
    notes: list[str] = []
    os.makedirs(SNAP_DIR, exist_ok=True)
    log(f"CZarp daily run — slate {today}, {now.strftime('%Y-%m-%d %H:%M')} CT")

    ledger = seal(read_ledger())        # rows from before the hash chain get sealed (in memory; written below)
    bad = verify_ledger(ledger)
    if bad:
        log(f"  ⚠ LEDGER INTEGRITY: {'; '.join(bad[:5])}")
        notes.append(f"Ledger integrity check FAILED: {'; '.join(bad[:5])}")
    # (f) grade first so the bankroll is current
    gi = []
    if a.resync:
        log("  resync: no grading, no new picks, ledger untouched")
    else:
        try:
            gi = grade(ledger, now)
            log(f"  graded {len(gi)} earlier plays")
        except Exception as e:
            log(f"  grading failed ({type(e).__name__}: {e})"); notes.append("Grading failed this run; it will retry next run.")
            gi = []

    # schedule
    try:
        games = [g for g in espn_scoreboard(date.fromisoformat(today)) if g["game_date"] == today]
    except Exception as e:
        games = []
        log(f"  ESPN scoreboard failed ({type(e).__name__}: {e})"); notes.append("ESPN scoreboard unavailable — no slate.")
    log(f"  ESPN slate: {len(games)} games ({sum(g['state'] == 'pre' for g in games)} not started)")

    new_rows = []
    top = []
    data = None
    nc = {}
    if games:
        # (a) data
        try:
            data = load_kenpom(today, refresh=not a.no_refresh)
        except Exception as e:
            data = None
            log(f"  KenPom data unavailable ({type(e).__name__}: {e})"); notes.append("KenPom data unavailable — no projections.")
        fm = fanmatch_preds(today)
        # (b) prices
        pre = [g for g in games if g["state"] == "pre"]
        for g in games:                 # all games, so the top-edges list works on a late run too
            g.update(espn_dk_lines(g["event_id"]))
        log(f"  ESPN/DK lines: {sum(g.get('current_home_spread') is not None for g in pre)} spreads, "
            f"{sum(g.get('current_total') is not None for g in pre)} totals of {len(pre)} upcoming games")
        if data is not None:
            project_all(games, data)
            nc = tag_new_coaches(games, today)
            if PACE_FIX:
                log(f"  totals PACE FIX on (w={PACE_FIX_MKT_W:g}): {apply_pace_fix(games, today)} v1 totals re-paced")
            nlock = sum(any(x["locked"] for x in coach_locks(g)) for g in games if not g.get("skip"))
            nlock_t = sum(any(x["locked"] for x in coach_locks(g, market="total")) for g in games if not g.get("skip"))
            log(f"  NEW COACH: {len(nc)} teams with a new head coach; {nlock} games on the slate locked "
                f"for sides, {nlock_t} for totals (lock = {coach_lock_n('total')} games totals / {coach_lock_n('spread')} sides)")
            for g in games:
                kp = fm.get(frozenset([g.get("kp_home") or "", g.get("kp_away") or ""]))
                if kp and kp[1] is not None:
                    g["kp_margin_home"] = kp[1] if kp[0] == g.get("kp_home") else -kp[1]
        d1pre = [g for g in pre if not g.get("skip")]
        odds_api_fallback(d1pre, today, allow=not a.no_odds_api)
        board = {}
        if not a.no_kalshi:
            try:
                import czarp.kalshi as kx
                board = kx.fetch_cbb_board()
                log(f"  Kalshi board: {len(board)} games")
            except Exception as e:
                log(f"  Kalshi fetch failed ({type(e).__name__}) — no Kalshi reference prices")
        for g in games:
            if board and g.get("kp_home") and g.get("kp_away"):
                try:
                    import czarp.kalshi as kx
                    g["kalshi"] = kx.find_game(board, g["kp_home"], g["kp_away"])
                except Exception:
                    pass
        # snapshot of every projection + price (for later analysis)
        snap = pd.DataFrame([{k: v for k, v in g.items() if k not in ("tip_utc", "kalshi")} for g in games])
        snap["snapshot_ct"] = now.isoformat(timespec="seconds")
        if not (a.dry_run or a.resync):
            snap.to_csv(os.path.join(SNAP_DIR, f"{today}_{now.strftime('%H%M')}.csv"), index=False)
        # (d) plays
        if data is not None:
            new_rows = [] if a.resync else flag_plays(games, bankroll(ledger), board, now)
            have = set(ledger.pick_id.astype(str))
            have_gm = {(str(e), m) for e, m in zip(ledger.event_id, ledger.market)}
            # one pick per game+market (any tier), first price kept
            new_rows = [r for r in new_rows if r["pick_id"] not in have and (str(r["event_id"]), r["market"]) not in have_gm]
            off_new = [r for r in new_rows if r.get("tier") != "track"]
            log(f"  new plays: {len(off_new)} ({sum(r['market'] == 'spread' for r in off_new)} sides, "
                f"{sum(r['market'] == 'total' for r in off_new)} totals); tracked (not played): "
                f"{sum(r.get('tier') == 'track' for r in new_rows)} sides")
            cands = candidates(games, now)
            played = {r["pick_id"] for r in new_rows if r.get("tier") != "track"} | \
                set(official(ledger).loc[lambda d: d.game_date == today, "pick_id"].astype(str))
            top = top_edges(cands, played)
            nflag = sum(c["flagged"] for c in cands if c["market"] == "spread")
            log(f"  line-move flag (spread moved >= {LINE_MOVE_PTS:g} pts): {nflag} sides"
                + (" — excluded" if LINE_MOVE_EXCLUDE else " — flag only"))
            nt = sum(c["market"] == "total" and (c.get("move_against") or 0) >= TOTAL_MOVE_SKIP_PTS for c in cands)
            log(f"  totals moved >= {TOTAL_MOVE_SKIP_PTS:g} pts against our side: {nt} skipped")
            for ln in top_edges_md(top):
                log("  " + ln if ln else "")
            attach_skip_reasons(games, now, bankroll(ledger))
    else:
        notes.append("No games on the slate today.")

    # (e) ledger
    if new_rows:
        ledger = append_picks(ledger, new_rows, now)
    if not (a.dry_run or a.resync):
        write_ledger(ledger)
    # optional Supabase mirror (CSV ledger stays the source of truth)
    if not a.dry_run and os.getenv("SUPABASE_URL") and os.getenv("SUPABASE_SERVICE_KEY"):
        try:
            from czarp.supabase_writer import sync
            if data is not None:
                cfg = data["config"]
            else:
                from czarp.config import load_config
                cfg = load_config()
            n = sync(games, ledger, cfg, now, log=log, data=data,
                     new_coach=nc if data is not None else None)
            log(f"  Supabase: synced {n}")
        except Exception as e:
            log(f"  Supabase sync failed ({type(e).__name__}: {e}) — CSV ledger unaffected")
            notes.append("Supabase sync failed this run (CSV ledger is unaffected).")
        # finals for yesterday + any older non-final games in the lookback window (and today, for a late run)
        try:
            from czarp.sync_finals import DEFAULT_LOOKBACK, sync_finals
            s = sync_finals(lookback=DEFAULT_LOOKBACK, now=now, log=log)
            log(f"  finals: {s['updated']} updated, {s['matched']} matched, "
                f"{s['unmatched_espn']} ESPN games not in db")
        except Exception as e:
            log(f"  finals sync failed ({type(e).__name__}: {e}) — will retry next run")
            notes.append("Finals sync failed this run; scores will retry next run.")
    # (g) summary
    path = write_summary(today, now, games, new_rows, ledger, gi, notes, top,
                         path=os.path.join(OUT_DIR, f"{today}.dryrun.md") if a.dry_run
                         else (os.path.join(OUT_DIR, f"{today}.resync.md") if a.resync else None))
    log(f"  summary → {path}; ledger → {LEDGER} ({len(ledger)} rows)")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        traceback.print_exc()
        sys.exit(1)
