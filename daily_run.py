"""
daily_run.py — fully automated daily PAPER-trading pipeline (no real bets are placed).

    python daily_run.py                    # today (America/Chicago)
    python daily_run.py --date 2026-11-02  # a specific slate
    options: --no-refresh (use cached KenPom CSVs)  --no-odds-api  --no-kalshi  --dry-run (don't write ledger)
             --exclude-line-moves (drop sides whose spread moved >= 2 pts; default flag only)

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

Rules (from the walk-forward backtest, Jan 2025 – Apr 2026, see outputs/walkforward/):
  Both rules require the edge to clear the threshold vs the OPENING line AND the current line
  (same side), exactly as backtested; no opening line -> no play.
  SIDES  — v2 model-only margin vs the DK line; edge% = |model − line| / DK total >= 4%
           (vs open: n=85, 60.0% ATS, CLV +1.1; vs close it lost — bet early).
  TOTALS — original v1 total vs the DK total; edge% = |model − total| / total >= 3%
           (vs open: n=1248, 54.2%, +3.6% ROI, CLV +1.6; vs close it lost — bet early).
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
TOP_N_EDGES = 10
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
    "result", "pnl",
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
            "event_id": str(e["id"]), "tip_utc": utc, "tip_ct": utc.astimezone(CENTRAL).strftime("%Y-%m-%d %H:%M"),
            "game_date": utc.astimezone(CENTRAL).strftime("%Y-%m-%d"),
            "home": comp["home"]["team"].get("location"), "away": comp["away"]["team"].get("location"),
            "home_full": comp["home"]["team"].get("displayName"), "away_full": comp["away"]["team"].get("displayName"),
            "home_score": _num(comp["home"].get("score")), "away_score": _num(comp["away"].get("score")),
            "neutral": bool(c.get("neutralSite")), "completed": bool(st.get("completed")),
            "state": st.get("state"),  # pre / in / post
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
    """Model tag names (display suffix stripped) + per-team tentative flags for the front end."""
    def split(tags):
        tags = list(tags or [])
        return [t[:-len(TENTATIVE_SUFFIX)] if t.endswith(TENTATIVE_SUFFIX) else t for t in tags], \
            any(t.endswith(TENTATIVE_SUFFIX) for t in tags)
    away, at = split(r.get("team1_tags"))       # team1 = away in project_all
    home, ht = split(r.get("team2_tags"))
    return {"home": home, "away": away, "home_tentative": ht, "away_tentative": at,
            "tentative": bool(r.get("tags_tentative", False)) or ht or at, "notes": r.get("matchup_notes", [])}


def project_all(games: list[dict], data: dict) -> None:
    import model as M
    import czarp.legacy_v1 as V1
    from backtest import NameMatcher
    match = NameMatcher(data["ratings"]["TeamName"].astype(str))
    try:
        tt = M._context(data, data["config"])["tt"]
        v1data = v1_frames(tt)
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
        except Exception as e:
            g["skip"] = f"v2 failed: {e}"
            continue
        if v1data is not None:
            try:
                r1 = V1.project_game(a, h, None if g["neutral"] else False, v1data)
                g["v1_home_margin"], g["v1_total"] = -float(r1["spread"]), float(r1["total"])
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
        if eo is not None and ec is not None and eo >= thr and (ec < thr or not c["same_side"]):
            why.append("edge gone vs current" + ("" if c["same_side"] else " (flipped side)"))
        elif (eo is not None and eo < thr) or (ec is not None and ec < thr):
            low = min(x for x in (eo, ec) if x is not None)
            why.append(f"below {thr:.0%} threshold ({low:.1%})")
        # the line-move flag is about the side's spread; it applies to sides only
        if c["market"] == "spread" and c["flagged"] and LINE_MOVE_EXCLUDE:
            why.append(f"line-move flag: spread moved {c['line_move']:g} pts (likely news/injury)")
        c["reasons"] = why
        c["rule_ok"] = not why
    return out


def flag_plays(games, bankroll, board, now) -> list[dict]:
    ts = now.isoformat(timespec="seconds")
    plays = []
    for c in candidates(games, now):
        if not c["rule_ok"]:
            continue
        plays.append(dict(market=c["market"], rule=c["rule"], side=c["side"], line=c["line"], price=c["price"],
                          open_line=c["open_line"], model_value=c["model_value"], market_value=c["market_value"],
                          edge_pct=c["edge_pct"], edge_pct_vs_open=c["edge_pct_vs_open"], p_est=c["p_est"],
                          g=c["g"], ts=ts))
    rows = []
    for pl in sorted(plays, key=lambda x: -x["edge_pct"]):
        g = pl.pop("g")
        stake = kelly_stake(pl["p_est"], pl["price"], bankroll)
        if stake < MIN_STAKE:          # price too juiced for the assumed hit rate -> no +EV bet
            continue
        rows.append({
            "pick_id": f"{g['event_id']}-{pl['market']}", "created_ct": pl.pop("ts"), "game_date": g["game_date"],
            "event_id": g["event_id"], "tip_ct": g["tip_ct"], "away": g["kp_away"], "home": g["kp_home"],
            "neutral": g["neutral"], **pl, "kp_margin_home": g.get("kp_margin_home"), "stake": stake,
            "bankroll_at_pick": round(bankroll, 2), "price_source": g.get("price_source", "espn:DraftKings"),
            "kalshi_ref": kalshi_ref(board, g, pl["market"], pl["side"], pl["line"]),
            "model_version": g.get("model_version", "czarp-v2") if pl["market"] == "spread" else V1_VERSION,
            "status": "open",
        })
    return rows


# ── ledger + grading ──────────────────────────────────────────────────────────
def read_ledger() -> pd.DataFrame:
    if os.path.exists(LEDGER):
        df = pd.read_csv(LEDGER, dtype={"event_id": str, "pick_id": str})
        for c in LEDGER_COLS:
            if c not in df:
                df[c] = None
        return df[LEDGER_COLS]
    return pd.DataFrame(columns=LEDGER_COLS)


def write_ledger(df: pd.DataFrame) -> None:
    os.makedirs(LEDGER_DIR, exist_ok=True)
    tmp = LEDGER + ".tmp"
    df[LEDGER_COLS].to_csv(tmp, index=False)
    os.replace(tmp, LEDGER)


def grade(ledger: pd.DataFrame, now: datetime) -> list[int]:
    """Grade open plays whose games are final; record closing line + CLV."""
    for c in ("status", "graded_ct", "away_score", "home_score", "close_line", "close_price", "clv_pts", "result", "pnl"):
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
                ledger.loc[i, ["status", "result", "pnl", "graded_ct"]] = ["void", "void", 0.0, now.isoformat(timespec="seconds")]
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
            if res > 0:
                result, pnl = "win", stake * (american_to_decimal(r.price) - 1)
            elif res < 0:
                result, pnl = "loss", -stake
            else:
                result, pnl = "push", 0.0
            ledger.loc[i, ["status", "graded_ct", "away_score", "home_score", "close_line", "close_price",
                           "clv_pts", "result", "pnl"]] = [
                "graded", now.isoformat(timespec="seconds"), as_, hs, close, cpx, clv, result, round(pnl, 2)]
            graded.append(i)
    return graded


def bankroll(ledger: pd.DataFrame) -> float:
    return BANKROLL0 + pd.to_numeric(ledger.loc[ledger.status == "graded", "pnl"], errors="coerce").fillna(0).sum()


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
        rows.append(dict(c, pick_id=pid, qualifies=pid in played_ids, reasons=reasons, rank_edge=key(c)))
    return rows


def _fmt_line(x, market):
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return "—"
    return f"{x:+g}" if market == "spread" else f"{x:g}"


def top_edges_md(rows) -> list[str]:
    L = [f"## Top {TOP_N_EDGES} edges (all games, sides + totals, regardless of threshold)",
         "_Edge% = |CZarp line − Vegas line| ÷ Vegas total. Spread lines are from the picked side; "
         f"thresholds {RULES['spread'][1]:.0%} sides / {RULES['total'][1]:.0%} totals, needed vs open AND current. "
         f"Line move = DK change open→current (spread for sides, total for totals); a side whose spread moved "
         f"≥{LINE_MOVE_PTS:g} pts is flagged ⚠ as likely news/injury._", ""]
    if not rows:
        return L + ["No priced D-I games to rank.", ""]
    L.append("| # | Tip (CT) | Game | Market | Pick | Vegas open | Vegas current | CZarp line | Edge% vs open | "
             "Edge% vs current | Line move | Qualifies | Reason if not / notes |")
    L.append("|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for i, r in enumerate(rows, 1):
        g, m = r["g"], r["market"]
        czarp = -r["model_value"] if m == "spread" else r["model_value"]
        eo = "—" if r["edge_pct_vs_open"] is None else f"{r['edge_pct_vs_open']:.1%}"
        ec = "—" if r["edge_pct"] is None else f"{r['edge_pct']:.1%}"
        if m == "spread":
            mv = "—" if r["line_move"] is None else (f"{r['line_move']:g}" + (" ⚠" if r["flagged"] else ""))
        else:
            mv = "—" if r["open_line"] is None or r["line"] is None else f"{abs(r['line'] - r['open_line']):g}"
        L.append(f"| {i} | {str(g['tip_ct'])[11:]} | {g['kp_away']} @ {g['kp_home']}{' (N)' if g['neutral'] else ''} | "
                 f"{'Side' if m == 'spread' else 'Total'} | {r['side']} | {_fmt_line(r['open_line'], m)} | "
                 f"{_fmt_line(r['line'], m)} | {_fmt_line(round(czarp, 1), m)} | {eo} | {ec} | {mv} | "
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
    today_rows = ledger[(ledger.game_date == today) & (ledger.status != "void")]
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
    L.extend(top_edges_md(top or []))
    yday = (date.fromisoformat(today) - timedelta(days=1)).isoformat()
    yr = ledger[ledger.game_date == yday]
    L.append(f"## Yesterday ({yday}) results")
    if yr.empty:
        L.append("No plays yesterday.")
    else:
        L.append(f"Record: {_rec(yr)}")
        L.append("")
        L.append("| Game | Bet | Result | Score | Close | CLV | P&L |")
        L.append("|---|---|---|---|---|---|---|")
        for _, r in yr.iterrows():
            sc = "" if pd.isna(r.home_score) else f"{int(r.away_score)}-{int(r.home_score)}"
            L.append(f"| {r.away} @ {r.home} | {r.side} {r.line} | {r.result if pd.notna(r.result) else 'pending'} | {sc} | "
                     f"{'' if pd.isna(r.close_line) else r.close_line} | {'' if pd.isna(r.clv_pts) else f'{float(r.clv_pts):+g}'} | "
                     f"{'' if pd.isna(r.pnl) else f'${float(r.pnl):+.2f}'} |")
    L.append("")
    L.append("## Running record")
    L.append(f"- All: {_rec(ledger)}")
    for m in ("spread", "total"):
        L.append(f"- {m.title()}s: {_rec(ledger[ledger.market == m])}")
    pend = (ledger.status == "open").sum()
    L.append(f"- Open (ungraded) plays: {pend}")
    L.append("")
    L.append("## Notes")
    L.append(f"- Rules: sides = v2 model-only vs DK line, edge% = |model − line| / DK total ≥ {RULES['spread'][1]:.0%}; "
             f"totals = original v1 total vs DK total, edge% ≥ {RULES['total'][1]:.0%}; each must clear its threshold vs BOTH "
             "the opening and the current line (same side). Edges only showed up vs *opening* lines in the backtest "
             "(they lost vs the close), so run this as early as possible.")
    L.append(f"- Line-move flag: a side whose DK spread moved ≥{LINE_MOVE_PTS:g} pts from open to current is marked "
             "'likely news/injury'. " + ("Flagged sides are EXCLUDED from plays." if LINE_MOVE_EXCLUDE else
             "Flag only — the backtest showed flagged side picks did better, not worse (see "
             "outputs/walkforward/line_move_study.txt), so they are not excluded."))
    L.append("- Stakes: quarter Kelly with a conservative hit-rate estimate "
             f"({RULES['spread'][2]:.1%} sides / {RULES['total'][2]:.1%} totals), capped at 2.5% of bankroll.")
    for n in notes:
        L.append(f"- {n}")
    with open(path, "w") as fh:
        fh.write("\n".join(L) + "\n")
    return path


# ── main ─────────────────────────────────────────────────────────────────────
def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--date")
    ap.add_argument("--no-refresh", action="store_true")
    ap.add_argument("--no-odds-api", action="store_true")
    ap.add_argument("--no-kalshi", action="store_true")
    ap.add_argument("--dry-run", action="store_true",
                    help="don't write ledger/snapshot/Supabase; report goes to outputs/daily/<date>.dryrun.md")
    ap.add_argument("--exclude-line-moves", action="store_true",
                    help=f"exclude sides whose spread moved >= {LINE_MOVE_PTS:g} pts open->current (default: flag only)")
    ap.add_argument("--config", help="model config JSON (feature list + weights); default czarp_config.json / built-in v2")
    a = ap.parse_args(argv)
    if a.config:
        os.environ["CZARP_CONFIG"] = a.config
    global LINE_MOVE_EXCLUDE
    if a.exclude_line_moves:
        LINE_MOVE_EXCLUDE = True
    now = now_ct()
    today = a.date or now.date().isoformat()
    notes: list[str] = []
    os.makedirs(SNAP_DIR, exist_ok=True)
    log(f"CZarp daily run — slate {today}, {now.strftime('%Y-%m-%d %H:%M')} CT")

    ledger = read_ledger()
    # (f) grade first so the bankroll is current
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
        if not a.dry_run:
            snap.to_csv(os.path.join(SNAP_DIR, f"{today}_{now.strftime('%H%M')}.csv"), index=False)
        # (d) plays
        if data is not None:
            new_rows = flag_plays(games, bankroll(ledger), board, now)
            have = set(ledger.pick_id.astype(str))
            new_rows = [r for r in new_rows if r["pick_id"] not in have]     # one pick per game+market, first price kept
            log(f"  new plays: {len(new_rows)} ({sum(r['market'] == 'spread' for r in new_rows)} sides, "
                f"{sum(r['market'] == 'total' for r in new_rows)} totals)")
            cands = candidates(games, now)
            played = {r["pick_id"] for r in new_rows} | set(ledger.loc[ledger.game_date == today, "pick_id"].astype(str))
            top = top_edges(cands, played)
            nflag = sum(c["flagged"] for c in cands if c["market"] == "spread")
            log(f"  line-move flag (spread moved >= {LINE_MOVE_PTS:g} pts): {nflag} sides"
                + (" — excluded" if LINE_MOVE_EXCLUDE else " — flag only"))
            for ln in top_edges_md(top):
                log("  " + ln if ln else "")
    else:
        notes.append("No games on the slate today.")

    # (e) ledger
    if new_rows:
        ledger = pd.concat([ledger, pd.DataFrame(new_rows)[LEDGER_COLS[:LEDGER_COLS.index("status") + 1]]], ignore_index=True)
    if not a.dry_run:
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
            n = sync(games, ledger, cfg, now, log=log)
            log(f"  Supabase: synced {n}")
        except Exception as e:
            log(f"  Supabase sync failed ({type(e).__name__}: {e}) — CSV ledger unaffected")
            notes.append("Supabase sync failed this run (CSV ledger is unaffected).")
    # (g) summary
    path = write_summary(today, now, games, new_rows, ledger, gi, notes, top,
                         path=os.path.join(OUT_DIR, f"{today}.dryrun.md") if a.dry_run else None)
    log(f"  summary → {path}; ledger → {LEDGER} ({len(ledger)} rows)")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        traceback.print_exc()
        sys.exit(1)
