"""Kalshi public market data for men's college basketball (no auth needed).

Series (verified 2026-11-01 against api.elections.kalshi.com/trade-api/v2):
  KXNCAAMBGAME   moneyline: one market per team, "Utah wins", yes_sub_title = team
  KXNCAAMBSPREAD ladder: "Texas wins by over 17.5 points" (floor_strike 17.5,
                 strike_type greater); usually only the favorite's side is listed
  KXNCAAMBTOTAL  ladder: "Over 167.5 points" (floor_strike)
All three use fee_type quadratic_with_maker_fees. Robinhood's CBB event
contracts are KalshiEX contracts (Robinhood charges its own commission on top).
Prices are dollars per $1 contract (strings like "0.9500").
"""
from __future__ import annotations

import re
from collections import defaultdict

import requests

BASE = "https://api.elections.kalshi.com/trade-api/v2"
SERIES = {"moneyline": "KXNCAAMBGAME", "spread": "KXNCAAMBSPREAD", "total": "KXNCAAMBTOTAL"}
_SPREAD_RE = re.compile(r"^(?P<team>.+?) wins by over (?P<strike>[\d.]+) points", re.I)


def _f(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def fetch_markets(series_ticker: str, status: str = "open", session=None, max_pages: int = 20) -> list[dict]:
    s = session or requests.Session()
    out, cursor = [], None
    for _ in range(max_pages):
        params = {"series_ticker": series_ticker, "status": status, "limit": 1000}
        if cursor:
            params["cursor"] = cursor
        r = s.get(f"{BASE}/markets", params=params, timeout=20)
        r.raise_for_status()
        j = r.json()
        out.extend(j.get("markets", []))
        cursor = j.get("cursor")
        if not cursor:
            break
    return out


def game_key(event_ticker: str) -> str:
    """KXNCAAMBSPREAD-26NOV02CHSTEX -> 26NOV02CHSTEX (shared across series)."""
    return event_ticker.split("-", 1)[1] if "-" in event_ticker else event_ticker


def _quote(m: dict) -> dict:
    return {
        "ticker": m.get("ticker"),
        "yes_ask": _f(m.get("yes_ask_dollars")), "yes_bid": _f(m.get("yes_bid_dollars")),
        "no_ask": _f(m.get("no_ask_dollars")), "no_bid": _f(m.get("no_bid_dollars")),
        "last": _f(m.get("last_price_dollars")),
        "volume": _f(m.get("volume_fp")) or _f(m.get("volume")),
        "open_interest": _f(m.get("open_interest_fp")) or _f(m.get("open_interest")),
        "close_time": m.get("close_time"),
    }


def parse_markets(markets: list[dict]) -> dict[str, dict]:
    """Group raw markets into games: {game_key: {"title", "teams", "moneyline":
    {team: quote}, "spread": [ {team, strike, quote} ], "total": [ {strike, quote} ]}}."""
    games: dict[str, dict] = defaultdict(lambda: {"moneyline": {}, "spread": [], "total": [], "teams": set()})
    for m in markets:
        ev = m.get("event_ticker", "")
        key = game_key(ev)
        g = games[key]
        q = _quote(m)
        if ev.startswith(SERIES["moneyline"] + "-"):
            team = m.get("yes_sub_title") or re.sub(r"\s+wins$", "", m.get("title", ""))
            g["moneyline"][team] = q
            g["teams"].add(team)
        elif ev.startswith(SERIES["spread"] + "-"):
            mt = _SPREAD_RE.match(m.get("title") or m.get("yes_sub_title") or "")
            if mt:
                strike = _f(m.get("floor_strike")) or float(mt.group("strike"))
                g["spread"].append({"team": mt.group("team"), "strike": strike, **q})
                g["teams"].add(mt.group("team"))
        elif ev.startswith(SERIES["total"] + "-"):
            strike = _f(m.get("floor_strike"))
            if strike is not None:
                g["total"].append({"strike": strike, **q})
        rules = m.get("rules_primary") or ""
        mt = re.search(r"in the (.+?) men's college basketball game", rules)
        if mt:
            g["title"] = mt.group(1)
    for g in games.values():
        g["teams"] = sorted(g["teams"])
        g["spread"].sort(key=lambda x: (x["team"], x["strike"]))
        g["total"].sort(key=lambda x: x["strike"])
    return dict(games)


def fetch_cbb_board(status: str = "open") -> dict[str, dict]:
    s = requests.Session()
    markets = []
    for series in SERIES.values():
        markets.extend(fetch_markets(series, status=status, session=s))
    return parse_markets(markets)


# Kalshi display name -> KenPom TeamName (only where they differ)
KALSHI_TO_KENPOM = {
    "Arkansas-Pine Bluff": "Arkansas Pine Bluff", "Bethune-Cookman": "Bethune Cookman",
    "Cal State Fullerton": "Cal St. Fullerton", "Cal State Northridge": "CSUN",
    "California Baptist": "Cal Baptist", "Central Connecticut St.": "Central Connecticut",
    "FDU": "Fairleigh Dickinson", "Florida International": "FIU", "Gardner-Webb": "Gardner Webb",
    "Hawai'i": "Hawaii", "Louisiana-Monroe": "Louisiana Monroe", "Loyola Maryland": "Loyola MD",
    "Maryland-Eastern Shore": "Maryland Eastern Shore", "Miami (FL)": "Miami FL",
    "Miami (OH)": "Miami OH", "NC St.": "N.C. State", "UConn": "Connecticut",
    "Ole Miss": "Mississippi", "UT Martin": "Tennessee Martin", "IU Indianapolis": "IU Indy",
    "New Orleans": "LSU New Orleans", "Queens University": "Queens", "Sam Houston": "Sam Houston St.",
    "Southeast Missouri St.": "Southeast Missouri", "Southern University": "Southern",
    "Texas A&M-Corpus Christi": "Texas A&M Corpus Chris", "UMass": "Massachusetts",
    "University at Albany": "Albany",
}


def _norm(name: str) -> str:
    name = KALSHI_TO_KENPOM.get(name, name)
    n = name.lower().replace("state", "st.").replace("saint", "st.")
    return re.sub(r"[^a-z0-9]", "", n)


def find_game(board: dict, team1: str, team2: str) -> dict | None:
    """Locate a game on the Kalshi board by team names (KenPom-style names;
    Kalshi mostly uses the same 'Chicago St.' style)."""
    a, b = _norm(team1), _norm(team2)
    for g in board.values():
        names = [_norm(t) for t in g["teams"]]
        title = _norm(g.get("title", ""))
        hit_a = a in names or (a and a in title)
        hit_b = b in names or (b and b in title)
        if hit_a and hit_b:
            return g
    return None


def team_in(names: list[str], team: str) -> str | None:
    t = _norm(team)
    for n in names:
        if _norm(n) == t:
            return n
    return None
