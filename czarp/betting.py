"""Turn a projection + market prices into bet recommendations.

Every edge is model probability minus the break-even probability of the price
you would actually pay (vig included for sportsbooks; fee included for
Kalshi/Robinhood). Tiers and quarter-Kelly stakes come from BettingConfig.
"""
from __future__ import annotations

from dataclasses import asdict

from .config import BettingConfig, load_config
from .probability import (cover_prob, devig_two_way, evaluate, margin_over_prob,
                          over_prob, win_prob, american_to_prob)
from . import kalshi as kx

DEFAULT_JUICE = -110


def _bet(market, side, line, price, model_p, ev, source, fair=None, extra=None):
    d = {"market": market, "side": side, "line": line, "price": price, "source": source,
         "model_prob": round(model_p, 4), "fair_prob": None if fair is None else round(fair, 4),
         "implied_prob": round(ev.implied_prob, 4), "edge": round(ev.edge, 4),
         "tier": ev.tier, "stake_pct": round(ev.stake_pct, 4)}
    if extra:
        d.update(extra)
    return d


def evaluate_vegas_game(result: dict, cfg: BettingConfig | None = None) -> list[dict]:
    """Uses fields added by odds_fetcher.match_vegas_to_game. vegas_spread is
    team1's line in betting notation (-4.5 = team1 favored by 4.5)."""
    cfg = cfg or load_config().betting
    margin = result.get("spread_raw", result.get("spread"))
    total = result.get("total_raw", result.get("total"))
    t1, t2 = result["team1"], result["team2"]
    bets = []
    src = f"vegas:{result.get('source_book') or '?'}"
    sp = result.get("vegas_spread")
    if sp is not None and margin is not None:
        p1 = cover_prob(margin, sp, cfg.margin_sd)
        pr1 = result.get("t1_spread_price") or DEFAULT_JUICE
        pr2 = result.get("t2_spread_price") or DEFAULT_JUICE
        f1, f2 = devig_two_way(pr1, pr2)
        bets.append(_bet("spread", t1, sp, pr1, p1, evaluate(p1, cfg, american=pr1), src, f1))
        bets.append(_bet("spread", t2, -sp, pr2, 1 - p1, evaluate(1 - p1, cfg, american=pr2), src, f2))
    ml1, ml2 = result.get("vegas_t1_ml"), result.get("vegas_t2_ml")
    if ml1 is not None and ml2 is not None and margin is not None:
        p1 = win_prob(margin, cfg.margin_sd)
        f1, f2 = devig_two_way(ml1, ml2)
        bets.append(_bet("moneyline", t1, None, ml1, p1, evaluate(p1, cfg, american=ml1), src, f1))
        bets.append(_bet("moneyline", t2, None, ml2, 1 - p1, evaluate(1 - p1, cfg, american=ml2), src, f2))
    tl = result.get("vegas_total")
    if tl and total is not None:
        po = over_prob(total, tl, cfg.total_sd)
        op = result.get("over_price") or DEFAULT_JUICE
        up = result.get("under_price") or DEFAULT_JUICE
        fo, fu = devig_two_way(op, up)
        bets.append(_bet("total", "Over", tl, op, po, evaluate(po, cfg, american=op), src, fo))
        bets.append(_bet("total", "Under", tl, up, 1 - po, evaluate(1 - po, cfg, american=up), src, fu))
    return bets


def evaluate_kalshi_game(result: dict, game: dict, cfg: BettingConfig | None = None,
                         fee_profile: str | None = None, min_price: float = 0.02,
                         max_price: float = 0.98) -> list[dict]:
    """Evaluate every Kalshi contract for one game. Buys are at the ASK (taker),
    YES or NO, whichever side the model favors; fee per fee_profile."""
    cfg = cfg or load_config().betting
    profile = fee_profile or cfg.kalshi_fee_profile
    margin = result.get("spread_raw", result.get("spread"))
    total = result.get("total_raw", result.get("total"))
    t1, t2 = result["team1"], result["team2"]
    src = f"kalshi:{profile}"
    bets = []

    def add(market, side, line, ask, p, ticker, extra=None):
        if ask is None or not (min_price <= ask <= max_price):
            return
        ev = evaluate(p, cfg, contract_price=ask, fee_profile=profile)
        bets.append(_bet(market, side, line, ask, p, ev, src, extra={"ticker": ticker, **(extra or {})}))

    names = list(game.get("moneyline", {}).keys())
    for team, sign in ((t1, 1), (t2, -1)):
        k = kx.team_in(names, team)
        if not k:
            continue
        q = game["moneyline"][k]
        p = win_prob(sign * margin, cfg.margin_sd)
        add("moneyline", f"{team} YES", None, q["yes_ask"], p, q["ticker"])
        add("moneyline", f"{team} NO", None, q["no_ask"], 1 - p, q["ticker"])

    for leg in game.get("spread", []):
        if kx.team_in([leg["team"]], t1):
            m = margin
        elif kx.team_in([leg["team"]], t2):
            m = -margin
        else:
            continue
        p = margin_over_prob(m, leg["strike"], cfg.margin_sd)
        add("spread", f"{leg['team']} by >{leg['strike']} YES", leg["strike"], leg["yes_ask"], p, leg["ticker"])
        add("spread", f"{leg['team']} by >{leg['strike']} NO", leg["strike"], leg["no_ask"], 1 - p, leg["ticker"])

    for leg in game.get("total", []):
        p = over_prob(total, leg["strike"], cfg.total_sd)
        add("total", f"Over {leg['strike']} YES", leg["strike"], leg["yes_ask"], p, leg["ticker"])
        add("total", f"Over {leg['strike']} NO", leg["strike"], leg["no_ask"], 1 - p, leg["ticker"])
    return bets


def best_bets(bets: list[dict], only_tiered: bool = True, one_per_market: bool = True) -> list[dict]:
    """Sort by edge. Kalshi ladders produce many correlated rungs for the same
    view, so by default keep only the best bet per market type (spread/total/ML)."""
    out = [b for b in bets if b["tier"]] if only_tiered else list(bets)
    out = sorted(out, key=lambda b: b["edge"], reverse=True)
    if one_per_market:
        seen, keep = set(), []
        for b in out:
            if b["market"] not in seen:
                seen.add(b["market"]); keep.append(b)
        out = keep
    return out


def evaluate_game(result: dict, source: str = "vegas", *, kalshi_board: dict | None = None,
                  cfg: BettingConfig | None = None) -> list[dict]:
    """Price-source toggle. source="vegas": result must already be enriched by
    odds_fetcher.match_vegas_to_game. source="kalshi": looks the game up on a
    Kalshi board (kalshi.fetch_cbb_board())."""
    cfg = cfg or load_config().betting
    if source == "vegas":
        return evaluate_vegas_game(result, cfg)
    if source in ("kalshi", "robinhood", "prediction_market"):
        board = kalshi_board if kalshi_board is not None else kx.fetch_cbb_board()
        game = kx.find_game(board, result["team1"], result["team2"])
        if not game:
            return []
        profile = None
        if source == "robinhood":
            profile = "robinhood"
        return evaluate_kalshi_game(result, game, cfg, fee_profile=profile)
    raise ValueError(f"unknown price source {source!r}")
