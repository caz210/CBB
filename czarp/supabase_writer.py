"""Optional Supabase mirror for daily_run.py (outputs only; CSV ledger is the source of truth).

Runs only when SUPABASE_URL and SUPABASE_SERVICE_KEY are set. Talks to the PostgREST API
(/rest/v1) with plain `requests` — no extra dependency. Writes: model_configs, teams, games,
projections (v2 + v1 totals; spreads/totals/scores rounded to 0.1, the site rounds to 0.5 for display), market_lines (DK open/current, Kalshi board), picks (full ledger
upsert, so grading updates flow through). Schema: db/schema.sql. The key is never logged.
"""
from __future__ import annotations

import math
import os
from datetime import datetime

import pandas as pd
import requests

SPORT, LEAGUE = "cbb", "ncaam"
V1_VERSION = "czarp-v1-totals"
DEFAULT_VERSION = "czarp-v2"      # model shown on the public slate (model_configs.is_default)


def _clean(v):
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    if hasattr(v, "item"):          # numpy scalar
        return _clean(v.item())
    return v


def _row(d: dict) -> dict:
    return {k: _clean(v) for k, v in d.items()}


class Client:
    def __init__(self, url: str | None = None, key: str | None = None):
        # SUPABASE_REST_URL overrides the REST base (self-hosted PostgREST / local testing)
        self.base = os.getenv("SUPABASE_REST_URL") or (url or os.environ["SUPABASE_URL"]).rstrip("/") + "/rest/v1"
        key = key or os.environ["SUPABASE_SERVICE_KEY"]
        self.s = requests.Session()
        self.s.headers.update({"apikey": key, "Authorization": f"Bearer {key}",
                               "Content-Type": "application/json"})

    def get(self, table: str, **params) -> list[dict]:
        r = self.s.get(f"{self.base}/{table}", params=params, timeout=30)
        self._check(r, table)
        return r.json()

    def upsert(self, table: str, rows: list[dict], on_conflict: str | None = None,
               returning: bool = True, ignore_duplicates: bool = False) -> list[dict]:
        if not rows:
            return []
        prefer = ["resolution=ignore-duplicates" if ignore_duplicates else "resolution=merge-duplicates",
                  "return=representation" if returning else "return=minimal"]
        params = {"on_conflict": on_conflict} if on_conflict else {}
        # PostgREST bulk bodies need identical keys: send one batch per key set
        groups: dict[tuple, list] = {}
        for x in rows:
            groups.setdefault(tuple(sorted(x)), []).append(_row(x))
        out = []
        for batch in groups.values():
            for i in range(0, len(batch), 500):
                r = self.s.post(f"{self.base}/{table}", params=params, json=batch[i:i + 500],
                                headers={"Prefer": ",".join(prefer)}, timeout=60)
                self._check(r, table)
                if returning and r.text:
                    out.extend(r.json())
        return out

    def delete(self, table: str, **params) -> None:
        r = self.s.delete(f"{self.base}/{table}", params=params, timeout=30)
        self._check(r, table)

    def insert(self, table: str, rows: list[dict]) -> None:
        self.upsert(table, rows, returning=False, ignore_duplicates=True)

    @staticmethod
    def _check(r, table):
        if r.status_code >= 300:
            raise RuntimeError(f"{table}: HTTP {r.status_code} {r.text[:300]}")


def _season(d: str) -> int:
    dt = datetime.fromisoformat(d)
    return dt.year + 1 if dt.month >= 7 else dt.year


def _ncdf(x: float) -> float:
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def sync(games: list[dict], ledger: pd.DataFrame, cfg, now: datetime, log=print, client: Client | None = None) -> str:
    c = client or Client()
    sport = c.get("sports", code=f"eq.{SPORT}", select="id")
    if not sport:
        raise RuntimeError("sports row 'cbb' missing — apply db/schema.sql first")
    sid = sport[0]["id"]
    base = {"sport_id": sid, "league": LEAGUE}
    ts = now.isoformat(timespec="seconds")

    # model configs
    mc = []
    if cfg is not None:
        ver = getattr(cfg, "model_version", "czarp-v2")
        mc.append({**base, "model_version": ver,
                   "description": "CZarp v2 (sides): config-driven features", "features": cfg.features,
                   "weights": cfg.weights_dict(), "is_default": ver == DEFAULT_VERSION, "updated_at": ts})
    mc.append({**base, "model_version": V1_VERSION, "description": "Original v1 formula (main:model.py), totals only",
               "features": [{"name": "v1_formula", "kind": "legacy", "weight": 1.0, "enabled": True}],
               "weights": {}, "is_default": False, "updated_at": ts})
    c.upsert("model_configs", mc, on_conflict="model_version", returning=False)

    # teams + games (today's slate, plus any ledger games not yet mirrored)
    gl = [g for g in games if g.get("kp_home") and g.get("kp_away")]
    season = _season(gl[0]["game_date"]) if gl else None
    names = sorted({g["kp_home"] for g in gl} | {g["kp_away"] for g in gl})
    teams = c.upsert("teams", [{**base, "season": season, "name": n} for n in names], on_conflict="league,name")
    tid = {t["name"]: t["id"] for t in teams}

    def status(g):
        if g.get("completed"):
            return "final"
        return {"pre": "scheduled", "in": "in_progress"}.get(g.get("state"), "scheduled")

    grows = [{**base, "season": _season(g["game_date"]), "date": g["game_date"], "home": g["kp_home"],
              "away": g["kp_away"], "home_team_id": tid.get(g["kp_home"]), "away_team_id": tid.get(g["kp_away"]),
              "tipoff": g["tip_utc"].isoformat() if g.get("tip_utc") is not None else None,
              "neutral": bool(g.get("neutral")), "external_id": str(g["event_id"]),
              "external_ids": {"espn": str(g["event_id"])}, "status": status(g),
              "home_score": int(g["home_score"]) if g.get("home_score") is not None and g.get("completed") else None,
              "away_score": int(g["away_score"]) if g.get("away_score") is not None and g.get("completed") else None,
              "updated_at": ts} for g in gl]
    # ledger games from earlier days (keeps graded scores in sync)
    seen = {r["external_id"] for r in grows}
    for _, r in ledger.drop_duplicates("event_id").iterrows():
        if str(r.event_id) in seen:
            continue
        done = pd.notna(r.get("home_score"))
        grows.append({**base, "season": _season(r.game_date), "date": r.game_date, "home": r.home, "away": r.away,
                      "neutral": str(r.neutral) == "True", "external_id": str(r.event_id),
                      "external_ids": {"espn": str(r.event_id)}, "status": "final" if done else "scheduled",
                      "tipoff": (pd.Timestamp(r.tip_ct).tz_localize("America/Chicago").isoformat() if pd.notna(r.get("tip_ct")) else None),
                      "home_score": int(r.home_score) if done else None,
                      "away_score": int(r.away_score) if done else None, "updated_at": ts})
    gid = {x["external_id"]: x["id"] for x in c.upsert("games", grows, on_conflict="league,external_id")}

    # projections
    msd = getattr(getattr(cfg, "betting", None), "margin_sd", 11.4)
    tsd = getattr(getattr(cfg, "betting", None), "total_sd", 16.9)
    proj = []
    for g in gl:
        if g.get("v2_home_margin") is None or str(g["event_id"]) not in gid:
            continue
        hm, T = g["v2_home_margin"], g["v2_total"]
        L, MT = g.get("current_home_spread"), g.get("current_total")
        proj.append({**base, "season": _season(g["game_date"]), "game_id": gid[str(g["event_id"])],
                     "model_version": g.get("model_version", "czarp-v2"),
                     "proj_home": round((T + hm) / 2, 1), "proj_away": round((T - hm) / 2, 1),
                     "spread": round(-hm, 1), "total": round(T, 1),
                     "home_win_prob": round(g.get("v2_home_win_prob", _ncdf(hm / msd)), 4),
                     "home_cover_prob": round(_ncdf((hm + L) / msd), 4) if L is not None else None,
                     "over_prob": round(1 - _ncdf((MT - T) / tsd), 4) if MT is not None else None,
                     "archetype_tags": g.get("archetype_tags", {}), "created_at": ts})
        if g.get("v1_total") is not None:
            t1, m1 = g["v1_total"], g.get("v1_home_margin")
            proj.append({**base, "season": _season(g["game_date"]), "game_id": gid[str(g["event_id"])],
                         "model_version": V1_VERSION, "total": round(t1, 1),
                         "spread": round(-m1, 1) if m1 is not None else None,
                         "over_prob": round(1 - _ncdf((MT - t1) / tsd), 4) if MT is not None else None,
                         "created_at": ts})
    c.insert("projections", proj)

    # market lines: DK open + current; Kalshi board snapshot
    ml = []
    for g in gl:
        k = gid.get(str(g["event_id"]))
        if k is None:
            continue
        for lt in ("open", "current"):
            if g.get(f"{lt}_home_spread") is None and g.get(f"{lt}_total") is None:
                continue
            ml.append({"game_id": k, "source": "vegas", "line_type": lt,
                       "book": (g.get("price_source") or "espn:DraftKings").split(":")[-1],
                       "spread": g.get(f"{lt}_home_spread"), "total": g.get(f"{lt}_total"),
                       "prices": _row({x: g.get(f"{lt}_{x}") for x in ("home_spread_price", "away_spread_price",
                                                                         "over_price", "under_price")}),
                       "fetched_at": ts})
        kg = g.get("kalshi")
        if kg:
            mlk = kg.get("moneyline", {})
            import czarp.kalshi as kx
            hk, ak = kx.team_in(list(mlk), g["kp_home"]), kx.team_in(list(mlk), g["kp_away"])
            ml.append({"game_id": k, "source": "kalshi", "line_type": "current",
                       "ml_home": mlk[hk]["yes_ask"] if hk else None, "ml_away": mlk[ak]["yes_ask"] if ak else None,
                       "prices": {"spread": [_row({x: l.get(x) for x in ("team", "strike", "yes_ask", "no_ask", "last")})
                                             for l in kg.get("spread", [])],
                                  "total": [_row({x: l.get(x) for x in ("strike", "yes_ask", "no_ask", "last")})
                                            for l in kg.get("total", [])]},
                       "fetched_at": ts})
    c.insert("market_lines", ml)

    # picks: full ledger upsert
    pk = []
    for _, r in ledger.iterrows():
        k = gid.get(str(r.event_id))
        if k is None:
            continue
        line = float(r.line)
        bet = f"{r.side} {line:+g}" if r.market == "spread" else f"{r.side} {line:g}"
        pk.append({**base, "season": _season(r.game_date), "pick_key": str(r.pick_id), "game_id": k,
                   "market": r.market, "bet": bet, "line_or_price": line, "price": _clean(r.price),
                   "edge": round(float(r.edge_pct), 4), "kelly_stake": _clean(r.stake),
                   "model_version": r.model_version if isinstance(r.model_version, str) and r.model_version
                   else ("czarp-v2" if r.market == "spread" else V1_VERSION),
                   "created_at": r.created_ct,
                   "result": r.result if isinstance(r.result, str) else None,
                   "closing_line": _clean(r.close_line), "clv": _clean(r.clv_pts), "pnl": _clean(r.pnl),
                   "graded_at": r.graded_ct if isinstance(r.graded_ct, str) else None,
                   "model_prob": _clean(pd.to_numeric(r.get("p_est"), errors="coerce")),
                   "bankroll_at_pick": _clean(pd.to_numeric(r.get("bankroll_at_pick"), errors="coerce")),
                   "is_paper": True})
    c.upsert("picks", pk, on_conflict="pick_key", returning=False)
    # ledger is the source of truth: drop mirrored picks that are no longer in it (e.g. a re-run with a new rule)
    keep = {str(x) for x in ledger.pick_id}
    remote = {x["pick_key"] for x in c.get("picks", select="pick_key", league=f"eq.{LEAGUE}")}
    stale = sorted(remote - keep)
    for i in range(0, len(stale), 100):
        c.delete("picks", pick_key="in.(" + ",".join(f'"{k}"' for k in stale[i:i + 100]) + ")")
    return (f"{len(grows)} games, {len(proj)} projections, {len(ml)} market lines, {len(pk)} picks"
            + (f", removed {len(stale)} stale picks" if stale else ""))
