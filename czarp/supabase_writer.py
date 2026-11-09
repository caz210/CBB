"""Optional Supabase mirror for daily_run.py (outputs only; CSV ledger is the source of truth).

Runs only when SUPABASE_URL and SUPABASE_SERVICE_KEY are set. Talks to the PostgREST API
(/rest/v1) with plain `requests` — no extra dependency. Writes: model_configs, teams, games, team_profiles (daily Four Factors stamp),
projections (v2 + v1 totals; reason / proj_possessions / upset_alert; spreads/totals rounded to 0.1),
market_lines (DK open/current, Kalshi board), picks (full ledger upsert).
Schema: db/schema.sql + db/migrations/. The key is never logged.
"""
from __future__ import annotations

import math
import os
from datetime import datetime
from zoneinfo import ZoneInfo

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


def _str(v):
    return v if isinstance(v, str) and v else None


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


_ET = ZoneInfo("America/New_York")
_CT = ZoneInfo("America/Chicago")


def tipoff_iso(tip, time_tbd: bool = False) -> str | None:
    """ISO tip-off for games.tipoff, or None when the time isn't real.

    Never writes a placeholder: TBD games (ESPN timeValid false / STATUS_TBD), date-only values,
    naive datetimes, and midnight-local stand-ins (00:00 ET or CT, ESPN's TBD default) all return None.
    00:00Z itself is a legit 7 PM ET tip and is kept.
    """
    if tip is None or time_tbd:
        return None
    if isinstance(tip, str):
        if len(tip.strip()) <= 10:  # 'YYYY-MM-DD' only
            return None
        try:
            tip = datetime.fromisoformat(tip.replace("Z", "+00:00"))
        except ValueError:
            return None
    if not isinstance(tip, datetime):
        try:
            tip = pd.Timestamp(tip).to_pydatetime()
        except Exception:
            return None
    if tip.tzinfo is None:
        return None
    for z in (_ET, _CT):
        lt = tip.astimezone(z)
        if lt.hour == 0 and lt.minute == 0:
            return None
    return tip.isoformat()


def _tip_ct_iso(tip_ct) -> str | None:
    if tip_ct is None or (not isinstance(tip_ct, str) and pd.isna(tip_ct)):
        return None
    s = str(tip_ct).strip()
    if len(s) <= 10:
        return None
    return tipoff_iso(pd.Timestamp(s).tz_localize(_CT).to_pydatetime())


def _season(d: str) -> int:
    dt = datetime.fromisoformat(d)
    return dt.year + 1 if dt.month >= 7 else dt.year


def _ncdf(x: float) -> float:
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def sync(games: list[dict], ledger: pd.DataFrame, cfg, now: datetime, log=print, client: Client | None = None,
         data: dict | None = None, new_coach: dict | None = None) -> str:
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

    # team_profiles: stamp every D-I team for today (as_of = America/Chicago date of `now`)
    n_profiles = 0
    if data is not None and season is not None:
        try:
            import model as M
            from czarp import teamstats as TS
            ctx = M._context(data, cfg)
            as_of = now.astimezone(_CT).date().isoformat()
            # Ensure ALL D-I teams exist so profiles can FK
            all_names = sorted(ctx["tt"].index.astype(str))
            missing = [n for n in all_names if n not in tid]
            if missing:
                extra = c.upsert("teams", [{**base, "season": season, "name": n} for n in missing],
                                 on_conflict="league,name")
                tid.update({t["name"]: t["id"] for t in extra})
            profiles = TS.build_team_profiles(
                ctx["tt"], ctx["tags"], as_of=as_of, season=season,
                hca_df=data.get("hca"), new_coach=new_coach or {})
            prows = []
            for pr in profiles:
                teid = tid.get(pr["name"])
                if teid is None:
                    continue
                f = pr["factors"]
                prows.append({
                    "team_id": teid, **base, "season": pr["season"], "as_of": pr["as_of"],
                    "games_played": pr["games_played"], "tentative": pr["tentative"],
                    "efg": f.get("efg"), "efg_pctile": f.get("efg_pctile"),
                    "d_efg": f.get("d_efg"), "d_efg_pctile": f.get("d_efg_pctile"),
                    "to_pct": f.get("to_pct"), "to_pct_pctile": f.get("to_pct_pctile"),
                    "d_to_pct": f.get("d_to_pct"), "d_to_pct_pctile": f.get("d_to_pct_pctile"),
                    "or_pct": f.get("or_pct"), "or_pct_pctile": f.get("or_pct_pctile"),
                    "opp_or_pct": f.get("opp_or_pct"), "opp_or_pct_pctile": f.get("opp_or_pct_pctile"),
                    "ftr": f.get("ftr"), "ftr_pctile": f.get("ftr_pctile"),
                    "d_ftr": f.get("d_ftr"), "d_ftr_pctile": f.get("d_ftr_pctile"),
                    "tempo": f.get("tempo"), "tempo_pctile": f.get("tempo_pctile"),
                    "fg3a_rate": f.get("fg3a_rate"), "fg3a_rate_pctile": f.get("fg3a_rate_pctile"),
                    "fg3_pct": f.get("fg3_pct"), "fg3_pct_pctile": f.get("fg3_pct_pctile"),
                    "opp_fg3_pct": f.get("opp_fg3_pct"), "opp_fg3_pct_pctile": f.get("opp_fg3_pct_pctile"),
                    "ft_pct": f.get("ft_pct"), "ft_pct_pctile": f.get("ft_pct_pctile"),
                    "adj_oe": pr.get("adj_oe"), "adj_de": pr.get("adj_de"), "hca": pr.get("hca"),
                    "tags": pr.get("tags") or [], "tags_all": pr.get("tags_all") or [],
                    "new_coach": bool(pr.get("new_coach")),
                    "new_coach_games": pr.get("new_coach_games"),
                    "created_at": ts,
                })
            c.upsert("team_profiles", prows, on_conflict="team_id,as_of", returning=False)
            n_profiles = len(prows)
        except Exception as e:
            log(f"  team_profiles stamp skipped ({type(e).__name__}: {e})")

    def status(g):
        if g.get("completed"):
            return "final"
        return {"pre": "scheduled", "in": "in_progress"}.get(g.get("state"), "scheduled")

    grows = [{**base, "season": _season(g["game_date"]), "date": g["game_date"], "home": g["kp_home"],
              "away": g["kp_away"], "home_team_id": tid.get(g["kp_home"]), "away_team_id": tid.get(g["kp_away"]),
              "tipoff": tipoff_iso(g.get("tip_utc"), bool(g.get("time_tbd"))),
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
                      "tipoff": _tip_ct_iso(r.get("tip_ct")),
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
                     "archetype_tags": g.get("archetype_tags", {}),
                     "reason": g.get("reason"),
                     "proj_possessions": round(g["proj_possessions"], 1) if g.get("proj_possessions") is not None else None,
                     "upset_alert": bool(g.get("upset_alert")),
                     "upset_reason": g.get("upset_reason"),
                     "created_at": ts})
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
                   "is_paper": True,
                   # track record: tier ('official' = the plays, 'track' = shadow paper sides, flat 1u, never staked),
                   # live vs backfill, when the row was actually logged, flat-1u result, and the ledger hash-chain seal
                   "tier": _str(r.get("tier")) or "official", "source": _str(r.get("source")) or "live",
                   "logged_at": _str(r.get("logged_ct")),
                   "units_pnl": _clean(pd.to_numeric(r.get("pnl_units"), errors="coerce")),
                   "pick_hash": _str(r.get("pick_hash"))})
    c.upsert("picks", pk, on_conflict="pick_key", returning=False)
    # Picks are immutable once published (db trigger picks_immutable blocks updates to pick fields and deletes), so
    # mirrored picks missing from the ledger are reported, never deleted.
    keep = {str(x) for x in ledger.pick_id}
    remote = {x["pick_key"] for x in c.get("picks", select="pick_key", league=f"eq.{LEAGUE}")}
    stale = sorted(remote - keep)
    if stale:
        log(f"  Supabase: {len(stale)} published picks are not in the local ledger (kept; picks are immutable): "
            + ", ".join(stale[:5]))
    return (f"{len(grows)} games, {len(proj)} projections, {len(ml)} market lines, {len(pk)} picks"
            + (f", {n_profiles} team_profiles" if n_profiles else "")
            + (f", {len(stale)} published picks not in ledger" if stale else ""))
