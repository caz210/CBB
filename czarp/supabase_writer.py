"""Optional Supabase mirror for daily_run.py (outputs only; CSV ledger is the source of truth).

Runs only when SUPABASE_URL and SUPABASE_SERVICE_KEY are set. Talks to the PostgREST API
(/rest/v1) with plain `requests` — no extra dependency. Writes: model_configs, teams, games, team_profiles (daily Four Factors stamp),
projections (v2 + v1 totals; reason / proj_possessions / upset_alert / tv_rating; spreads/totals rounded to 0.1),
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

from czarp.tv_rating import game_tv_rating, quality_from_data

SPORT, LEAGUE = "cbb", "ncaam"
V1_VERSION = "czarp-v1-totals"
DEFAULT_VERSION = "czarp-v2"      # model shown on the public slate (model_configs.is_default)
PROJ_KEY = "game_id,model_version,created_at"   # projections unique key (db/schema.sql)
LOCK_FIELDS = (("fair_at_post", "fair_at_post"), ("line_at_post", "line_at_post"),
               ("total_at_post", "total_at_post"), ("open_line", "open_line"),
               ("edge_vs_open", "edge_vs_open_pct"))   # picks column <- official_picks_locked column


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


def _edge_frac(v):
    """Edge fraction rounded to 4 decimals (0.045 = 4.5%), or None."""
    v = _clean(v)
    return None if v is None else round(float(v), 4)


def team_scores(home_margin, v2_total, v1_total=None) -> tuple[float, float]:
    """Projected (home, away) points that agree with what customers see: they add up to the CZarp total
    (the czarp-v1-totals total when it exists, else the default model's total) and their gap equals the
    CZarp spread (the default model's home margin). Both are taken at the precision they are stored and
    shown (0.1), so home + away == total and home - away == -spread exactly; a score can land on .x5,
    hence two decimals. Neutral games need no special case: the margin already carries no home edge."""
    t = _clean(v1_total)
    t = float(v2_total) if t is None else float(t)
    m = round(float(home_margin), 1)
    t = round(t, 1)
    return round((t + m) / 2, 2), round((t - m) / 2, 2)


def _edge_fields(g: dict) -> dict:
    return {k: _edge_frac(g.get(k)) for k in (
        "edge_now_pct_spread", "edge_vs_open_pct_spread",
        "edge_now_pct_total", "edge_vs_open_pct_total")}


# LINE MOVED AWAY flag (db/migrations/2026-11-24_lean_tier_line_moved_away.sql): display only, never a play.
FLAG_COLS = ("line_moved_away", "line_moved_away_pts")


def _flag_fields(g: dict) -> dict:
    """projections.line_moved_away / line_moved_away_pts from daily_run.line_moved_away (stamped on g by
    attach_market_edges). None when the game has no model margin or no open/current spread."""
    f, pts = g.get("line_moved_away"), _clean(g.get("line_moved_away_pts"))
    return {"line_moved_away": None if f is None else bool(f),
            "line_moved_away_pts": None if pts is None else round(float(pts), 1)}


# CZarp LEAN picks (tier='lean') are mirrored to Supabase only when CZARP_SYNC_LEAN=1. Keep it OFF until BOTH
#   1. db/migrations/2026-11-24_lean_tier_line_moved_away.sql is applied (picks_tier_chk allows 'lean'; picks.new_coach),
#   2. the site reads tier='lean' as its own record (web/src/lib/data.ts maps unknown tiers to 'official' today).
# Lean rows always stay in the CSV ledger (hash-sealed) and are graded there either way.
def sync_lean_enabled() -> bool:
    return os.getenv("CZARP_SYNC_LEAN", "").strip().lower() in ("1", "true", "yes", "on")


def lean_schema_ready(c: "Client") -> bool:
    """True when the lean-tier migration is live: picks.new_coach exists. The same
    transaction also opens picks_tier_chk to 'lean', so this is a cheap proxy for both.
    A missing column (PGRST204) is False; any other probe error is raised for the caller
    to handle so official sync can still proceed."""
    try:
        c.get("picks", select="new_coach", limit=1)
        return True
    except RuntimeError as e:
        if "PGRST204" in str(e) or "new_coach" in str(e):
            return False
        raise


def _row(d: dict) -> dict:
    return {k: _clean(v) for k, v in d.items()}


def rest_base(url: str) -> str:
    """PostgREST base for a project URL. Accepts the plain project URL or one pasted with a trailing /rest/v1
    and/or trailing slashes (a common secret-entry mistake), and always returns '<project>/rest/v1'."""
    u = (url or "").strip().rstrip("/")
    while u.lower().endswith("/rest/v1"):
        u = u[: -len("/rest/v1")].rstrip("/")
    return u + "/rest/v1"


class Client:
    def __init__(self, url: str | None = None, key: str | None = None):
        # SUPABASE_REST_URL overrides the REST base (self-hosted PostgREST / local testing)
        self.base = os.getenv("SUPABASE_REST_URL") or rest_base(url or os.environ["SUPABASE_URL"])
        key = key or os.environ["SUPABASE_SERVICE_KEY"]
        self.s = requests.Session()
        self.s.headers.update({"apikey": key, "Authorization": f"Bearer {key}",
                               "Content-Type": "application/json"})

    def get(self, table: str, **params) -> list[dict]:
        r = self.s.get(f"{self.base}/{table}", params=params, timeout=30)
        self._check(r, table)
        return r.json()

    def get_all(self, table: str, order: str, page: int = 1000, **params) -> list[dict]:
        """Every row matching `params`, paged with limit/offset under a stable `order` (PostgREST caps a single
        response at max-rows, 1000 on Supabase by default)."""
        out, off = [], 0
        while True:
            rows = self.get(table, order=order, limit=str(page), offset=str(off), **params)
            out.extend(rows)
            if len(rows) < page:
                return out
            off += page

    def count(self, table: str, **params) -> int:
        """Exact row count for PostgREST filters without fetching rows (HEAD + Prefer: count=exact)."""
        r = self.s.head(f"{self.base}/{table}", params={"select": "id", **params},
                        headers={"Prefer": "count=exact"}, timeout=30)
        self._check(r, table)
        total = (r.headers.get("Content-Range") or "").rsplit("/", 1)[-1]
        if not total.isdigit():
            raise RuntimeError(f"{table}: no exact count in Content-Range")
        return int(total)

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

    def patch(self, table: str, row: dict, **params) -> None:
        """Update rows matching `params` (PostgREST filters, e.g. external_id='eq.123').

        Use this instead of a partial upsert: ON CONFLICT still validates NOT NULL
        columns on the would-be INSERT, so a scores-only upsert fails on sport_id.
        """
        if not row:
            return
        r = self.s.patch(f"{self.base}/{table}", params=params, json=_row(row),
                         headers={"Prefer": "return=minimal"}, timeout=30)
        self._check(r, table)

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
            # Same source daily_run uses for the NEW COACH lock. If the caller passed an
            # empty map (the Nov 8 stamp bug), detect it here so profiles stay in sync.
            from czarp.coach import detect_new_coaches, enrich_coach_profile, games_played
            nc_map = dict(new_coach or {})
            if not nc_map:
                try:
                    nc_map = detect_new_coaches(season)
                except Exception as e:
                    log(f"  new-coach detection for profiles failed ({type(e).__name__}: {e})")
                    nc_map = {}
            played = {}
            try:
                played = games_played(data["ratings"]) if data.get("ratings") is not None else {}
            except Exception:
                played = {}
            nc_map = enrich_coach_profile(nc_map, played)
            profiles = TS.build_team_profiles(
                ctx["tt"], ctx["tags"], as_of=as_of, season=season,
                hca_df=data.get("hca"), new_coach=nc_map)
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
                    "lock_sides_games": pr.get("lock_sides_games"),
                    "lock_totals_games": pr.get("lock_totals_games"),
                    "lock_sides_remaining": pr.get("lock_sides_remaining"),
                    "lock_totals_remaining": pr.get("lock_totals_remaining"),
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

    # Today's slate: map ESPN scores onto kp_home/kp_away by name (neutral flips).
    # Do not fall back to slot order; omit scores if the names cannot be matched.
    from czarp.sync_finals import map_espn_scores
    from backtest import NameMatcher
    slate_names = {g.get("kp_home") for g in gl} | {g.get("kp_away") for g in gl}
    slate_names.discard(None); slate_names.discard("")
    slate_match = NameMatcher(slate_names) if slate_names else None

    def _slate_scores(g):
        if not g.get("completed") or g.get("home_score") is None or g.get("away_score") is None:
            return None, None
        aligned = map_espn_scores(g.get("kp_home"), g.get("kp_away"),
                                  g.get("home"), g.get("away"),
                                  g.get("home_score"), g.get("away_score"),
                                  match=slate_match)
        if aligned is None:
            log(f"  games sync espn={g.get('event_id')}: ESPN teams {g.get('away')} / {g.get('home')} "
                f"do not match stored {g.get('kp_away')} / {g.get('kp_home')} — scores omitted")
            return None, None
        return aligned

    grows = []
    for g in gl:
        hs, aws = _slate_scores(g)
        grows.append({**base, "season": _season(g["game_date"]), "date": g["game_date"], "home": g["kp_home"],
                      "away": g["kp_away"], "home_team_id": tid.get(g["kp_home"]), "away_team_id": tid.get(g["kp_away"]),
                      "tipoff": tipoff_iso(g.get("tip_utc"), bool(g.get("time_tbd"))),
                      "neutral": bool(g.get("neutral")), "external_id": str(g["event_id"]),
                      "external_ids": {"espn": str(g["event_id"])}, "status": status(g),
                      "home_score": hs, "away_score": aws, "updated_at": ts})
    # Ledger games from earlier days: never copy ledger scores into games.
    # grade() used to write ESPN slot scores into the ledger, so a merge-upsert
    # of those values would undo a name-aligned backfill (game 1550 / WIU-Albany).
    # sync_finals owns final scores. Omit the keys so an UPDATE leaves existing
    # games.home_score / games.away_score alone. Do not edit ledger.csv here.
    # Always set team_ids: a ledger-first insert that omitted them (Wright St.
    # vs Air Force, 2026-11-02) never appeared on either team's game log.
    seen = {r["external_id"] for r in grows}
    led = ledger.drop_duplicates("event_id") if len(ledger) else ledger
    ledger_names = sorted({
        n for _, r in led.iterrows() for n in (str(r.home), str(r.away))
        if n and n not in tid and n != "nan"
    })
    if ledger_names:
        led_season = season or _season(str(led.iloc[0].game_date))
        extra = c.upsert("teams", [{**base, "season": led_season, "name": n} for n in ledger_names],
                         on_conflict="league,name")
        tid.update({t["name"]: t["id"] for t in extra})
    for _, r in led.iterrows():
        if str(r.event_id) in seen:
            continue
        done = pd.notna(r.get("home_score"))
        grows.append({**base, "season": _season(r.game_date), "date": r.game_date, "home": r.home, "away": r.away,
                      "home_team_id": tid.get(str(r.home)), "away_team_id": tid.get(str(r.away)),
                      "neutral": str(r.neutral) == "True", "external_id": str(r.event_id),
                      "external_ids": {"espn": str(r.event_id)}, "status": "final" if done else "scheduled",
                      "tipoff": _tip_ct_iso(r.get("tip_ct")), "updated_at": ts})
    gid = {x["external_id"]: x["id"] for x in c.upsert("games", grows, on_conflict="league,external_id")}

    # TV Rating: only when KenPom ratings were passed in (data=). Omit the
    # column when data is None so a ratings-less re-run cannot null out scores.
    qmap = quality_from_data(data) if data is not None else None

    # projections
    msd = getattr(getattr(cfg, "betting", None), "margin_sd", 11.4)
    tsd = getattr(getattr(cfg, "betting", None), "total_sd", 16.9)
    proj = []
    for g in gl:
        if g.get("v2_home_margin") is None or str(g["event_id"]) not in gid:
            continue
        hm, T = g["v2_home_margin"], g["v2_total"]
        L, MT = g.get("current_home_spread"), g.get("current_total")
        win_p = round(g.get("v2_home_win_prob", _ncdf(hm / msd)), 4)
        poss = round(g["proj_possessions"], 1) if g.get("proj_possessions") is not None else None
        total = round(T, 1)
        # Same TV Rating on both model rows, from the card's v2 win% and possessions.
        # A late-lines run has no KenPom `data`, so it carries the morning snapshot's tv_rating.
        extra = {}
        if qmap is not None:
            extra["tv_rating"] = game_tv_rating(qmap, g["kp_home"], g["kp_away"], win_p,
                                                possessions=poss, total=total)
        elif _clean(g.get("tv_rating")) is not None:
            extra["tv_rating"] = round(float(g["tv_rating"]), 1)
        proj.append({**base, "season": _season(g["game_date"]), "game_id": gid[str(g["event_id"])],
                     "model_version": g.get("model_version", "czarp-v2"),
                     **dict(zip(("proj_home", "proj_away"), team_scores(hm, T, g.get("v1_total")))),
                     "spread": round(-hm, 1), "total": total,
                     "home_win_prob": win_p,
                     "home_cover_prob": round(_ncdf((hm + L) / msd), 4) if L is not None else None,
                     "over_prob": round(1 - _ncdf((MT - T) / tsd), 4) if MT is not None else None,
                     "archetype_tags": g.get("archetype_tags", {}),
                     "reason": g.get("reason"),
                     "proj_possessions": poss,
                     "upset_alert": bool(g.get("upset_alert")),
                     "upset_reason": g.get("upset_reason"),
                     "skip_reasons_spread": g.get("skip_reasons_spread") or [],
                     "skip_reasons_total": g.get("skip_reasons_total") or [],
                     "skip_reason_spread": g.get("skip_reason_spread"),
                     "skip_reason_total": g.get("skip_reason_total"),
                     **_edge_fields(g),
                     **_flag_fields(g),
                     **extra,
                     # full precision (the 0.1-rounded spread/total above are what customers see); the intraday
                     # refresh recomputes edges from these so they match this run exactly
                     "home_margin_full": float(hm), "total_full": float(T),
                     "created_at": ts})
        if g.get("v1_total") is not None:
            t1, m1 = g["v1_total"], g.get("v1_home_margin")
            proj.append({**base, "season": _season(g["game_date"]), "game_id": gid[str(g["event_id"])],
                         "model_version": V1_VERSION, "total": round(t1, 1),
                         "spread": round(-m1, 1) if m1 is not None else None,
                         "over_prob": round(1 - _ncdf((MT - t1) / tsd), 4) if MT is not None else None,
                         "skip_reasons_total": g.get("skip_reasons_total") or [],
                         "skip_reason_total": g.get("skip_reason_total"),
                         **_edge_fields(g),
                         **_flag_fields(g),
                         **extra,
                         "home_margin_full": float(m1) if m1 is not None else None, "total_full": float(t1),
                         "created_at": ts})
    # Merge on the table's unique key so a re-run with the same stamp fills fields
    # (reason, pace, upset, skip reasons, stored edges) instead of silently keeping the old row.
    upsert_projections(c, proj, log=log)

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

    # picks: full ledger upsert. Locked post-time fields (model_line, open_line, edge_vs_open)
    # are attached only on INSERT of new pick_keys so existing immutable rows are not updated.
    existing_keys = {x["pick_key"] for x in c.get("picks", select="pick_key", league=f"eq.{LEAGUE}")}
    pk = []
    lean_on = sync_lean_enabled()
    n_lean_held = 0
    if lean_on:
        try:
            ready = lean_schema_ready(c)
        except Exception as e:
            ready = False
            log(f"  Supabase: CZARP_SYNC_LEAN=1 but lean preflight failed "
                f"({type(e).__name__}: {e}); CZarp Lean picks kept local, official sync continues")
        else:
            if not ready:
                log("  Supabase: CZARP_SYNC_LEAN=1 but lean migration is not applied "
                    "(picks.new_coach missing); CZarp Lean picks kept local, official sync continues")
        if not ready:
            lean_on = False
    for _, r in ledger.iterrows():
        if _str(r.get("tier")) == "lean" and not lean_on:
            n_lean_held += 1              # see sync_lean_enabled(): never mirrored before the db + site are ready
            continue
        k = gid.get(str(r.event_id))
        if k is None:
            continue
        line = float(r.line)
        bet = f"{r.side} {line:+g}" if r.market == "spread" else f"{r.side} {line:g}"
        row = {**base, "season": _season(r.game_date), "pick_key": str(r.pick_id), "game_id": k,
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
                   "pick_hash": _str(r.get("pick_hash"))}
        if lean_on:                       # picks.new_coach exists once the lean migration is applied
            nc = r.get("new_coach")
            row["new_coach"] = (nc.strip().lower() in ("true", "1")) if isinstance(nc, str) else bool(nc is True or nc == 1)
        if str(r.pick_id) not in existing_keys:
            # Locked post-time numbers. Same convention as line_or_price:
            # totals = model total; spreads = picked-side line. Never written on existing rows.
            mv = _clean(pd.to_numeric(r.get("model_value"), errors="coerce"))
            if r.market == "spread" and mv is not None:
                mv = -float(mv)
            evs = _clean(pd.to_numeric(r.get("edge_pct_vs_open"), errors="coerce"))
            tot = _clean(pd.to_numeric(r.get("total_at_post"), errors="coerce"))
            if tot is None and r.market == "total":
                tot = line
            row["fair_at_post"] = round(float(mv), 2) if mv is not None else None
            row["line_at_post"] = line
            row["total_at_post"] = tot
            row["open_line"] = _clean(pd.to_numeric(r.get("open_line"), errors="coerce"))
            row["edge_vs_open"] = round(float(evs), 4) if evs is not None else None
        pk.append(row)
    c.upsert("picks", pk, on_conflict="pick_key", returning=False)
    # Picks are immutable once published (db trigger picks_immutable blocks updates to pick fields and deletes), so
    # mirrored picks missing from the ledger are reported, never deleted.
    if n_lean_held:
        why = "lean migration not applied" if sync_lean_enabled() else "CZARP_SYNC_LEAN off"
        log(f"  Supabase: {n_lean_held} CZarp Lean picks kept local ({why})")
    keep = {str(x) for x in ledger.pick_id}
    remote = {x["pick_key"] for x in c.get("picks", select="pick_key", league=f"eq.{LEAGUE}")}
    stale = sorted(remote - keep)
    if stale:
        log(f"  Supabase: {len(stale)} published picks are not in the local ledger (kept; picks are immutable): "
            + ", ".join(stale[:5]))
    n_locks = 0
    try:
        n_locks = fill_pick_locks(c, log=log)
    except Exception as e:
        log(f"  pick lock fill skipped ({type(e).__name__}: {e})")
    return (f"{len(grows)} games, {len(proj)} projections, {len(ml)} market lines, {len(pk)} picks"
            + (f", {n_profiles} team_profiles" if n_profiles else "")
            + (f", {len(stale)} published picks not in ledger" if stale else "")
            + (f", {n_locks} picks lock-filled" if n_locks else ""))


LINE_PRICE_KEYS = ("home_spread_price", "away_spread_price", "over_price", "under_price")
FULL_PRECISION_COLS = ("home_margin_full", "total_full")   # db/migrations/2026-11-21_intraday_line_refresh.sql
# Drop flags first, then *_full, and only the group named in the PGRST204 error. Prod already has
# *_full; dropping both groups at once would null them out until the lean migration is applied.
OPTIONAL_PROJ_GROUPS = (FLAG_COLS, FULL_PRECISION_COLS)
OPTIONAL_PROJ_COLS = FLAG_COLS + FULL_PRECISION_COLS


def _optional_groups_named(err: str) -> list[tuple[str, ...]]:
    """Optional column groups named in a PostgREST PGRST204 message (one column at a time)."""
    return [g for g in OPTIONAL_PROJ_GROUPS if any(k in err for k in g)]


def upsert_projections(c: "Client", rows: list[dict], log=print) -> None:
    """Upsert projections. If the database is missing optional columns (PostgREST PGRST204),
    retry without only the group named in the error: flag columns first, then full-precision
    columns. home_margin_full / total_full are never dropped unless the error names them."""
    pending, dropped = rows, []
    while True:
        try:
            c.upsert("projections", pending, on_conflict=PROJ_KEY, returning=False)
            if dropped:
                log("  projections: optional columns missing (apply 2026-11-21_intraday_line_refresh.sql / "
                    "2026-11-24_lean_tier_line_moved_away.sql) — written without " + ", ".join(dropped))
            return
        except RuntimeError as e:
            msg = str(e)
            drop = [k for g in _optional_groups_named(msg) for k in g if any(k in r for r in pending)]
            if "PGRST204" not in msg or not drop:
                raise
            pending = [{k: v for k, v in r.items() if k not in drop} for r in pending]
            dropped.extend(drop)


def newest_vegas_current(rows: list[dict]) -> dict[int, dict]:
    """game_id -> newest source='vegas', line_type='current' market_lines row (what slate_games shows)."""
    out: dict[int, dict] = {}
    for r in rows:
        if r.get("source", "vegas") != "vegas" or r.get("line_type", "current") != "current":
            continue
        k = int(r["game_id"])
        if k not in out or str(r.get("fetched_at") or "") > str(out[k].get("fetched_at") or ""):
            out[k] = r
    return out


def missing_line_fill_rows(newest: dict[int, dict], fills: dict[int, dict], ts: str) -> list[dict]:
    """New 'current' vegas rows that fill ONLY what is missing.

    `fills` maps game_id -> fresh lines (current_home_spread / current_total / current_*_price / price_source).
    A game is filled only when its newest current row lacks a spread or a total (or it has none). The new row
    carries the existing spread/total/prices unchanged and adds only the missing value(s), so slate_games (newest
    row wins) never loses a line. Existing rows are never updated or deleted; games with nothing new are skipped."""
    out = []
    for k, f in fills.items():
        old = newest.get(int(k)) or {}
        osp, otot = _clean(old.get("spread")), _clean(old.get("total"))
        if osp is not None and otot is not None:
            continue                                     # nothing missing: never touch it
        nsp, ntot = _clean(f.get("current_home_spread")), _clean(f.get("current_total"))
        add_sp, add_tot = osp is None and nsp is not None, otot is None and ntot is not None
        if not (add_sp or add_tot):
            continue
        prices = dict(old.get("prices") or {})
        for x in LINE_PRICE_KEYS:
            fresh = _clean(f.get(f"current_{x}"))
            if fresh is not None and ((add_sp and "spread" in x) or (add_tot and x in ("over_price", "under_price"))):
                prices[x] = fresh
        out.append({"game_id": int(k), "source": "vegas", "line_type": "current",
                    "book": (f.get("price_source") or old.get("book") or "espn:DraftKings").split(":")[-1],
                    "spread": float(osp) if osp is not None else (float(nsp) if add_sp else None),
                    "total": float(otot) if otot is not None else (float(ntot) if add_tot else None),
                    "prices": _row(prices), "fetched_at": ts})
    return out


def insert_missing_lines(c: "Client", newest: dict[int, dict], fills: dict[int, dict], ts: str, log=print) -> int:
    """Insert-only write of missing_line_fill_rows (no upsert/update/delete; never touches picks)."""
    rows = missing_line_fill_rows(newest, fills, ts)
    if rows:
        c.insert("market_lines", rows)
    log(f"  market_lines: filled {len(rows)} game(s) missing a Vegas line")
    return len(rows)


def _locked_rows(c: Client, vcols: str) -> list[dict]:
    """Post-time lock rows for every tier. picks_locked (2026-11-25 migration) covers official, track and
    lean; official_picks_locked is official-only after that migration. Before it is applied, picks_locked
    does not exist (PGRST205 / 42P01) and the old all-tiers official_picks_locked is read instead."""
    try:
        return c.get("picks_locked", select=vcols)
    except RuntimeError as e:
        if "PGRST205" in str(e) or "42P01" in str(e):
            return c.get("official_picks_locked", select=vcols)
        raise


def fill_pick_locks(c: Client, log=print) -> int:
    """Fill NULL post-time lock columns on published picks (every tier) from the picks_locked view.

    Only NULL columns are written (each PATCH is filtered on `col=is.null`), which is exactly what the
    picks_immutable trigger allows; no hashed or pick fields are touched. The view reads the projection and
    DK lines as of the pick's logged_at, so values reflect post time, not the latest run.
    """
    cols = ",".join(["pick_key"] + [col for col, _ in LOCK_FIELDS])
    picks = c.get("picks", select=cols, league=f"eq.{LEAGUE}")
    todo = {p["pick_key"]: p for p in picks if any(p.get(col) is None for col, _ in LOCK_FIELDS)}
    if not todo:
        return 0
    vcols = ",".join(["pick_id"] + [v for _, v in LOCK_FIELDS])
    locked = {r["pick_id"]: r for r in _locked_rows(c, vcols)}
    n = 0
    for key, p in todo.items():
        v = locked.get(key)
        if not v:
            continue
        row = {col: v[vcol] for col, vcol in LOCK_FIELDS if p.get(col) is None and v.get(vcol) is not None}
        for col, val in row.items():
            # one column per PATCH, guarded by col IS NULL: never overwrites a stored lock
            c.patch("picks", {col: val}, pick_key=f"eq.{key}", **{col: "is.null"})
        n += bool(row)
    if n:
        log(f"  picks: filled post-time lock columns on {n} picks (NULL columns only)")
    return n
