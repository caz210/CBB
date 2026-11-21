"""refresh_lines.py — intraday Vegas line refresh for the public slate (PAPER ONLY; never posts picks).

Christian's schedule (America/Chicago local time, so DST is handled here, not in cron):
  * normal day : 10 AM, 1 PM, 4 PM, 7 PM CT
  * light day  : 12 PM and 5 PM CT — a slate with fewer than LIGHT_DAY_MAX_GAMES (20) D-I games
The GitHub Actions workflow fires at every UTC hour that can be one of those CT hours in either CST or CDT;
this script decides from the CT clock and today's game count whether this hour is a run hour (--force skips that).

Each run, for today's games that have not tipped:
  1. pulls the current DraftKings line from ESPN (free); The Odds API only for games ESPN has no line for, through
     daily_run.odds_api_fallback under the 'refresh' slot of the shared daily budget (data/ledger/odds_api_usage.json);
  2. inserts a new source='vegas', line_type='current' market_lines row for a game ONLY when its line or prices
     changed (a value ESPN no longer shows is carried forward from the newest row, never dropped);
  3. writes a new projections row per model (czarp-v2 and czarp-v1-totals) with the 7:15 projections copied
     unchanged and edge_now_pct_*, edge_vs_open_pct_*, skip_reasons_*, skip_reason_*, home_cover_prob and
     over_prob recomputed against the newest line with daily_run's own rules (incl. line_moved_against;
     kelly_too_small for every market whose edge clears the bar; new-coach holds carried over from the 7:15 row),
     stamped edges_as_of = created_at = this run. Rows are written only when something changed, so a re-run in
     the same hour with the same lines writes nothing (idempotent).
It never reads or writes the CSV ledger, never writes picks (only reads published picks' pnl for the paper
bankroll and their logged_at so it never stamps a projection inside official_picks_locked's 5-minute window),
never touches the picks_immutable trigger or the hash chain, and never calls KenPom.

    python refresh_lines.py                # run if this CT hour is a run hour for today's slate
    python refresh_lines.py --force        # run now regardless of the hour (still budget-capped)
    python refresh_lines.py --dry-run      # compute and report only: no writes, no Odds API call
    options: --no-odds-api
"""
from __future__ import annotations

import argparse
import math
import os
import sys
from datetime import date, datetime, timedelta, timezone

import pandas as pd

import daily_run as dr
from czarp import supabase_writer as sw

JOB = "refresh"
LIGHT_DAY_MAX_GAMES = 20                 # fewer than this many D-I games on today's slate = a light day
NORMAL_RUN_HOURS_CT = (10, 13, 16, 19)   # 10 AM, 1 PM, 4 PM, 7 PM CT
LIGHT_RUN_HOURS_CT = (12, 17)            # 12 PM, 5 PM CT
ALL_RUN_HOURS_CT = tuple(sorted(set(NORMAL_RUN_HOURS_CT) | set(LIGHT_RUN_HOURS_CT)))
# UTC hours the GitHub Actions workflow (main: .github/workflows/line-refresh.yml) fires at: every CT run hour in
# both CST (UTC-6) and CDT (UTC-5). Off-hours exit before any network call. Kept in sync by a test.
CRON_UTC_HOURS = (0, 1, 15, 16, 17, 18, 19, 21, 22, 23)
PICK_QUIET_MIN = 10   # never stamp a projection within this many minutes of a pick being logged on that game
V2, V1 = sw.DEFAULT_VERSION, sw.V1_VERSION
EDGE_COLS = dr.EDGE_FIELDS
SKIP_COLS = ("skip_reasons_spread", "skip_reasons_total", "skip_reason_spread", "skip_reason_total")
PROB_COLS = ("home_cover_prob", "over_prob")
RECOMPUTED = EDGE_COLS + SKIP_COLS + PROB_COLS
NOT_COPIED = {"id", "created_at", "edges_as_of"} | set(RECOMPUTED)
PRICE_KEYS = sw.LINE_PRICE_KEYS


# ── schedule ────────────────────────────────────────────────────────────────────
def is_light_day(n_games: int) -> bool:
    return n_games < LIGHT_DAY_MAX_GAMES


def run_hours(n_games: int) -> tuple[int, ...]:
    return LIGHT_RUN_HOURS_CT if is_light_day(n_games) else NORMAL_RUN_HOURS_CT


def maybe_run_hour(now) -> bool:
    """Cheap pre-check (no network): could this CT hour be a run hour on any kind of day?"""
    return now.astimezone(dr.CENTRAL).hour in ALL_RUN_HOURS_CT


def should_run(now, n_games: int) -> tuple[bool, str]:
    h = now.astimezone(dr.CENTRAL).hour
    kind = "light" if is_light_day(n_games) else "normal"
    hours = run_hours(n_games)
    label = ", ".join(f"{(x - 1) % 12 + 1}{'am' if x < 12 else 'pm'}" for x in hours)
    if n_games <= 0:
        return False, "no games on today's slate"
    if h not in hours:
        return False, f"{kind} day ({n_games} games): runs at {label} CT, not {h}:00"
    return True, f"{kind} day ({n_games} games), {h}:00 CT run"


# ── helpers ─────────────────────────────────────────────────────────────────────
def _f(v):
    v = sw._clean(v)
    if v is None or v == "":
        return None
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(x) else x


def _ts(v):
    if not v:
        return None
    t = pd.Timestamp(v)
    return t.tz_localize("UTC") if t.tzinfo is None else t


def _newest(rows, key="created_at"):
    rows = [r for r in rows if _ts(r.get(key)) is not None]
    return max(rows, key=lambda r: _ts(r[key])) if rows else None


def _norm(v):
    """Comparable form of a stored/recomputed value (numeric(…,4) round-trip, lists, None)."""
    if isinstance(v, (list, tuple)):
        return tuple(v)
    if isinstance(v, bool):
        return v
    x = _f(v) if not isinstance(v, str) else (_f(v) if _f(v) is not None else v)
    return round(x, 4) if isinstance(x, float) else x


def _ncdf(x: float) -> float:
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def _sds() -> tuple[float, float]:
    try:
        from czarp.config import load_config
        b = load_config().betting
        return float(b.margin_sd), float(b.total_sd)
    except Exception:
        return 11.4, 16.9


def coach_holds_from(row: dict | None) -> dict:
    """The 7:15 run's new-coach holds per market, from its stored codes + text (KenPom is not re-read intraday;
    games-played counts do not change during the day)."""
    out = {"spread": [], "total": []}
    for m in out:
        codes = (row or {}).get(f"skip_reasons_{m}") or []
        if "new_coach_lock" not in codes:
            continue
        text = (row or {}).get(f"skip_reason_{m}") or ""
        held = [s for s in text.split("; ") if dr.is_coach_hold_reason(s)]
        out[m] = held or ["Paused until game (new coach)"]
    return out


def merged_line(old: dict | None, fresh: dict) -> dict:
    """Newest current values: fresh where present, else carried forward from the newest stored row."""
    old = old or {}
    oprices = dict(old.get("prices") or {})
    sp, tot = _f(fresh.get("current_home_spread")), _f(fresh.get("current_total"))
    prices = dict(oprices)
    # A fresh price replaces the stored one. A stored price is dropped only when its line moved and no fresh
    # price came with the new number (an old price must not sit next to a different line).
    for val, oval, keys in ((sp, _f(old.get("spread")), ("home_spread_price", "away_spread_price")),
                            (tot, _f(old.get("total")), ("over_price", "under_price"))):
        if val is None:
            continue
        for x in keys:
            v = _f(fresh.get(f"current_{x}"))
            if v is not None:
                prices[x] = v
            elif val != oval:
                prices.pop(x, None)
    return {"spread": sp if sp is not None else _f(old.get("spread")),
            "total": tot if tot is not None else _f(old.get("total")),
            "prices": {k: prices[k] for k in PRICE_KEYS if prices.get(k) is not None},
            "book": (fresh.get("price_source") or "").split(":")[-1] or old.get("book") or "DraftKings"}


def line_row_if_changed(game_id: int, old: dict | None, fresh: dict, ts: str) -> dict | None:
    """A new market_lines 'current' row when the line or prices changed, else None. Never drops a value."""
    m = merged_line(old, fresh)
    if m["spread"] is None and m["total"] is None:
        return None
    if old is not None:
        oprices = {k: _f(v) for k, v in (old.get("prices") or {}).items() if k in PRICE_KEYS and _f(v) is not None}
        if (_f(old.get("spread")), _f(old.get("total")), oprices) == (m["spread"], m["total"], m["prices"]):
            return None
    return {"game_id": int(game_id), "source": "vegas", "line_type": "current", "book": m["book"],
            "spread": m["spread"], "total": m["total"], "prices": m["prices"], "fetched_at": ts}


def build_game(game: dict, v2: dict, v1: dict | None, open_row: dict | None, cur_row: dict | None) -> dict:
    """A daily_run-style game dict from stored rows, so daily_run's own candidates()/attach_skip_reasons()
    compute the edges and skip codes (same formulas, thresholds and hold rules as the 7:15 run)."""
    tip = _ts(game.get("tipoff"))
    prices = (cur_row or {}).get("prices") or {}
    g = {"event_id": str(game.get("external_id") or game["id"]), "game_date": game.get("date"),
         "kp_home": game.get("home"), "kp_away": game.get("away"), "state": "pre",
         "tip_utc": tip.to_pydatetime() if tip is not None else datetime(2100, 1, 1, tzinfo=timezone.utc),
         "open_home_spread": _f((open_row or {}).get("spread")), "open_total": _f((open_row or {}).get("total")),
         "current_home_spread": _f((cur_row or {}).get("spread")), "current_total": _f((cur_row or {}).get("total")),
         "coach_holds": coach_holds_from(v2)}
    for x in PRICE_KEYS:
        g[f"current_{x}"] = _f(prices.get(x))
    if _f(v2.get("spread")) is not None:
        g["v2_home_margin"] = -_f(v2["spread"])
    if _f(v2.get("total")) is not None:
        g["v2_total"] = _f(v2["total"])
    if v1 is not None and _f(v1.get("total")) is not None:
        g["v1_total"] = _f(v1["total"])
    return g


def projection_rows(g: dict, v2: dict, v1: dict | None, ts: str, sds=None) -> list[dict]:
    """New projections rows: the base rows' projections copied unchanged, market-dependent fields recomputed."""
    msd, tsd = sds or _sds()
    L, MT = g.get("current_home_spread"), g.get("current_total")
    edges = sw._edge_fields(g)
    out = []
    hm, T = g.get("v2_home_margin"), g.get("v2_total")
    row = {k: v for k, v in v2.items() if k not in NOT_COPIED}
    row.update(edges)
    row.update({"skip_reasons_spread": g.get("skip_reasons_spread") or [],
                "skip_reasons_total": g.get("skip_reasons_total") or [],
                "skip_reason_spread": g.get("skip_reason_spread"),
                "skip_reason_total": g.get("skip_reason_total"),
                "home_cover_prob": round(_ncdf((hm + L) / msd), 4) if hm is not None and L is not None else None,
                "over_prob": round(1 - _ncdf((MT - T) / tsd), 4) if T is not None and MT is not None else None,
                "created_at": ts, "edges_as_of": ts})
    out.append(row)
    if v1 is not None:
        t1 = g.get("v1_total")
        r1 = {k: v for k, v in v1.items() if k not in NOT_COPIED}
        r1.update(edges)
        r1.update({"skip_reasons_total": g.get("skip_reasons_total") or [],
                   "skip_reason_total": g.get("skip_reason_total"),
                   "over_prob": round(1 - _ncdf((MT - t1) / tsd), 4) if t1 is not None and MT is not None else None,
                   "created_at": ts, "edges_as_of": ts})
        out.append(r1)
    return out


def _changed(new: dict, base: dict) -> bool:
    return any(_norm(new.get(k)) != _norm(base.get(k)) for k in RECOMPUTED if k in new)


def supabase_bankroll(client) -> float:
    """Paper bankroll the same way daily_run.bankroll() counts it (official tier, graded P&L). Read-only."""
    rows = client.get("picks", select="tier,result,pnl", league=f"eq.{sw.LEAGUE}")
    pnl = sum(_f(r.get("pnl")) or 0.0 for r in rows
              if (r.get("tier") in (None, "official")) and r.get("result") and _f(r.get("pnl")) is not None)
    return dr.BANKROLL0 + pnl


def recompute_edges(client, games: list[dict], now, bankroll: float, ts: str, force_ids=(), dry_run=False,
                    log=print) -> dict:
    """Write recomputed projections rows for not-started `games` (rows of the games table) against the newest
    stored Vegas current line. Shared by refresh_lines.py and pretip_check.py. Never posts picks.
    `force_ids`: game ids whose line just changed (a row is written even when no edge moved, so edges_as_of
    is never older than market_fetched_at). Returns counts."""
    stats = {"games": 0, "written": 0, "unchanged": 0, "no_projection": 0, "started": 0, "pick_quiet": 0}
    utc_now = pd.Timestamp(now).tz_convert("UTC")
    todo = []
    for g in games:
        tip = _ts(g.get("tipoff"))
        if g.get("status") in ("final", "in_progress") or (tip is not None and tip <= utc_now):
            stats["started"] += 1
            continue
        todo.append(g)
    if not todo:
        return stats
    ids = ",".join(str(int(g["id"])) for g in todo)
    projs = client.get("projections", select="*", game_id=f"in.({ids})",
                       model_version=f"in.({V2},{V1})", order="created_at.desc")
    lines = client.get("market_lines", select="game_id,source,line_type,book,spread,total,prices,fetched_at",
                       game_id=f"in.({ids})", source="eq.vegas", line_type="in.(open,current)")
    recent = client.get("picks", select="game_id,logged_at,created_at", game_id=f"in.({ids})")
    quiet = {int(p["game_id"]) for p in recent
             if (_ts(p.get("logged_at") or p.get("created_at")) or pd.Timestamp(0, tz="UTC"))
             > utc_now - timedelta(minutes=PICK_QUIET_MIN)}
    by_p, by_l = {}, {}
    for r in projs:
        by_p.setdefault((int(r["game_id"]), r.get("model_version")), []).append(r)
    for r in lines:
        by_l.setdefault((int(r["game_id"]), r.get("line_type")), []).append(r)
    sds, rows = _sds(), []
    force = {int(x) for x in force_ids}
    for game in todo:
        k = int(game["id"])
        v2 = _newest(by_p.get((k, V2), []))
        v1 = _newest(by_p.get((k, V1), []))
        if v2 is None:
            stats["no_projection"] += 1
            continue
        if k in quiet:
            stats["pick_quiet"] += 1
            continue
        g = build_game(game, v2, v1, _newest(by_l.get((k, "open"), []), "fetched_at"),
                       _newest(by_l.get((k, "current"), []), "fetched_at"))
        dr.attach_skip_reasons([g], now, bankroll)
        new = projection_rows(g, v2, v1, ts, sds)
        stats["games"] += 1
        if k in force or _changed(new[0], v2) or (v1 is not None and len(new) > 1 and _changed(new[1], v1)):
            rows.extend(new)
            stats["written"] += 1
        else:
            stats["unchanged"] += 1
    assert all("pick_key" not in r and "pick_hash" not in r for r in rows)
    if rows and not dry_run:
        client.upsert("projections", rows, on_conflict=sw.PROJ_KEY, returning=False)
    log(f"  edges: {stats['written']} game(s) re-stamped, {stats['unchanged']} unchanged, "
        f"{stats['no_projection']} without a projection, {stats['pick_quiet']} skipped (pick just logged)"
        + (" [dry run: nothing written]" if dry_run else ""))
    return stats


# ── lines ───────────────────────────────────────────────────────────────────────
def fresh_lines(pre: list[dict], espn_games: dict, lines_for=None, allow_api=True, today="", log=print) -> dict:
    """game_id -> fresh current lines. ESPN/DK first (free); the Odds API ('refresh' slot) only for games
    still missing a spread or a total."""
    lines_for = lines_for or dr.espn_dk_lines
    work = {}
    for g in pre:
        eid = str(g["external_id"])
        e = espn_games.get(eid, {})
        f = lines_for(eid) or {}
        w = {"event_id": eid, "home_full": e.get("home_full") or g["home"], "away_full": e.get("away_full") or g["away"]}
        for x in ("current_home_spread", "current_total") + tuple(f"current_{p}" for p in PRICE_KEYS):
            w[x] = _f(f.get(x))
        w["price_source"] = f"espn:{f.get('book') or 'DraftKings'}"
        work[int(g["id"])] = w
    missing = [w for w in work.values() if w["current_home_spread"] is None or w["current_total"] is None]
    log(f"  ESPN/DK: {len(work) - len(missing)} of {len(work)} games with both lines")
    if missing and allow_api:
        n_issues = len(dr.ODDS_API_COUNTER_ISSUES)
        _out, dr.log = dr.log, log
        try:
            dr.odds_api_fallback(missing, today, allow=True, job=JOB)
        except Exception as e:                 # optional; ESPN lines still get written
            log(f"  Odds API fallback failed ({type(e).__name__}: {e})")
        finally:
            dr.log = _out
        for why in dr.ODDS_API_COUNTER_ISSUES[n_issues:]:
            log(f"  Odds API call refused (fail closed): {why}")
    return work


def espn_index(day: date, scoreboard=None) -> dict:
    sb = scoreboard or dr.espn_scoreboard
    return {str(e["event_id"]): e for e in sb(day)}


def run(now, client, today: str | None = None, force=False, dry_run=False, allow_api=True,
        scoreboard=None, lines_for=None, log=print) -> dict:
    today = today or now.astimezone(dr.CENTRAL).date().isoformat()
    games = client.get("games", select="id,external_id,home,away,tipoff,status,date",
                       date=f"eq.{today}", league=f"eq.{sw.LEAGUE}")
    ok, why = should_run(now, len(games))
    log(f"refresh_lines {today} {now.astimezone(dr.CENTRAL):%H:%M} CT: {why}" + (" [--force]" if force and not ok else ""))
    if not games or not (ok or force):
        return {"ran": False, "reason": why, "games": len(games)}
    espn = espn_index(date.fromisoformat(today), scoreboard)
    utc_now = pd.Timestamp(now).tz_convert("UTC")
    pre = []
    for g in games:
        e = espn.get(str(g.get("external_id")))
        tip = _ts(g.get("tipoff")) or (pd.Timestamp(e["tip_utc"]) if e else None)
        if g.get("status") == "final" or not e or e.get("state") != "pre" or tip is None or tip <= utc_now:
            continue
        pre.append(g)
    log(f"  {len(pre)} of {len(games)} games not started")
    if not pre:
        return {"ran": True, "reason": why, "games": len(games), "pre": 0, "lines_written": 0}
    work = fresh_lines(pre, espn, lines_for=lines_for, allow_api=allow_api and not dry_run, today=today, log=log)
    ids = ",".join(str(int(g["id"])) for g in pre)
    newest = sw.newest_vegas_current(client.get(
        "market_lines", select="game_id,source,line_type,book,spread,total,prices,fetched_at",
        game_id=f"in.({ids})", source="eq.vegas", line_type="eq.current"))
    ts = now.isoformat(timespec="seconds")
    new_lines = [r for k, w in work.items() if (r := line_row_if_changed(k, newest.get(k), w, ts)) is not None]
    if new_lines and not dry_run:
        client.insert("market_lines", new_lines)
    log(f"  market_lines: {len(new_lines)} game(s) with a new line" + (" [dry run]" if dry_run else ""))
    stats = recompute_edges(client, pre, now, supabase_bankroll(client), ts,
                            force_ids={r["game_id"] for r in new_lines}, dry_run=dry_run, log=log)
    return {"ran": True, "reason": why, "games": len(games), "pre": len(pre), "lines_written": len(new_lines),
            **{f"edges_{k}": v for k, v in stats.items()}}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--force", action="store_true", help="run now regardless of the CT hour gate")
    ap.add_argument("--dry-run", action="store_true", help="no writes, no Odds API call")
    ap.add_argument("--no-odds-api", action="store_true")
    a = ap.parse_args(argv)
    now = dr.now_ct()
    if not a.force and not maybe_run_hour(now):
        print(f"refresh_lines: {now:%H:%M} CT is not a refresh hour ({', '.join(map(str, ALL_RUN_HOURS_CT))}) — nothing to do")
        return 0
    if not (os.getenv("SUPABASE_URL") and os.getenv("SUPABASE_SERVICE_KEY")):
        print("refresh_lines: SUPABASE_URL / SUPABASE_SERVICE_KEY not set", file=sys.stderr)
        return 1
    out = run(now, sw.Client(), force=a.force, dry_run=a.dry_run, allow_api=not a.no_odds_api)
    print("refresh_lines: " + ", ".join(f"{k}={v}" for k, v in out.items()))
    return 0


if __name__ == "__main__":
    sys.exit(main())
