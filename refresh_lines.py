"""refresh_lines.py — intraday Vegas line refresh for the public slate (PAPER ONLY; never posts picks).

Christian's schedule (America/Chicago local time, so DST is handled here, not in cron):
  * normal day : 10 AM, 1 PM, 4 PM, 7 PM CT
  * light day  : 12 PM and 5 PM CT — a slate with fewer than LIGHT_DAY_MAX_GAMES (20) D-I games
The GitHub Actions workflow fires at :05 of every UTC hour that can be one of those CT hours in either CST or CDT;
this script decides from the CT clock and today's game count (a HEAD count, no full read) whether `now` is inside a
run slot, H:00 to H+1:05 CT, so a firing up to 60 minutes late still runs. If a scheduled run starts later than that,
or in an off hour after a slot has opened, it catches up the latest still-unclaimed same-day slot (hours whose H:00 CT
has already passed and that have no line_refresh_runs row; --force slots >= 1000 are ignored) instead of exiting.
Each (CT day, slot) runs once (line_refresh_runs primary key); --force skips the slot gate and uses its own key,
but never the paid-call cap.

Odds API hard cap (fail closed, server-side; see "Odds API: server-side hard cap" below): every run first reads the
API's own x-requests-used / x-requests-remaining from the free /v4/sports endpoint (key in a header). A paid call
needs that read to succeed, credits above the reserve, and a free row in odds_api_refresh_claims (at most 4 per CT
day, enforced by the table's primary key + CHECK). The local/Actions-cache usage file is not used by this job.
Worst case across all jobs: daily 1 + pretip 1 + refresh 4 = 6 paid calls a day.

Each run, for today's games that have not tipped:
  1. pulls the current DraftKings line from ESPN (free); The Odds API only for games ESPN has no line for;
  2. inserts a new source='vegas', line_type='current' market_lines row for a game ONLY when its line or prices
     changed (a value ESPN no longer shows is carried forward from the newest row, never dropped);
  3. for a game whose line inputs changed since its newest projections row, writes a new projections row per model
     (czarp-v2 and czarp-v1-totals): projections copied unchanged; ONLY the changed market's edge_now_pct_*,
     edge_vs_open_pct_*, skip_reasons_*, skip_reason_* and cover/over prob recomputed with daily_run's own rules
     from the full-precision model numbers (projections.home_margin_full / total_full), the other market copied
     as-is; stamped edges_as_of = created_at = this run. An unchanged line writes nothing.
It never reads or writes the CSV ledger, never writes picks (it reads published picks' pnl for the paper bankroll,
and their logged_at so it never writes a projection within PICK_QUIET_MIN = 10 minutes after a pick is logged;
official_picks_locked reads projections up to 5 minutes past logged_at), never touches the picks_immutable trigger
or the hash chain, and never calls KenPom. All reads happen before the first write.

Requires db/migrations/2026-11-21_intraday_line_refresh.sql (checked before any write).

    python refresh_lines.py                # run if now is inside today's refresh slot
    python refresh_lines.py --force        # run now regardless of the slot (paid calls still capped)
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
# The GitHub Actions schedule (main: .github/workflows/line-refresh.yml): every CT run hour in both CST (UTC-6)
# and CDT (UTC-5), at minute 5. Off-hours exit before any network call. tests/test_refresh_lines.py parses the
# workflow file when it is in the checkout, else compares with this snapshot, so the two cannot drift.
WORKFLOW_CRON = "5 0,1,15-19,21-23 * * *"
CRON_MINUTE = 5
CRON_UTC_HOURS = (0, 1, 15, 16, 17, 18, 19, 21, 22, 23)
# A run counts for slot H (e.g. 10 AM) from H:00 until H+1:05 CT: the H:05 firing may start up to 60 minutes late
# and still run, while the next hour's firing (H+1:05, which can never start early) is already outside the window.
SLOT_GRACE_MIN = 65
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


def slot_for(now, hours) -> int | None:
    """The run slot (CT hour) whose window [H:00, H:00 + SLOT_GRACE_MIN) contains `now`, else None.
    Slot windows are on today's CT date only (a 7 PM slot never spills past midnight: 65 min < 5 h)."""
    t = now.astimezone(dr.CENTRAL)
    for h in hours:
        start = t.replace(hour=h, minute=0, second=0, microsecond=0)
        if start <= t < start + timedelta(minutes=SLOT_GRACE_MIN):
            return h
    return None


def due_slots(now, hours) -> list[int]:
    """Today's run hours (CT) whose H:00 has already been reached. Future hours are omitted."""
    t = now.astimezone(dr.CENTRAL)
    return [h for h in hours if t.replace(hour=h, minute=0, second=0, microsecond=0) <= t]


def claimed_real_slots(client, today: str) -> set[int]:
    """Scheduled slots already in line_refresh_runs today. --force keys (>= 1000) are ignored."""
    rows = client.get_all(RUNS_TABLE, order="slot", select="slot", day=f"eq.{today}")
    out = set()
    for r in rows:
        s = _f(r.get("slot"))
        if s is not None and int(s) < FORCED_SLOT_BASE:
            out.add(int(s))
    return out


def latest_unclaimed_slot(now, hours, claimed) -> int | None:
    """The latest due slot that has no line_refresh_runs row yet, else None."""
    taken = {int(s) for s in claimed}
    due = [h for h in due_slots(now, hours) if h not in taken]
    return max(due) if due else None


def maybe_run_hour(now) -> bool:
    """Cheap pre-check (no network): could `now` be inside a run slot on any kind of day?"""
    return slot_for(now, ALL_RUN_HOURS_CT) is not None


def maybe_catch_up(now) -> bool:
    """Cheap pre-check (no network): at least one possible run hour (normal or light) has started today,
    so an off-hour firing might still need to catch up a missed slot."""
    return bool(due_slots(now, ALL_RUN_HOURS_CT))


def should_run(now, n_games: int) -> tuple[bool, str]:
    t = now.astimezone(dr.CENTRAL)
    kind = "light" if is_light_day(n_games) else "normal"
    hours = run_hours(n_games)
    label = ", ".join(f"{(x - 1) % 12 + 1}{'am' if x < 12 else 'pm'}" for x in hours)
    if n_games <= 0:
        return False, "no games on today's slate"
    h = slot_for(now, hours)
    if h is None:
        return False, f"{kind} day ({n_games} games): runs at {label} CT, not {t:%H:%M}"
    late = (t - t.replace(hour=h, minute=CRON_MINUTE, second=0, microsecond=0)).total_seconds() / 60
    return True, f"{kind} day ({n_games} games), {h}:00 CT run" + (f" ({late:.0f} min late)" if late > 15 else "")


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


# ── edges ───────────────────────────────────────────────────────────────────────
# Line inputs each market's edges / skip codes depend on (daily_run.candidates): the spread edge divides by the
# total, so a total move re-stamps the spread too.
SPREAD_INPUTS = ("cur_spread", "cur_total", "open_spread", "open_total", "home_spread_price", "away_spread_price")
TOTAL_INPUTS = ("cur_total", "open_total", "over_price", "under_price")
MARKET_FIELDS = {
    "spread": ("edge_now_pct_spread", "edge_vs_open_pct_spread", "skip_reasons_spread", "skip_reason_spread",
               "home_cover_prob"),
    "total": ("edge_now_pct_total", "edge_vs_open_pct_total", "skip_reasons_total", "skip_reason_total",
              "over_prob"),
}


def line_inputs(cur_row: dict | None, open_row: dict | None) -> dict:
    prices = (cur_row or {}).get("prices") or {}
    out = {"cur_spread": _f((cur_row or {}).get("spread")), "cur_total": _f((cur_row or {}).get("total")),
           "open_spread": _f((open_row or {}).get("spread")), "open_total": _f((open_row or {}).get("total"))}
    for x in PRICE_KEYS:
        out[x] = _f(prices.get(x))
    return out


def changed_markets(now_in: dict, base_in: dict) -> set:
    out = set()
    if any(now_in.get(k) != base_in.get(k) for k in SPREAD_INPUTS):
        out.add("spread")
    if any(now_in.get(k) != base_in.get(k) for k in TOTAL_INPUTS):
        out.add("total")
    return out


def _as_of(rows, ts, key="fetched_at"):
    """Newest row with rows[key] <= ts (the line a projections row stamped at `ts` was computed against)."""
    t = _ts(ts)
    return _newest([r for r in rows if t is not None and _ts(r.get(key)) is not None and _ts(r[key]) <= t], key)


def model_inputs(v2: dict, v1: dict | None) -> tuple[dict, bool]:
    """Full-precision model numbers (projections.home_margin_full / total_full, written by the 7 AM run). Rows
    written before that column existed fall back to the stored 0.1-rounded values; the flag says so."""
    hm, T = _f(v2.get("home_margin_full")), _f(v2.get("total_full"))
    t1 = _f((v1 or {}).get("total_full"))
    exact = hm is not None and T is not None and (v1 is None or t1 is not None)
    if hm is None and _f(v2.get("spread")) is not None:
        hm = -_f(v2["spread"])
    if T is None:
        T = _f(v2.get("total"))
    if t1 is None and v1 is not None:
        t1 = _f(v1.get("total"))
    return {"v2_home_margin": hm, "v2_total": T, "v1_total": t1}, exact


def build_game(game: dict, v2: dict, v1: dict | None, open_row: dict | None, cur_row: dict | None) -> dict:
    """A daily_run-style game dict, so daily_run's own candidates()/attach_skip_reasons() compute the edges and
    skip codes (same formulas, thresholds, hold rules and precision as the 7:15 run)."""
    tip = _ts(game.get("tipoff"))
    li = line_inputs(cur_row, open_row)
    g = {"event_id": str(game.get("external_id") or game["id"]), "game_date": game.get("date"),
         "kp_home": game.get("home"), "kp_away": game.get("away"), "state": "pre",
         "tip_utc": tip.to_pydatetime() if tip is not None else datetime(2100, 1, 1, tzinfo=timezone.utc),
         "open_home_spread": li["open_spread"], "open_total": li["open_total"],
         "current_home_spread": li["cur_spread"], "current_total": li["cur_total"],
         "coach_holds": coach_holds_from(v2)}
    for x in PRICE_KEYS:
        g[f"current_{x}"] = li[x]
    m, _ = model_inputs(v2, v1)
    g.update({k: v for k, v in m.items() if v is not None})
    return g


def projection_rows(g: dict, v2: dict, v1: dict | None, ts: str, markets: set, sds=None) -> list[dict]:
    """New projections rows: every base column copied unchanged; only the fields of `markets` (the markets whose
    line inputs changed) recomputed from g. Unchanged markets keep the base row's edges and skip codes exactly."""
    msd, tsd = sds or _sds()
    L, MT = g.get("current_home_spread"), g.get("current_total")
    hm, T, t1 = g.get("v2_home_margin"), g.get("v2_total"), g.get("v1_total")
    edges = sw._edge_fields(g)
    new2 = {
        "spread": {"edge_now_pct_spread": edges["edge_now_pct_spread"],
                   "edge_vs_open_pct_spread": edges["edge_vs_open_pct_spread"],
                   "skip_reasons_spread": g.get("skip_reasons_spread") or [],
                   "skip_reason_spread": g.get("skip_reason_spread"),
                   "home_cover_prob": round(_ncdf((hm + L) / msd), 4) if hm is not None and L is not None else None},
        "total": {"edge_now_pct_total": edges["edge_now_pct_total"],
                  "edge_vs_open_pct_total": edges["edge_vs_open_pct_total"],
                  "skip_reasons_total": g.get("skip_reasons_total") or [],
                  "skip_reason_total": g.get("skip_reason_total"),
                  "over_prob": round(1 - _ncdf((MT - T) / tsd), 4) if T is not None and MT is not None else None},
    }
    # LINE MOVED AWAY flag rides with the spread inputs; written only once the db has the columns (the base row read
    # back from projections carries them), so an unapplied migration never breaks the refresh insert.
    flags = sw._flag_fields(g)
    if "line_moved_away" in v2:
        new2["spread"].update(flags)
    keep = lambda row: {k: v for k, v in row.items() if k not in ("id", "created_at", "edges_as_of")}
    row = keep(v2)
    for m in markets:
        row.update(new2[m])
    row.update({"created_at": ts, "edges_as_of": ts})
    out = [row]
    if v1 is not None:
        r1 = keep(v1)
        if "spread" in markets:     # the v1 row carries the game-level edges for both markets (supabase_writer.sync)
            r1.update({k: new2["spread"][k] for k in ("edge_now_pct_spread", "edge_vs_open_pct_spread")})
            if "line_moved_away" in v1:
                r1.update(flags)
        if "total" in markets:
            r1.update({k: new2["total"][k] for k in ("edge_now_pct_total", "edge_vs_open_pct_total",
                                                     "skip_reasons_total", "skip_reason_total")})
            r1["over_prob"] = round(1 - _ncdf((MT - t1) / tsd), 4) if t1 is not None and MT is not None else None
        r1.update({"created_at": ts, "edges_as_of": ts})
        out.append(r1)
    return out


def supabase_bankroll(picks: list[dict]) -> float:
    """Paper bankroll the same way daily_run.bankroll() counts it (official tier, graded P&L). Read-only."""
    pnl = sum(_f(r.get("pnl")) or 0.0 for r in picks
              if (r.get("tier") in (None, "official")) and r.get("result") and _f(r.get("pnl")) is not None)
    return dr.BANKROLL0 + pnl


def read_picks(client, ids: str) -> list[dict]:
    return client.get_all("picks", order="id", select="id,game_id,logged_at,created_at,tier,result,pnl",
                          league=f"eq.{sw.LEAGUE}", **({"game_id": f"in.({ids})"} if ids else {}))


def preflight(client) -> None:
    """Fail before any write if the migration is not applied (PostgREST would reject the new columns)."""
    client.get("projections", select="id,edges_as_of,home_margin_full,total_full", limit="1")
    client.get(RUNS_TABLE, select="day", limit="1")
    client.get(CLAIMS_TABLE, select="day", limit="1")


def recompute_edges(client, games: list[dict], now, bankroll: float | None, ts: str, dry_run=False,
                    log=print, picks: list[dict] | None = None) -> dict:
    """Write new projections rows for not-started `games` (rows of the games table) whose line inputs changed since
    their newest projections row. Shared by refresh_lines.py and pretip_check.py. Never posts picks.
      * unchanged line (spread, total, open, prices) -> nothing written;
      * changed market -> recomputed with daily_run.attach_skip_reasons from the full-precision model numbers;
      * the other market's edges / skip codes are copied unchanged.
    Games with a pick logged in the last PICK_QUIET_MIN (10) minutes are skipped (official_picks_locked reads
    projections up to 5 minutes past logged_at)."""
    stats = {"games": 0, "written": 0, "unchanged": 0, "no_projection": 0, "started": 0, "pick_quiet": 0,
             "rounded_inputs": 0}
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
    projs = client.get_all("projections", order="created_at.desc,id.desc", select="*", game_id=f"in.({ids})",
                           model_version=f"in.({V2},{V1})")
    lines = client.get_all("market_lines", order="fetched_at.desc,id.desc",
                           select="id,game_id,source,line_type,book,spread,total,prices,fetched_at",
                           game_id=f"in.({ids})", source="eq.vegas", line_type="in.(open,current)")
    if picks is None:
        picks = read_picks(client, ids)
    if bankroll is None:
        bankroll = supabase_bankroll(picks)
    quiet = {int(p["game_id"]) for p in picks
             if (_ts(p.get("logged_at") or p.get("created_at")) or pd.Timestamp(0, tz="UTC"))
             > utc_now - timedelta(minutes=PICK_QUIET_MIN)}
    by_p, by_l = {}, {}
    for r in projs:
        by_p.setdefault((int(r["game_id"]), r.get("model_version")), []).append(r)
    for r in lines:
        by_l.setdefault((int(r["game_id"]), r.get("line_type")), []).append(r)
    sds, rows = _sds(), []
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
        opens, curs = by_l.get((k, "open"), []), by_l.get((k, "current"), [])
        cur_now, open_now = _newest(curs, "fetched_at"), _newest(opens, "fetched_at")
        base_in = line_inputs(_as_of(curs, v2.get("created_at")), _as_of(opens, v2.get("created_at")))
        markets = changed_markets(line_inputs(cur_now, open_now), base_in)
        stats["games"] += 1
        if not markets:
            stats["unchanged"] += 1
            continue
        g = build_game(game, v2, v1, open_now, cur_now)
        if not model_inputs(v2, v1)[1]:
            stats["rounded_inputs"] += 1
        dr.attach_skip_reasons([g], now, bankroll)
        rows.extend(projection_rows(g, v2, v1, ts, markets, sds))
        stats["written"] += 1
    assert all("pick_key" not in r and "pick_hash" not in r for r in rows)
    if rows and not dry_run:
        client.upsert("projections", rows, on_conflict=sw.PROJ_KEY, returning=False)
    log(f"  edges: {stats['written']} game(s) re-stamped (changed markets only), {stats['unchanged']} unchanged, "
        f"{stats['no_projection']} without a projection, {stats['pick_quiet']} skipped (pick just logged)"
        + (f", {stats['rounded_inputs']} from pre-migration rounded model numbers" if stats["rounded_inputs"] else "")
        + (" [dry run: nothing written]" if dry_run else ""))
    return stats


# ── Odds API: server-side hard cap ─────────────────────────────────────────────
# The refresh job's paid Odds API calls are claimed in Supabase, not in a local/Actions-cache file, so a missing,
# stale or unsaved cache can never hide earlier calls:
#   odds_api_refresh_claims (day, n) primary key, CHECK n between 1 and 4 -> at most 4 refresh paid calls per CT day,
#     enforced by the database even across concurrent runs, --force and re-runs;
#   line_refresh_runs (day, slot) primary key -> one run per scheduled slot per CT day (double firings skip);
#     each row also records the API's own x-requests-used / x-requests-remaining at the start of the run.
# Before any paid call the run must have read the API's usage from the free /v4/sports endpoint (key in a header);
# if that read fails, or credits are under the reserve, or the API's own numbers show >= 6 paid calls account-wide
# since today's first refresh run, the call is refused (ESPN only). daily (1) + pretip (1) stay on the box's file,
# so the worst case is 1 + 1 + 4 = 6 paid calls a day.
CLAIMS_TABLE = "odds_api_refresh_claims"
RUNS_TABLE = "line_refresh_runs"
REFRESH_PAID_CALLS_PER_DAY = 4                 # = CHECK (n between 1 and 4) in the migration
ODDS_CREDITS_PER_CALL = 3                      # spreads, totals, h2h x 1 region (odds_fetcher.fetch_vegas_lines)
FORCED_SLOT_BASE = 1000                        # --force runs: slot = 1000 + CT minute of day (never a real slot)
assert REFRESH_PAID_CALLS_PER_DAY == dr.ODDS_API_JOB_SLOTS["refresh"]
assert dr.ODDS_API_JOB_SLOTS["daily"] + dr.ODDS_API_JOB_SLOTS["pretip"] + REFRESH_PAID_CALLS_PER_DAY \
    <= dr.ODDS_API_MAX_CALLS_PER_DAY


def read_odds_usage(key: str | None, get=None) -> dict | None:
    """The API's own usage from the FREE /v4/sports endpoint (0 credits), key in the x-api-key header.
    None on any failure (fail closed: no paid call)."""
    if not key:
        return None
    try:
        r = (get or dr._S.get)(dr.ODDS_API_SPORTS_URL, headers={"x-api-key": key}, timeout=20)
        if r.status_code != 200:
            return None
        used, rem = float(r.headers.get("x-requests-used")), float(r.headers.get("x-requests-remaining"))
    except Exception:
        return None
    if not (math.isfinite(used) and math.isfinite(rem)):
        return None
    return {"used": used, "remaining": rem}


class RefreshBudget:
    """Claims refresh paid calls in Supabase (see the block comment above). claim() -> (ok, why)."""

    def __init__(self, client, day: str, slot: int, usage: dict | None, baseline_used: float | None, log=print,
                 reread=None):
        self.c, self.day, self.slot, self.usage, self.baseline, self.log = client, day, slot, usage, baseline_used, log
        self.reread = reread

    def claim(self) -> tuple[bool, str]:
        if self.usage is None:
            return False, "Odds API usage read failed (free /v4/sports); refusing paid calls"
        u = self.reread() if self.reread else self.usage     # the API's numbers right before the call
        if u is None:
            return False, "Odds API usage re-read failed; refusing paid call"
        if u["remaining"] < dr.ODDS_API_RESERVE:
            return False, f"{u['remaining']:g} credits left (< reserve {dr.ODDS_API_RESERVE})"
        if self.baseline is None:
            return False, "no verified start-of-day usage baseline; refusing paid call"
        seen = math.ceil(max(0.0, u["used"] - self.baseline) / ODDS_CREDITS_PER_CALL)
        if seen + 1 > dr.ODDS_API_MAX_CALLS_PER_DAY:
            return False, f"API shows {seen} paid calls since today's first refresh run (cap {dr.ODDS_API_MAX_CALLS_PER_DAY})"
        for n in range(1, REFRESH_PAID_CALLS_PER_DAY + 1):
            got = self.c.upsert(CLAIMS_TABLE, [{"day": self.day, "n": n, "slot": self.slot,
                                                "requests_used": u["used"], "requests_remaining": u["remaining"]}],
                                on_conflict="day,n", returning=True, ignore_duplicates=True)
            if got:
                return True, ""
        return False, f"refresh paid-call cap used ({REFRESH_PAID_CALLS_PER_DAY}/{REFRESH_PAID_CALLS_PER_DAY} today)"


def register_run(client, day: str, slot: int, usage: dict | None) -> tuple[bool, float | None]:
    """Insert this run's (day, slot) row. Returns (first run of this slot?, start-of-day API usage baseline =
    the smallest x-requests-used recorded by any refresh run today, None when none was recorded)."""
    got = client.upsert(RUNS_TABLE, [{"day": day, "slot": slot,
                                      "requests_used": (usage or {}).get("used"),
                                      "requests_remaining": (usage or {}).get("remaining")}],
                        on_conflict="day,slot", returning=True, ignore_duplicates=True)
    rows = client.get_all(RUNS_TABLE, order="slot", select="slot,requests_used", day=f"eq.{day}")
    used = [_f(r.get("requests_used")) for r in rows if _f(r.get("requests_used")) is not None]
    return bool(got), (min(used) if used else None)


# ── lines ───────────────────────────────────────────────────────────────────────
def fresh_lines(pre: list[dict], espn_games: dict, lines_for=None, allow_api=True, today="", log=print,
                claim=None) -> dict:
    """game_id -> fresh current lines. ESPN/DK first (free); the Odds API only for games still missing a spread or
    a total, and only through `claim` (RefreshBudget.claim; None = no paid call)."""
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
        _out, dr.log = dr.log, log
        try:
            dr.odds_api_fallback(missing, today, allow=True, job=JOB,
                                 claim=claim or (lambda: (False, "no paid-call budget for this run")))
        except Exception as e:                 # optional; ESPN lines still get written
            log(f"  Odds API fallback failed ({type(e).__name__})")
        finally:
            dr.log = _out
    return work


def espn_index(day: date, scoreboard=None) -> dict:
    sb = scoreboard or dr.espn_scoreboard
    return {str(e["event_id"]): e for e in sb(day)}


def _odds_key():
    try:
        import odds_fetcher as of
        return of.ODDS_API_KEY
    except Exception:
        return None


def run(now, client, today: str | None = None, force=False, dry_run=False, allow_api=True,
        scoreboard=None, lines_for=None, log=print, odds_key=None, usage_get=None) -> dict:
    t = now.astimezone(dr.CENTRAL)
    today = today or t.date().isoformat()
    flt = {"date": f"eq.{today}", "league": f"eq.{sw.LEAGUE}"}
    # the day type needs only a count (HEAD + Prefer: count=exact); the full games read happens only on a run
    n = client.count("games", **flt)
    hours = run_hours(n) if n else ()
    ok, why = should_run(now, n)
    catch_slot = None
    if n and not ok and not force and due_slots(now, hours):
        catch_slot = latest_unclaimed_slot(now, hours, claimed_real_slots(client, today))
        if catch_slot is not None:
            why = f"catch-up of {catch_slot}:00 CT slot; {why}"
    log(f"refresh_lines {today} {t:%H:%M} CT: {why}" + (" [--force]" if force and not ok else ""))
    if not n or not (ok or force or catch_slot is not None):
        return {"ran": False, "reason": why, "games": n}
    slot = catch_slot if catch_slot is not None else (slot_for(now, hours) if ok and not force else None)
    if slot is None:                           # --force: its own key (never a scheduled slot); paid calls still capped
        slot = FORCED_SLOT_BASE + t.hour * 60 + t.minute
    if not dry_run:
        preflight(client)                      # migration applied? fail before any write
    key = odds_key if odds_key is not None else (_odds_key() if allow_api and not dry_run else None)
    usage = read_odds_usage(key, usage_get) if (allow_api and not dry_run and key) else None
    if key and usage is None:
        log("  Odds API usage read failed: ESPN lines only this run")
    budget = None
    if not dry_run:
        first, baseline = register_run(client, today, slot, usage)
        if not first:
            log(f"  slot {slot} already ran today — nothing to do")
            return {"ran": False, "reason": f"slot {slot} already ran", "games": n}
        if usage is not None:
            budget = RefreshBudget(client, today, slot, usage, baseline, log=log,
                                   reread=lambda: read_odds_usage(key, usage_get))
    games = client.get_all("games", order="id", select="id,external_id,home,away,tipoff,status,date", **flt)
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
        return {"ran": True, "reason": why, "games": n, "pre": 0, "lines_written": 0}
    ids = ",".join(str(int(g["id"])) for g in pre)
    picks = read_picks(client, ids)            # every read before the first write
    all_picks = read_picks(client, "")
    newest = sw.newest_vegas_current(client.get_all(
        "market_lines", order="fetched_at.desc,id.desc",
        select="id,game_id,source,line_type,book,spread,total,prices,fetched_at",
        game_id=f"in.({ids})", source="eq.vegas", line_type="eq.current"))
    work = fresh_lines(pre, espn, lines_for=lines_for, allow_api=budget is not None, today=today, log=log,
                       claim=budget.claim if budget else None)
    ts = now.isoformat(timespec="seconds")
    new_lines = [r for k, w in work.items() if (r := line_row_if_changed(k, newest.get(k), w, ts)) is not None]
    if new_lines and not dry_run:
        client.insert("market_lines", new_lines)
    log(f"  market_lines: {len(new_lines)} game(s) with a new line" + (" [dry run]" if dry_run else ""))
    if dry_run:      # the edges below read stored lines; show what the new rows would do without writing them
        client = _Overlay(client, new_lines)
    stats = recompute_edges(client, pre, now, supabase_bankroll(all_picks), ts, dry_run=dry_run, log=log,
                            picks=picks)
    return {"ran": True, "reason": why, "games": n, "pre": len(pre), "lines_written": len(new_lines),
            **{f"edges_{k}": v for k, v in stats.items()}}


class _Overlay:
    """Read-through view of a client with extra (unwritten) market_lines rows, for --dry-run."""

    def __init__(self, c, extra):
        self.c, self.extra = c, extra

    def get_all(self, table, **params):
        rows = self.c.get_all(table, **params)
        return rows + [dict(r) for r in self.extra] if table == "market_lines" else rows

    def __getattr__(self, name):
        return getattr(self.c, name)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--force", action="store_true",
                    help="run now regardless of the slot gate (paid Odds API calls stay capped in Supabase)")
    ap.add_argument("--dry-run", action="store_true", help="no writes, no Odds API call")
    ap.add_argument("--no-odds-api", action="store_true")
    a = ap.parse_args(argv)
    now = dr.now_ct()
    if not a.force and not maybe_run_hour(now) and not maybe_catch_up(now):
        print(f"refresh_lines: {now:%H:%M} CT is not inside a refresh slot "
              f"({', '.join(map(str, ALL_RUN_HOURS_CT))}:00 + {SLOT_GRACE_MIN} min CT) — nothing to do")
        return 0
    if not (os.getenv("SUPABASE_URL") and os.getenv("SUPABASE_SERVICE_KEY")):
        print("refresh_lines: SUPABASE_URL / SUPABASE_SERVICE_KEY not set", file=sys.stderr)
        return 1
    out = run(now, sw.Client(), force=a.force, dry_run=a.dry_run, allow_api=not a.no_odds_api)
    print("refresh_lines: " + ", ".join(f"{k}={v}" for k, v in out.items()))
    return 0


if __name__ == "__main__":
    sys.exit(main())
