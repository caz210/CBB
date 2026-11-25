"""pretip_check.py — pre-tip line check for today's open paper plays.

Re-pulls the current DraftKings line (ESPN's free feed; The Odds API only for plays ESPN has no line
for, using the pre-tip job's slot of the shared daily budget in data/ledger/odds_api_usage.json)
for every open ledger play that has not tipped yet, and reports a play when
  * its line moved >= --move pts AGAINST us since the pick (negative CLV so far), or
  * its edge% vs the current line dropped below the rule threshold (or flipped sides).
It also reports BIG MOVES (DK total moved >= 4 pts or spread >= 3 pts open -> now, either direction)
on ANY game tipping within --big-within minutes (default 90), not only our plays — info only.
Prints a short text summary; prints NOTHING when there is nothing to flag (exit 0 either way).
It never changes prices, stakes or graded results: the only ledger write is the 'pretip_flag' column
of open, not-yet-started plays (skipped with --dry-run).
MISSING LINES: today's not-started games whose newest Vegas line in Supabase market_lines lacks a spread or a
total get the missing value(s) filled (ESPN/DK first, then the reserved Odds API call, official plays first,
then by tip time) as new insert-only market_lines rows, then re-stamp those games' edges / skip reasons with a new
projections row (refresh_lines.recompute_edges, the same function the intraday refresh uses). Existing lines,
picks, grading and the ledger are never touched. Skipped with --dry-run; --no-odds-api still fills from ESPN.

    python pretip_check.py                 # today's slate (America/Chicago), annotate ledger
CLOSING LINES: open official and lean plays tipping within --close-within minutes (default 30) get a closing-line
snapshot in Supabase closing_line_snapshots (insert-only): ESPN/DK every run, plus The Odds API when a target game has
no Odds API snapshot yet. All Odds API use in one run shares ONE claimed 'pretip' call (daily_run.ODDS_API_JOB_SLOTS).

    python pretip_check.py --dry-run       # report only, no ledger write, no Odds API call
    options: --date YYYY-MM-DD  --within MIN (only games tipping in the next MIN minutes)
             --move PTS (default 2)  --no-odds-api  -v (progress to stderr)
             --big-within MIN (default 90; 0 = no big-move section)
             --close-within MIN (default 30; 0 = no closing-line snapshot)
"""
from __future__ import annotations

import argparse
import contextlib
import os
import sys
from datetime import timedelta

import pandas as pd

import daily_run as dr

MOVE_PTS = 2.0
BIG_WITHIN_MIN = 90


def _snapshot(today: str) -> dict:
    """event_id -> latest daily_run snapshot row (CZarp numbers + KenPom names) for today."""
    import glob
    import os
    files = sorted(glob.glob(os.path.join(dr.SNAP_DIR, f"{today}_*.csv")))
    if not files:
        return {}
    try:
        df = pd.read_csv(files[-1], dtype={"event_id": str})
    except Exception:
        return {}
    out = {}
    for r in df.to_dict("records"):
        out[str(r["event_id"])] = {k: (None if (not isinstance(v, str) and pd.isna(v)) else v) for k, v in r.items()}
    return out


def big_moves_soon(today: str, now, within: float, led: pd.DataFrame, log=lambda m: None,
                   scoreboard=None, lines=None) -> list[dict]:
    """BIG MOVES on any not-started game tipping within `within` minutes (fresh DK lines)."""
    from czarp import big_moves as BM
    from datetime import date
    scoreboard = scoreboard or dr.espn_scoreboard
    lines = lines or dr.espn_dk_lines
    utc_now = now.astimezone(dr.timezone.utc)
    games = [g for g in scoreboard(date.fromisoformat(today))
             if g.get("state") == "pre" and utc_now < g["tip_utc"] <= utc_now + timedelta(minutes=within)]
    if not games:
        return []
    snap, ctx = _snapshot(today), BM.load_context(today)
    reasons = ctx.get("reasons", {})
    off = led[(led.tier.fillna("official") == "official") & (led.status != "void")] if len(led) else led
    played = set(off.pick_id.astype(str)) if len(off) else set()
    out = []
    for g in games:
        s = snap.get(str(g["event_id"]), {})
        g = {**g, **{k: s.get(k) for k in ("kp_home", "kp_away", "v1_total", "v2_total", "v2_home_margin") if s.get(k) is not None}}
        g.update({k: v for k, v in (lines(str(g["event_id"])) or {}).items() if v is not None})
        for m in ("total", "spread"):
            pid = f"{g['event_id']}-{m}"
            r = BM.build_row(g, m, None, official=pid in played)
            if r is None:
                continue
            if not r["official_pick"] and reasons.get(pid):
                r["skip_reason"] = reasons[pid]
            out.append(r)
    log(f"  big moves tipping within {within:g} min: {len(out)}")
    return out


def side_line(row, ln: dict):
    """(current line from the picked side's perspective, current total) from ESPN/DK lines."""
    T = ln.get("current_total")
    if row.market == "spread":
        L = ln.get("current_home_spread")
        if L is None:
            return None, T
        return (L if row.side == row.home else -L), T
    return T, T


def check_play(row, ln: dict, move_pts: float = MOVE_PTS) -> list[str]:
    """Reasons to flag one ledger play given fresh DK lines (empty list = fine)."""
    cur, T = side_line(row, ln)
    if cur is None:
        return ["no current DK line (could be off the board — check news)"]
    pick, mv = float(row.line), float(row.model_value)
    thr = dr.RULES["spread" if row.market == "spread" else "total"][1]
    out = []
    if row.market == "spread":
        clv = pick - cur                       # same convention as grade(): + = we beat the current number
        gap = mv + cur                         # model margin for our side + our side's spread
        edge = abs(gap) / T if T else None
        same = gap > 0
    else:
        over = row.side == "Over"
        clv = (cur - pick) if over else (pick - cur)
        gap = (mv - cur) if over else (cur - mv)
        edge = abs(mv - cur) / cur if cur else None
        same = gap > 0
    if clv <= -move_pts:
        out.append(f"line moved {-clv:g} against us ({pick:+g} → {cur:+g})" if row.market == "spread"
                   else f"line moved {-clv:g} against us ({pick:g} → {cur:g})")
    # totals: only the line-move check (edge vs current is no longer a totals rule — daily_run.py)
    if row.market == "spread" and edge is not None and (not same or edge < thr):
        out.append(f"edge vs current {edge:.1%} < {thr:.0%}" + ("" if same else " (model now on the other side)"))
    return out


def _run_odds_api(games: list[dict], today: str, log, cache: dict | None = None) -> None:
    """daily_run.odds_api_fallback under the 'pretip' slot. Its prints go to stderr: stdout is the alert text.
    `cache`: this run's shared Odds API board (one paid call per pretip_check run, whoever asks first)."""
    n_issues = len(dr.ODDS_API_COUNTER_ISSUES)
    try:
        dr.log_lines.clear()
        _out, dr.log = dr.log, log
        with contextlib.redirect_stdout(sys.stderr):
            dr.odds_api_fallback(games, today, allow=True, job="pretip", cache=cache)
    except Exception as e:                     # optional; never block the play checks
        log(f"  Odds API fallback failed ({type(e).__name__}: {e})")
    finally:
        dr.log = _out
    for why in dr.ODDS_API_COUNTER_ISSUES[n_issues:]:
        # always shown (no -v needed), on stderr: stdout is the alert text
        print(f"pretip_check: Odds API call refused (fail closed): {why}. "
              "Check with `python3 -m czarp.odds_usage --check`; recovery steps in CZARP_V2.md.", file=sys.stderr)


def _odds_api_lines(rows, today, allow, log, cache: dict | None = None) -> dict:
    """Odds API lines for plays ESPN has no line for, via daily_run's budget-aware fallback."""
    if not allow or not rows:
        return {}
    games = [{"event_id": str(r.event_id), "home_full": r.home, "away_full": r.away,
              "current_home_spread": None, "current_total": None} for r in rows]
    _run_odds_api(games, today, log, cache)
    return {g["event_id"]: g for g in games if g.get("current_home_spread") is not None or g.get("current_total") is not None}


def fill_missing_lines(today: str, now, led: pd.DataFrame, allow_api: bool, log=lambda m: None,
                       client=None, lines=None, cache: dict | None = None) -> dict:
    """Fill today's not-started games whose newest Vegas line in market_lines lacks a spread or total.

    Order: official open plays first, then by tip time. ESPN/DK (free) first; the reserved pre-tip Odds API call
    only if something is still missing. Writes insert-only market_lines rows via czarp.supabase_writer (missing
    values only; existing lines kept). Returns {event_id: fresh lines} for every game it found lines for, so the
    play checks below can use them. Never touches picks, grading or the ledger."""
    from czarp import supabase_writer as sw
    if client is None:
        if not (os.getenv("SUPABASE_URL") and os.getenv("SUPABASE_SERVICE_KEY")):
            log("  missing lines: no Supabase credentials — skipped")
            return {}
        client = sw.Client()
    lines = lines or dr.espn_dk_lines
    utc_now = pd.Timestamp(now).tz_convert("UTC")
    games = [g for g in client.get_all("games", order="id", select="id,external_id,home,away,tipoff,status",
                                           date=f"eq.{today}")
             if g.get("status") != "final" and g.get("tipoff") and pd.Timestamp(g["tipoff"]) > utc_now]
    if not games:
        return {}
    ids = ",".join(str(int(g["id"])) for g in games)
    newest = sw.newest_vegas_current(client.get_all(
        "market_lines", order="fetched_at.desc,id.desc",
        select="id,game_id,source,line_type,book,spread,total,prices,fetched_at",
        game_id=f"in.({ids})", source="eq.vegas", line_type="eq.current"))
    gaps = [g for g in games if sw._clean((newest.get(int(g["id"])) or {}).get("spread")) is None
            or sw._clean((newest.get(int(g["id"])) or {}).get("total")) is None]
    if not gaps:
        log("  missing lines: none — 0 Odds API credits used")
        return {}
    off = set()
    if len(led):
        o = led[(led.status == "open") & (led.tier.fillna("official") == "official") & (led.game_date.astype(str) == today)]
        off = set(o.event_id.astype(str))
    gaps.sort(key=lambda g: (str(g["external_id"]) not in off, str(g["tipoff"])))
    snap = _snapshot(today)
    work = []
    for g in gaps:
        old = newest.get(int(g["id"])) or {}
        s = snap.get(str(g["external_id"]), {})
        w = {"game_id": int(g["id"]), "event_id": str(g["external_id"]),
             "home_full": s.get("home_full") or g["home"], "away_full": s.get("away_full") or g["away"],
             "current_home_spread": sw._clean(old.get("spread")), "current_total": sw._clean(old.get("total"))}
        fresh = lines(w["event_id"]) or {}
        for key in ("current_home_spread", "current_total"):
            if w[key] is None and fresh.get(key) is not None:
                w[key] = fresh[key]
                pfx = ("current_home_spread_price", "current_away_spread_price") if key == "current_home_spread" \
                    else ("current_over_price", "current_under_price")
                for x in pfx:
                    w[x] = fresh.get(x)
                w["price_source"] = f"espn:{fresh.get('book') or 'DraftKings'}"
        work.append(w)
    still = [w for w in work if w["current_home_spread"] is None or w["current_total"] is None]
    log(f"  missing lines: {len(gaps)} game(s); {len(gaps) - len(still)} filled from ESPN/DK, {len(still)} left")
    if still and allow_api:
        _run_odds_api(still, today, log, cache)
    fills = {w["game_id"]: w for w in work}
    ts = now.isoformat(timespec="seconds")
    n = sw.insert_missing_lines(client, newest, fills, ts, log=log)
    if n:
        # A new current line must never sit next to a stale edge: recompute edges / skip reasons for the games
        # that just got a row, with the same function the intraday refresh uses (never posts picks).
        filled_ids = {r["game_id"] for r in sw.missing_line_fill_rows(newest, fills, ts)}
        try:
            import refresh_lines as RL
            RL.recompute_edges(client, [g for g in games if int(g["id"]) in filled_ids], now,
                               dr.bankroll(led) if len(led) else dr.BANKROLL0, ts, log=log)
        except Exception as e:                 # never block the play checks
            log(f"  edge recompute after fill failed ({type(e).__name__}: {e})")
    return {w["event_id"]: w for w in work
            if w["current_home_spread"] is not None or w["current_total"] is not None}


# ── Closing-line snapshot ───────────────────────────────────────────────────────
CLOSE_WINDOW_MIN = 30          # plays tipping within the next 30 minutes get a closing-line snapshot
CLOSE_TABLE = "closing_line_snapshots"
CLOSE_TIERS = ("official", "lean")


def closing_targets(led: pd.DataFrame, today: str, now, window: float = CLOSE_WINDOW_MIN) -> dict:
    """event_id -> {"tip": naive CT Timestamp, "pick_ids": [...]} for today's OPEN official/lean plays whose game tips
    within `window` minutes (and has not tipped). Track-tier and void/graded rows are never targets."""
    if led is None or not len(led):
        return {}
    naive_now = pd.Timestamp(now.replace(tzinfo=None))
    tips = pd.to_datetime(led.tip_ct, errors="coerce")
    tier = led.tier.fillna("official") if "tier" in led else pd.Series("official", index=led.index)
    m = ((led.status == "open") & (led.game_date.astype(str) == today) & tier.isin(CLOSE_TIERS)
         & (tips > naive_now) & (tips <= naive_now + timedelta(minutes=window)))
    out = {}
    for i in led.index[m]:
        t = out.setdefault(str(led.at[i, "event_id"]), {"tip": tips[i], "pick_ids": []})
        t["pick_ids"].append(str(led.at[i, "pick_id"]))
    return out


def _snap_row(g: dict, ev: str, today: str, source: str, book, ln: dict, ts: str, now, pick_ids) -> dict | None:
    spread, total = ln.get("current_home_spread"), ln.get("current_total")
    if spread is None and total is None:
        return None
    tip = pd.Timestamp(g["tipoff"]) if g.get("tipoff") else None
    mins = round((tip - pd.Timestamp(now).tz_convert("UTC")).total_seconds() / 60, 1) if tip is not None else None
    return {"game_id": int(g["id"]), "event_id": ev, "game_date": today, "source": source, "book": book,
            "home_spread": spread, "total": total,
            "home_spread_price": ln.get("current_home_spread_price"),
            "away_spread_price": ln.get("current_away_spread_price"),
            "over_price": ln.get("current_over_price"), "under_price": ln.get("current_under_price"),
            "tipoff": g.get("tipoff"), "captured_at": ts, "minutes_to_tip": mins, "pick_ids": sorted(pick_ids)}


def closing_line_snapshot(today: str, now, led: pd.DataFrame, allow_api: bool, log=lambda m: None, client=None,
                          lines=None, cache: dict | None = None, window: float = CLOSE_WINDOW_MIN) -> int:
    """Snapshot the closing spread/total of every game with an open official or lean play tipping within `window`
    minutes, into closing_line_snapshots (insert-only; never picks, the ledger or the hash chain):
      * ESPN/DK (free) every run;
      * The Odds API (multi-book, via the run's ONE shared 'pretip' call) when at least one target game has no
        Odds API snapshot yet, or when this run already holds the board (free). A refused claim = ESPN only.
    Returns rows written. Skips (logs, 0) without Supabase credentials or before the migration is applied."""
    from czarp import supabase_writer as sw
    targets = closing_targets(led, today, now, window)
    if not targets:
        return 0
    if client is None:
        if not (os.getenv("SUPABASE_URL") and os.getenv("SUPABASE_SERVICE_KEY")):
            log("  closing lines: no Supabase credentials — skipped")
            return 0
        client = sw.Client()
    lines = lines or dr.espn_dk_lines
    utc_now = pd.Timestamp(now).tz_convert("UTC")
    evs = ",".join(sorted(targets))
    games = [g for g in client.get_all("games", order="id", select="id,external_id,home,away,tipoff,status",
                                       date=f"eq.{today}", external_id=f"in.({evs})")
             if g.get("status") != "final" and g.get("tipoff") and pd.Timestamp(g["tipoff"]) > utc_now]
    if not games:
        return 0
    ids = ",".join(str(int(g["id"])) for g in games)
    try:
        done = {int(r["game_id"]) for r in client.get_all(CLOSE_TABLE, order="id", select="game_id",
                                                          game_id=f"in.({ids})", source="eq.oddsapi")}
    except Exception as e:                     # table missing (migration not applied): nothing to write to
        log(f"  closing lines: {CLOSE_TABLE} not readable ({type(e).__name__}) — skipped")
        return 0
    ts = now.isoformat(timespec="seconds")
    rows, snap = [], _snapshot(today)
    for g in games:
        ev = str(g["external_id"])
        ln = {k: sw._clean(v) for k, v in (lines(ev) or {}).items()}
        r = _snap_row(g, ev, today, "espn", ln.get("book") or "DraftKings", ln, ts, now, targets[ev]["pick_ids"])
        if r:
            rows.append(r)
    need_api = [g for g in games if int(g["id"]) not in done]
    if allow_api and (need_api or (cache is not None and "board" in cache)):
        work = []
        for g in games:
            s = snap.get(str(g["external_id"]), {})
            work.append({"game": g, "event_id": str(g["external_id"]),
                         "home_full": s.get("home_full") or g["home"], "away_full": s.get("away_full") or g["away"],
                         "current_home_spread": None, "current_total": None})
        _run_odds_api(work, today, log, cache)
        for w in work:
            src = str(w.get("price_source") or "")
            if src.startswith("oddsapi:"):
                r = _snap_row(w["game"], w["event_id"], today, "oddsapi", src.split(":", 1)[1] or None, w, ts, now,
                              targets[w["event_id"]]["pick_ids"])
                if r:
                    rows.append(r)
    if rows:
        client.insert(CLOSE_TABLE, rows)
    log(f"  closing lines: {len(rows)} snapshot row(s) for {len(games)} game(s) tipping within {window:g} min "
        f"({sum(r['source'] == 'oddsapi' for r in rows)} from The Odds API)")
    return len(rows)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--date")
    ap.add_argument("--dry-run", action="store_true", help="report only: no ledger write, no Odds API credits")
    ap.add_argument("--within", type=float, default=None, help="only games tipping within this many minutes")
    ap.add_argument("--move", type=float, default=MOVE_PTS, help=f"flag a move >= this many pts against us (default {MOVE_PTS:g})")
    ap.add_argument("--no-odds-api", action="store_true")
    ap.add_argument("--big-within", type=float, default=BIG_WITHIN_MIN,
                    help=f"report big moves on any game tipping within this many minutes (default {BIG_WITHIN_MIN}; 0 = off)")
    ap.add_argument("--close-within", type=float, default=CLOSE_WINDOW_MIN,
                    help=f"closing-line snapshot for official/lean plays tipping within this many minutes "
                         f"(default {CLOSE_WINDOW_MIN}; 0 = off)")
    ap.add_argument("-v", "--verbose", action="store_true")
    a = ap.parse_args(argv)
    log = (lambda m: print(m, file=sys.stderr, flush=True)) if a.verbose else (lambda m: None)
    now = dr.now_ct()
    today = a.date or now.date().isoformat()
    led = dr.read_ledger()
    naive_now = pd.Timestamp(now.replace(tzinfo=None))
    tips = pd.to_datetime(led.tip_ct, errors="coerce")
    # official plays only: the TRACK and LEAN tiers are paper-only shadow tracking, never bet, so no pre-tip alerts
    m = (led.status == "open") & (led.game_date.astype(str) == today) & (tips > naive_now) & (led.tier.fillna("official") == "official")
    if a.within is not None:
        m &= tips <= naive_now + timedelta(minutes=a.within)
    todo = led[m]
    log(f"pretip_check {today} {now:%H:%M} CT: {len(todo)} open plays not started")
    big = []
    if a.big_within and a.big_within > 0:
        try:
            big = big_moves_soon(today, now, a.big_within, led, log=log)
        except Exception as e:
            log(f"  big-move check failed ({type(e).__name__}: {e})")
    if big:
        from czarp import big_moves as BM
        print(f"Big moves {today} {now:%H:%M} CT — games tipping within {a.big_within:g} min (info only, not plays):")
        for ln in BM.pretip_lines(big):
            print(ln)
    filled, board = {}, {}                     # board: this run's ONE shared Odds API call (pretip slot)
    if not a.dry_run:
        try:
            filled = fill_missing_lines(today, now, led, allow_api=not a.no_odds_api, log=log, cache=board)
        except Exception as e:
            log(f"  missing-line fill failed ({type(e).__name__}: {e})")
    if not a.dry_run and a.close_within and a.close_within > 0:
        try:
            closing_line_snapshot(today, now, led, allow_api=not a.no_odds_api, log=log, cache=board,
                                  window=a.close_within)
        except Exception as e:                 # never block the play checks
            log(f"  closing-line snapshot failed ({type(e).__name__}: {e})")
    if todo.empty:
        return 0
    lines, missing = {}, []
    for i, r in todo.iterrows():
        ln = dr.espn_dk_lines(str(r.event_id))
        f = filled.get(str(r.event_id)) or {}
        ln = {**{k: v for k, v in f.items() if k.startswith("current_") and v is not None}, **{k: v for k, v in ln.items() if v is not None}}
        if (r.market == "spread" and ln.get("current_home_spread") is None) or \
                (r.market == "total" and ln.get("current_total") is None):
            missing.append(r)
        lines[i] = ln
    if missing:
        extra = _odds_api_lines(missing, today, allow=not (a.no_odds_api or a.dry_run), log=log, cache=board)
        for i, r in todo.iterrows():
            if str(r.event_id) in extra:
                lines[i] = {**lines[i], **{k: v for k, v in extra[str(r.event_id)].items() if v is not None}}
    flags = {}
    for i, r in todo.iterrows():
        why = check_play(r, lines[i], a.move)
        log(f"  {r.pick_id}: {'; '.join(why) or 'ok'}")
        if why:
            flags[i] = why
    if not flags:
        return 0
    stamp = now.strftime("%H:%M")
    print(f"Pre-tip check {today} {stamp} CT — {len(flags)} of {len(todo)} open plays flagged:")
    for i, why in flags.items():
        r = todo.loc[i]
        bet = f"{r.side} {float(r.line):+g}" if r.market == "spread" else f"{r.side} {float(r.line):g}"
        print(f"- {str(r.tip_ct)[11:]} CT {r.away} @ {r.home}: {bet} — {'; '.join(why)}")
    if not a.dry_run:
        notes = {str(todo.loc[i, "pick_id"]): f"{now.isoformat(timespec='minutes')}: {'; '.join(why)}"
                 for i, why in flags.items()}
        try:
            write_flags(notes, wait=LEDGER_LOCK_WAIT_S)
        except dr.LedgerBusy as e:
            log(f"  pretip_flag not written ({e}); the alert above still stands")
    return 0


LEDGER_LOCK_WAIT_S = 120.0


def write_flags(notes: dict, wait: float | None = None) -> int:
    """Write pretip_flag annotations by pick_id under the shared ledger lock, on a FRESH read of the ledger, so a
    grade written by grade_finals.py (or anything else) since this run's first read is never overwritten.
    Only rows still open get a flag; nothing but the annotation changes."""
    with dr.ledger_lock(wait=wait):
        led = dr.read_ledger()
        before = led.drop(columns=["pretip_flag"]).copy()
        led["pretip_flag"] = led["pretip_flag"].astype(object)
        n = 0
        for i in led.index[(led.status == "open") & led.pick_id.astype(str).isin(notes)]:
            led.loc[i, "pretip_flag"] = notes[str(led.at[i, "pick_id"])]
            n += 1
        assert led.drop(columns=["pretip_flag"]).equals(before)     # nothing but the annotation changed
        if n:
            dr.write_ledger(led)
        return n


if __name__ == "__main__":
    sys.exit(main())
