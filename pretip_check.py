"""pretip_check.py — pre-tip line check for today's open paper plays.

Re-pulls the current DraftKings line (ESPN's free feed; The Odds API only for plays ESPN has no line
for, and only within the daily_run budget in data/ledger/odds_api_usage.json) for every open ledger play
that has not tipped yet, and reports a play when
  * its line moved >= --move pts AGAINST us since the pick (negative CLV so far), or
  * its edge% vs the current line dropped below the rule threshold (or flipped sides).
Prints a short text summary; prints NOTHING when there is nothing to flag (exit 0 either way).
It never changes prices, stakes or graded results: the only ledger write is the 'pretip_flag' column
of open, not-yet-started plays (skipped with --dry-run).

    python pretip_check.py                 # today's slate (America/Chicago), annotate ledger
    python pretip_check.py --dry-run       # report only, no ledger write, no Odds API call
    options: --date YYYY-MM-DD  --within MIN (only games tipping in the next MIN minutes)
             --move PTS (default 2)  --no-odds-api  -v (progress to stderr)
"""
from __future__ import annotations

import argparse
import sys
from datetime import timedelta

import pandas as pd

import daily_run as dr

MOVE_PTS = 2.0


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


def _odds_api_lines(rows, today, allow, log) -> dict:
    """Odds API lines for plays ESPN has no line for, via daily_run's budget-aware fallback."""
    if not allow or not rows:
        return {}
    games = [{"event_id": str(r.event_id), "home_full": r.home, "away_full": r.away,
              "current_home_spread": None, "current_total": None} for r in rows]
    try:
        dr.log_lines.clear()
        _out, dr.log = dr.log, log
        dr.odds_api_fallback(games, today, allow=True)
    finally:
        dr.log = _out
    return {g["event_id"]: g for g in games if g.get("current_home_spread") is not None or g.get("current_total") is not None}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--date")
    ap.add_argument("--dry-run", action="store_true", help="report only: no ledger write, no Odds API credits")
    ap.add_argument("--within", type=float, default=None, help="only games tipping within this many minutes")
    ap.add_argument("--move", type=float, default=MOVE_PTS, help=f"flag a move >= this many pts against us (default {MOVE_PTS:g})")
    ap.add_argument("--no-odds-api", action="store_true")
    ap.add_argument("-v", "--verbose", action="store_true")
    a = ap.parse_args(argv)
    log = (lambda m: print(m, file=sys.stderr, flush=True)) if a.verbose else (lambda m: None)
    now = dr.now_ct()
    today = a.date or now.date().isoformat()
    led = dr.read_ledger()
    naive_now = pd.Timestamp(now.replace(tzinfo=None))
    tips = pd.to_datetime(led.tip_ct, errors="coerce")
    m = (led.status == "open") & (led.game_date.astype(str) == today) & (tips > naive_now)
    if a.within is not None:
        m &= tips <= naive_now + timedelta(minutes=a.within)
    todo = led[m]
    log(f"pretip_check {today} {now:%H:%M} CT: {len(todo)} open plays not started")
    if todo.empty:
        return 0
    lines, missing = {}, []
    for i, r in todo.iterrows():
        ln = dr.espn_dk_lines(str(r.event_id))
        if (r.market == "spread" and ln.get("current_home_spread") is None) or \
                (r.market == "total" and ln.get("current_total") is None):
            missing.append(r)
        lines[i] = ln
    if missing:
        extra = _odds_api_lines(missing, today, allow=not (a.no_odds_api or a.dry_run), log=log)
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
        before = led.drop(columns=["pretip_flag"]).copy()
        led["pretip_flag"] = led["pretip_flag"].astype(object)
        for i, why in flags.items():
            led.loc[i, "pretip_flag"] = f"{now.isoformat(timespec='minutes')}: {'; '.join(why)}"
        assert led.drop(columns=["pretip_flag"]).equals(before)     # nothing but the annotation changed
        dr.write_ledger(led)
    return 0


if __name__ == "__main__":
    sys.exit(main())
