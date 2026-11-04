"""
backfill_track.py — add SHADOW 'track' picks for past slates from the saved morning snapshot (paper only).

    python backfill_track.py --date 2026-11-03            # dry run: show what would be logged
    python backfill_track.py --date 2026-11-03 --write    # append to the ledger (source=backfill), grade, write

The snapshot is the one the live daily run used (its time matches that day's official created_ct); otherwise the
first snapshot of the day. Every rule is re-applied exactly as daily_run.candidates() does today, at the snapshot's
time and prices. Rows are marked source='backfill' with created_ct = the snapshot time (the price the pick would
have been logged at) and logged_ct = now, so the public record shows they were reconstructed, not logged live.
Official plays are never added here (only compared with the ledger as a consistency check).
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import sys
from datetime import datetime

import pandas as pd

import daily_run as dr


def pick_snapshot(day: str, ledger: pd.DataFrame) -> str:
    snaps = sorted(glob.glob(os.path.join(dr.SNAP_DIR, f"{day}_*.csv")))
    if not snaps:
        raise SystemExit(f"no snapshot for {day} in {dr.SNAP_DIR}")
    live = ledger[(ledger.game_date == day) & (ledger.source == "live")].created_ct.dropna()
    if len(live):
        hhmm = str(live.min())[11:16].replace(":", "")
        for s in snaps:
            if s.endswith(f"_{hhmm}.csv"):
                return s
    return snaps[0]


def _v(x):
    if isinstance(x, float) and pd.isna(x):
        return None
    return x


def games_from_snapshot(path: str) -> tuple[list[dict], datetime]:
    df = pd.read_csv(path, dtype={"event_id": str})
    games = []
    for r in df.to_dict("records"):
        g = {k: _v(v) for k, v in r.items()}
        g["tip_utc"] = pd.Timestamp(g["tip_ct"]).tz_localize(dr.CENTRAL).tz_convert("UTC").to_pydatetime()
        g["neutral"] = str(g.get("neutral")) == "True"
        if isinstance(g.get("archetype_tags"), str):
            try:
                g["archetype_tags"] = json.loads(g["archetype_tags"].replace("'", '"'))
            except Exception:
                pass
        games.append(g)
    now = datetime.fromisoformat(str(df.snapshot_ct.iloc[0]))
    return games, now


def backfill(day: str, write: bool, log=print) -> list[dict]:
    led = dr.read_ledger()
    snap = pick_snapshot(day, led)
    games, at = games_from_snapshot(snap)
    # NEW COACH tags: early-season games played are well under the 8-game side lock, so 0 is exact for the lock
    dr.tag_new_coaches(games, day)
    for g in games:
        for k in ("coach_home", "coach_away"):
            if g.get(k):
                g[k]["games"] = 0
    rows = dr.flag_plays(games, dr.bankroll(led), {}, at, include_track=True, source="backfill")
    off = sorted(r["pick_id"] for r in rows if r["tier"] == "official")
    have_off = sorted(dr.official(led).loc[lambda d: d.game_date == day, "pick_id"].astype(str))
    log(f"{day}: snapshot {os.path.basename(snap)} ({at:%H:%M} CT); official re-derived {off} vs ledger {have_off}"
        + ("" if set(off) <= set(have_off) else "  ⚠ mismatch (rules changed since the live run; not added)"))
    track = [r for r in rows if r["tier"] == "track"]
    for c in dr.candidates(games, at):
        if c["market"] == "spread" and (c["edge_pct_vs_open"] or 0) >= 0.03 and c["tier"] is None:
            log(f"   not track: {c['side']} {c['line']:+g} (open {c['open_line']:+g}) edge open "
                f"{c['edge_pct_vs_open']:.1%} / current {c['edge_pct'] or 0:.1%} — {'; '.join(c['reasons'])}")
    for r in track:
        log(f"   TRACK: {r['away']} @ {r['home']} {r['side']} {r['line']:+g} ({r['price']:+g}) open {r['open_line']:+g} "
            f"edge open {r['edge_pct_vs_open']:.1%} / current {r['edge_pct']:.1%}")
    if not track:
        log("   no track picks")
    if write and track:
        now = dr.now_ct()
        led = dr.append_picks(led, track, now)
        dr.grade(led, now)
        dr.write_ledger(led)
        log(f"   wrote {len(track)} track picks (source=backfill) and graded")
    return track


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--date", action="append", required=True)
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--track-ignore-coach-lock", action="store_true",
                    help="also log NEW COACH-locked sides as track (daily_run.TRACK_IGNORE_COACH_LOCK)")
    a = ap.parse_args(argv)
    if a.track_ignore_coach_lock:
        dr.TRACK_IGNORE_COACH_LOCK = True
    for d in a.date:
        backfill(d, a.write)
    return 0


if __name__ == "__main__":
    sys.exit(main())
