"""High-total Unders paper tracker (INFO ONLY — never a pick, verdict, stake, or ledger row).

Logs every game where CZarp's v1 total (the official totals model) leans UNDER a posted DK
total of HTU_MIN_LINE (155) or more by HTU_MIN_GAP (4) points or more. "Posted" = the current
total at the daily run (7 AM CT); the open line is kept alongside. Graded once the game is final,
vs the posted line and vs the open. First log wins; rows are never re-priced.

File: data/ledger/high_total_unders.csv (separate from the immutable ledger; no hash chain).

CLI::
    python -m czarp.htu_tracker --seed 2026-11-02 2026-11-11   # from the 7 AM snapshots
    python -m czarp.htu_tracker --grade                         # grade pending rows from ESPN finals
    python -m czarp.htu_tracker --record
"""
from __future__ import annotations

import argparse
import math
import os
import sys
from datetime import date, datetime, timedelta

import pandas as pd

PATH = os.path.join("data", "ledger", "high_total_unders.csv")
HTU_MIN_LINE = 155.0
HTU_MIN_GAP = 4.0
COLS = ["event_id", "game_date", "tip_ct", "away", "home", "posted_line", "open_line", "czarp_total",
        "gap", "logged_ct", "source", "status", "away_score", "home_score", "final_total",
        "result_vs_posted", "result_vs_open", "graded_ct"]


def _f(x):
    try:
        v = float(x)
        return None if math.isnan(v) else v
    except (TypeError, ValueError):
        return None


def qualifies(g: dict) -> dict | None:
    if g.get("skip"):
        return None
    cz = _f(g.get("v1_total"))
    posted = _f(g.get("current_total"))
    if posted is None:
        posted = _f(g.get("open_total"))
    if cz is None or posted is None:
        return None
    gap = posted - cz
    if posted < HTU_MIN_LINE or gap < HTU_MIN_GAP - 1e-9:
        return None
    return {"event_id": str(g.get("event_id")), "game_date": g.get("game_date"), "tip_ct": g.get("tip_ct"),
            "away": g.get("kp_away") or g.get("away"), "home": g.get("kp_home") or g.get("home"),
            "posted_line": posted, "open_line": _f(g.get("open_total")), "czarp_total": round(cz, 1),
            "gap": round(gap, 1), "status": "open"}


def read(path: str | None = None) -> pd.DataFrame:
    path = path or PATH
    if os.path.exists(path):
        df = pd.read_csv(path, dtype={"event_id": str})
        for c in COLS:
            if c not in df:
                df[c] = None
        return df[COLS]
    return pd.DataFrame(columns=COLS)


def write(df: pd.DataFrame, path: str | None = None) -> None:
    path = path or PATH
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    df[COLS].to_csv(path, index=False)


def log_games(df: pd.DataFrame, games, now: datetime, source: str = "live") -> tuple[pd.DataFrame, int]:
    """Append qualifying not-yet-started games (first log wins)."""
    have = set(df.event_id.astype(str))
    rows = []
    for g in games:
        if source == "live" and g.get("state") not in (None, "pre"):
            continue
        q = qualifies(g)
        if q and q["event_id"] not in have:
            q.update(logged_ct=now.isoformat(timespec="seconds"), source=source)
            rows.append(q)
            have.add(q["event_id"])
    if rows:
        df = pd.concat([df, pd.DataFrame(rows, columns=COLS)], ignore_index=True) if len(df) else pd.DataFrame(rows, columns=COLS)
    return df, len(rows)


def _res(total, line):
    if total is None or line is None:
        return None
    return "push" if total == line else ("win" if total < line else "loss")


def grade(df: pd.DataFrame, finals, now: datetime) -> tuple[pd.DataFrame, int]:
    """finals: iterable of dicts with event_id/home_score/away_score/completed."""
    fx = {str(g.get("event_id")): g for g in finals if g.get("completed")}
    n = 0
    df = df.copy()
    for c in ("status", "result_vs_posted", "result_vs_open", "graded_ct"):
        df[c] = df[c].astype(object)
    for c in ("away_score", "home_score", "final_total"):
        df[c] = pd.to_numeric(df[c], errors="coerce").astype(float)
    for i, r in df.iterrows():
        if r.status == "graded":
            continue
        g = fx.get(str(r.event_id))
        if not g:
            continue
        hs, as_ = _f(g.get("home_score")), _f(g.get("away_score"))
        if hs is None or as_ is None or hs + as_ <= 0:
            continue
        tot = hs + as_
        df.loc[i, ["away_score", "home_score", "final_total"]] = [as_, hs, tot]
        df.loc[i, "result_vs_posted"] = _res(tot, _f(r.posted_line))
        df.loc[i, "result_vs_open"] = _res(tot, _f(r.open_line))
        df.loc[i, "status"] = "graded"
        df.loc[i, "graded_ct"] = now.isoformat(timespec="seconds")
        n += 1
    return df, n


def _wl(s: pd.Series) -> tuple[int, int, int]:
    return int((s == "win").sum()), int((s == "loss").sum()), int((s == "push").sum())


def record(df: pd.DataFrame) -> dict:
    g = df[df.status == "graded"]
    wp, lp, pp = _wl(g.result_vs_posted)
    wo, lo, po = _wl(g.result_vs_open)
    return {"n": len(df), "graded": len(g), "pending": int((df.status != "graded").sum()),
            "posted": (wp, lp, pp), "open": (wo, lo, po)}


def record_line(df: pd.DataFrame) -> str:
    r = record(df)
    def fmt(t):
        w, l, p = t
        pct = f" ({100 * w / (w + l):.0f}%)" if w + l else ""
        return f"{w}-{l}" + (f"-{p}" if p else "") + pct
    return (f"High-total Unders tracker (paper, info only; CZarp Under by {HTU_MIN_GAP:g}+ on posted totals "
            f"{HTU_MIN_LINE:g}+): {fmt(r['posted'])} vs posted, {fmt(r['open'])} vs open"
            + (f", {r['pending']} pending" if r["pending"] else "") + ".")


def seed_from_snapshots(start: str, end: str, snap_dir: str, now: datetime, path: str | None = None) -> int:
    import glob
    df = read(path)
    n = 0
    for d in pd.date_range(start, end):
        fs = sorted(glob.glob(os.path.join(snap_dir, f"{d:%Y-%m-%d}_*.csv")))
        if not fs:
            continue
        snap = pd.read_csv(fs[-1])
        snap = snap[snap.game_date == f"{d:%Y-%m-%d}"]
        games = [{k: (None if (isinstance(v, float) and math.isnan(v)) else v) for k, v in r.items()}
                 for r in snap.to_dict(orient="records")]
        for g in games:
            if g.get("skip") in ("", "nan"):
                g["skip"] = None
        df, k = log_games(df, games, datetime.fromisoformat(str(snap.snapshot_ct.iloc[0])) if "snapshot_ct" in snap and len(snap) else now,
                          source="seed")
        n += k
    write(df, path)
    return n


def main(argv=None) -> int:
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(root)
    sys.path.insert(0, root)
    import daily_run as dr
    from czarp import season_form as SF
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--seed", nargs=2, metavar=("START", "END"))
    ap.add_argument("--grade", action="store_true")
    ap.add_argument("--record", action="store_true")
    a = ap.parse_args(argv)
    now = dr.now_ct()
    if a.seed:
        print(f"seeded {seed_from_snapshots(a.seed[0], a.seed[1], dr.SNAP_DIR, now)} rows")
    if a.grade or a.seed:
        df = read()
        finals = SF.load_finals(now.date() + timedelta(days=1), dr.espn_scoreboard)
        df, n = grade(df, finals, now)
        write(df)
        print(f"graded {n} rows")
    print(record_line(read()))
    return 0


if __name__ == "__main__":
    sys.exit(main())
