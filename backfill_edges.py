"""Backfill stored per-market edges on existing projection rows (PR #23 columns).

Fills NULL edge_now_pct_* / edge_vs_open_pct_* on the latest czarp-v2 and
czarp-v1-totals projection for each game that already has a projection and an
opening line. Uses daily_run.attach_market_edges (same formulas as the live
pipeline). Verdict/lean on slate_games are view-computed from these columns.

Never writes picks, the ledger, or skip-reason text. Never overwrites a stored edge.

    python backfill_edges.py [--start 2026-11-02] [--end YYYY-MM-DD] [--dry-run]
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timezone

import pandas as pd

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

from czarp.supabase_writer import DEFAULT_VERSION, V1_VERSION, Client, _edge_fields
import daily_run as dr


EDGE_COLS = (
    "edge_now_pct_spread", "edge_vs_open_pct_spread",
    "edge_now_pct_total", "edge_vs_open_pct_total",
)


def get_all(c: Client, table: str, page: int = 1000, **params) -> list[dict]:
    out, off = [], 0
    while True:
        rows = c.get(table, limit=str(page), offset=str(off), order="id", **params)
        out.extend(rows)
        if len(rows) < page:
            return out
        off += page


def _ts(x):
    return pd.Timestamp(x) if x else None


def _f(v):
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _line_pair(rows, last: bool) -> tuple[float | None, float | None]:
    rows = sorted(rows, key=lambda r: _ts(r.get("fetched_at")) or pd.Timestamp(0, tz="UTC"))
    seq = reversed(rows) if last else rows
    spread = next((_f(r.get("spread")) for r in seq if _f(r.get("spread")) is not None), None)
    seq = reversed(rows) if last else rows
    total = next((_f(r.get("total")) for r in seq if _f(r.get("total")) is not None), None)
    return spread, total


def _latest(projs, game_id, version):
    xs = [r for r in projs if r.get("game_id") == game_id and r.get("model_version") == version]
    if not xs:
        return None
    return max(xs, key=lambda r: _ts(r.get("created_at")) or pd.Timestamp(0, tz="UTC"))


def _needs_fill(row: dict | None) -> bool:
    return bool(row) and any(row.get(c) is None for c in EDGE_COLS)


def _tip_utc(g) -> datetime:
    t = _ts(g.get("tipoff"))
    if t is None:
        return datetime(2000, 1, 1, tzinfo=timezone.utc)
    if t.tzinfo is None:
        t = t.tz_localize("UTC")
    return t.to_pydatetime()


def build_patches(games, lines, projs) -> list[dict]:
    """Pure: [{game_id, date, home, away, proj_id, model_version, fields}, ...]

    One patch per latest v2 / v1 projection that still has a NULL edge column.
    Fields contain only the NULL columns we can fill. Open + latest current
    Vegas lines; v2/v1 numbers from that projection row (spread stored as
    -home margin).
    """
    ml = defaultdict(list)
    for r in lines:
        ml[r["game_id"]].append(r)
    out = []
    for g in games:
        gid = g["id"]
        rows = ml.get(gid, [])
        opens = [r for r in rows if r.get("line_type") == "open"]
        curs = [r for r in rows if r.get("line_type") == "current"]
        o_sp, o_t = _line_pair(opens, last=False)
        c_sp, c_t = _line_pair(curs, last=True)
        if o_sp is None and o_t is None:
            continue
        v2, v1 = _latest(projs, gid, DEFAULT_VERSION), _latest(projs, gid, V1_VERSION)
        if not _needs_fill(v2) and not _needs_fill(v1):
            continue
        if v2 is None and v1 is None:
            continue
        gd = dict(event_id=str(g.get("external_id") or gid), game_date=g.get("date"),
                  kp_home=g.get("home"), kp_away=g.get("away"),
                  state="post", tip_utc=_tip_utc(g),
                  open_home_spread=o_sp, current_home_spread=c_sp,
                  open_total=o_t, current_total=c_t)
        if v2 is not None and _f(v2.get("spread")) is not None:
            gd["v2_home_margin"] = -_f(v2.get("spread"))
        if v2 is not None and _f(v2.get("total")) is not None:
            gd["v2_total"] = _f(v2.get("total"))
        if v1 is not None and _f(v1.get("total")) is not None:
            gd["v1_total"] = _f(v1.get("total"))
        elif v2 is not None and _f(v2.get("total")) is not None:
            # official totals use v1; if that row is missing, leave totals unset
            pass
        dr.attach_market_edges([gd], dr.now_ct())
        edges = _edge_fields(gd)
        for row in (v2, v1):
            if not _needs_fill(row):
                continue
            fields = {k: edges[k] for k in EDGE_COLS if row.get(k) is None and edges.get(k) is not None}
            if not fields:
                continue
            out.append(dict(game_id=gid, date=g.get("date"), home=g.get("home"), away=g.get("away"),
                            proj_id=row["id"], model_version=row.get("model_version"), fields=fields))
    return out


def apply_patches(c: Client, patches: list[dict], log=print) -> int:
    n = 0
    for p in patches:
        # one column per PATCH, guarded by col IS NULL: never overwrites a stored edge
        for col, val in p["fields"].items():
            c.patch("projections", {col: val}, id=f"eq.{p['proj_id']}", **{col: "is.null"})
        n += 1
    if n:
        log(f"  edges: filled NULL edge columns on {n} projection rows")
    return n


def slate_counts(patches: list[dict]) -> dict[str, int]:
    """Games (slate_games rows) whose latest default-model projection was filled, by date."""
    seen = set()
    counts: dict[str, int] = {}
    for p in patches:
        if p.get("model_version") != DEFAULT_VERSION:
            continue
        key = (p["game_id"], p["date"])
        if key in seen:
            continue
        seen.add(key)
        counts[p["date"]] = counts.get(p["date"], 0) + 1
    return dict(sorted(counts.items()))


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start", default="2026-11-02")
    ap.add_argument("--end", default=None)
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args(argv)
    end = a.end or dr.now_ct().date().isoformat()
    c = Client()
    games = get_all(c, "games", select="id,external_id,date,home,away,tipoff,neutral",
                    league="eq.ncaam", date=f"gte.{a.start}")
    games = [g for g in games if g["date"] <= end]
    ids = [g["id"] for g in games]
    lines, projs = [], []
    sel = "id,game_id,model_version,spread,total,created_at," + ",".join(EDGE_COLS)
    for i in range(0, len(ids), 150):
        chunk = ",".join(map(str, ids[i:i + 150]))
        lines += get_all(c, "market_lines", select="id,game_id,line_type,spread,total,fetched_at",
                         source="eq.vegas", game_id=f"in.({chunk})")
        projs += get_all(c, "projections", select=sel, game_id=f"in.({chunk})")
    patches = build_patches(games, lines, projs)
    by_date = slate_counts(patches)
    n_v2 = sum(p["model_version"] == DEFAULT_VERSION for p in patches)
    n_v1 = sum(p["model_version"] == V1_VERSION for p in patches)
    print(f"backfill edges {a.start}..{end}: {len(games)} games, {len(lines)} lines, "
          f"{len(projs)} projections -> {len(patches)} projection patches "
          f"({n_v2} {DEFAULT_VERSION}, {n_v1} {V1_VERSION})")
    print("  slate_games rows to fill by date: " + (", ".join(f"{d}={n}" for d, n in by_date.items()) or "none"))
    if a.dry_run:
        for p in patches:
            if p.get("model_version") != DEFAULT_VERSION:
                continue
            f = p["fields"]
            print(f"  {p['date']} {p['away']} @ {p['home']}: " +
                  ", ".join(f"{k}={v}" for k, v in f.items()))
        return 0
    apply_patches(c, patches)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
