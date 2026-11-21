"""The Odds API usage counter (data/ledger/odds_api_usage.json): strict load, fail-closed, explicit init.

The file is the only thing enforcing the daily call cap, so a missing, unreadable or malformed file must never
read as "0 calls today". load() raises UsageError instead; callers refuse the call and leave the file alone.
The file is created only by an explicit init, and only when absent:

    python3 -m czarp.odds_usage --init     # create a valid empty counter if (and only if) it does not exist
    python3 -m czarp.odds_usage --check    # validate the existing file (read-only); exit 1 if it is not usable

The path is anchored to the repo checkout (this file's location), not the working directory. The 7 AM run and
pretip_check must therefore run on the same machine from the same checkout to share one counter.

Schema (the pre-2026-11-21 format, without calls_by_job, is still valid; no migration needed):
    {"calls": {"YYYY-MM-DD": int>=0, ...},
     "calls_by_job": {"YYYY-MM-DD": {"daily": int>=0, "pretip": int>=0}, ...},   # optional
     "remaining": null | number | numeric string,                               # optional
     "last_call_ct": str}                                                       # optional
"""
from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
USAGE_PATH = os.path.join(REPO_ROOT, "data", "ledger", "odds_api_usage.json")
EMPTY = {"calls": {}, "remaining": None}
_DAY = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class UsageError(Exception):
    """The usage counter is missing, unreadable or malformed: refuse Odds API calls."""


def _count(v) -> bool:
    return isinstance(v, int) and not isinstance(v, bool) and v >= 0


def validate(u) -> None:
    if not isinstance(u, dict):
        raise UsageError("top level is not an object")
    calls = u.get("calls")
    if not isinstance(calls, dict):
        raise UsageError("'calls' missing or not an object")
    for d, n in calls.items():
        if not (isinstance(d, str) and _DAY.match(d)) or not _count(n):
            raise UsageError(f"bad calls entry {d!r}: {n!r}")
    by = u.get("calls_by_job", {})
    if not isinstance(by, dict):
        raise UsageError("'calls_by_job' is not an object")
    for d, jobs in by.items():
        if not (isinstance(d, str) and _DAY.match(d)) or not isinstance(jobs, dict):
            raise UsageError(f"bad calls_by_job entry {d!r}")
        for j, n in jobs.items():
            if not isinstance(j, str) or not _count(n):
                raise UsageError(f"bad calls_by_job[{d}] entry {j!r}: {n!r}")
        if sum(jobs.values()) > calls.get(d, 0):
            raise UsageError(f"calls_by_job[{d}] exceeds calls[{d}]")
    rem = u.get("remaining")
    if rem is not None:
        try:
            x = float(rem)
        except (TypeError, ValueError):
            raise UsageError(f"'remaining' is not numeric: {rem!r}") from None
        if isinstance(rem, bool) or not math.isfinite(x):
            raise UsageError(f"'remaining' is not a finite number: {rem!r}")
    if "last_call_ct" in u and not isinstance(u["last_call_ct"], str):
        raise UsageError("'last_call_ct' is not a string")


def load(path: str = USAGE_PATH) -> dict:
    """The validated counter. Raises UsageError when missing, unreadable, not JSON or off-schema."""
    try:
        with open(path) as fh:
            u = json.load(fh)
    except FileNotFoundError:
        raise UsageError(f"usage file missing ({path}); run `python3 -m czarp.odds_usage --init`") from None
    except (OSError, ValueError) as e:            # ValueError covers bad JSON and bad encodings
        raise UsageError(f"usage file unreadable ({type(e).__name__})") from None
    validate(u)
    return u


def write(u: dict, path: str = USAGE_PATH) -> None:
    """Atomic replace of an EXISTING, validated counter (never used to create it)."""
    validate(u)
    tmp = f"{path}.tmp{os.getpid()}"
    try:
        with open(tmp, "w") as fh:
            json.dump(u, fh, indent=1, allow_nan=False)
            fh.flush()
            os.fsync(fh.fileno())          # data on disk before the rename, so a crash can't leave an empty file
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def init(path: str = USAGE_PATH) -> str:
    """Create a valid empty counter only if the file is absent. Never touches an existing file."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    try:
        with open(path, "x") as fh:
            json.dump(EMPTY, fh, indent=1)
    except FileExistsError:
        return "exists (left unchanged)"
    return "created"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--init", action="store_true", help="create a valid empty counter if absent")
    g.add_argument("--check", action="store_true", help="validate the existing counter (read-only)")
    ap.add_argument("--path", default=USAGE_PATH)
    a = ap.parse_args(argv)
    if a.init:
        print(f"{a.path}: {init(a.path)}")
        if os.path.exists(a.path):
            try:
                load(a.path)
            except UsageError as e:
                print(f"WARNING: existing file is not usable: {e}", file=sys.stderr)
                return 1
        return 0
    try:
        u = load(a.path)
    except UsageError as e:
        print(f"{a.path}: NOT usable: {e}", file=sys.stderr)
        return 1
    days = sorted(u["calls"])[-3:]
    print(f"{a.path}: ok ({len(u['calls'])} day(s) recorded; last: "
          + (", ".join(f"{d}={u['calls'][d]}" for d in days) or "none") + ")")
    return 0


if __name__ == "__main__":
    sys.exit(main())
