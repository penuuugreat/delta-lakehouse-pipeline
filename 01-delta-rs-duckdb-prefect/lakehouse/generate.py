"""Simulated source system: drops one CSV per day into data/source/.

Each file contains
  * ~200 NEW orders for that day
  * ~15 UPDATES to the previous day's orders (status changes) -> exercises MERGE
  * deliberately dirty rows (null/negative amounts, bad status, exact duplicates)
"""
import argparse
import random
from datetime import date, datetime, timedelta

import pandas as pd

from .paths import DEFAULT, Paths

COUNTRIES = ["KE", "UG", "TZ", "NG", "ZA", "GH"]


def _order_id(day: date, i: int) -> str:
    return f"O{day:%Y%m%d}{i:04d}"


def _base_orders(day: date, n: int) -> list[dict]:
    """Deterministic 'true' orders for a day (so the next day can update them)."""
    rng = random.Random(day.toordinal())
    rows = []
    for i in range(n):
        ts = datetime.combine(day, datetime.min.time()) + timedelta(seconds=rng.randint(0, 86399))
        rows.append(
            dict(
                order_id=_order_id(day, i),
                customer_id=f"C{rng.randint(1, 500):04d}",
                country=rng.choice(COUNTRIES),
                amount=round(rng.uniform(5, 500), 2),
                status=rng.choice(["placed", "paid"]),
                order_ts=ts.isoformat(sep=" "),
                updated_at=ts.isoformat(sep=" "),
            )
        )
    return rows


def generate_day(day: date, n: int = 200, paths: Paths = DEFAULT, with_updates: bool = True) -> str:
    rng = random.Random(day.toordinal() * 7 + 1)
    new_rows = _base_orders(day, n)

    # --- inject dirty data into new rows
    for r in new_rows:
        x = rng.random()
        if x < 0.03:
            r["amount"] = None
        elif x < 0.05:
            r["amount"] = -abs(r["amount"])
        elif x < 0.06:
            r["status"] = "unknown"
    dupes = [dict(r) for r in rng.sample(new_rows, 4)]

    # --- updates to yesterday's orders
    updates = []
    for r in (rng.sample(_base_orders(day - timedelta(days=1), n), 15) if with_updates else []):
        r = dict(r)
        r["status"] = rng.choice(["shipped", "cancelled"])
        r["updated_at"] = (
            datetime.combine(day, datetime.min.time()) + timedelta(seconds=rng.randint(0, 86399))
        ).isoformat(sep=" ")
        updates.append(r)

    df = pd.DataFrame(new_rows + dupes + updates)
    paths.source.mkdir(parents=True, exist_ok=True)
    out = paths.source / f"orders_{day:%Y-%m-%d}.csv"
    df.to_csv(out, index=False)
    return str(out)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2024-01-01")
    ap.add_argument("--days", type=int, default=1)
    ap.add_argument("--no-updates", action="store_true", help="skip updates to previous-day orders (use for the very first day)")
    a = ap.parse_args()
    start = date.fromisoformat(a.start)
    for k in range(a.days):
        print("wrote", generate_day(start + timedelta(days=k), with_updates=not (a.no_updates and k == 0)))
