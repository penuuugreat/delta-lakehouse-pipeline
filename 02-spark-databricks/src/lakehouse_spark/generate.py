"""Simulated source system: one CSV per day (new orders + updates to yesterday + dirty rows)."""
import argparse
import random
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

from .tables import RAW_COLS

COUNTRIES = ["KE", "UG", "TZ", "NG", "ZA", "GH"]


def _base_orders(day: date, n: int) -> list[dict]:
    rng = random.Random(day.toordinal())
    rows = []
    for i in range(n):
        ts = datetime.combine(day, datetime.min.time()) + timedelta(seconds=rng.randint(0, 86399))
        rows.append(dict(
            order_id=f"O{day:%Y%m%d}{i:04d}", customer_id=f"C{rng.randint(1, 500):04d}",
            country=rng.choice(COUNTRIES), amount=round(rng.uniform(5, 500), 2),
            status=rng.choice(["placed", "paid"]),
            order_ts=ts.isoformat(sep=" "), updated_at=ts.isoformat(sep=" "),
        ))
    return rows


def generate_day(day: date, out_dir: str, n: int = 200, with_updates: bool = True) -> str:
    rng = random.Random(day.toordinal() * 7 + 1)
    new_rows = _base_orders(day, n)
    for r in new_rows:
        x = rng.random()
        if x < 0.03:
            r["amount"] = None
        elif x < 0.05:
            r["amount"] = -abs(r["amount"])
        elif x < 0.06:
            r["status"] = "unknown"
    dupes = [dict(r) for r in rng.sample(new_rows, 4)]
    updates = []
    if with_updates:
        for r in rng.sample(_base_orders(day - timedelta(days=1), n), 15):
            r = dict(r)
            r["status"] = rng.choice(["shipped", "cancelled"])
            r["updated_at"] = (datetime.combine(day, datetime.min.time())
                               + timedelta(seconds=rng.randint(0, 86399))).isoformat(sep=" ")
            updates.append(r)
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    out = Path(out_dir) / f"orders_{day:%Y-%m-%d}.csv"
    pd.DataFrame(new_rows + dupes + updates)[RAW_COLS].to_csv(out, index=False)
    return str(out)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2024-01-01")
    ap.add_argument("--days", type=int, default=1)
    ap.add_argument("--out", default="data/source")
    ap.add_argument("--no-updates", action="store_true", help="skip updates (use for the very first day)")
    a = ap.parse_args()
    for k in range(a.days):
        print("wrote", generate_day(date.fromisoformat(a.start) + timedelta(days=k), a.out,
                                    with_updates=not (a.no_updates and k == 0)))
