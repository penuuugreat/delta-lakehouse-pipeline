"""Data-quality gates. Run between layers so bad data never reaches gold."""
from dataclasses import dataclass

from .delta_utils import open_table
from .paths import DEFAULT, Paths
from .silver import ALLOWED_STATUS


class DataQualityError(Exception):
    pass


@dataclass
class Check:
    name: str
    passed: bool
    detail: str = ""


def check_silver(paths: Paths = DEFAULT) -> list[Check]:
    t = open_table(paths.silver)
    if t is None:
        return []
    df = t.to_pandas()
    return [
        Check("silver: row count > 0", len(df) > 0, f"({len(df)} rows)"),
        Check("silver: order_id not null", bool(df["order_id"].notna().all())),
        Check("silver: order_id unique", bool(df["order_id"].is_unique), f"({df['order_id'].duplicated().sum()} dupes)"),
        Check("silver: amount >= 0", bool((df["amount"] >= 0).all())),
        Check("silver: status in allowed set", bool(df["status"].isin(ALLOWED_STATUS).all())),
        Check("silver: order_date matches order_ts", bool((df["order_ts"].dt.date == df["order_date"]).all())),
    ]


def check_gold(paths: Paths = DEFAULT) -> list[Check]:
    g, s = open_table(paths.gold), open_table(paths.silver)
    if g is None or s is None:
        return []
    gd, sd = g.to_pandas(), s.to_pandas()
    return [
        Check("gold: (order_date,country) unique", not gd.duplicated(["order_date", "country"]).any()),
        Check("gold: net_revenue >= 0", bool((gd["net_revenue"] >= 0).all())),
        Check("gold: net_orders <= orders", bool((gd["net_orders"] <= gd["orders"]).all())),
        Check(
            "gold<->silver reconciliation (order counts)",
            int(gd["orders"].sum()) == len(sd),
            f"(gold={int(gd['orders'].sum())} silver={len(sd)})",
        ),
    ]


def assert_all(checks: list[Check]) -> None:
    for c in checks:
        print(f"   {'PASS' if c.passed else 'FAIL'}  {c.name} {c.detail}")
    failed = [c for c in checks if not c.passed]
    if failed:
        raise DataQualityError("; ".join(f"{c.name} {c.detail}" for c in failed))
