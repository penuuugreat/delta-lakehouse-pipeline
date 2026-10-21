"""Data-quality gates. A failure raises, which fails the Workflow task and blocks downstream tasks."""
from dataclasses import dataclass

from pyspark.sql import functions as F

from .lake import Lake
from .tables import ALLOWED_STATUS, GOLD, SILVER


class DataQualityError(Exception):
    pass


@dataclass
class Check:
    name: str
    passed: bool
    detail: str = ""


def check_silver(lake: Lake) -> list[Check]:
    s = lake.read(SILVER)
    r = s.agg(
        F.count("*").alias("n"),
        F.sum(F.col("order_id").isNull().cast("int")).alias("null_ids"),
        F.countDistinct("order_id").alias("distinct_ids"),
        F.sum((F.col("amount") < 0).cast("int")).alias("negative"),
        F.sum((~F.col("status").isin(*ALLOWED_STATUS)).cast("int")).alias("bad_status"),
        F.sum((F.to_date("order_ts") != F.col("order_date")).cast("int")).alias("date_mismatch"),
    ).first()
    n = r["n"] or 0
    return [
        Check("silver: order_id not null", (r["null_ids"] or 0) == 0),
        Check("silver: order_id unique", (r["distinct_ids"] or 0) == n, f"({n - (r['distinct_ids'] or 0)} dupes)"),
        Check("silver: amount >= 0", (r["negative"] or 0) == 0),
        Check("silver: status in allowed set", (r["bad_status"] or 0) == 0),
        Check("silver: order_date matches order_ts", (r["date_mismatch"] or 0) == 0),
    ]


def check_gold(lake: Lake) -> list[Check]:
    g, s = lake.read(GOLD), lake.read(SILVER)
    dup_keys = g.groupBy("order_date", "country").count().where("count > 1").count()
    r = g.agg(
        F.sum((F.col("net_revenue") < 0).cast("int")).alias("neg"),
        F.sum((F.col("net_orders") > F.col("orders")).cast("int")).alias("bad_net"),
        F.sum("orders").alias("orders"),
    ).first()
    silver_n = s.count()
    gold_n = r["orders"] or 0
    return [
        Check("gold: (order_date,country) unique", dup_keys == 0, f"({dup_keys} dup keys)"),
        Check("gold: net_revenue >= 0", (r["neg"] or 0) == 0),
        Check("gold: net_orders <= orders", (r["bad_net"] or 0) == 0),
        Check("gold<->silver reconciliation (order counts)", gold_n == silver_n, f"(gold={gold_n} silver={silver_n})"),
    ]


def assert_all(checks: list[Check]) -> None:
    for c in checks:
        print(f"   {'PASS' if c.passed else 'FAIL'}  {c.name} {c.detail}")
    failed = [c for c in checks if not c.passed]
    if failed:
        raise DataQualityError("; ".join(f"{c.name} {c.detail}" for c in failed))
