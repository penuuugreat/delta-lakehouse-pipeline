"""Unit tests for pure transformation logic. Runs on plain Spark, no Delta required."""
from datetime import datetime

from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from lakehouse_spark.tables import RAW_COLS
from lakehouse_spark.transforms import dedupe_latest, gold_aggregate, validate

BRONZE_IN = StructType(
    [StructField(c, StringType()) for c in RAW_COLS]
    + [StructField("_source_file", StringType()), StructField("_ingested_at", TimestampType())]
)
T0 = datetime(2024, 1, 5, 0, 0, 0)


def _row(**kw):
    base = dict(order_id="O1", customer_id="C1", country="KE", amount="10.5", status="paid",
                order_ts="2024-01-01 10:00:00", updated_at="2024-01-01 10:00:00",
                _source_file="f.csv", _ingested_at=T0)
    base.update(kw)
    return tuple(base[f.name] for f in BRONZE_IN.fields)


def test_valid_row_is_typed(spark):
    good, bad = validate(spark.createDataFrame([_row()], BRONZE_IN))
    assert bad.count() == 0
    r = good.first()
    assert r["amount"] == 10.5 and str(r["order_date"]) == "2024-01-01"


def test_each_rule_has_a_reason(spark):
    rows = [
        _row(order_id="A", amount=None),
        _row(order_id="B", amount="-5"),
        _row(order_id="C", status="unknown"),
        _row(order_id=None),
        _row(order_id="E", order_ts="not-a-date"),
        _row(order_id="F", amount="abc"),
        _row(order_id="G", country=None),
        _row(order_id="H", status=None),
    ]
    good, bad = validate(spark.createDataFrame(rows, BRONZE_IN))
    assert good.count() == 0
    reasons = {r["order_id"]: r["_reject_reason"] for r in bad.collect()}
    assert reasons == {
        "A": "invalid_amount", "B": "negative_amount", "C": "invalid_status", None: "missing_order_id",
        "E": "invalid_timestamp", "F": "invalid_amount", "G": "missing_country", "H": "invalid_status",
    }


def test_rows_are_partitioned_without_loss(spark):
    rows = [_row(order_id="A"), _row(order_id="B", amount=None), _row(order_id="C", status="x")]
    df = spark.createDataFrame(rows, BRONZE_IN)
    good, bad = validate(df)
    assert good.count() + bad.count() == df.count()


def test_dedupe_keeps_latest_update_then_latest_ingest(spark):
    rows = [
        _row(order_id="A", status="placed", updated_at="2024-01-01 10:00:00"),
        _row(order_id="A", status="shipped", updated_at="2024-01-02 09:00:00"),   # newest -> wins
        _row(order_id="B", status="placed", _ingested_at=datetime(2024, 1, 5, 1)),
        _row(order_id="B", status="paid", _ingested_at=datetime(2024, 1, 5, 2)),   # tie -> later ingest wins
    ]
    good, _ = validate(spark.createDataFrame(rows, BRONZE_IN))
    out = {r["order_id"]: r["status"] for r in dedupe_latest(good).collect()}
    assert out == {"A": "shipped", "B": "paid"}


def test_gold_excludes_cancelled_from_revenue_but_not_from_orders(spark):
    rows = [
        _row(order_id="A", amount="100", status="paid"),
        _row(order_id="B", amount="50", status="shipped"),
        _row(order_id="C", amount="30", status="cancelled"),
        _row(order_id="D", amount="20", status="cancelled", country="UG"),
    ]
    good, _ = validate(spark.createDataFrame(rows, BRONZE_IN))
    g = {r["country"]: r for r in gold_aggregate(good).collect()}
    assert (g["KE"]["orders"], g["KE"]["net_orders"], g["KE"]["net_revenue"]) == (3, 2, 150.0)
    assert g["KE"]["avg_order_value"] == 60.0
    assert (g["UG"]["orders"], g["UG"]["net_orders"], g["UG"]["net_revenue"]) == (1, 0, 0.0)  # all cancelled -> 0, not NULL
