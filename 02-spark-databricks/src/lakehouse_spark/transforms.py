"""Pure DataFrame logic: no I/O, so it is unit-testable on a plain local SparkSession."""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from .tables import ALLOWED_STATUS, RAW_COLS


def validate(raw: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split bronze rows into (valid typed rows, rejected raw rows with a reason).

    try_cast returns NULL instead of raising on bad input, on any cluster/ANSI setting.
    The first failing rule wins (order matters).
    """
    df = (
        raw.withColumn("_amount", F.expr("try_cast(amount as double)"))
        .withColumn("_order_ts", F.expr("try_cast(order_ts as timestamp)"))
        .withColumn("_updated_at", F.expr("try_cast(updated_at as timestamp)"))
    )
    reason = (
        F.when(F.col("order_id").isNull() | (F.trim(F.col("order_id")) == ""), "missing_order_id")
        .when(F.col("_amount").isNull(), "invalid_amount")
        .when(F.col("_amount") < 0, "negative_amount")
        .when(F.col("status").isNull() | ~F.col("status").isin(*ALLOWED_STATUS), "invalid_status")
        .when(F.col("country").isNull(), "missing_country")
        .when(F.col("_order_ts").isNull() | F.col("_updated_at").isNull(), "invalid_timestamp")
    )
    df = df.withColumn("_reject_reason", reason)

    good = df.where(F.col("_reject_reason").isNull()).select(
        "order_id", "customer_id", "country",
        F.col("_amount").alias("amount"),
        "status",
        F.col("_order_ts").alias("order_ts"),
        F.col("_updated_at").alias("updated_at"),
        F.to_date("_order_ts").alias("order_date"),
        "_source_file", "_ingested_at",
    )
    bad = df.where(F.col("_reject_reason").isNotNull()).select(
        *RAW_COLS, "_source_file", "_ingested_at", "_reject_reason"
    )
    return good, bad


def dedupe_latest(df: DataFrame) -> DataFrame:
    """One row per order_id: newest updated_at wins, ingestion time breaks ties.
    Required before MERGE: Delta errors if several source rows match one target row."""
    w = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc(), F.col("_ingested_at").desc())
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def gold_aggregate(silver: DataFrame) -> DataFrame:
    live = F.col("status") != "cancelled"
    return silver.groupBy("order_date", "country").agg(
        F.count("*").cast("long").alias("orders"),
        F.sum(F.when(live, 1).otherwise(0)).cast("long").alias("net_orders"),
        F.round(F.coalesce(F.sum(F.when(live, F.col("amount"))), F.lit(0.0)), 2).alias("net_revenue"),
        F.round(F.avg("amount"), 2).alias("avg_order_value"),
    )
