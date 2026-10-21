"""Table names, schemas and DDL (explicit schemas = a data contract, not inferred)."""
import os

from pyspark.sql.types import (DateType, DoubleType, LongType, StringType,
                               StructField, StructType, TimestampType)

BRONZE = "bronze_orders"
SILVER = "silver_orders"
QUARANTINE = "silver_orders_quarantine"
GOLD = "gold_daily_revenue"

ALLOWED_STATUS = ["placed", "paid", "shipped", "cancelled"]
RAW_COLS = ["order_id", "customer_id", "country", "amount", "status", "order_ts", "updated_at"]

RAW_SCHEMA = StructType([StructField(c, StringType()) for c in RAW_COLS])

BRONZE_SCHEMA = StructType(
    list(RAW_SCHEMA.fields)
    + [
        StructField("_source_file", StringType()),
        StructField("_ingested_at", TimestampType()),
        StructField("ingest_date", DateType()),
    ]
)
SILVER_SCHEMA = StructType(
    [
        StructField("order_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("country", StringType()),
        StructField("amount", DoubleType()),
        StructField("status", StringType()),
        StructField("order_ts", TimestampType()),
        StructField("updated_at", TimestampType()),
        StructField("order_date", DateType()),
        StructField("_source_file", StringType()),
        StructField("_ingested_at", TimestampType()),
    ]
)
QUARANTINE_SCHEMA = StructType(
    list(RAW_SCHEMA.fields)
    + [
        StructField("_source_file", StringType()),
        StructField("_ingested_at", TimestampType()),
        StructField("_reject_reason", StringType()),
    ]
)
GOLD_SCHEMA = StructType(
    [
        StructField("order_date", DateType()),
        StructField("country", StringType()),
        StructField("orders", LongType()),
        StructField("net_orders", LongType()),
        StructField("net_revenue", DoubleType()),
        StructField("avg_order_value", DoubleType()),
    ]
)


def ensure_tables(lake) -> None:
    """Idempotent DDL. Safe to run at the start of every job."""
    cfg, spark = lake.cfg, lake.spark
    if cfg.is_local:
        os.makedirs(cfg.landing, exist_ok=True)
    else:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{cfg.schema}")
        for vol in ("landing", "checkpoints"):
            spark.sql(f"CREATE VOLUME IF NOT EXISTS {cfg.catalog}.{cfg.schema}.{vol}")
        os.makedirs(cfg.landing, exist_ok=True)  # Auto Loader needs the directory to exist

    lake.create(BRONZE, BRONZE_SCHEMA, partition_by=["ingest_date"])
    # Change Data Feed on silver lets gold find "what changed" without passing state between tasks.
    lake.create(SILVER, SILVER_SCHEMA, partition_by=["order_date"],
                properties={"delta.enableChangeDataFeed": "true"})
    lake.create(QUARANTINE, QUARANTINE_SCHEMA)
    lake.create(GOLD, GOLD_SCHEMA)
