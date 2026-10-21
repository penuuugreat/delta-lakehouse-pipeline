"""BRONZE: raw, append-only, all strings + lineage columns.

Databricks: Auto Loader (cloudFiles). Its checkpoint records which files were processed,
so only NEW files are read, even with millions of files in the landing folder.
Local: Auto Loader does not exist outside Databricks, so a batch fallback skips files
already present in bronze (`_source_file`). Same output schema, same downstream code.
"""
from pyspark.sql import functions as F

from .lake import Lake
from .tables import BRONZE, BRONZE_SCHEMA, RAW_COLS, RAW_SCHEMA


def ingest_autoloader(lake: Lake) -> None:
    stream = (
        lake.spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("pathGlobFilter", "orders_*.csv")
        .schema(RAW_SCHEMA)
        .load(lake.cfg.landing)
    )
    out = stream.select(
        *RAW_COLS,
        F.col("_metadata.file_path").alias("_source_file"),
        F.current_timestamp().alias("_ingested_at"),
        F.current_date().alias("ingest_date"),
    )
    lake.write_stream(out, BRONZE, lake.checkpoint("bronze"))


def ingest_batch(lake: Lake) -> int:
    raw = (
        lake.spark.read.option("header", "true")
        .option("pathGlobFilter", "orders_*.csv")
        .schema(RAW_SCHEMA)
        .csv(lake.cfg.landing)
        .select(*RAW_COLS, F.col("_metadata.file_path").alias("_source_file"))
    )
    if lake.exists(BRONZE):
        done = lake.read(BRONZE).select("_source_file").distinct()
        raw = raw.join(done, "_source_file", "left_anti")
    out = (
        raw.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("ingest_date", F.current_date())
        .select(*[f.name for f in BRONZE_SCHEMA.fields])
        .persist()
    )
    n = out.count()
    if n:
        lake.append(out, BRONZE)
    out.unpersist()
    return n
