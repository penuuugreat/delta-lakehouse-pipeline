"""Thin storage abstraction: the same code addresses Unity Catalog tables or local Delta paths."""
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from .config import Config


class Lake:
    def __init__(self, spark: SparkSession, cfg: Config):
        self.spark, self.cfg = spark, cfg

    # ---- addressing
    def ref(self, name: str) -> str:
        return str(self.cfg.local_lake / name) if self.cfg.is_local else f"{self.cfg.catalog}.{self.cfg.schema}.{name}"

    def exists(self, name: str) -> bool:
        if self.cfg.is_local:
            return DeltaTable.isDeltaTable(self.spark, self.ref(name))
        return self.spark.catalog.tableExists(self.ref(name))

    def table(self, name: str) -> DeltaTable:
        if self.cfg.is_local:
            return DeltaTable.forPath(self.spark, self.ref(name))
        return DeltaTable.forName(self.spark, self.ref(name))

    # ---- DDL
    def create(self, name: str, schema: StructType, partition_by=None, properties=None) -> None:
        b = DeltaTable.createIfNotExists(self.spark)
        b = b.location(self.ref(name)) if self.cfg.is_local else b.tableName(self.ref(name))
        b = b.addColumns(schema)
        if partition_by:
            b = b.partitionedBy(*partition_by)
        for k, v in (properties or {}).items():
            b = b.property(k, v)
        b.execute()

    # ---- reads
    def read(self, name: str) -> DataFrame:
        if self.cfg.is_local:
            return self.spark.read.format("delta").load(self.ref(name))
        return self.spark.read.table(self.ref(name))

    def read_stream(self, name: str) -> DataFrame:
        if self.cfg.is_local:
            return self.spark.readStream.format("delta").load(self.ref(name))
        return self.spark.readStream.table(self.ref(name))

    def read_change_feed_stream(self, name: str) -> DataFrame:
        r = (self.spark.readStream.format("delta")
             .option("readChangeFeed", "true")
             .option("startingVersion", 0))
        return r.load(self.ref(name)) if self.cfg.is_local else r.table(self.ref(name))

    # ---- writes
    def append(self, df: DataFrame, name: str, **options) -> None:
        w = df.write.format("delta").mode("append")
        for k, v in options.items():
            w = w.option(k, v)
        w.save(self.ref(name)) if self.cfg.is_local else w.saveAsTable(self.ref(name))

    def write_stream(self, df: DataFrame, name: str, checkpoint: str):
        """Run a stream with Trigger.AvailableNow: process everything new, then stop (job-friendly)."""
        w = (df.writeStream.format("delta")
             .option("checkpointLocation", checkpoint)
             .trigger(availableNow=True))
        q = w.start(self.ref(name)) if self.cfg.is_local else w.toTable(self.ref(name))
        q.awaitTermination()
        return q

    def checkpoint(self, name: str) -> str:
        return f"{self.cfg.checkpoints}/{name}"
