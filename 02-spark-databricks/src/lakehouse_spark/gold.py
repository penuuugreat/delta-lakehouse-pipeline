"""GOLD: daily revenue by country.

Incremental logic: stream silver's CHANGE DATA FEED (Trigger.AvailableNow). Each micro-batch
tells us exactly which order_dates were inserted/updated; only those dates are re-aggregated
and MERGEd. No hand-over of state between tasks, and gold stays correct after late updates.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .lake import Lake
from .tables import GOLD, SILVER
from .transforms import gold_aggregate


def process_batch(lake: Lake, changes: DataFrame, batch_id: int) -> None:
    dates = (
        changes.where(F.col("_change_type").isin("insert", "update_postimage"))
        .select("order_date").distinct()
    )
    if dates.isEmpty():
        return
    silver = lake.read(SILVER).join(F.broadcast(dates), "order_date", "left_semi")
    agg = gold_aggregate(silver)
    (
        lake.table(GOLD).alias("t")
        .merge(agg.alias("s"), "t.order_date = s.order_date AND t.country = s.country")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"[gold] batch={batch_id} recomputed_dates={dates.count()}")


def run(lake: Lake) -> None:
    q = (
        lake.read_change_feed_stream(SILVER)
        .writeStream.foreachBatch(lambda df, bid: process_batch(lake, df, bid))
        .option("checkpointLocation", lake.checkpoint("gold"))
        .trigger(availableNow=True)
        .start()
    )
    q.awaitTermination()
