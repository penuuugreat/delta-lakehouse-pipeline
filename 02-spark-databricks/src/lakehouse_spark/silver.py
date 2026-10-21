"""SILVER: typed, validated, de-duplicated, one row per order_id.

Incremental logic
  * Structured Streaming from bronze with Trigger.AvailableNow: the CHECKPOINT is the watermark.
  * foreachBatch: validate -> quarantine bad rows -> dedupe -> MERGE (newer updated_at wins).
  * Quarantine append uses txnAppId/txnVersion = Delta idempotent writes, so a retried
    micro-batch cannot double-insert. MERGE is idempotent by construction.
"""
from pyspark.sql import DataFrame

from .lake import Lake
from .tables import QUARANTINE, SILVER, BRONZE
from .transforms import dedupe_latest, validate


def process_batch(lake: Lake, batch_df: DataFrame, batch_id: int) -> None:
    n_in = batch_df.count()
    if n_in == 0:
        return
    good, bad = validate(batch_df)
    deduped = dedupe_latest(good)
    n_deduped, n_bad, n_good = deduped.count(), bad.count(), good.count()
    n_dupes = n_good - n_deduped
    assert n_in == n_deduped + n_dupes + n_bad, "row reconciliation failed"

    if n_bad:
        lake.append(bad, QUARANTINE, txnAppId="silver_quarantine", txnVersion=batch_id)

    if n_deduped:
        (
            lake.table(SILVER).alias("t")
            .merge(deduped.alias("s"), "t.order_id = s.order_id AND t.order_date = s.order_date")
            .whenMatchedUpdateAll(condition="s.updated_at >= t.updated_at")
            .whenNotMatchedInsertAll()
            .execute()
        )
    print(f"[silver] batch={batch_id} in={n_in} valid={n_deduped} dupes_dropped={n_dupes} quarantined={n_bad}")


def run(lake: Lake) -> None:
    q = (
        lake.read_stream(BRONZE)
        .writeStream.foreachBatch(lambda df, bid: process_batch(lake, df, bid))
        .option("checkpointLocation", lake.checkpoint("silver"))
        .trigger(availableNow=True)
        .start()
    )
    q.awaitTermination()
