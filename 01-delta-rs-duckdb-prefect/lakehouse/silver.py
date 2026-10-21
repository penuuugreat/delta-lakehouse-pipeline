"""SILVER: typed, validated, de-duplicated, one row per order_id.

Incremental logic
  1. Read only bronze rows with _batch_id > watermark.
  2. Validate; bad rows go to a quarantine table with a reason (never silently dropped).
  3. De-duplicate inside the batch (latest updated_at wins).
  4. MERGE into silver on order_id; only overwrite if the incoming row is newer,
     so late/stale records can't clobber fresher data.
  5. Advance the watermark LAST -> a crash re-processes the batch safely (idempotent).
"""
import pandas as pd
import pyarrow as pa
from deltalake import write_deltalake

from .bronze import RAW_COLS
from .delta_utils import open_table
from .paths import DEFAULT, Paths, get_watermark, set_watermark

ALLOWED_STATUS = {"placed", "paid", "shipped", "cancelled"}

SILVER_SCHEMA = pa.schema(
    [
        ("order_id", pa.string()),
        ("customer_id", pa.string()),
        ("country", pa.string()),
        ("amount", pa.float64()),
        ("status", pa.string()),
        ("order_ts", pa.timestamp("us")),
        ("updated_at", pa.timestamp("us")),
        ("order_date", pa.date32()),
        ("_batch_id", pa.int64()),
    ]
)
QUARANTINE_SCHEMA = pa.schema(
    [(c, pa.string()) for c in RAW_COLS]
    + [("_source_file", pa.string()), ("_batch_id", pa.int64()), ("_reject_reason", pa.string())]
)


def validate(raw: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split raw bronze rows into (valid typed rows, rejected raw rows + reason)."""
    df = raw.copy()
    df["amount_n"] = pd.to_numeric(df["amount"], errors="coerce")
    df["order_ts_n"] = pd.to_datetime(df["order_ts"], errors="coerce")
    df["updated_at_n"] = pd.to_datetime(df["updated_at"], errors="coerce")

    rules = [  # first failing rule wins
        ("missing_order_id", df["order_id"].isna() | (df["order_id"].fillna("").str.strip() == "")),
        ("invalid_amount", df["amount_n"].isna()),
        ("negative_amount", df["amount_n"] < 0),
        ("invalid_status", ~df["status"].isin(ALLOWED_STATUS)),
        ("missing_country", df["country"].isna()),
        ("invalid_timestamp", df["order_ts_n"].isna() | df["updated_at_n"].isna()),
    ]
    reason = pd.Series(None, index=df.index, dtype="object")
    for name, mask in rules:
        reason = reason.mask(reason.isna() & mask, name)

    ok = df[reason.isna()]
    good = pd.DataFrame(
        {
            "order_id": ok["order_id"],
            "customer_id": ok["customer_id"],
            "country": ok["country"],
            "amount": ok["amount_n"],
            "status": ok["status"],
            "order_ts": ok["order_ts_n"],
            "updated_at": ok["updated_at_n"],
            "order_date": ok["order_ts_n"].dt.date,
            "_batch_id": ok["_batch_id"],
        }
    )
    bad = raw[reason.notna()].copy()
    bad["_reject_reason"] = reason[reason.notna()]
    return good, bad


def transform(paths: Paths = DEFAULT) -> dict:
    wm = get_watermark(paths, "silver_bronze_batch", 0)
    summary = {"rows_in": 0, "valid": 0, "duplicates_dropped": 0, "quarantined": 0, "affected_dates": []}
    bronze = open_table(paths.bronze)
    if bronze is None:
        return summary

    raw = bronze.to_pyarrow_table(filters=[("_batch_id", ">", wm)]).to_pandas()
    if raw.empty:
        return summary

    good, bad = validate(raw)
    deduped = good.sort_values(["updated_at", "_batch_id"]).drop_duplicates("order_id", keep="last")
    dupes = len(good) - len(deduped)
    assert len(raw) == len(deduped) + dupes + len(bad), "row reconciliation failed"

    # --- quarantine (idempotent: remove this batch's rows before re-appending)
    if len(bad):
        q = pa.Table.from_pandas(
            bad[[f.name for f in QUARANTINE_SCHEMA]], schema=QUARANTINE_SCHEMA, preserve_index=False
        )
        qt = open_table(paths.quarantine)
        if qt is not None:
            qt.delete(f"_batch_id > {wm}")
        write_deltalake(paths.quarantine, q, mode="append")

    # --- upsert into silver
    if len(deduped):
        src = pa.Table.from_pandas(deduped[[f.name for f in SILVER_SCHEMA]], schema=SILVER_SCHEMA, preserve_index=False)
        st = open_table(paths.silver)
        if st is None:
            write_deltalake(paths.silver, src, partition_by=["order_date"])
        else:
            (
                st.merge(
                    source=src,
                    predicate="t.order_id = s.order_id AND t.order_date = s.order_date",
                    source_alias="s",
                    target_alias="t",
                )
                .when_matched_update_all(predicate="s.updated_at >= t.updated_at")
                .when_not_matched_insert_all()
                .execute()
            )

    set_watermark(paths, "silver_bronze_batch", int(raw["_batch_id"].max()))
    summary.update(
        rows_in=len(raw),
        valid=len(deduped),
        duplicates_dropped=dupes,
        quarantined=len(bad),
        affected_dates=sorted({d.isoformat() for d in deduped["order_date"]}),
    )
    return summary
