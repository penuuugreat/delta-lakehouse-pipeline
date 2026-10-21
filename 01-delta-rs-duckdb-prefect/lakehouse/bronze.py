"""BRONZE: raw, append-only, as-received (all strings) + lineage metadata.

Incremental logic: a file is ingested once. We find already-loaded files by
reading `_source_file` from the Delta table itself (no external state to drift).
"""
from datetime import datetime, timezone

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
from deltalake import write_deltalake

from .delta_utils import open_table
from .paths import DEFAULT, Paths

RAW_COLS = ["order_id", "customer_id", "country", "amount", "status", "order_ts", "updated_at"]
BRONZE_SCHEMA = pa.schema(
    [(c, pa.string()) for c in RAW_COLS]
    + [
        ("_source_file", pa.string()),
        ("_ingested_at", pa.string()),
        ("ingest_date", pa.string()),
        ("_batch_id", pa.int64()),
    ]
)


def ingest(paths: Paths = DEFAULT) -> dict:
    dt = open_table(paths.bronze)
    done, batch_id = set(), 1
    if dt is not None:
        t = dt.to_pyarrow_table(columns=["_source_file", "_batch_id"])
        done = set(t["_source_file"].to_pylist())
        batch_id = pc.max(t["_batch_id"]).as_py() + 1

    files = [f for f in sorted(paths.source.glob("orders_*.csv")) if f.name not in done]
    if not files:
        return {"files": 0, "rows": 0, "batch_id": None}

    now = datetime.now(timezone.utc)
    frames = []
    for f in files:
        df = pd.read_csv(f, dtype=str)[RAW_COLS]
        df["_source_file"] = f.name
        frames.append(df)
    df = pd.concat(frames, ignore_index=True)
    df["_ingested_at"] = now.isoformat()
    df["ingest_date"] = now.strftime("%Y-%m-%d")
    df["_batch_id"] = batch_id

    table = pa.Table.from_pandas(df, schema=BRONZE_SCHEMA, preserve_index=False)
    write_deltalake(paths.bronze, table, mode="append", partition_by=["ingest_date"])
    return {"files": len(files), "rows": len(df), "batch_id": batch_id}
