"""EXTRACT with dlt (data load tool): same landing-folder contract as extract.py.

What dlt does for us (so we no longer hand-write it):
  * incremental cursor on `updated_at` + boundary de-duplication by `order_id`,
    stored in dlt's own state (no custom watermark code)
  * HTTP retries / backoff (dlt.sources.helpers.requests)
  * schema + load bookkeeping (load ids, load history)
  * writing the output files; the destination can later become S3/ADLS/a warehouse
    by changing one line.

dlt writes data/source/orders_<load_id>.<file_id>.csv, which Bronze's `orders_*.csv`
glob already picks up. The extra `_dlt_load_id` / `_dlt_id` columns are ignored by
Bronze, which selects RAW_COLS only.

Needs: pip install "dlt[filesystem]"
"""
import os

import dlt
from dlt.sources.helpers import requests

from .paths import DEFAULT, Paths

PAGE_SIZE = 500
INITIAL = "1970-01-01 00:00:00"


@dlt.resource(
    name="orders",
    primary_key="order_id",
    write_disposition="append",  # Bronze is append-only; Silver does the MERGE
    # keep timestamps as text, exactly as received (Bronze = raw strings)
    columns={"order_ts": {"data_type": "text"}, "updated_at": {"data_type": "text"}},
)
def orders(
    url: str,
    token: str = "",
    updated_at=dlt.sources.incremental("updated_at", initial_value=INITIAL),
):
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    page = 1
    while True:
        r = requests.get(
            url,
            params={"updated_since": updated_at.start_value, "page": page, "page_size": PAGE_SIZE},
            headers=headers,
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
        yield data["results"]
        if not data.get("next"):
            break
        page += 1


def pull(paths: Paths = DEFAULT) -> dict:
    # dlt gzips CSVs by default (orders_*.csv.gz); Bronze globs plain orders_*.csv
    os.environ.setdefault("NORMALIZE__DATA_WRITER__DISABLE_COMPRESSION", "true")
    pipeline = dlt.pipeline(
        pipeline_name="orders_api",
        pipelines_dir=str(paths.root / "state" / "dlt"),
        destination=dlt.destinations.filesystem(
            bucket_url=paths.root.resolve().as_uri(),
            layout="{table_name}_{load_id}.{file_id}.{ext}",
            # warning is about `replace`/`drop` dispositions; we only append
            warn_unsafe_layout_separators=False,
        ),
        dataset_name="source",  # -> <root>/source == Paths.source
    )
    info = pipeline.run(
        orders(os.environ["SOURCE_URL"], os.environ.get("SOURCE_TOKEN", "")),
        loader_file_format="csv",
    )
    # loads == 0 means nothing new at the source (idempotent no-op); row counts are
    # reported by Bronze on the files dlt wrote
    return {"backend": "dlt", "loads": len(info.loads_ids), "file": None}


if __name__ == "__main__":
    print(pull())
