"""EXTRACT: pull new/changed orders from a REST API into the landing folder.

The landing folder (data/source/) is the contract: Bronze ingests any new
`orders_*.csv` it finds there, whoever wrote it. So Bronze/Silver/Gold are untouched,
and the source can be swapped (generator -> hand-written API client -> dlt) without
touching them.

Config (env vars)
  SOURCE_URL       API endpoint. If unset, extract is skipped (the CSV-drop / generator
                   workflow and the existing tests keep working unchanged).
  SOURCE_TOKEN     optional bearer token
  EXTRACT_BACKEND  "requests" (default, this file) or "dlt" (see extract_dlt.py)

Assumed API contract: GET SOURCE_URL?updated_since=<ts>&page=<n>&page_size=<n>
                      -> {"results": [ {RAW_COLS...} ], "next": <truthy if more pages>}
"""
import os
from datetime import datetime, timezone

import pandas as pd
import requests

from .bronze import RAW_COLS
from .paths import DEFAULT, Paths, get_watermark, set_watermark

WM_KEY = "extract_orders_updated_at"
PAGE_SIZE = 500


def _get(url: str, params: dict, token: str) -> dict:
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    r = requests.get(url, params=params, headers=headers, timeout=30)
    r.raise_for_status()  # a failed call raises -> the Prefect task retries
    return r.json()


def _pull_requests(paths: Paths) -> dict:
    url = os.environ["SOURCE_URL"]
    token = os.environ.get("SOURCE_TOKEN", "")
    since = get_watermark(paths, WM_KEY, default="1970-01-01 00:00:00")

    rows, page = [], 1
    while True:  # pagination
        data = _get(url, {"updated_since": since, "page": page, "page_size": PAGE_SIZE}, token)
        rows += data["results"]
        if not data.get("next"):
            break
        page += 1

    df = pd.DataFrame(rows, columns=RAW_COLS)

    # APIs are often inclusive (>=), so drop what we already have -> re-runs are a no-op.
    # Rows with an unparseable updated_at are KEPT: Silver quarantines them with a reason
    # instead of them vanishing silently here.
    ts = pd.to_datetime(df["updated_at"], errors="coerce", utc=True)
    df = df[ts.isna() | (ts > pd.to_datetime(since, utc=True))]
    ts = ts.loc[df.index]
    if df.empty:
        return {"rows": 0, "file": None}

    paths.source.mkdir(parents=True, exist_ok=True)
    name = f"orders_{datetime.now(timezone.utc):%Y-%m-%dT%H%M%S%f}.csv"  # unique name -> Bronze loads it once
    df.to_csv(paths.source / name, index=False)

    # Advance the watermark only AFTER the file is safely written: a crash can cause a
    # replay (harmless, Silver MERGEs) but never a gap.
    if ts.notna().any():
        set_watermark(paths, WM_KEY, df.loc[ts.idxmax(), "updated_at"])
    return {"rows": len(df), "file": name}


def pull(paths: Paths = DEFAULT) -> dict:
    if not os.environ.get("SOURCE_URL"):
        return {"skipped": "SOURCE_URL not set", "rows": 0, "file": None}
    if os.environ.get("EXTRACT_BACKEND", "requests") == "dlt":
        from .extract_dlt import pull as pull_dlt  # imported lazily: dlt is optional

        return pull_dlt(paths)
    return _pull_requests(paths)


if __name__ == "__main__":
    print(pull())
