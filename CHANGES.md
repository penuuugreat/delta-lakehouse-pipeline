# Changes: connector / extract step (project 01 only)

Project 02 (Spark/Databricks) is unchanged.

## New files (01-delta-rs-duckdb-prefect/)
- `lakehouse/extract.py`      REST API extractor: pagination, `updated_at` watermark, no-op re-runs; dispatches to dlt via `EXTRACT_BACKEND=dlt`
- `lakehouse/extract_dlt.py`  same landing-folder contract, extraction done by dlt
- `scripts/mock_api.py`       stdlib mock REST API (inclusive `updated_since`, pagination)
- `scripts/demo_api.py`       end-to-end demo: day 1 -> day 2 incremental -> no-op re-run
- `tests/test_extract.py`     3 tests against a real local mock API

## Edited files
- `lakehouse/pipeline.py`     calls `extract.pull()` before Bronze (skipped when `SOURCE_URL` is unset)
- `flows/prefect_flow.py`     new `extract_source` task (3 retries) ahead of `ingest_bronze`
- `Makefile`                  `demo-api`, `demo-api-dlt` targets
- `requirements.txt`          `requests`, `dlt[filesystem]`
- `README.md`, `docs/TRADEOFFS.md`, `../README.md`   documentation

## Unchanged on purpose
`bronze.py`, `silver.py`, `gold.py`, `quality.py` and the original tests.

## Verified (in a sandbox, Python 3.12, dlt 1.31)
- `pytest`: 8 passed (5 original + 3 new)
- `python scripts/demo_api.py` and `EXTRACT_BACKEND=dlt python scripts/demo_api.py`: both give
  day 1 = 204 rows, day 2 = 219 rows, re-run = 0 (no-op); silver 376 rows, gold reconciles
- Not verified: any real third-party API, Prefect scheduling with the new task, project 02
