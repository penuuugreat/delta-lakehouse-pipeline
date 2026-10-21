# Design decisions & trade-offs

## Why Delta Lake (vs Parquet, Iceberg, a warehouse)?
* **ACID commits + MERGE:** incremental upserts into silver/gold are the core of this project. Plain Parquet would force "read everything, rewrite everything" and can leave readers with half-written data.
* **Time travel / history:** `DeltaTable.history()` gives an audit trail and cheap debugging (`make demo` prints it).
* **Why `delta-rs` instead of Spark:** the data is small; a JVM + Spark cluster adds cost and setup with no benefit. The same tables open unchanged in Spark/Databricks/Fabric if the data grows.
* **Why not Iceberg:** equally valid technically; Delta has the more mature lightweight Python writer today. Choosing Iceberg would matter more for multi-engine catalogs.
* **Why not just load into Postgres/BigQuery:** a warehouse would work, but the goal here is to demonstrate lakehouse mechanics (open format, layers as tables, schema enforcement) on cheap storage.

## Why these partition keys?
* **Bronze → `ingest_date`:** bronze is append-only and queried by "what arrived when" (replays, debugging). Each daily run writes into one partition; no rewrite of old data.
* **Silver → `order_date`:** business queries and the gold rebuild filter by order date, so partition pruning applies. `order_date` is **immutable** for an order, which lets the MERGE predicate include `t.order_date = s.order_date` so an update never needs to move a row between partitions.
* **Not `country` or `customer_id`:** low-cardinality (6 values → skew) or high-cardinality (tiny-file explosion) respectively.
* **Gold → unpartitioned:** it is tiny (days × countries). Partitioning would just create many small files. Revisit past roughly 1 GB.
* **Rule of thumb applied:** partition only when each partition holds ≥ ~100 MB–1 GB; at this demo's scale partitioning is mostly illustrative, which is stated openly rather than hidden.

## Incremental strategy
| Choice | Alternative | Why |
|---|---|---|
| Bronze tracks loaded **files** by reading `_source_file` from the table itself | external state DB / file moves | State lives in the same transactional store as the data → can't drift |
| Silver watermark = monotonically increasing **`_batch_id`** | `_ingested_at` timestamp | Timestamps can tie/skew across clocks; an integer is deterministic |
| Watermark advanced **after** the MERGE | before | Crash → batch is re-processed; MERGE + quarantine cleanup make that safe (**at-least-once + idempotent = effectively once**) |
| MERGE `WHEN MATCHED AND s.updated_at >= t.updated_at` | blind overwrite | Late/out-of-order records can't overwrite fresher data (tested) |
| Gold recomputes **affected dates only**, then MERGEs | full rebuild | Cost scales with change, not with history. Full rebuild is available by passing `affected_dates=None` |

## Data-quality trade-offs
* **Quarantine instead of fail-the-job for row-level problems:** a few bad rows shouldn't block the day's data, but they must be visible (reasons are counted in `make demo`). **Table-level** violations *do* fail the job because they indicate a bug, not dirty input.
* **Custom checks instead of Great Expectations/dbt tests:** fewer dependencies and everything is readable in one file. The trade-off is no auto-generated docs or rich reporting; see "Ideas to extend".
* **Checks read whole tables:** fine at this scale. At scale, restrict checks to the affected partitions.

## Orchestration
* **Prefect over Airflow:** a flow is ordinary Python, runs locally with `python flows/prefect_flow.py`, and gets retries/scheduling in a few lines. Airflow is the better fit when an org already runs it or needs many cross-team DAGs.
* **Business logic lives in `lakehouse/`, not in the flow:** the orchestrator is a thin wrapper, so the pipeline is testable without it and portable to Airflow/Dagster/cron.
* **Retries only on ingest:** transform steps are deterministic; retrying them hides bugs.

## Known limitations (deliberately out of scope)
* No schema evolution handling beyond Delta's enforcement (new columns would fail loudly in silver, by design).
* MERGE on silver scans the partitions it touches; very large tables would benefit from `OPTIMIZE`/Z-ORDER on `order_id`.
* Hard deletes in the source are not propagated (would need tombstones or CDC).
* Single-writer assumption; concurrent runs need an orchestrator-level lock.

## Extract: hand-written client vs dlt

| Decision | Chosen | Alternative | Why / trade-off |
|---|---|---|---|
| Hand-off to Bronze | **Land CSVs in `data/source/`**, Bronze unchanged | Write straight into the Bronze Delta table | One contract for every source and the existing tests still pass. Cost: one extra copy of the raw data on disk |
| Default extractor | `requests` client (`extract.py`) | dlt / Airbyte / Fivetran | ~50 lines, no extra dependency, easy to read. But pagination, retries, state and schema drift are ours to maintain, per source |
| Optional extractor | **dlt** (`extract_dlt.py`) | Airbyte (service + UI), Fivetran (paid SaaS) | dlt is a Python library, so it runs on a laptop and inside Prefect with no server. Gives incremental state, retries, load history and ready-made sources. Cost: a new dependency, and its conventions (see below) |
| Watermark | Stored in `state/watermarks.json` (requests) or dlt state (dlt) | Derive from Bronze `max(updated_at)` | Cheap and explicit. The two backends keep **separate** state, so switching backend mid-stream re-pulls from the start (harmless: Silver MERGEs) |
| Delivery guarantee | At-least-once: watermark advances only after the file is written | Exactly-once | A crash can replay rows, never skip them; Silver's MERGE (newer `updated_at` wins) makes replays harmless |

dlt specifics worth knowing: it gzips CSVs by default (disabled so Bronze's `orders_*.csv` glob matches),
it adds `_dlt_load_id` / `_dlt_id` columns (ignored: Bronze selects `RAW_COLS`), and it warns that a `_`
separator in the file layout is unsafe for `replace`/`drop`; we only append, so the warning is switched off.

Limitations: rows sharing the exact last `updated_at` that arrive *after* a pull can be missed by the
`>` watermark filter (requests backend); hard deletes at the source are still not propagated.
Tested against a local mock API only.
