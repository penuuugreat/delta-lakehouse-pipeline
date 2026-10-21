# Design decisions & trade-offs (Spark / Databricks version)

## Why Spark + Databricks here (vs. the delta-rs + DuckDB version)
| | delta-rs + DuckDB + Prefect | Spark + Databricks Workflows (this) |
|---|---|---|
| Scale ceiling | one machine | horizontal; same code at 100× data |
| Start-up / cost | instant, free | cluster spin-up (minutes) and DBU cost; overkill for ~400 rows |
| Incremental state | hand-written watermark in a JSON file | **checkpoints** managed by the engine |
| Governance | none | Unity Catalog: grants, lineage, audit, volumes |
| Ingestion | glob + `_source_file` bookkeeping | **Auto Loader** (file notification / directory listing at scale) |
| Testability | everything runs locally | pure logic is local; Delta/Auto Loader parts need JARs/workspace |
| Portability | runs anywhere | tied to Databricks features (Auto Loader, UC Volumes) |

The honest summary: for this data size Spark is the wrong tool on cost alone. It is chosen to show the patterns that matter at scale and in enterprise Databricks shops.

## Why Delta (and MERGE)
ACID commits make streaming writes and `MERGE` safe under retries; `DESCRIBE HISTORY` and `VERSION AS OF` give audit and time travel; Change Data Feed gives row-level change events. Plain Parquet would force full rewrites for every update.

## Incremental strategy
| Choice | Alternative | Why |
|---|---|---|
| **Auto Loader** for bronze | `COPY INTO`, or batch read + anti-join on `_source_file` | Scales to millions of files without re-listing, schema-aware, exactly-once file processing via checkpoint. Trade-off: Databricks-only; local runs use the anti-join fallback, so Auto Loader itself is not covered by local tests |
| **Streaming with `Trigger.AvailableNow`** bronze→silver | batch job with manual watermark | Gets checkpointed, exactly-once-ish processing while still running as a finite batch job (cheap; no 24/7 cluster). Trade-off: reprocessing requires resetting the checkpoint, and state lives outside the table |
| **`foreachBatch` + `MERGE`** | append + dedupe at query time | Silver is a true current-state table. Trade-off: MERGE is heavier than append; mitigated by pruning on `order_date` in the match condition |
| `MERGE … WHEN MATCHED AND s.updated_at >= t.updated_at` | blind update | Stale / out-of-order records cannot overwrite newer data |
| Quarantine append with **`txnAppId`/`txnVersion`** | plain append | A retried micro-batch cannot double-write (Delta idempotent writes) |
| Gold driven by **Change Data Feed** | pass affected dates between tasks, or full rebuild | Decoupled tasks (gold needs no knowledge of silver's run), correct under late updates, cost ∝ change. Trade-off: CDF adds storage overhead and must be enabled before the data arrives |

## Partitioning / layout
* **Bronze `ingest_date`:** append-only, queried by arrival time; each run touches one partition.
* **Silver `order_date`:** immutable per order, so the MERGE condition `t.order_date = s.order_date` prunes partitions and updates never move rows between partitions.
* **Gold unpartitioned:** days × countries is tiny; partitioning only creates small files.
* **Modern alternative:** on Unity Catalog managed tables, **liquid clustering** (`CLUSTER BY (order_date)`) replaces Hive-style partitioning and can be changed without rewriting data. Not used here because Hive-style partitions are portable to the local Delta runtime and are easier to explain; at real scale prefer liquid clustering.
* At this data size, partitioning is illustrative: partitions should hold roughly ≥1 GB each to pay off.

## Data quality
* **Quarantine vs fail:** dirty *rows* are expected and must not block the day (quarantine + counted); a violated *table invariant* indicates a bug and fails the run.
* **Gates are separate Workflow tasks**, so failures show up in the run graph and alerting, and gold physically cannot run on bad silver.
* **Custom checks vs DLT/Lakeflow expectations:** Lakeflow Declarative Pipelines would express the same rules declaratively (`EXPECT ... ON VIOLATION DROP/FAIL`) with built-in lineage and event logs. Plain PySpark was chosen so the logic is unit-testable locally and portable; DLT is the natural next step on Databricks.

## Orchestration
Databricks Workflows keeps scheduling, retries (on bronze only; transforms are deterministic), alerting and the run graph in the platform, with no extra system to operate. Trade-off versus Airflow/Dagster: tight coupling to Databricks and fewer cross-system integrations. Business logic lives in the wheel, not in the job definition, so tasks are replaceable.

## Known limitations
* Hard deletes in the source are not propagated (needs tombstones or CDC).
* Schema evolution: explicit schemas fail loudly on new columns by design; Auto Loader's rescue/evolution modes are not enabled.
* Single-job assumption (`max_concurrent_runs: 1`); the checkpoint must not be shared by concurrent runs.
* The Delta end-to-end tests have not been executed by the author in the build environment; CI runs them.
