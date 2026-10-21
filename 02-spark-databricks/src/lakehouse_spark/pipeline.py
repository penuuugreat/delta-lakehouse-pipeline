"""Whole pipeline in one process (used locally and by tests). On Databricks each step is a Workflow task."""
from . import bronze, gold, quality, silver
from .lake import Lake
from .tables import ensure_tables


def run_all(lake: Lake) -> None:
    ensure_tables(lake)
    print("[bronze]")
    if lake.cfg.is_local:
        print("   rows loaded:", bronze.ingest_batch(lake))
    else:
        bronze.ingest_autoloader(lake)
    print("[silver]")
    silver.run(lake)
    quality.assert_all(quality.check_silver(lake))
    print("[gold]")
    gold.run(lake)
    quality.assert_all(quality.check_gold(lake))
