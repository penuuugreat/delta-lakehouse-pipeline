"""Workflow task entry points (see [project.scripts] in pyproject.toml)."""
import argparse

from pyspark.sql import SparkSession

from . import bronze, gold, quality, silver
from .config import Config
from .lake import Lake
from .tables import ensure_tables


def _lake(extra=None) -> tuple[Lake, argparse.Namespace]:
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", default="main")
    p.add_argument("--schema", default="lakehouse")
    p.add_argument("--landing-path", default=None)
    p.add_argument("--checkpoint-root", default=None)
    if extra:
        extra(p)
    a = p.parse_args()
    cfg = Config(catalog=a.catalog, schema=a.schema, landing_path=a.landing_path, checkpoint_root=a.checkpoint_root)
    return Lake(SparkSession.builder.getOrCreate(), cfg), a


def setup_main():
    lake, _ = _lake()
    ensure_tables(lake)


def bronze_main():
    lake, _ = _lake()
    ensure_tables(lake)
    bronze.ingest_autoloader(lake)


def silver_main():
    lake, _ = _lake()
    silver.run(lake)


def gold_main():
    lake, _ = _lake()
    gold.run(lake)


def dq_main():
    lake, a = _lake(lambda p: p.add_argument("--layer", choices=["silver", "gold"], required=True))
    checks = quality.check_silver(lake) if a.layer == "silver" else quality.check_gold(lake)
    quality.assert_all(checks)
