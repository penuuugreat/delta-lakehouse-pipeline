"""End-to-end tests on real Delta tables (MERGE, CDF, streaming checkpoints).
Opt-in: RUN_DELTA_TESTS=1 pytest -v   (needs Java + internet to fetch Delta JARs)."""
from datetime import date

import pandas as pd
import pytest

from lakehouse_spark.config import Config
from lakehouse_spark.generate import generate_day
from lakehouse_spark.lake import Lake
from lakehouse_spark.pipeline import run_all
from lakehouse_spark.quality import DataQualityError, assert_all, check_gold, check_silver
from lakehouse_spark.tables import BRONZE, GOLD, QUARANTINE, RAW_COLS, SILVER


@pytest.fixture
def env(delta_spark, tmp_path):
    cfg = Config(mode="local", local_root=str(tmp_path / "data"),
                 landing_path=str(tmp_path / "landing"), checkpoint_root=str(tmp_path / "ckpt"))
    return Lake(delta_spark, cfg), cfg


def _versions(lake, name):
    return lake.table(name).history().count()


def test_incremental_idempotent_and_updates_applied(env):
    lake, cfg = env
    generate_day(date(2024, 1, 1), cfg.landing, with_updates=False)
    run_all(lake)
    b1, s1 = lake.read(BRONZE).count(), lake.read(SILVER).count()
    assert b1 > 0 and s1 > 0 and lake.read(QUARANTINE).count() > 0

    v_silver, v_gold = _versions(lake, SILVER), _versions(lake, GOLD)
    run_all(lake)                                             # nothing new -> no new commits
    assert (_versions(lake, SILVER), _versions(lake, GOLD)) == (v_silver, v_gold)
    assert lake.read(BRONZE).count() == b1

    generate_day(date(2024, 1, 2), cfg.landing)               # new file with updates to day 1
    run_all(lake)
    assert lake.read(BRONZE).count() > b1
    silver = lake.read(SILVER)
    assert silver.count() == silver.select("order_id").distinct().count()
    assert silver.where("order_date = '2024-01-01' AND status IN ('shipped','cancelled')").count() > 0
    assert _versions(lake, SILVER) > v_silver


def test_stale_update_does_not_overwrite_newer_data(env):
    lake, cfg = env
    generate_day(date(2024, 1, 1), cfg.landing, with_updates=False)
    run_all(lake)
    row = lake.read(SILVER).where("status IN ('placed','paid')").first()
    stale = pd.DataFrame([dict(order_id=row["order_id"], customer_id=row["customer_id"],
                               country=row["country"], amount=str(row["amount"]), status="cancelled",
                               order_ts=str(row["order_ts"]), updated_at="2000-01-01 00:00:00")])[RAW_COLS]
    stale.to_csv(f"{cfg.landing}/orders_2024-01-09.csv", index=False)
    run_all(lake)
    after = lake.read(SILVER).where(f"order_id = '{row['order_id']}'").first()
    assert after["status"] == row["status"]


def test_gold_matches_silver_after_late_update(env):
    lake, cfg = env
    generate_day(date(2024, 1, 1), cfg.landing, with_updates=False)
    generate_day(date(2024, 1, 2), cfg.landing)
    run_all(lake)
    assert_all(check_silver(lake))
    assert_all(check_gold(lake))


def test_quality_gate_fails_on_corrupt_gold(env):
    lake, cfg = env
    generate_day(date(2024, 1, 1), cfg.landing, with_updates=False)
    run_all(lake)
    lake.append(lake.read(GOLD).limit(1), GOLD)               # duplicate key
    with pytest.raises(DataQualityError):
        assert_all(check_gold(lake))
