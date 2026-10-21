from datetime import date

import pandas as pd
import pyarrow as pa
import pytest
from deltalake import write_deltalake

from lakehouse.delta_utils import open_table
from lakehouse.generate import generate_day
from lakehouse.paths import Paths
from lakehouse.pipeline import run_pipeline
from lakehouse.quality import DataQualityError, assert_all, check_gold, check_silver


@pytest.fixture
def p(tmp_path):
    return Paths(tmp_path)


def test_incremental_load_is_idempotent_and_applies_updates(p):
    generate_day(date(2024, 1, 1), paths=p, with_updates=False)
    r1 = run_pipeline(p)
    assert r1["bronze"]["files"] == 1 and r1["silver"]["valid"] > 0
    assert r1["silver"]["quarantined"] > 0 and r1["silver"]["duplicates_dropped"] > 0

    # re-run with no new files -> nothing happens
    r2 = run_pipeline(p)
    assert r2["bronze"]["files"] == 0 and r2["silver"]["rows_in"] == 0

    silver_v = open_table(p.silver).version()
    generate_day(date(2024, 1, 2), paths=p)
    r3 = run_pipeline(p)
    assert r3["bronze"]["files"] == 1                      # only the NEW file
    assert open_table(p.silver).version() == silver_v + 1  # one MERGE commit

    s = open_table(p.silver).to_pandas()
    assert s["order_id"].is_unique
    jan1 = s[s["order_date"] == date(2024, 1, 1)]
    assert jan1["status"].isin(["shipped", "cancelled"]).any()  # updates landed


def test_stale_update_does_not_overwrite_newer_data(p):
    generate_day(date(2024, 1, 1), paths=p, with_updates=False)
    run_pipeline(p)
    s = open_table(p.silver).to_pandas()
    row = s[s["status"].isin(["placed", "paid"])].iloc[0]

    stale = pd.DataFrame([dict(
        order_id=row.order_id, customer_id=row.customer_id, country=row.country,
        amount=row.amount, status="cancelled", order_ts=str(row.order_ts),
        updated_at="2000-01-01 00:00:00")])
    stale.to_csv(p.source / "orders_2024-01-09.csv", index=False)
    run_pipeline(p)

    after = open_table(p.silver).to_pandas().set_index("order_id").loc[row.order_id]
    assert after["status"] == row.status


def test_quality_gate_fails_on_bad_data(p):
    generate_day(date(2024, 1, 1), paths=p, with_updates=False)
    run_pipeline(p)
    assert_all(check_silver(p))
    assert_all(check_gold(p))

    bad = open_table(p.gold).to_pandas().head(1)
    write_deltalake(p.gold, pa.Table.from_pandas(bad, preserve_index=False), mode="append")  # duplicate key
    with pytest.raises(DataQualityError):
        assert_all(check_gold(p))
