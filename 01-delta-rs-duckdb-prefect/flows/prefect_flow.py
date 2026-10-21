"""Prefect orchestration: retries, observability and a daily 02:00 schedule.

  python flows/prefect_flow.py            # run once
  python flows/prefect_flow.py --serve    # run on a schedule (UI: `prefect server start`)
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from prefect import flow, task  # noqa: E402

from lakehouse import bronze, extract, gold, silver  # noqa: E402
from lakehouse.paths import DEFAULT  # noqa: E402
from lakehouse.quality import assert_all, check_gold, check_silver  # noqa: E402


@task(retries=3, retry_delay_seconds=30)
def extract_source():
    return extract.pull(DEFAULT)


@task(retries=2, retry_delay_seconds=10)
def ingest_bronze():
    return bronze.ingest(DEFAULT)


@task
def transform_silver():
    return silver.transform(DEFAULT)


@task
def dq_silver():
    assert_all(check_silver(DEFAULT))


@task
def build_gold(dates):
    return gold.build(DEFAULT, dates)


@task
def dq_gold():
    assert_all(check_gold(DEFAULT))


@flow(name="daily-lakehouse")
def daily_lakehouse():
    extract_source()
    ingest_bronze()
    s = transform_silver()
    dq_silver()
    build_gold(s["affected_dates"])
    dq_gold()


if __name__ == "__main__":
    if "--serve" in sys.argv:
        daily_lakehouse.serve(name="daily-lakehouse", cron="0 2 * * *")
    else:
        daily_lakehouse()
