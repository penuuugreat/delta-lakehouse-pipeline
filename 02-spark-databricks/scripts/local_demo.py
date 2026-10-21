"""Local end-to-end demo (needs Java 17/21 + internet once for Delta JARs).

  python scripts/local_demo.py
"""
import shutil
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from lakehouse_spark.config import Config  # noqa: E402
from lakehouse_spark.generate import generate_day  # noqa: E402
from lakehouse_spark.lake import Lake  # noqa: E402
from lakehouse_spark.local import get_spark  # noqa: E402
from lakehouse_spark.pipeline import run_all  # noqa: E402
from lakehouse_spark.tables import BRONZE, GOLD, QUARANTINE, SILVER  # noqa: E402

shutil.rmtree("data", ignore_errors=True)
cfg = Config(mode="local", local_root="data", landing_path="data/source", checkpoint_root="data/_checkpoints")
spark = get_spark()
spark.sparkContext.setLogLevel("ERROR")
lake = Lake(spark, cfg)


def summary():
    for name in (BRONZE, SILVER, QUARANTINE, GOLD):
        h = [row["operation"] for row in lake.table(name).history().select("operation").collect()][::-1]
        print(f"{name:26s} rows={lake.read(name).count():5d} versions={len(h)} history={h}")


print("\n===== DAY 1 =====")
generate_day(date(2024, 1, 1), cfg.landing, with_updates=False)
run_all(lake)
print("\n===== DAY 2: new file (incremental) =====")
generate_day(date(2024, 1, 2), cfg.landing)
run_all(lake)
print("\n===== RE-RUN, no new data (idempotent) =====")
run_all(lake)
print()
summary()
print("\nQuarantine reasons:")
lake.read(QUARANTINE).groupBy("_reject_reason").count().show()
lake.read(GOLD).orderBy("order_date", "country").show(8)
