import os

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Plain local Spark (no Delta needed): enough for the pure transformation tests."""
    s = (SparkSession.builder.master("local[2]").appName("unit")
         .config("spark.sql.shuffle.partitions", "2").config("spark.ui.enabled", "false")
         .getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


@pytest.fixture(scope="session")
def delta_spark():
    """Spark + Delta. Needs internet to fetch JARs, so it is opt-in: RUN_DELTA_TESTS=1."""
    if os.environ.get("RUN_DELTA_TESTS") != "1":
        pytest.skip("set RUN_DELTA_TESTS=1 to run Delta end-to-end tests")
    from lakehouse_spark.local import get_spark
    s = get_spark("delta-tests")
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()
