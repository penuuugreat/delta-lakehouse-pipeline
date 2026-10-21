"""Local SparkSession with Delta (needs internet once to fetch the Delta JARs)."""
from pyspark.sql import SparkSession


def get_spark(app: str = "lakehouse-local") -> SparkSession:
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.master("local[2]").appName(app)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
