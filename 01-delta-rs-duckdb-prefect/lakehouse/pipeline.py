"""Plain-Python pipeline: bronze -> silver -> [DQ gate] -> gold -> [DQ gate]."""
from . import bronze, extract, gold, silver
from .paths import DEFAULT, Paths
from .quality import assert_all, check_gold, check_silver


def run_pipeline(paths: Paths = DEFAULT) -> dict:
    print("[extract] pulling from source API (skipped if SOURCE_URL is unset)")
    print("   ", extract.pull(paths))

    print("[bronze] ingesting new files")
    b = bronze.ingest(paths)
    print("   ", b)

    print("[silver] transforming new batches")
    s = silver.transform(paths)
    print("   ", s)
    assert_all(check_silver(paths))

    print("[gold] aggregating affected dates")
    g = gold.build(paths, s["affected_dates"])
    print("   ", g)
    assert_all(check_gold(paths))
    return {"bronze": b, "silver": s, "gold": g}


if __name__ == "__main__":
    run_pipeline()
