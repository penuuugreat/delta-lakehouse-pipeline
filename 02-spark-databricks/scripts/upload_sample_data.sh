#!/usr/bin/env bash
# Generate 3 days of sample CSVs and upload them to the UC Volume landing folder.
# usage: scripts/upload_sample_data.sh [catalog] [schema]
set -euo pipefail
CATALOG="${1:-main}"; SCHEMA="${2:-lakehouse}"
python -m lakehouse_spark.generate --start 2024-01-01 --days 3 --no-updates --out data/source
for f in data/source/orders_*.csv; do
  databricks fs cp "$f" "dbfs:/Volumes/${CATALOG}/${SCHEMA}/landing/orders/$(basename "$f")" --overwrite
done
