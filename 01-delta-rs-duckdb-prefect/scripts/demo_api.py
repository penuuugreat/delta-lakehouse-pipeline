"""End-to-end demo with a REAL (mock) API as the source instead of dropped CSVs.

  python scripts/demo_api.py                      # hand-written client (requests)
  EXTRACT_BACKEND=dlt python scripts/demo_api.py  # same run, extraction done by dlt

Day 1 load -> day 2 incremental load -> re-run with no new data (no-op) -> summary.
"""
import os
import sys
import threading
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from mock_api import make_server  # noqa: E402

from lakehouse.generate import generate_day  # noqa: E402
from lakehouse.paths import DEFAULT, Paths  # noqa: E402
from lakehouse.pipeline import run_pipeline  # noqa: E402

PORT = 8765


def main() -> None:
    feed = Paths(DEFAULT.root / "api_feed")  # the "remote system" the API reads from
    srv = make_server(feed.source, PORT)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    os.environ["SOURCE_URL"] = f"http://127.0.0.1:{PORT}/orders"
    print("backend:", os.environ.get("EXTRACT_BACKEND", "requests"))

    print("\n===== DAY 1: source has data =====")
    generate_day(date(2024, 1, 1), paths=feed, with_updates=False)
    run_pipeline(DEFAULT)

    print("\n===== DAY 2: new + updated orders appear at the source (incremental) =====")
    generate_day(date(2024, 1, 2), paths=feed)
    run_pipeline(DEFAULT)

    print("\n===== RE-RUN with no new data (idempotent no-op) =====")
    run_pipeline(DEFAULT)

    print()
    from lakehouse import show  # noqa: F401  (prints the summary on import)

    srv.shutdown()


if __name__ == "__main__":
    main()
