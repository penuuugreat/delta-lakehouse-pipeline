"""Extract tests: a real local mock API, no mocking of `requests` internals."""
import sys
import threading
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from mock_api import make_server  # noqa: E402

from lakehouse import extract  # noqa: E402
from lakehouse.generate import generate_day  # noqa: E402
from lakehouse.paths import Paths  # noqa: E402
from lakehouse.pipeline import run_pipeline  # noqa: E402


@pytest.fixture
def api(tmp_path, monkeypatch):
    feed = Paths(tmp_path / "feed")
    srv = make_server(feed.source, 0)  # port 0 = any free port
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    monkeypatch.setenv("SOURCE_URL", f"http://127.0.0.1:{srv.server_address[1]}/orders")
    yield feed
    srv.shutdown()


def test_skipped_when_no_source_url(tmp_path, monkeypatch):
    monkeypatch.delenv("SOURCE_URL", raising=False)
    assert extract.pull(Paths(tmp_path))["rows"] == 0


def test_incremental_pull_and_noop_rerun(api, tmp_path):
    p = Paths(tmp_path / "lake")
    generate_day(date(2024, 1, 1), paths=api, with_updates=False)
    r1 = extract.pull(p)
    assert r1["rows"] > 0 and (p.source / r1["file"]).exists()

    # inclusive API returns the boundary row again; the extractor must drop it
    assert extract.pull(p)["rows"] == 0

    generate_day(date(2024, 1, 2), paths=api)
    r3 = extract.pull(p)
    assert r3["rows"] > 0 and r3["file"] != r1["file"]


def test_end_to_end_api_to_gold(api, tmp_path):
    p = Paths(tmp_path / "lake")
    generate_day(date(2024, 1, 1), paths=api, with_updates=False)
    r1 = run_pipeline(p)
    assert r1["bronze"]["files"] == 1 and r1["silver"]["valid"] > 0

    r2 = run_pipeline(p)  # nothing new at the source
    assert r2["bronze"]["files"] == 0 and r2["silver"]["rows_in"] == 0
