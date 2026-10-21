"""Tiny mock of the source REST API (stdlib only, no extra dependencies).

Serves every orders_*.csv in a "feed" folder as JSON:
  GET /orders?updated_since=<ts>&page=<n>&page_size=<n>
  -> {"results": [...], "next": true|false}

Like many real APIs, `updated_since` is INCLUSIVE (>=), which is exactly the case the
extractor has to handle. Run standalone:  python scripts/mock_api.py --feed data/api_feed/source
"""
import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pandas as pd


def _load(feed: Path) -> pd.DataFrame:
    files = sorted(feed.glob("orders_*.csv"))
    if not files:
        return pd.DataFrame()
    df = pd.concat([pd.read_csv(f, dtype=str) for f in files], ignore_index=True)
    df["_ts"] = pd.to_datetime(df["updated_at"], errors="coerce", utc=True)
    return df.sort_values("_ts", kind="stable")


def make_server(feed: Path, port: int = 8765) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            u = urlparse(self.path)
            if u.path != "/orders":
                self.send_error(404)
                return
            q = {k: v[0] for k, v in parse_qs(u.query).items()}
            page, size = int(q.get("page", 1)), int(q.get("page_size", 500))
            df = _load(feed)  # re-read each call: new files appear like new source data
            if not df.empty and "updated_since" in q:
                df = df[df["_ts"] >= pd.to_datetime(q["updated_since"], utc=True)]
            chunk = df.iloc[(page - 1) * size : page * size]
            body = {
                "results": json.loads(chunk.drop(columns="_ts", errors="ignore").to_json(orient="records")),
                "next": bool(len(df) > page * size),
            }
            payload = json.dumps(body).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *args):  # keep demo output clean
            pass

    return ThreadingHTTPServer(("127.0.0.1", port), Handler)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--feed", required=True)
    ap.add_argument("--port", type=int, default=8765)
    a = ap.parse_args()
    print(f"mock API on http://127.0.0.1:{a.port}/orders  (feed: {a.feed})")
    make_server(Path(a.feed), a.port).serve_forever()
