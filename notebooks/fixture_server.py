"""A bounded, read-only synthetic FHIR R4 fixture for notebook pagination checks."""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlsplit


class FixtureServer:
    """Serve explicit synthetic resources with opaque next-link cursors."""

    def __init__(self, path: Path) -> None:
        self.data = json.loads(path.read_text())
        self.requests: list[dict] = []
        data, requests = self.data, self.requests

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: object) -> None:
                pass

            def do_GET(self) -> None:
                parsed = urlsplit(self.path)
                kind = parsed.path.lstrip("/")
                query = parse_qs(parsed.query)
                if kind == "metadata":
                    payload = {
                        "resourceType": "CapabilityStatement",
                        "status": "active",
                        "date": "2024-01-01",
                        "kind": "instance",
                        "fhirVersion": "4.0.1",
                        "format": ["json"],
                        "rest": [
                            {
                                "mode": "server",
                                "resource": [
                                    {
                                        "type": x,
                                        "interaction": [{"code": "search-type"}],
                                    }
                                    for x in ["Patient", "Observation"]
                                ],
                            }
                        ],
                    }
                    requests.append({"resource": "metadata", "rows": 0, "offset": 0})
                elif kind in {"Patient", "Observation"}:
                    if set(query) - {"_count", "cursor"}:
                        self.send_error(400, "Unsupported fixture query")
                        return
                    try:
                        size = int(query.get("_count", ["2"])[0])
                        cursor = query.get("cursor", ["row-0"])[0]
                        if not cursor.startswith("row-"):
                            raise ValueError
                        offset = int(cursor[4:])
                        if size not in {2, 4} or offset < 0:
                            raise ValueError
                    except ValueError:
                        self.send_error(400, "Unsupported fixture paging")
                        return
                    rows = data[kind][offset : offset + size]
                    payload = {
                        "resourceType": "Bundle",
                        "type": "searchset",
                        "total": len(data[kind]),
                        "entry": [
                            {"resource": r, "search": {"mode": "match"}} for r in rows
                        ],
                        "link": [],
                    }
                    if offset + size < len(data[kind]):
                        params = urlencode(
                            {"cursor": f"row-{offset + size}", "_count": size}
                        )
                        payload["link"].append(
                            {
                                "relation": "next",
                                "url": f"http://127.0.0.1:{self.server.server_port}/{kind}?{params}",
                            }
                        )
                    requests.append(
                        {"resource": kind, "rows": len(rows), "offset": offset}
                    )
                else:
                    self.send_error(404, "Fixture route unavailable")
                    return
                body = json.dumps(payload).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/fhir+json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_port}"

    def close(self) -> None:
        """Stop only this fixture server."""
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
