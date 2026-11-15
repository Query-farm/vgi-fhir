"""Strict, bounded loopback FHIR subset over authored synthetic resources only."""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlsplit


class FixtureServer:
    """Serve read/search/metadata without proxying, credentials or mutating routes."""

    def __init__(self, path: Path) -> None:
        self.data = json.loads(path.read_text())
        self.requests: list[dict] = []
        data, requests = self.data, self.requests

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: object) -> None:
                pass

            def reply(
                self,
                status,
                payload,
                *,
                kind="unknown",
                operation="rejected",
                rows=0,
                offset=0,
            ):
                requests.append(
                    {
                        "method": self.command,
                        "path": urlsplit(self.path).path,
                        "query": urlsplit(self.path).query,
                        "resource": kind,
                        "operation": operation,
                        "status": status,
                        "rows": rows,
                        "offset": offset,
                    }
                )
                body = json.dumps(payload).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/fhir+json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def reject(self, status, message):
                self.reply(
                    status,
                    {
                        "resourceType": "OperationOutcome",
                        "issue": [
                            {
                                "severity": "error",
                                "code": "not-supported",
                                "diagnostics": message,
                            }
                        ],
                    },
                )

            def do_GET(self):
                parsed = urlsplit(self.path)
                if parsed.scheme or parsed.netloc or self.headers.get("Authorization"):
                    self.reject(
                        400, "Fixture accepts relative paths without credentials"
                    )
                    return
                kind = parsed.path.lstrip("/")
                query = parse_qs(parsed.query, keep_blank_values=True)
                if kind == "metadata" and not query:
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
                                        "interaction": [
                                            {"code": "search-type"},
                                            {"code": "read"},
                                        ],
                                    }
                                    for x in ["Patient", "Observation"]
                                ],
                            }
                        ],
                    }
                    self.reply(200, payload, kind="metadata", operation="metadata")
                    return
                parts = kind.split("/")
                if (
                    len(parts) == 2
                    and parts[0] in {"Patient", "Observation"}
                    and not query
                ):
                    resource = next(
                        (r for r in data[parts[0]] if r["id"] == parts[1]), None
                    )
                    if resource is None:
                        self.reject(404, "Synthetic resource not found")
                    else:
                        self.reply(
                            200, resource, kind=parts[0], operation="read", rows=1
                        )
                    return
                if kind not in {"Patient", "Observation"}:
                    self.reject(404, "Fixture route unavailable")
                    return
                if set(query) - {"_count", "cursor"} or any(
                    len(v) != 1 for v in query.values()
                ):
                    self.reject(400, "Unsupported fixture query")
                    return
                try:
                    size = int(query.get("_count", ["2"])[0])
                    cursor = query.get("cursor", ["row-0"])[0]
                    if not cursor.startswith("row-"):
                        raise ValueError
                    offset = int(cursor[4:])
                    if size not in {2, 4} or offset < 0 or offset > len(data[kind]):
                        raise ValueError
                except ValueError:
                    self.reject(400, "Unsupported fixture paging")
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
                self.reply(
                    200,
                    payload,
                    kind=kind,
                    operation="search",
                    rows=len(rows),
                    offset=offset,
                )

            def do_POST(self):
                self.reject(405, "Synthetic fixture is read-only")

            do_PUT = do_POST
            do_DELETE = do_POST
            do_PATCH = do_POST

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_port}"

    def close(self) -> None:
        """Stop only this fixture server."""
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
