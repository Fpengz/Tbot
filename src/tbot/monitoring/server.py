"""Small localhost-only observability server for a single bot runtime."""
from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from typing import Any

from .metrics import Metrics


@dataclass(frozen=True, slots=True)
class Health:
    live: bool
    ready: bool
    detail: dict[str, Any]


class ObservabilityServer:
    def __init__(self, *, metrics: Metrics, health: Callable[[], Health], status: Callable[[], dict[str, Any]]) -> None:
        self.metrics, self.health, self.status = metrics, health, status
        self._server: ThreadingHTTPServer | None = None
        self._thread: Thread | None = None

    def start(self, host: str = "127.0.0.1", port: int = 8080) -> int:
        parent = self
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                routes = {
                    "/healthz": lambda: (parent.health().live, parent.health().detail, "application/json"),
                    "/readyz": lambda: (parent.health().ready, parent.health().detail, "application/json"),
                    "/status": lambda: (True, parent.status(), "application/json"),
                    "/metrics": lambda: (True, parent.metrics.prometheus(), "text/plain; version=0.0.4"),
                }
                route = routes.get(self.path)
                if route is None:
                    self.send_error(HTTPStatus.NOT_FOUND)
                    return
                healthy, body, content_type = route()
                encoded = body.encode() if isinstance(body, str) else json.dumps(body, default=str, sort_keys=True).encode()
                self.send_response(HTTPStatus.OK if healthy else HTTPStatus.SERVICE_UNAVAILABLE)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers(); self.wfile.write(encoded)
            def log_message(self, *_: object) -> None: pass
        self._server = ThreadingHTTPServer((host, port), Handler)
        self._thread = Thread(target=self._server.serve_forever, name="tbot-observability", daemon=True)
        self._thread.start()
        return self._server.server_port

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown(); self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=2)
