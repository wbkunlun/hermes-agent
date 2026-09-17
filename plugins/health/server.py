"""Thread-side HTTP surface for the (fork, wehermes) health plugin.

stdlib-only ``ThreadingHTTPServer`` on a daemon thread: no aiohttp, no shared
event loop — the endpoint keeps answering when the gateway's asyncio loop is
wedged, which is precisely the failure it exists to report. The payload is
built per request by an injected ``build_detail`` callable (wired in
``__init__.register``) so this module stays import-standalone. Bind failure is
non-fatal (the control-socket philosophy): another process may already serve.
"""

from __future__ import annotations

import json
import logging
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Dict, Optional

Builder = Callable[[], Dict[str, Any]]
_KNOWN_PATHS = ("/health", "/health/detail")


def make_handler(*, build_detail: Builder, logger: logging.Logger) -> type:
    class HealthHandler(BaseHTTPRequestHandler):
        server_version = "hermes-health/0.1"

        def _respond(self, code: int, body: Dict[str, Any], head_only: bool = False) -> None:
            payload = json.dumps(body, ensure_ascii=False).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            if not head_only:
                self.wfile.write(payload)

        def _dispatch(self, head_only: bool = False) -> None:
            try:
                detail = build_detail()
                if self.path.split("?", 1)[0] == "/health":
                    body = {"status": detail.get("status"), "checked_at": detail.get("checked_at")}
                else:
                    body = detail
                self._respond(503 if detail.get("status") == "down" else 200, body, head_only)
            except Exception:
                logger.exception("health: payload build failed")
                self._respond(500, {"status": "unknown", "error": "health payload build failed"},
                              head_only)

        def do_GET(self) -> None:
            if self.path.split("?", 1)[0] in _KNOWN_PATHS:
                self._dispatch()
            else:
                self._respond(404, {"error": "not found"})

        def do_HEAD(self) -> None:
            if self.path.split("?", 1)[0] in _KNOWN_PATHS:
                self._dispatch(head_only=True)
            else:
                self._respond(404, {"error": "not found"}, head_only=True)

        def log_message(self, fmt: str, *args: Any) -> None:  # no per-request stderr noise
            logger.debug("health: " + fmt, *args)

    return HealthHandler


def start_server(*, build_detail: Builder, host: str = "127.0.0.1", port: int = 7860,
                 logger: Optional[logging.Logger] = None) -> Optional[ThreadingHTTPServer]:
    """Serve /health + /health/detail on a daemon thread; None when the bind
    fails (caller logs and moves on — the gateway must never die for this)."""
    log = logger or logging.getLogger(__name__)
    handler = make_handler(build_detail=build_detail, logger=log)
    try:
        server = ThreadingHTTPServer((host, port), handler)
    except OSError as exc:
        log.warning("health: not serving on %s:%s (%s); continuing without the endpoint",
                    host, port, exc)
        return None
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, name="health-server", daemon=True).start()
    log.info("health: serving GET /health and /health/detail on %s:%s",
             host, server.server_address[1])
    return server
