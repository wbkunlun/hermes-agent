"""Server half of the WeCom send relay (fork 2026-10-26, wehermes).

A stdlib ``ThreadingHTTPServer`` on a daemon thread, bound to 127.0.0.1 ONLY — this
endpoint SENDS messages, so the health endpoint's configurable host is deliberately
not replicated. One endpoint, ``POST /notify``, gated by a constant-time secret
compare; the port and the secret are published to the machine lock dir so a
same-user client can find the gateway (the same dir the token-scoped locks live in).

The send runs on the GATEWAY's event loop — the adapter's per-chat queues and its
pending-response futures are bound there, so a send on any other loop would park a
future nobody wakes. The request thread schedules the coroutine with
``safe_schedule_threadsafe`` and waits for its result.
"""

from __future__ import annotations

import hmac
import json
import logging
import os
import secrets
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Callable, Optional

from plugins.platforms.wecom.relay_client import rendezvous_path

logger = logging.getLogger(__name__)

DEFAULT_PORT = 7861
MAX_BODY_BYTES = 1024 * 1024
DISPATCH_TIMEOUT_SECONDS = 30.0
# The serve_forever poll interval bounds how long shutdown() blocks the caller.
_POLL_INTERVAL_SECONDS = 0.2


def _consume_detached_result(future) -> None:
    """Observe an abandoned dispatch's outcome (no 'exception never retrieved' noise)."""
    try:
        future.result()
    except Exception:
        pass


def _make_handler(*, dispatch: Callable, loop, secret: str, log: logging.Logger) -> type:
    class RelayHandler(BaseHTTPRequestHandler):
        def log_message(self, fmt, *args):  # stdlib signature
            log.debug("wecom-relay: " + fmt, *args)

        def do_POST(self):  # stdlib naming
            if self.path != "/notify":
                self._reply(404, {"error": "not found"})
                return
            provided = (self.headers.get("X-Relay-Secret") or "").encode("utf-8", "replace")
            # fork 2026-10-26 (release fix): compare BYTES — str compare_digest raises
            # TypeError on non-ASCII input (HTTP headers are latin-1 decodable), which
            # crashed the handler into a traceback instead of answering 401.
            if not hmac.compare_digest(provided, secret.encode("utf-8")):
                self._reply(401, {"error": "invalid secret"})
                return
            try:
                length = int(self.headers.get("Content-Length") or 0)
            except ValueError:
                length = -1
            if length <= 0 or length > MAX_BODY_BYTES:
                self._reply(413, {"error": "bad body size"})
                return
            try:
                payload = json.loads(self.rfile.read(length))
                chat_id = str(payload["chat_id"])
                message = str(payload["message"])
            except Exception:
                self._reply(400, {"error": "expected JSON {chat_id, message}"})
                return
            result = self._deliver(chat_id, message)
            self._reply(200 if result.get("success") else 502, result)

        def do_GET(self):  # stdlib naming
            self._reply(405, {"error": "POST /notify only"})

        def _deliver(self, chat_id: str, message: str) -> dict:
            from agent.async_utils import safe_schedule_threadsafe

            if loop is None or not loop.is_running():
                return {"error": "gateway event loop is not running"}
            coro = dispatch(chat_id, message)
            future = safe_schedule_threadsafe(
                coro, loop, logger=log, log_message="wecom-relay: dispatch schedule failed")
            if future is None:
                return {"error": "could not schedule the send on the gateway loop"}
            try:
                result = future.result(timeout=DISPATCH_TIMEOUT_SECONDS)
            except TimeoutError:
                future.add_done_callback(_consume_detached_result)
                return {"error": "WeCom relay dispatch timed out"}
            except Exception as exc:
                return {"error": f"WeCom relay dispatch failed: {exc}"}
            return result if isinstance(result, dict) else {"error": "relay dispatch returned no result"}

        def _reply(self, code: int, payload: dict) -> None:
            body = json.dumps(payload).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    return RelayHandler


def publish_rendezvous(port: int, secret: str, log: logging.Logger) -> Optional[Path]:
    """Publish ``{port, secret, pid}`` so same-user clients can find the relay.

    fork 2026-10-26 (release fix): the file holds the live bearer secret, so it is
    written 0600 (the surrounding lock dir stays the shared per-user one)."""
    path = rendezvous_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(json.dumps({"port": port, "secret": secret, "pid": os.getpid()}))
        os.chmod(path, 0o600)  # a pre-existing file keeps whatever mode it had
        return path
    except OSError as exc:
        log.warning("wecom-relay: could not publish the rendezvous file %s (%s) — "
                    "out-of-process senders cannot find the relay", path, exc)
        return None


def start_server(*, dispatch: Callable, loop, port: Optional[int] = None,
                 logger_: Optional[logging.Logger] = None,
                 secret: Optional[str] = None,
                 publish: bool = True) -> Optional[ThreadingHTTPServer]:
    """Serve ``POST /notify`` on 127.0.0.1; None when the bind fails.

    A failed bind is logged at ERROR: the bot-identity lock refuses the competing
    connection, so without the relay an out-of-process sender can only fail — a
    silent bind failure would hide that.
    """
    log = logger_ or logger
    secret = secret or secrets.token_urlsafe(32)
    try:
        port = int(port if port is not None else (os.getenv("WECOM_RELAY_PORT") or DEFAULT_PORT))
    except (TypeError, ValueError):
        # fork 2026-10-26 (release fix): a typo'd port is an optional-relay problem,
        # not a WeCom startup failure — it used to raise out of connect() and take the
        # whole platform down.
        log.warning("wecom-relay: WECOM_RELAY_PORT=%r is not a port number — using %s",
                    os.getenv("WECOM_RELAY_PORT"), DEFAULT_PORT)
        port = DEFAULT_PORT
    try:
        server = ThreadingHTTPServer(
            ("127.0.0.1", port),
            _make_handler(dispatch=dispatch, loop=loop, secret=secret, log=log),
        )
    except OSError as exc:
        log.error("wecom-relay: NOT serving on 127.0.0.1:%s (%s) — out-of-process senders "
                  "will be refused by the bot-identity lock; free the port or set WECOM_RELAY_PORT",
                  port, exc)
        return None
    server.daemon_threads = True
    server.relay_secret = secret  # type: ignore[attr-defined]
    if publish:
        published = publish_rendezvous(server.server_address[1], secret, log)
        # fork 2026-10-26 (release fix): remember the file so _stop_relay_server can
        # unpublish it — a stale rendezvous routes later senders to a dead endpoint.
        server.relay_rendezvous_path = published  # type: ignore[attr-defined]
    threading.Thread(
        target=server.serve_forever, kwargs={"poll_interval": _POLL_INTERVAL_SECONDS},
        name="wecom-relay", daemon=True,
    ).start()
    log.info("wecom-relay: serving POST /notify on 127.0.0.1:%s", server.server_address[1])
    return server
