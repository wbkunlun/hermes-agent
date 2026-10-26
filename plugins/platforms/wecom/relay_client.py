"""Client half of the WeCom send relay (fork 2026-10-26, wehermes).

The WeCom server allows ONE live WS subscription per bot. A sender in another
process (an external script, the cron worker, ``hermes send``) must therefore not
open its own connection: it displaces the gateway's subscription and every send in
the gap hits the dead window (the 2026-10 cron-report incident — the script's
standalone adapter displaced the gateway, and the report that landed in the gap was
booked ``delivery_failed``). Instead the sender hands the message to the gateway,
which delivers through the single live connection and owns the dead-window recovery
(the parked-group redelivery).

The gateway publishes ``<machine lock dir>/wecom-relay.json`` (port + secret) and
this client posts to it. ``try_relay_send`` returns the relay's ANSWER — success or
error; the gateway owned the attempt either way — and None ONLY when the relay is
unreachable or unconfigured, so the caller can fall through to its own path (which
the bot-identity lock then gates).

fork 2026-10-26: the lock-dir derivation mirrors ``gateway/status.py::_get_lock_dir``
(machine-scoped, per-OS-user; a relative ``$XDG_STATE_HOME`` is invalid and ignored,
same rule as upstream). It is replicated here so a platform plugin never imports a
private helper — marker grep on resync (upstream may move it).
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# fork 2026-10-26 (release fix): strictly greater than the server's dispatch timeout,
# so a slow-but-real server answer always beats the client's read timeout (equal
# timeouts raced: the client gave up exactly when the server answered, took it as
# "no relay", and the caller fell through to the connection the lock then refused).
CLIENT_TIMEOUT_SECONDS = 35.0
RENDEZVOUS_FILENAME = "wecom-relay.json"


def rendezvous_path() -> Path:
    """Where the gateway publishes the relay's port + secret (fork 2026-10-26)."""
    override = os.getenv("HERMES_GATEWAY_LOCK_DIR")
    if override:
        return Path(override) / RENDEZVOUS_FILENAME
    state_home_env = os.getenv("XDG_STATE_HOME") or ""
    state_home = Path(state_home_env) if os.path.isabs(state_home_env) else Path.home() / ".local" / "state"
    return state_home / "hermes" / "gateway-locks" / RENDEZVOUS_FILENAME


def read_rendezvous(bot_id: Optional[str] = None) -> Optional[dict]:
    """The gateway's published ``{port, secret, pid, bot_id}`` — None when
    absent/unreadable/stale. fork 2026-10-26 (release fix): a dead publisher's file is
    ignored (the adapter unlinks it on clean shutdown, but a crashed gateway leaves it
    behind, and posting to the dead port would read as "no relay" anyway). The
    rendezvous file is MACHINE-scoped, so a second gateway (another bot) can own it —
    a file published for another bot is not ours to use."""
    try:
        doc = json.loads(rendezvous_path().read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not (isinstance(doc, dict) and doc.get("port") and doc.get("secret")):
        return None
    pid = doc.get("pid")
    if isinstance(pid, int) and pid > 0 and not _pid_alive(pid):
        return None
    published_bot = doc.get("bot_id")
    if bot_id and published_bot and str(published_bot) != str(bot_id):
        return None  # another bot's relay — using it would send through the wrong bot
    return doc


def _pid_alive(pid: int) -> bool:
    """Is the publisher still running? fork 2026-10-26 (release fix): os.kill(pid, 0)
    TERMINATES the target on Windows (no sig==0 special case) — a probe that killed the
    gateway on every send. The repo's own stdlib-only helper is the safe answer."""
    try:
        from hermes_cli._subprocess_compat import pid_exists_stdlib
    except Exception:
        return True  # no probe available — assume alive, let the POST decide
    try:
        return bool(pid_exists_stdlib(int(pid)))
    except Exception:
        return True


def try_relay_send(chat_id: str, message: str, *, port: Optional[int] = None,
                   secret: Optional[str] = None,
                   bot_id: Optional[str] = None,
                   timeout: float = CLIENT_TIMEOUT_SECONDS) -> Optional[dict]:
    """Hand *message* to the gateway's relay; None when the relay is unreachable.

    A relay ANSWER (success or error) is final — the gateway owned the attempt, so
    the caller must NOT retry through its own connection (that is what displaces the
    gateway). Only "no relay" returns None. ``bot_id`` scopes the lookup: the
    rendezvous file is machine-scoped, so a relay published for ANOTHER bot must not
    be used (it would deliver through the wrong bot's subscription).
    """
    from urllib.error import HTTPError, URLError
    from urllib.request import ProxyHandler, Request, build_opener

    if port is None or secret is None:
        doc = read_rendezvous(bot_id=bot_id)
        if not doc:
            return None
        port, secret = int(doc["port"]), str(doc["secret"])
    body = json.dumps({"chat_id": str(chat_id), "message": str(message)}).encode("utf-8")
    request = Request(
        f"http://127.0.0.1:{int(port)}/notify",
        data=body,
        method="POST",
        headers={"Content-Type": "application/json", "X-Relay-Secret": str(secret)},
    )
    # fork 2026-10-26 (release fix): bypass the system proxy — a corporate/macOS proxy
    # that does not exempt 127.0.0.1 dialed the loopback POST through it and the relay
    # was silently unreachable (the caller then opened the connection the lock refuses).
    opener = build_opener(ProxyHandler({}))
    try:
        with opener.open(request, timeout=timeout) as response:
            payload = json.loads(response.read())
    except HTTPError as exc:
        # The gateway answered with an error status — its body is the answer.
        try:
            return json.loads(exc.read())
        except Exception:
            return {"error": f"wecom relay HTTP {exc.code}"}
    except (URLError, TimeoutError, OSError, ValueError):
        return None
    return payload if isinstance(payload, dict) else None
