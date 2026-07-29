"""audit-callback plugin — report command/skill execution to the Agent
Execution Audit API.

Conforms to the platform intake contract ``POST /api/v1/agent/audit`` with an
``IngestSingleRequest`` body (see the platform OpenAPI "Agent Execution Audit
API"). The platform is an append-only ledger: it receives, persists
(``t_audit_log``), fans out to structured logs + SIEM, and dedups by
``event_id``. Authorization / allow decisions are NOT made here — this plugin
only *reports* what the agent did.

Design choices
--------------

* **Reports at ``post_tool_call``, not ``pre_tool_call``.** The schema carries
  post-execution fields (``exit_code``, ``result``, ``duration_ms``,
  ``stdout_sha256`` …), so the record is most useful once the tool has run (or
  been blocked). Risk classification (hardline / dangerous) is recomputed from
  the command at this point — ``args`` is still available on the post hook.
* **Async fire-and-forget.** A single daemon worker drains a bounded queue;
  the hook only does a non-blocking ``Queue.put``. HTTP I/O, retries and
  response handling happen off the agent thread, wrapped so any failure is
  logged at debug and discarded — audit reporting can never block or break the
  agent loop.
* **Idempotent.** Each event gets a client-generated ``event_id`` (UUID). The
  worker retries transient failures (connect / timeout / 5xx) a couple of times
  with short backoff; the platform dedups by ``event_id``, so retries are safe.
* **Auth.``Authorization: <CONTROL_PLANE_AUTH>`` — the per-sandbox JWT injected
  at deploy time (value already includes the ``Bearer `` prefix, per the
  platform's sandbox-injection flow). Falls back to ``HERMES_AUDIT_TOKEN``
  (used as ``Bearer <token>``) for non-sandbox setups. When neither is set the
  plugin still POSTs (the platform will 401); set the URL empty to disable.

Reportable events
-----------------

* ``terminal`` commands classified as ``hardline`` (→ ``risk_level=critical``)
  or ``dangerous`` (→ ``high``) by the SAME detectors the command guard uses.
* ``skill_manage`` calls (install / update / delete) → ``risk_level=medium``.
* Optionally EVERY ``terminal`` command at ``info`` when
  ``HERMES_AUDIT_REPORT_ALL_COMMANDS=1``.

Env knobs: ``HERMES_AUDIT_CALLBACK_URL`` (intake URL; empty = off),
``CONTROL_PLANE_AUTH`` / ``HERMES_AUDIT_TOKEN``, ``HERMES_AUDIT_TIMEOUT``
(default 3s), ``HERMES_AUDIT_REPORT_ALL_COMMANDS``, optional ``SANDBOX_ID``.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import queue
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration (read from env each call so changes apply live)
# ---------------------------------------------------------------------------

_TRUTHY = {"1", "true", "yes", "on"}

# severity (internal) -> risk_level (API enum)
_RISK_LEVEL = {
    "hardline": "critical",
    "dangerous": "high",
    "skill": "medium",
    "info": "info",
}


def _intake_url() -> str:
    return os.environ.get("HERMES_AUDIT_CALLBACK_URL", "").strip()


def _auth_header() -> str:
    """Authorization header value, or '' if no credential is configured.

    CONTROL_PLANE_AUTH is the sandbox-injected JWT and already includes the
    ``Bearer `` prefix (per the platform spec); HERMES_AUDIT_TOKEN is a bare
    token that we wrap ourselves.
    """
    cp = os.environ.get("CONTROL_PLANE_AUTH", "").strip()
    if cp:
        return cp
    tok = os.environ.get("HERMES_AUDIT_TOKEN", "").strip()
    if tok:
        return f"Bearer {tok}"
    return ""


def _timeout() -> float:
    try:
        return float(os.environ.get("HERMES_AUDIT_TIMEOUT", "3"))
    except (TypeError, ValueError):
        return 3.0


def _report_all_commands() -> bool:
    return os.environ.get("HERMES_AUDIT_REPORT_ALL_COMMANDS", "").strip().lower() in _TRUTHY


def _sandbox_id() -> str:
    return os.environ.get("SANDBOX_ID", "").strip()


# ---------------------------------------------------------------------------
# Bounded queue + single daemon worker
# ---------------------------------------------------------------------------

_QUEUE: "queue.Queue[Any]" = queue.Queue(maxsize=1000)
_WORKER_LOCK = threading.Lock()
_WORKER_STARTED = False
_DROPPED = 0  # best-effort overflow counter


def _dropped_count() -> int:
    return _DROPPED


def _ensure_worker() -> None:
    global _WORKER_STARTED
    if _WORKER_STARTED:
        return
    with _WORKER_LOCK:
        if _WORKER_STARTED:
            return
        _WORKER_STARTED = True
        t = threading.Thread(target=_drain, name="audit-callback-worker", daemon=True)
        t.start()
        logger.debug("audit-callback: worker thread started")


def _enqueue(body: Dict[str, Any]) -> None:
    global _DROPPED
    try:
        _QUEUE.put(body, block=False)
    except queue.Full:
        _DROPPED += 1
        logger.warning("audit-callback: queue full (%d dropped), discarding event", _DROPPED)


def _post_one(client, url: str, headers: Dict[str, str], body: Dict[str, Any]):
    """Single POST. Returns the httpx Response; raises on network error."""
    return client.post(url, content=json.dumps(body, default=str), headers=headers)


def _post_with_retry(client, url: str, headers: Dict[str, str], body: Dict[str, Any]) -> None:
    """POST with a couple of transient-error retries. Never raises.

    Retries only on connect/timeout/5xx. 2xx and 4xx (incl. 409 duplicate) are
    terminal — the platform dedups by ``event_id``, so a duplicate is success.
    """
    import httpx

    transient = (httpx.ConnectError, httpx.TimeoutException, httpx.RemoteProtocolError)
    backoff = (0.5, 1.0)
    event_id = body.get("event_id", "?")
    for attempt in range(3):
        try:
            resp = _post_one(client, url, headers, body)
        except transient as exc:
            if attempt < 2:
                time.sleep(backoff[attempt])
                continue
            logger.debug("audit-callback: giving up on %s after transient errors: %s", event_id, exc)
            return
        except Exception as exc:  # noqa: BLE001 — non-transient; don't retry
            logger.debug("audit-callback: POST error for %s (no retry): %s", event_id, exc)
            return
        code = resp.status_code
        if code < 500:
            if code >= 400:
                logger.debug("audit-callback: server returned %d for %s", code, event_id)
            return
        if attempt < 2:  # 5xx → retry
            time.sleep(backoff[attempt])
            continue
        logger.debug("audit-callback: server returned %d for %s after retries", code, event_id)
        return


def _drain() -> None:
    """Pop events off the queue and POST them. Never raises."""
    import httpx

    client = httpx.Client(timeout=_timeout())
    while True:
        body = _QUEUE.get()  # blocks until an event arrives
        url = _intake_url()
        if not url:
            # URL was unset between enqueue and drain — nothing to do.
            continue
        headers = {"Content-Type": "application/json"}
        auth = _auth_header()
        if auth:
            headers["Authorization"] = auth
        trace_id = body.get("trace_id") or ""
        if trace_id:
            headers["X-Trace-ID"] = trace_id
        try:
            _post_with_retry(client, url, headers, body)
        except Exception as exc:  # noqa: BLE001 — never break the loop
            logger.debug("audit-callback: drain error: %s", exc)


# ---------------------------------------------------------------------------
# Classification + payload building
# ---------------------------------------------------------------------------

def _classify_terminal(command: str) -> Tuple[str, str, list]:
    """Return (severity, reason, matched_rules) using the guard's own detectors.

    severity ∈ {"hardline", "dangerous", "info"}; reason is a human description;
    matched_rules is a list of rule keys for the CommandPayload.
    """
    try:
        from tools.approval import detect_dangerous_command, detect_hardline_command
    except Exception:
        return ("info", "", [])
    try:
        is_hardline, hardline_desc = detect_hardline_command(command)
        if is_hardline:
            desc = hardline_desc or "hardline block"
            return ("hardline", desc, [f"hardline:{desc}"])
        is_dangerous, pattern_key, danger_desc = detect_dangerous_command(command)
        if is_dangerous:
            return ("dangerous", danger_desc or "dangerous", [pattern_key] if pattern_key else [])
    except Exception:
        pass
    return ("info", "", [])


def _parse_terminal_result(result: Any) -> Tuple[Optional[int], str, str, str]:
    """Best-effort (exit_code, stdout, stderr, cwd) from a terminal tool result."""
    if not isinstance(result, str):
        return (None, "", "", "")
    try:
        data = json.loads(result)
    except Exception:
        return (None, "", "", "")
    if not isinstance(data, dict):
        return (None, "", "", "")
    exit_code = data.get("exit_code", data.get("returncode"))
    if exit_code is not None:
        try:
            exit_code = int(exit_code)
        except (TypeError, ValueError):
            exit_code = None
    stdout = str(data.get("output", data.get("stdout", "")) or "")
    stderr = str(data.get("error", data.get("stderr", "")) or "")
    cwd = str(data.get("cwd", "") or "")
    return (exit_code, stdout, stderr, cwd)


def _sha_and_preview(text: str, limit: int = 2048) -> Tuple[str, str]:
    if not text:
        return ("", "")
    data = text.encode("utf-8", errors="ignore")
    return (hashlib.sha256(data).hexdigest(), text[:limit])


def _decide(status: str, result_text: Any, exit_code: Optional[int]) -> Tuple[str, str]:
    """Infer (decision, result_enum) from post-hook signals.

    decision ∈ {"allowed", "blocked"}; result ∈ {"success", "error", "timeout"}.
    Blocked = a guard/allowlist/plugin refused the call (status or result text).
    """
    rt = result_text if isinstance(result_text, str) else ""
    rt_lower = rt.lower()
    blocked = (status == "blocked") or ("blocked" in rt_lower)
    if "timeout" in rt_lower or status == "timeout":
        return ("blocked" if blocked else "allowed", "timeout")
    if blocked:
        return ("blocked", "error")
    if exit_code is not None and exit_code != 0:
        return ("allowed", "error")
    if status in {"error", "failed"}:
        return ("allowed", "error")
    return ("allowed", "success")


def _summarize_skill_args(args: Any) -> Dict[str, Any]:
    if not isinstance(args, dict):
        return {}
    out: Dict[str, Any] = {}
    for key in ("action", "name", "skill_name", "skill", "source", "version", "force"):
        val = args.get(key)
        if val is None:
            continue
        if isinstance(val, str) and len(val) > 200:
            val = val[:200] + "…"
        out[key] = val
    return out


def _common_fields(tool_call_id: str, trace_id: str) -> Dict[str, Any]:
    body: Dict[str, Any] = {
        "event_id": uuid.uuid4().hex,
        "event_time": datetime.now(timezone.utc).isoformat(),
        "execution_id": tool_call_id or "",
        "trace_id": trace_id,
    }
    sb = _sandbox_id()
    if sb:
        body["resource_type"] = "sandbox"
        body["resource_id"] = sb
    return body


def _build_command_body(
    command: str,
    args: Any,
    result: Any,
    status: str,
    duration_ms: int,
    severity: str,
    reason: str,
    matched_rules: list,
    tool_call_id: str,
    trace_id: str,
) -> Dict[str, Any]:
    exit_code, stdout, stderr, result_cwd = _parse_terminal_result(result)
    decision, result_enum = _decide(status, result, exit_code)
    stdout_sha, stdout_preview = _sha_and_preview(stdout)
    stderr_sha, stderr_preview = _sha_and_preview(stderr)
    cwd = result_cwd or (str(args.get("workdir") or "") if isinstance(args, dict) else "")

    payload: Dict[str, Any] = {
        "command": command,
        "cwd": cwd,
        "matched_rules": matched_rules,
        "exit_code": exit_code,
        "stdout_sha256": stdout_sha,
        "stdout_preview": stdout_preview,
        "stderr_sha256": stderr_sha,
        "stderr_preview": stderr_preview,
    }
    if isinstance(args, dict):
        if args.get("timeout") is not None:
            payload["timeout_sec"] = args.get("timeout")
        if args.get("interactive") is not None:
            payload["interactive"] = bool(args.get("interactive"))

    body = _common_fields(tool_call_id, trace_id)
    body.update({
        "event_type": "command",
        "action": "command.exec",
        "risk_level": _RISK_LEVEL.get(severity, "info"),
        "risk_reason": reason,
        "decision": decision,
        "result": result_enum,
        "exit_code": exit_code,
        "duration_ms": int(duration_ms or 0),
        "payload": payload,
    })
    return body


def _build_skill_body(
    args: Any,
    result: Any,
    status: str,
    duration_ms: int,
    tool_call_id: str,
    trace_id: str,
) -> Dict[str, Any]:
    summary = _summarize_skill_args(args)
    exit_code, _stdout, _stderr, _cwd = _parse_terminal_result(result)
    decision, result_enum = _decide(status, result, exit_code)
    action = str(summary.get("action") or "")
    name = str(summary.get("name") or summary.get("skill_name") or summary.get("skill") or "")
    version = str(summary.get("version") or "")
    destructive = bool(summary.get("force")) or action.lower() in {"delete", "uninstall", "remove", "update"}

    payload: Dict[str, Any] = {
        "summary": f"{action} {name}".strip(),
        "destructive": destructive,
        "caller_type": "agent",
    }

    body = _common_fields(tool_call_id, trace_id)
    body.update({
        "event_type": "skill",
        "action": "skill.invoke",
        "risk_level": "medium",
        "risk_reason": "skill lifecycle mutation (install/update/delete)",
        "decision": decision,
        "result": result_enum,
        "exit_code": exit_code,
        "duration_ms": int(duration_ms or 0),
        "payload": payload,
    })
    if name:
        body["skill_name"] = name
    if version:
        body["skill_version"] = version
    return body


# ---------------------------------------------------------------------------
# Hook
# ---------------------------------------------------------------------------

def _on_post_tool_call(
    tool_name: str = "",
    args: Any = None,
    result: Any = None,
    task_id: str = "",
    session_id: str = "",
    tool_call_id: str = "",
    turn_id: str = "",
    api_request_id: str = "",
    duration_ms: int = 0,
    status: str = "",
    error_type: Any = None,
    error_message: Any = None,
    middleware_trace: Any = None,
    **_: Any,
) -> None:
    """Observe a completed tool call and enqueue an audit event if reportable.

    Always returns None — observer only. Wrapped so a failure here can never
    propagate into the dispatch path.
    """
    try:
        if not _intake_url():
            return None  # feature off — no-op, no worker

        trace_id = api_request_id or turn_id or ""
        body: Optional[Dict[str, Any]] = None

        if tool_name == "terminal" and isinstance(args, dict):
            command = str(args.get("command") or "")
            severity, reason, matched_rules = _classify_terminal(command)
            if severity in {"hardline", "dangerous"} or _report_all_commands():
                body = _build_command_body(
                    command, args, result, status, duration_ms,
                    severity, reason, matched_rules, tool_call_id, trace_id,
                )
        elif tool_name == "skill_manage":
            body = _build_skill_body(
                args, result, status, duration_ms, tool_call_id, trace_id,
            )

        if body is None:
            return None

        _ensure_worker()
        _enqueue(body)
    except Exception as exc:  # noqa: BLE001 — never break the agent loop
        logger.debug("audit-callback: post_tool_call handler error (%s)", exc)
    return None


def register(ctx) -> None:
    ctx.register_hook("post_tool_call", _on_post_tool_call)
