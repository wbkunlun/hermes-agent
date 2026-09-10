"""audit-callback plugin — report command/skill execution to the Agent
Execution Audit API.

Conforms to the platform intake contract ``POST /api/v1/agent/audit`` with an
``IngestBatchRequest`` body (top-level ``items``, ≤100 events; the endpoint
also accepts a bare ``IngestSingleRequest``). See the platform OpenAPI
"Agent Execution Audit API" for the full schema. The platform is an append-
only ledger: it receives, persists (``t_audit_log``), fans out to structured
logs + SIEM, and dedups by ``event_id``. Authorization / allow decisions are
NOT made here — this plugin only *reports* what the agent did.

Schema notes (v0.5)
-------------------

* ``actor`` is **server-overridden** from the auth principal (sandbox id for
  machine reports). The client may send a value but the platform ignores it;
  we therefore do not send it and let the server fill it from the JWT.
* ``username`` carries the **real triggering user** — the human who initiated
  the turn (WeWork sender_user / WeCom userid). For machine reports the
  server keeps the agent's claimed value. Empty for CLI / cron / autonomous
  flows.
* ``actor_type`` and ``channel`` are sent as ``"agent"`` (this plugin runs in
  the agent runtime).
* ``env`` is optional — populated from ``HERMES_AUDIT_ENV`` when set.

Design choices
--------------

* **Reports at ``post_tool_call``, not ``pre_tool_call``.** The schema carries
  post-execution fields (``exit_code``, ``result``, ``duration_ms``,
  ``stdout_sha256`` …), so the record is most useful once the tool has run (or
  been blocked). Risk classification (hardline / dangerous) is recomputed from
  the command at this point — ``args`` is still available on the post hook.
* **Batched async fire-and-forget.** Events are queued; the daemon worker
  collects up to 100 per POST and ships as ``{"items": [...]}`` so the wire
  stays cheap. The hook only does a non-blocking ``Queue.put``. HTTP I/O,
  retries and per-item response handling happen off the agent thread, wrapped
  so any failure is logged at debug and discarded — audit reporting can never
  block or break the agent loop.
* **Idempotent.** Each event gets a client-generated ``event_id`` (UUID). The
  worker retries transient failures (connect / timeout / 5xx) a couple of times
  with short backoff; the platform dedups by ``event_id``, so retries are safe.
  Per-item ``status:"duplicate"`` in the BatchResult is not an error — it means
  the platform already had the event.
* **Auth.** ``Authorization: <CONTROL_PLANE_AUTH>`` — the per-sandbox JWT
  injected at deploy time (value already includes the ``Bearer `` prefix, per
  the platform's sandbox-injection flow). Falls back to ``HERMES_AUDIT_TOKEN``
  (used as ``Bearer <token>``) for non-sandbox setups. When neither is set the
  plugin still POSTs (the platform will 401); set the URL empty to disable.

Reportable events
-----------------

* ``terminal`` commands classified as ``hardline`` (→ ``risk_level=critical``)
  or ``dangerous`` (→ ``high``) by the SAME detectors the command guard uses.
* ``skill_manage`` calls (install / update / delete) → ``risk_level=medium``.
* ``skill_view`` calls (skill content loaded into prompt) → ``risk_level=medium``.
* ``write_file``/``patch`` calls → ``info`` (empty writes escalate to ``medium``).
* ``execute_code`` calls (arbitrary local Python — can spawn subprocesses
  that bypass the terminal guard) → ``high`` when the code matches process-
  spawn / dynamic-exec indicators, else ``medium``.
* ``computer_use`` calls (desktop control) → ``medium``.
* Optionally EVERY ``terminal`` command at ``info`` when
  ``HERMES_AUDIT_REPORT_ALL_COMMANDS=1``.

Env knobs: ``HERMES_AUDIT_CALLBACK_URL`` (explicit intake URL; if empty,
derives ``<CONTROL_PLANE_URL>/api/v1/agent/audit`` — the same control-plane
base address the whitelist fetch uses; neither set = off),
``CONTROL_PLANE_AUTH`` / ``HERMES_AUDIT_TOKEN``, ``HERMES_AUDIT_TIMEOUT``
(default 3s), ``HERMES_AUDIT_REPORT_ALL_COMMANDS``, ``HERMES_AUDIT_ENV``
(optional ``env`` field on every event), optional ``SANDBOX_ID``.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import queue
import re
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
    # blocked low-severity command = unauthorized attempt (fork, 2026-09-03)
    "medium": "medium",
    "info": "info",
}

# Process-spawn / dynamic-exec indicators for execute_code risk classification.
_CODE_RISKY_RE = re.compile(
    r"subprocess|os\.system|os\.popen|\bpopen\s*\(|shell\s*=\s*True"
    r"|(?<!\.)\beval\s*\(|(?<!\.)\bexec\s*\(|\b__import__\s*\("
    r"|os\.exec\w*|os\.spawn\w*|posix_spawn|pty\.spawn|pty\.fork"
    r"|multiprocessing|ctypes|commands\.getoutput",
    re.IGNORECASE,
)

_HTTP_WARNED = False  # plain-http intake URL warned once per process


def _intake_url() -> str:
    """Audit intake endpoint.

    Explicit ``HERMES_AUDIT_CALLBACK_URL`` wins; otherwise derive
    ``<CONTROL_PLANE_URL>/api/v1/agent/audit`` from the control-plane base
    address (same origin as the whitelist fetch). Empty = feature off.
    """
    explicit = os.environ.get("HERMES_AUDIT_CALLBACK_URL", "").strip()
    if explicit:
        return explicit
    base = (os.environ.get("CONTROL_PLANE_URL") or "").strip().rstrip("/")
    if base:
        return f"{base}/api/v1/agent/audit"
    return ""


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


def _audit_env() -> str:
    """Optional ``env`` field on every event (e.g. ``dev`` / ``staging`` / ``prod``)."""
    return os.environ.get("HERMES_AUDIT_ENV", "").strip()


# ---------------------------------------------------------------------------
# Bounded queue + single daemon worker
# ---------------------------------------------------------------------------

_QUEUE: "queue.Queue[Any]" = queue.Queue(maxsize=1000)
_WORKER_LOCK = threading.Lock()
_WORKER_STARTED = False
_DROPPED = 0  # best-effort overflow counter

# Drain: collect up to N events per POST, then ship as ``{"items": [...]}``.
# The first event blocks for up to _BATCH_WAIT to allow other producers to
# enqueue into the same batch; subsequent drains are non-blocking. 100 is the
# spec's per-batch cap (IngestBatchRequest.maxItems).
_BATCH_MAX = 100
_BATCH_WAIT = 0.05  # seconds


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
    """POST a single event with a couple of transient-error retries. Never raises.

    Retries only on connect/timeout/5xx. 2xx and 4xx are terminal — the
    platform dedups by ``event_id``, so a 409 duplicate is success.
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


def _post_batch_with_retry(
    client, url: str, headers: Dict[str, str], events: list,
) -> None:
    """POST a batch of events (or one wrapped in items) with retries. Never raises.

    The body is always ``{"items": [...]}`` so the worker has a single code
    path; the platform accepts IngestBatchRequest on the same endpoint that
    accepts IngestSingleRequest. Retries on transient errors / 5xx; on 2xx
    per-item statuses are processed (duplicate is fine, error logged).
    """
    import httpx

    body = {"items": events}
    transient = (httpx.ConnectError, httpx.TimeoutException, httpx.RemoteProtocolError)
    backoff = (0.5, 1.0)
    for attempt in range(3):
        try:
            resp = _post_one(client, url, headers, body)
        except transient as exc:
            if attempt < 2:
                time.sleep(backoff[attempt])
                continue
            logger.debug(
                "audit-callback: giving up on batch of %d after transient errors: %s",
                len(events), exc,
            )
            return
        except Exception as exc:  # noqa: BLE001 — non-transient; don't retry
            logger.debug("audit-callback: batch POST error (no retry): %s", exc)
            return
        code = resp.status_code
        if code < 500:
            if code >= 400:
                snippet = getattr(resp, "text", "") or ""
                logger.debug(
                    "audit-callback: server returned %d for batch of %d: %s",
                    code, len(events), snippet[:200],
                )
            else:
                _log_batch_results(resp, events)
            return
        if attempt < 2:  # 5xx → retry the whole batch
            time.sleep(backoff[attempt])
            continue
        logger.debug(
            "audit-callback: server returned %d for batch of %d after retries",
            code, len(events),
        )
        return


def _log_batch_results(resp, events: list) -> None:
    """Process per-item statuses from a BatchResult (HTTP 200).

    ``status:"duplicate"`` is expected and silent (platform dedup);
    ``status:"error"`` is logged at debug so a misbehaving intake is visible
    in logs without flooding them.
    """
    try:
        data = resp.json()
    except Exception:
        return
    items = data.get("items") or []
    if not isinstance(items, list):
        return
    for item in items:
        if not isinstance(item, dict):
            continue
        status = item.get("status")
        if status == "error":
            event_id = item.get("event_id", "?")
            message = item.get("message", "")
            logger.debug("audit-callback: per-item error for %s: %s", event_id, message)
        # 'ok' and 'duplicate' are both fine — silent.


def _collect_batch() -> list:
    """Collect up to ``_BATCH_MAX`` events from the queue.

    Waits briefly for the first event (to allow batching under load); once
    the first event arrives, drains whatever else is queued without further
    waiting. Returns an empty list when no event arrives within
    ``_BATCH_WAIT``.
    """
    events: list = []
    try:
        first = _QUEUE.get(timeout=_BATCH_WAIT)
    except queue.Empty:
        return events
    events.append(first)
    while len(events) < _BATCH_MAX:
        try:
            events.append(_QUEUE.get_nowait())
        except queue.Empty:
            break
    return events


def _drain() -> None:
    """Pull batches off the queue and POST them. Never raises."""
    import httpx

    client = httpx.Client(timeout=_timeout())
    while True:
        events = _collect_batch()
        if not events:
            continue
        url = _intake_url()
        if not url:
            # URL was unset between enqueue and drain — nothing to do.
            continue
        headers = {"Content-Type": "application/json"}
        auth = _auth_header()
        if auth:
            headers["Authorization"] = auth
        # Forward the trace id of the first event in the batch as the
        # batch-level X-Trace-ID — individual events keep their own trace_id
        # in the body. Falls back to the first event's trace_id.
        trace_id = ""
        for ev in events:
            trace_id = str(ev.get("trace_id") or "")
            if trace_id:
                break
        if trace_id:
            headers["X-Trace-ID"] = trace_id
        try:
            _post_batch_with_retry(client, url, headers, events)
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


def _parse_terminal_result(result: Any) -> Tuple[Optional[int], str, str, str, bool]:
    """Best-effort (exit_code, stdout, stderr, cwd, blocked) from a tool result.

    ``blocked`` is the machine-readable refusal flag: the terminal tool
    returns ``{"status": "blocked", ...}`` when a guard refuses the call.
    """
    if not isinstance(result, str):
        return (None, "", "", "", False)
    try:
        data = json.loads(result)
    except Exception:
        return (None, "", "", "", False)
    if not isinstance(data, dict):
        return (None, "", "", "", False)
    exit_code = data.get("exit_code", data.get("returncode"))
    if exit_code is not None:
        try:
            exit_code = int(exit_code)
        except (TypeError, ValueError):
            exit_code = None
    stdout = str(data.get("output", data.get("stdout", "")) or "")
    stderr = str(data.get("error", data.get("stderr", "")) or "")
    cwd = str(data.get("cwd", "") or "")
    blocked = data.get("status") == "blocked"
    return (exit_code, stdout, stderr, cwd, blocked)


def _sha_and_preview(text: str, limit: int = 2048) -> Tuple[str, str]:
    if not text:
        return ("", "")
    data = text.encode("utf-8", errors="ignore")
    return (hashlib.sha256(data).hexdigest(), text[:limit])


def _decide(status: str, result_text: Any, exit_code: Optional[int],
            blocked_flag: bool = False) -> Tuple[str, str]:
    """Infer (decision, result_enum) from post-hook signals.

    decision ∈ {"allowed", "blocked"}; result ∈ {"success", "error", "timeout"}.
    Blocked = a guard/allowlist/plugin refused the call: prefer the machine-
    readable ``status:"blocked"`` from the tool result, fall back to the
    historical substring heuristic.
    """
    rt = result_text if isinstance(result_text, str) else ""
    rt_lower = rt.lower()
    blocked = blocked_flag or (status == "blocked") or ("blocked" in rt_lower)
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


def _current_username() -> str:
    """The real terminal user who triggered this turn (spec ``username``).

    Read from the gateway's task-local session context — the same
    ``SessionSource`` the dispatch bound from the platform adapter (e.g.
    WeWork ``sender_user`` at plugins/platforms/wework/adapter.py, WeCom
    ``userid``). Empty for CLI / cron / local sessions and any context where
    no channel identity was bound. Capped at 128 chars per the spec.
    """
    try:
        from gateway.session_context import get_session_env
    except Exception:  # noqa: BLE001 — plugin must stay loadable standalone
        return ""
    return str(get_session_env("HERMES_SESSION_USER_NAME", "") or "").strip()[:128]


def _common_fields(tool_call_id: str, trace_id: str) -> Dict[str, Any]:
    """Fields attached to every event: identity, timing, sandbox.

    Note: ``actor`` is intentionally NOT sent. The spec marks it as
    server-overridden from the auth principal (sandbox id for machine
    reports); client self-report is ignored. ``username`` is the real
    triggering user and is the only client-attested identity.
    """
    body: Dict[str, Any] = {
        "event_id": uuid.uuid4().hex,
        "event_time": datetime.now(timezone.utc).isoformat(),
        "execution_id": tool_call_id or "",
        "trace_id": trace_id,
        "actor_type": "agent",
        "channel": "agent",
    }
    sb = _sandbox_id()
    if sb:
        body["resource_type"] = "sandbox"
        body["resource_id"] = sb
    env = _audit_env()
    if env:
        body["env"] = env
    username = _current_username()
    if username:
        body["username"] = username
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
    exit_code, stdout, stderr, result_cwd, blocked = _parse_terminal_result(result)
    decision, result_enum = _decide(status, result, exit_code, blocked)
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
    exit_code, _stdout, _stderr, _cwd, blocked = _parse_terminal_result(result)
    decision, result_enum = _decide(status, result, exit_code, blocked)
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


def _build_skill_view_body(
    args: Any,
    result: Any,
    status: str,
    duration_ms: int,
    tool_call_id: str,
    trace_id: str,
) -> Dict[str, Any]:
    """skill_view loads a skill's content into the conversation; the model
    then executes it. Execution itself is prompt-driven (no separate tool
    call), so reporting the load is the only auditable signal that a skill
    ran — reported on every invocation (fork, 2026-09-03)."""
    summary_args = args if isinstance(args, dict) else {}
    data: Dict[str, Any] = {}
    if isinstance(result, str):
        try:
            parsed = json.loads(result)
            if isinstance(parsed, dict):
                data = parsed
        except Exception:
            pass
    elif isinstance(result, dict):
        data = result

    name = str(
        summary_args.get("name")
        or data.get("name")
        or ""
    )
    ok = bool(data.get("success", True)) and status != "error"
    decision, result_enum = _decide(status, result, None, False)

    payload: Dict[str, Any] = {
        "summary": f"load {name}".strip(),
        "skill_name": name,
        "file_path": str(summary_args.get("file_path") or ""),
        "caller_type": "agent",
    }

    body = _common_fields(tool_call_id, trace_id)
    body.update({
        "event_type": "skill",
        "action": "skill.invoke",
        "risk_level": "medium",
        "risk_reason": "skill loaded into conversation (content injected into prompt)",
        "decision": decision if ok else "blocked",
        "result": result_enum,
        "duration_ms": int(duration_ms or 0),
        "payload": payload,
    })
    if name:
        body["skill_name"] = name
    return body


def _build_file_write_body(
    tool_name: str,
    args: Any,
    result: Any,
    status: str,
    duration_ms: int,
    tool_call_id: str,
    trace_id: str,
) -> Dict[str, Any]:
    """write_file/patch mutate file content — the only remaining data-
    destructive channel when deletion is whitelisted out (an agent blocked
    on ``rm`` can still blank/overwrite files). Reported on every call
    (fork, 2026-09-03): empty/whitespace-only writes escalate to medium
    (truncation candidates); regular writes report at info."""
    summary_args = args if isinstance(args, dict) else {}
    path = str(summary_args.get("path") or "")
    content = summary_args.get("content")
    content_str = content if isinstance(content, str) else ""
    # Only write_file carries the full new content — a blank write there is a
    # truncation candidate. patch args are a diff (no "content" field); flag
    # it empty only when the content field is genuinely present-but-blank.
    is_empty = (
        tool_name == "write_file" and not content_str.strip()
    ) or (
        tool_name != "write_file" and isinstance(content, str) and not content_str.strip()
    )
    content_sha, _preview = _sha_and_preview(content_str, limit=0)
    size = len(content_str.encode("utf-8", errors="ignore"))
    exit_code, _stdout, _stderr, _cwd, blocked = _parse_terminal_result(result)
    decision, result_enum = _decide(status, result, exit_code, blocked)

    payload: Dict[str, Any] = {
        "summary": f"{tool_name} {path}".strip(),
        "path": path,
        "tool": tool_name,
        "written_bytes": size,
        "content_sha256": content_sha,
        "empty_write": is_empty,
        "caller_type": "agent",
    }

    body = _common_fields(tool_call_id, trace_id)
    body.update({
        "event_type": "command",
        "action": "file.write",
        "risk_level": "medium" if is_empty else "info",
        "risk_reason": (
            "empty write to file (potential truncation)" if is_empty
            else "file content mutation (write/edit)"
        ),
        "decision": decision,
        "result": result_enum,
        "exit_code": exit_code,
        "duration_ms": int(duration_ms or 0),
        "payload": payload,
    })
    return body


def _build_code_body(
    code: str,
    result: Any,
    status: str,
    duration_ms: int,
    tool_call_id: str,
    trace_id: str,
) -> Dict[str, Any]:
    """execute_code runs arbitrary local Python — subprocess/os.system never
    pass through the terminal guard, so every call is reported (HIGH when the
    code matches process-spawn / dynamic-exec indicators, else MEDIUM)."""
    exit_code, stdout, _stderr, _cwd, blocked = _parse_terminal_result(result)
    decision, result_enum = _decide(status, result, exit_code, blocked)
    code_sha, code_preview = _sha_and_preview(code)
    risky = _CODE_RISKY_RE.search(code) is not None

    payload: Dict[str, Any] = {
        "code_sha256": code_sha,
        "code_preview": code_preview,
        "spawned_process_hint": risky,
        "caller_type": "agent",
    }

    body = _common_fields(tool_call_id, trace_id)
    body.update({
        "event_type": "command",
        "action": "code.exec",
        "risk_level": "high" if risky else "medium",
        "risk_reason": (
            "code spawns processes / dynamic exec" if risky
            else "arbitrary python execution (can bypass terminal guard)"
        ),
        "decision": decision,
        "result": result_enum,
        "exit_code": exit_code,
        "duration_ms": int(duration_ms or 0),
        "payload": payload,
    })
    return body


def _build_computer_use_body(
    args: Any,
    result: Any,
    status: str,
    duration_ms: int,
    tool_call_id: str,
    trace_id: str,
) -> Dict[str, Any]:
    exit_code, _stdout, _stderr, _cwd, blocked = _parse_terminal_result(result)
    decision, result_enum = _decide(status, result, exit_code, blocked)
    action = str(args.get("action") or "") if isinstance(args, dict) else ""

    payload: Dict[str, Any] = {
        "summary": action,
        "caller_type": "agent",
    }

    body = _common_fields(tool_call_id, trace_id)
    body.update({
        "event_type": "command",
        "action": "desktop.control",
        "risk_level": "medium",
        "risk_reason": "desktop control via cua-driver",
        "decision": decision,
        "result": result_enum,
        "exit_code": exit_code,
        "duration_ms": int(duration_ms or 0),
        "payload": payload,
    })
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
        url = _intake_url()
        if not url:
            return None  # feature off — no-op, no worker
        global _HTTP_WARNED
        if not _HTTP_WARNED and url.lower().startswith("http://"):
            _HTTP_WARNED = True
            logger.warning(
                "audit-callback: audit intake URL is plain http:// — "
                "audit payloads and the auth header travel unencrypted"
            )

        trace_id = api_request_id or turn_id or ""
        body: Optional[Dict[str, Any]] = None

        if tool_name == "terminal" and isinstance(args, dict):
            command = str(args.get("command") or "")
            severity, reason, matched_rules = _classify_terminal(command)
            # A blocked call is an unauthorized attempt — always reported
            # (fork, 2026-09-03), even when the command itself is not
            # classified dangerous and REPORT_ALL_COMMANDS is off.
            _b_exit, _b_out, _b_err, _b_cwd, blocked = _parse_terminal_result(result)
            if blocked and severity not in {"hardline", "dangerous"}:
                severity = "medium"
                reason = f"command blocked by approval guard/whitelist: {reason}".strip()
            if severity in {"hardline", "dangerous", "medium"} or _report_all_commands():
                body = _build_command_body(
                    command, args, result, status, duration_ms,
                    severity, reason, matched_rules, tool_call_id, trace_id,
                )
        elif tool_name == "skill_manage":
            body = _build_skill_body(
                args, result, status, duration_ms, tool_call_id, trace_id,
            )
        elif tool_name == "skill_view":
            body = _build_skill_view_body(
                args, result, status, duration_ms, tool_call_id, trace_id,
            )
        elif tool_name in ("write_file", "patch"):
            body = _build_file_write_body(
                tool_name, args, result, status, duration_ms,
                tool_call_id, trace_id,
            )
        elif tool_name == "execute_code" and isinstance(args, dict):
            body = _build_code_body(
                str(args.get("code") or ""), result, status, duration_ms,
                tool_call_id, trace_id,
            )
        elif tool_name == "computer_use":
            body = _build_computer_use_body(
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
