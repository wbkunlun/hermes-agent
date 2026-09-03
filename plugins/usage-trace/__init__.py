"""usage-trace — fork plugin (wehermes): continuous agent trace flow to local JSONL.

Appends one JSON line per event to ``$HERMES_HOME/usage-traces/<session_id>.jsonl``:

* ``api_request``        — per LLM API call: model/provider, canonical usage
                           tokens, duration, finish_reason (+ MoA advisor costs)
* ``api_error``          — failed API calls: status_code, retryable, error text
* ``user_prompt``        — once per user turn (deduped by turn_id)
* ``assistant_response`` — assistant text per API call
* ``tool_call``          — per tool call: name, duration, status/exit_code,
                           args/result under the capture mode
* ``approval``           — per approval decision: choice, surface, command
* ``session_end``        — session finalize marker

Turns are linked via ``turn_id`` + ``api_request_id`` (the local equivalent
of Claude Code's ``prompt.id`` correlation). Content capture modes
(``HERMES_USAGE_TRACE_CAPTURE``): ``metadata`` (sizes only), ``sanitized``
(default — ``agent.redact.redact_sensitive_text(force=True)`` then truncate;
redactor failure degrades that field to metadata), ``full`` (raw, truncated).

Fire-and-forget, mirroring audit-callback: hooks enqueue into a bounded
queue (2000, drop-oldest) and a single daemon writer appends batches to
disk. Hook failures never propagate into the agent loop. Old files are
pruned at writer start past ``HERMES_USAGE_TRACE_RETENTION_DAYS`` (30).

Env:
  HERMES_USAGE_TRACE=0                 master off-switch (default on)
  HERMES_USAGE_TRACE_CAPTURE           metadata|sanitized|full (default sanitized)
  HERMES_USAGE_TRACE_MAX_CHARS         default 12000
  HERMES_USAGE_TRACE_RETENTION_DAYS    default 30 (0 disables pruning)
  HERMES_USAGE_TRACE_DIR               override output directory
"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
from pathlib import Path
from queue import Empty, Full, Queue
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_SCHEMA = "hermes.usage_trace.v1"
_QUEUE_MAX = 2000
_BATCH_MAX = 64
_DEDUP_SET_MAX = 4096

# ".." collapses wholesale (path-traversal guard); other unsafe chars flatten.
_SANITIZE_RE = re.compile(r"\.\.|[^A-Za-z0-9._-]")
_USAGE_KEYS = (
    "input_tokens",
    "output_tokens",
    "cache_read_tokens",
    "cache_write_tokens",
    "reasoning_tokens",
    "prompt_tokens",
    "total_tokens",
    "request_count",
)

_queue: "Queue[Dict[str, Any]]" = Queue(maxsize=_QUEUE_MAX)
_dropped = 0
_worker: Optional[threading.Thread] = None
_worker_lock = threading.Lock()
_shutdown_evt = threading.Event()
_atexit_registered = False

_seen_user_turns: set = set()
_seen_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

def _enabled() -> bool:
    return os.getenv("HERMES_USAGE_TRACE", "1").strip().lower() not in {
        "0", "false", "off", "no",
    }


def _capture_mode() -> str:
    mode = os.getenv("HERMES_USAGE_TRACE_CAPTURE", "sanitized").strip().lower()
    return mode if mode in {"metadata", "sanitized", "full"} else "sanitized"


def _max_chars() -> int:
    try:
        return max(0, int(os.getenv("HERMES_USAGE_TRACE_MAX_CHARS", "12000")))
    except (TypeError, ValueError):
        return 12000


def _retention_days() -> int:
    try:
        return max(0, int(os.getenv("HERMES_USAGE_TRACE_RETENTION_DAYS", "30")))
    except (TypeError, ValueError):
        return 30


def _trace_dir() -> Path:
    override = os.getenv("HERMES_USAGE_TRACE_DIR", "").strip()
    if override:
        return Path(override)
    from hermes_constants import get_hermes_home
    return get_hermes_home() / "usage-traces"


# ---------------------------------------------------------------------------
# Content capture (three modes)
# ---------------------------------------------------------------------------

_REDACT_WARNED = False


def _sanitize(text: str) -> Optional[str]:
    """Redact secrets; ``None`` means the redactor failed (caller degrades)."""
    global _REDACT_WARNED
    try:
        from agent.redact import redact_sensitive_text
        return redact_sensitive_text(text, force=True)
    except Exception:
        if not _REDACT_WARNED:
            _REDACT_WARNED = True
            logger.warning(
                "usage-trace: redactor unavailable; sanitized fields degrade to metadata"
            )
        return None


def _capture_text(value: Any, *, text_key: str = "text") -> Dict[str, Any]:
    """Render a text payload under the active capture mode.

    metadata -> {"chars": N}; sanitized/full add ``text_key`` (when sanitized:
    redacted FIRST, then truncated — a secret straddling the truncation
    boundary must still match the redactor pattern). Redactor failure
    degrades to metadata — never write unredacted content in sanitized mode.
    """
    if value is None:
        return {"chars": 0}
    if isinstance(value, str):
        text = value
    else:
        try:
            text = json.dumps(value, ensure_ascii=False, default=str)
        except Exception:
            text = str(value)
    out: Dict[str, Any] = {"chars": len(text)}
    mode = _capture_mode()
    if mode == "metadata":
        return out
    if mode == "sanitized":
        safe = _sanitize(text)
        if safe is None:
            return out
        text = safe
    out[text_key] = text[: _max_chars()]
    return out


def _capture_obj(value: Any) -> Dict[str, Any]:
    """Render a structured (dict/list) payload under the active capture mode."""
    if value is None:
        return {"bytes": 0}
    try:
        text = json.dumps(value, ensure_ascii=False, default=str)
    except Exception:
        text = str(value)
    out: Dict[str, Any] = {"bytes": len(text.encode("utf-8", "replace"))}
    mode = _capture_mode()
    if mode == "metadata":
        return out
    if mode == "sanitized":
        safe = _sanitize(text)
        if safe is None:
            return out
        text = safe
    out["data"] = text[: _max_chars()]
    return out


# ---------------------------------------------------------------------------
# Session file routing
# ---------------------------------------------------------------------------

def _session_file(session_id: str) -> Path:
    """One JSONL per session; unsafe chars flattened, empty -> no-session."""
    sid = _SANITIZE_RE.sub("_", str(session_id or "").strip())
    return _trace_dir() / f"{sid or 'no-session'}.jsonl"


# ---------------------------------------------------------------------------
# Queue + writer
# ---------------------------------------------------------------------------

def _enqueue(event: Dict[str, Any]) -> None:
    """Non-blocking enqueue; bounded queue drops the oldest event on full."""
    global _dropped
    try:
        _queue.put_nowait(event)
        return
    except Full:
        pass
    try:
        _queue.get_nowait()  # drop oldest to make room
    except Empty:
        pass
    try:
        _queue.put_nowait(event)
    except Full:
        _dropped += 1
        if _dropped == 1 or _dropped % 100 == 0:
            logger.warning("usage-trace: queue full; dropped %d events", _dropped)


def _append_lines(path: Path, lines: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")


def _write_batch(batch: List[Dict[str, Any]]) -> None:
    """Group a batch by session file and append. Failures drop + count."""
    global _dropped
    by_file: Dict[Path, List[str]] = {}
    for ev in batch:
        line = json.dumps(ev, ensure_ascii=False, default=str)
        by_file.setdefault(_session_file(str(ev.get("session_id") or "")), []).append(line)
    for path, lines in by_file.items():
        try:
            _append_lines(path, lines)
        except OSError as exc:
            _dropped += len(lines)
            if _dropped == 1 or _dropped % 100 == 0:
                logger.warning(
                    "usage-trace: write to %s failed (%s); dropped %d events",
                    path, exc, _dropped,
                )


def _prune_old_files(now: float) -> None:
    """Delete *.jsonl older than the retention window (writer start only)."""
    days = _retention_days()
    if days <= 0:
        return
    cutoff = now - days * 86400.0
    try:
        for p in _trace_dir().glob("*.jsonl"):
            try:
                if p.stat().st_mtime < cutoff:
                    p.unlink()
            except OSError:
                pass
    except OSError:
        pass


def _writer_loop() -> None:
    _prune_old_files(time.time())
    while not _shutdown_evt.is_set():
        try:
            first = _queue.get(timeout=0.5)
        except Empty:
            continue
        batch = [first]
        while len(batch) < _BATCH_MAX:
            try:
                batch.append(_queue.get_nowait())
            except Empty:
                break
        _write_batch(batch)


def _ensure_worker() -> None:
    """Start the single daemon writer on first use (idempotent)."""
    global _worker, _atexit_registered
    if not _enabled():
        return
    if not _atexit_registered:
        _atexit_registered = True
        import atexit
        atexit.register(_flush_at_exit)
    with _worker_lock:
        if _worker is None or not _worker.is_alive():
            _shutdown_evt.clear()
            _worker = threading.Thread(
                target=_writer_loop, name="usage-trace-writer", daemon=True
            )
            _worker.start()


def _drain_now() -> None:
    """Synchronously flush pending events (tests / admin helper)."""
    batch: List[Dict[str, Any]] = []
    while True:
        try:
            batch.append(_queue.get_nowait())
        except Empty:
            break
    if batch:
        _write_batch(batch)


def _flush_at_exit() -> None:
    _shutdown_evt.set()
    w = _worker
    if w is not None and w.is_alive():
        w.join(timeout=2.0)
    _drain_now()


# ---------------------------------------------------------------------------
# Hook handlers — observer only, never raise into the agent loop
# ---------------------------------------------------------------------------

def _guard(fn):
    """Wrap a handler: disabled -> no-op; any exception -> debug log."""

    def wrapped(**kw: Any) -> None:
        try:
            if not _enabled():
                return
            fn(**kw)
        except Exception as exc:  # noqa: BLE001 — never break the agent loop
            logger.debug("usage-trace: %s handler error (%s)", fn.__name__, exc)

    return wrapped


def _base(
    event: str,
    *,
    session_id: str = "",
    task_id: str = "",
    turn_id: str = "",
    api_request_id: str = "",
) -> Dict[str, Any]:
    ev: Dict[str, Any] = {
        "ts": round(time.time(), 3),
        "event": event,
        "schema": _SCHEMA,
        "session_id": session_id or "",
    }
    if task_id:
        ev["task_id"] = task_id
    if turn_id:
        ev["turn_id"] = turn_id
    if api_request_id:
        ev["api_request_id"] = api_request_id
    return ev


def _assistant_text(msg: Any) -> str:
    """Extract printable text from an assistant message object."""
    content = getattr(msg, "content", None)
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = [
            p.get("text", "")
            for p in content
            if isinstance(p, dict) and p.get("type") == "text"
        ]
        return "\n".join(x for x in parts if x)
    return ""


def _on_post_api_request_inner(**kw: Any) -> None:
    usage = kw.get("usage") if isinstance(kw.get("usage"), dict) else {}
    model = kw.get("response_model") or kw.get("model") or ""
    link = dict(
        session_id=str(kw.get("session_id") or ""),
        task_id=str(kw.get("task_id") or ""),
        turn_id=str(kw.get("turn_id") or ""),
        api_request_id=str(kw.get("api_request_id") or ""),
    )
    ev = _base("api_request", **link)
    ev.update({
        "model": model,
        "provider": kw.get("provider") or "",
        "api_mode": kw.get("api_mode") or "",
        "api_call_count": kw.get("api_call_count", 0),
        "duration_ms": round(float(kw.get("api_duration") or 0.0) * 1000, 1),
        "finish_reason": kw.get("finish_reason") or "",
        "usage": {k: usage.get(k, 0) for k in _USAGE_KEYS},
        "assistant_content_chars": kw.get("assistant_content_chars", 0),
        "assistant_tool_call_count": kw.get("assistant_tool_call_count", 0),
    })
    moa = kw.get("moa_references")
    if isinstance(moa, list) and moa:
        ev["moa"] = [
            {
                "label": r.get("label"),
                "model": r.get("model"),
                "output_tokens": (r.get("usage") or {}).get("output_tokens"),
                "cost_usd": r.get("cost_usd"),
            }
            for r in moa
            if isinstance(r, dict)
        ]
    _ensure_worker()
    _enqueue(ev)

    text = _assistant_text(kw.get("assistant_message"))
    if text:
        rev = _base("assistant_response", **link)
        rev["model"] = model
        rev.update(_capture_text(text))
        _enqueue(rev)


def _on_api_request_error_inner(**kw: Any) -> None:
    ev = _base(
        "api_error",
        session_id=str(kw.get("session_id") or ""),
        task_id=str(kw.get("task_id") or ""),
        turn_id=str(kw.get("turn_id") or ""),
        api_request_id=str(kw.get("api_request_id") or ""),
    )
    ev.update({
        "model": kw.get("response_model") or kw.get("model") or "",
        "provider": kw.get("provider") or "",
        "duration_ms": round(float(kw.get("api_duration") or 0.0) * 1000, 1),
        "status_code": kw.get("status_code"),
        "retry_count": kw.get("retry_count"),
        "retryable": kw.get("retryable"),
    })
    if kw.get("error") is not None:
        ev["error"] = _capture_text(kw.get("error"))
    _ensure_worker()
    _enqueue(ev)


def _on_pre_llm_call_inner(**kw: Any) -> None:
    if kw.get("turn_type") != "user":
        return
    turn_id = str(kw.get("turn_id") or "")
    if not turn_id:
        return  # cannot dedup safely
    with _seen_lock:
        if turn_id in _seen_user_turns:
            return
        _seen_user_turns.add(turn_id)
        if len(_seen_user_turns) > _DEDUP_SET_MAX:
            _seen_user_turns.clear()  # bounded; worst case a duplicate line
    ev = _base(
        "user_prompt",
        session_id=str(kw.get("session_id") or ""),
        task_id=str(kw.get("task_id") or ""),
        turn_id=turn_id,
    )
    ev["platform"] = kw.get("platform") or ""
    ev.update(_capture_text(kw.get("user_message")))
    _ensure_worker()
    _enqueue(ev)


def _on_post_tool_call_inner(**kw: Any) -> None:
    result = kw.get("result")
    status = str(kw.get("status") or "")
    exit_code = None
    if isinstance(result, dict):
        exit_code = result.get("exit_code", result.get("returncode"))
    ev = _base(
        "tool_call",
        session_id=str(kw.get("session_id") or ""),
        task_id=str(kw.get("task_id") or ""),
        turn_id=str(kw.get("turn_id") or ""),
        api_request_id=str(kw.get("api_request_id") or ""),
    )
    ev.update({
        "tool_name": kw.get("tool_name") or "",
        "tool_call_id": kw.get("tool_call_id") or "",
        "duration_ms": kw.get("duration_ms", 0),
        "status": status,
        "exit_code": exit_code,
        "decision": "blocked" if status == "blocked" else "",
        "success": status != "blocked" and kw.get("error_type") is None,
        "args": _capture_obj(kw.get("args")),
        "result": _capture_obj(result),
    })
    _ensure_worker()
    _enqueue(ev)


def _on_post_approval_response_inner(**kw: Any) -> None:
    # approval hooks carry session_key (not session_id); route on it verbatim —
    # _session_file sanitizes unsafe chars either way.
    ev = _base(
        "approval",
        session_id=str(kw.get("session_key") or kw.get("session_id") or ""),
    )
    ev.update({
        "surface": kw.get("surface") or "",
        "choice": kw.get("choice") or "",
        "pattern_key": kw.get("pattern_key") or "",
        "coalesced": bool(kw.get("coalesced")),
    })
    # text_key="command" so the field reads naturally in the approval event
    ev.update(_capture_text(kw.get("command"), text_key="command"))
    _ensure_worker()
    _enqueue(ev)


def _on_session_finalize_inner(**kw: Any) -> None:
    ev = _base("session_end", session_id=str(kw.get("session_id") or ""))
    ev["reason"] = kw.get("reason") or ""
    _ensure_worker()
    _enqueue(ev)


_on_post_api_request = _guard(_on_post_api_request_inner)
_on_api_request_error = _guard(_on_api_request_error_inner)
_on_pre_llm_call = _guard(_on_pre_llm_call_inner)
_on_post_tool_call = _guard(_on_post_tool_call_inner)
_on_post_approval_response = _guard(_on_post_approval_response_inner)
_on_session_finalize = _guard(_on_session_finalize_inner)


def register(ctx) -> None:
    if not _enabled():
        logger.info("usage-trace: disabled via HERMES_USAGE_TRACE")
        return
    ctx.register_hook("post_api_request", _on_post_api_request)
    ctx.register_hook("api_request_error", _on_api_request_error)
    ctx.register_hook("pre_llm_call", _on_pre_llm_call)
    ctx.register_hook("post_tool_call", _on_post_tool_call)
    ctx.register_hook("post_approval_response", _on_post_approval_response)
    ctx.register_hook("on_session_finalize", _on_session_finalize)
    ctx.register_hook("on_session_end", _on_session_finalize)
