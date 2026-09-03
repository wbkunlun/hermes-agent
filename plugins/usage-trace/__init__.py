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
    text = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, default=str)
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
    out: Dict[str, Any] = {"bytes": len(text)}
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
    sid = _SANITIZE_RE.sub("_", (session_id or "").strip())
    return _trace_dir() / f"{sid or 'no-session'}.jsonl"


def _ensure_worker() -> None:
    """Start the writer thread — implemented in a later task; stub for tests."""
    return None
