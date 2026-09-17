"""Health logic for the (fork, wehermes) health plugin.

Pure, side-effect-free state + diagnostics: the passive model-signal counters
fed by plugin hooks, status derivations from them, read-only probes over the
gateway's file contract (``gateway_state.json``, ``state/gateway.heartbeat``)
and the shared readiness/memory rollups. No sockets, no threads, no relative
imports — the HTTP surface (``server.py``) and hook wiring (``__init__.py``)
live in siblings. Everything degrades to ``unknown``/``ok`` instead of raising:
a health endpoint that 500s on a missing file is worse than one that reports
honestly.
"""

from __future__ import annotations

import json
import os
import threading
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

# Freshness budget for the 30s loop heartbeat; mirrors gateway/memory_status
# (writer cadence 30s, 150s tolerates a briefly stalled loop without letting a
# long-dead gateway's last sample pose as current).
LOOP_HEARTBEAT_TTL_S = 150.0
_CONNECTED_STATES = {"connected", "running", "ok"}
_TRUTHY = {"1", "true", "yes", "y", "on"}


def env_flag(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    return raw.strip().lower() in _TRUTHY


def env_float(name: str, default: float) -> float:
    try:
        return max(1.0, float(os.environ.get(name, "") or default))
    except (TypeError, ValueError):
        return default


def env_int(name: str, default: int) -> int:
    try:
        return max(1, int(os.environ.get(name, "") or default))
    except (TypeError, ValueError):
        return default


def utc_now_iso(now_epoch: Optional[float] = None) -> Optional[str]:
    if now_epoch is None:
        return None
    return datetime.fromtimestamp(now_epoch, tz=timezone.utc).isoformat()


def _iso_to_epoch(value: Any) -> Optional[float]:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


class HealthCounters:
    """Thread-safe passive model-signal state fed by plugin hooks.

    All timestamps are epoch seconds; every mutator accepts ``ts`` so tests can
    drive deterministic timelines. The in-flight slot is a single global pair
    (start + model) — with concurrent agents the first completion clears it,
    which is acceptable imprecision for this deployment (stale in-flight is a
    degraded hint, never a down verdict). ``snapshot()`` returns a copy with
    ISO display strings added.
    """

    def __init__(self, *, window_s: float = 900.0, max_events: int = 200) -> None:
        self._window_s = float(window_s)
        self._events: deque = deque(maxlen=int(max_events))  # (ts, kind) kind: success|error
        self._lock = threading.Lock()
        self._last_success: Optional[float] = None
        self._last_error: Optional[float] = None
        self._last_inbound: Optional[float] = None
        self._last_tool: Optional[float] = None
        self._consecutive = 0
        self._last_error_info: Optional[Dict[str, Any]] = None
        self._in_flight_since: Optional[float] = None
        self._in_flight_model: Optional[str] = None

    # -- mutators (hook threads) --------------------------------------------------
    def on_pre(self, *, session_id: str = "", provider: str = "", model: str = "",
               ts: Optional[float] = None) -> None:
        when = time.time() if ts is None else ts
        with self._lock:
            self._in_flight_since = when
            self._in_flight_model = model or None

    def on_success(self, *, session_id: str = "", provider: str = "", model: str = "",
                   api_duration: Optional[float] = None, ts: Optional[float] = None) -> None:
        when = time.time() if ts is None else ts
        with self._lock:
            self._prune(when)
            self._events.append((when, "success"))
            self._last_success = when
            self._consecutive = 0
            self._in_flight_since = None
            self._in_flight_model = None

    def on_error(self, *, session_id: str = "", provider: str = "", model: str = "",
                 reason: Optional[str] = None, status_code: Optional[int] = None,
                 retryable: Optional[bool] = None, error_type: str = "",
                 ts: Optional[float] = None) -> None:
        when = time.time() if ts is None else ts
        with self._lock:
            self._prune(when)
            self._events.append((when, "error"))
            self._last_error = when
            self._consecutive += 1
            self._last_error_info = {
                "reason": reason or "", "status_code": status_code, "retryable": retryable,
                "error_type": error_type, "provider": provider, "model": model,
                "at": utc_now_iso(when)}
            self._in_flight_since = None
            self._in_flight_model = None

    def on_inbound(self, *, ts: Optional[float] = None) -> None:
        when = time.time() if ts is None else ts
        with self._lock:
            self._last_inbound = when

    def on_tool(self, *, session_id: str = "", ts: Optional[float] = None) -> None:
        when = time.time() if ts is None else ts
        with self._lock:
            self._last_tool = when

    def _prune(self, now: float) -> None:
        horizon = now - self._window_s
        while self._events and self._events[0][0] < horizon:
            self._events.popleft()

    # -- reader (health thread) ---------------------------------------------------
    def snapshot(self, *, now: Optional[float] = None) -> Dict[str, Any]:
        now = time.time() if now is None else now
        with self._lock:
            self._prune(now)
            events = list(self._events)
            last_success, last_error = self._last_success, self._last_error
            last_inbound, last_tool = self._last_inbound, self._last_tool
            consecutive = self._consecutive
            last_error_info = dict(self._last_error_info) if self._last_error_info else None
            in_flight_since, in_flight_model = self._in_flight_since, self._in_flight_model
        in_flight_age = max(0.0, now - in_flight_since) if in_flight_since is not None else None
        return {
            "last_success_ts": last_success, "last_success_at": utc_now_iso(last_success),
            "last_error_ts": last_error, "last_error_at": utc_now_iso(last_error),
            "last_inbound_ts": last_inbound, "last_inbound_at": utc_now_iso(last_inbound),
            "last_tool_ts": last_tool, "last_tool_at": utc_now_iso(last_tool),
            "consecutive_errors": consecutive, "last_error": last_error_info,
            "window_s": int(self._window_s),
            "calls": sum(1 for _, kind in events if kind == "success"),
            "errors": sum(1 for _, kind in events if kind == "error"),
            "in_flight": in_flight_since is not None,
            "in_flight_age_s": round(in_flight_age, 1) if in_flight_age is not None else None,
            "in_flight_model": in_flight_model,
        }


# ---------------------------------------------------------------------------
# Status derivations from a HealthCounters snapshot
# ---------------------------------------------------------------------------

def model_check(snapshot: Dict[str, Any], *, now_epoch: Optional[float] = None,
                consecutive_down: int = 3, inflight_stale_s: float = 600.0) -> Dict[str, Any]:
    """Passive model health. ``down`` needs ``consecutive_down`` failures with no
    interleaved success (the streak counter resets on success, so the streak
    alone proves it). ``degraded`` covers error/success mixes (≈ serving via
    fallback) and a call stuck in flight past ``inflight_stale_s`` (default
    tolerates long-thinking reasoning models). An idle window is ``no_data`` —
    silence is not sickness."""
    now = time.time() if now_epoch is None else now_epoch
    out: Dict[str, Any] = {
        "status": "ok",
        "last_success_at": snapshot.get("last_success_at"),
        "last_error_at": snapshot.get("last_error_at"),
        "consecutive_errors": int(snapshot.get("consecutive_errors") or 0),
        "last_error": snapshot.get("last_error"),
        "window_s": int(snapshot.get("window_s") or 0),
        "calls": int(snapshot.get("calls") or 0),
        "errors": int(snapshot.get("errors") or 0),
        "in_flight": bool(snapshot.get("in_flight")),
        "in_flight_age_s": snapshot.get("in_flight_age_s"),
        "in_flight_model": snapshot.get("in_flight_model"),
    }
    streak = out["consecutive_errors"]
    if out["in_flight"] and out["in_flight_age_s"] is not None and out["in_flight_age_s"] > inflight_stale_s:
        out["status"] = "degraded"
        out["detail"] = f"call in flight {int(out['in_flight_age_s'])}s without completing"
        out["remediation"] = "模型调用长时间无返回：查代理出口/provider 状态；持续超时可在 WeCom 发 /stop 重置会话"
        return out
    if streak >= consecutive_down:
        reason = (out["last_error"] or {}).get("reason") or ""
        out["status"] = "down"
        out["detail"] = f"{streak} consecutive API failures"
        if reason in {"auth", "billing"}:
            out["remediation"] = "模型认证/额度失败：检查 provider API key 是否失效或账户欠费"
        elif reason in {"rate_limit", "upstream_rate_limit"}:
            out["remediation"] = "模型持续限流：等限额重置或配置 fallback provider"
        else:
            out["remediation"] = "模型连续失败：查 provider 状态与网络出口；恢复无望时重启容器"
        return out
    has_traffic = out["calls"] or out["errors"] or out["in_flight"] or out["last_success_at"]
    if not has_traffic:
        out["status"] = "no_data"
        out["detail"] = "no API calls in the window (idle)"
        return out
    if streak > 0 or out["errors"] > 0:
        out["status"] = "degraded"
        out["detail"] = ("errors in window with successes (likely serving via fallback)"
                         if out["calls"] else "errors in window")
        out["remediation"] = "存在 API 错误但仍有成功（可能在走 fallback）：检查主 provider 状态/限额"
    return out


def stuck_check(snapshot: Dict[str, Any], *, now_epoch: Optional[float] = None,
                stuck_minutes: float = 10.0) -> Dict[str, Any]:
    """Global silent-stall detector: inbound seen, nothing progressed since,
    nothing in flight. Mirrors gateway/session_stall semantics (pending inbound
    + no progress) without replicating session-key derivation. Per-session
    precision is traded for key-format independence."""
    now = time.time() if now_epoch is None else now_epoch
    last_inbound = snapshot.get("last_inbound_ts")
    out: Dict[str, Any] = {"status": "ok"}
    if last_inbound is None:
        out["detail"] = "no inbound yet"
        return out
    candidates = [snapshot.get("last_success_ts"), snapshot.get("last_tool_ts")]
    last_progress = max((t for t in candidates if t is not None), default=None)
    unanswered_s = max(0.0, now - float(last_inbound))
    out["last_inbound_at"] = snapshot.get("last_inbound_at")
    out["unanswered_min"] = round(unanswered_s / 60.0, 1)
    if snapshot.get("in_flight"):
        out["detail"] = "call in progress (model check covers stale calls)"
        return out
    if last_progress is not None and last_progress >= float(last_inbound):
        out["detail"] = "progress after last inbound"
        return out
    if unanswered_s > stuck_minutes * 60.0:
        out["status"] = "degraded"
        out["detail"] = f"inbound unanswered for {int(unanswered_s // 60)} min with no work in flight"
        out["remediation"] = "有消息进来但无任何进展：在 WeCom 发 /stop 或 /new 重置会话；无效则重启容器"
    else:
        out["detail"] = "recent inbound still within grace window"
    return out
