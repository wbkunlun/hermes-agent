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


# ---------------------------------------------------------------------------
# Read-only probes over the gateway file contract
# ---------------------------------------------------------------------------

def _read_gateway_state(home: Path) -> Optional[Dict[str, Any]]:
    from gateway.status import read_runtime_status
    record = read_runtime_status(Path(home) / "gateway_state.json")
    return record if isinstance(record, dict) else None


def probe_process(home: Path) -> Dict[str, Any]:
    """Liveness by construction: an HTTP response proves this process serves.
    The state file adds ownership — a dead writer means a stale file, a live
    foreign PID means two gateways share one HERMES_HOME."""
    out: Dict[str, Any] = {"status": "ok", "pid": os.getpid()}
    try:
        record = _read_gateway_state(home)
    except Exception as exc:
        out["file"] = "unreadable"
        out["detail"] = f"gateway_state.json read failed: {type(exc).__name__}"
        return out
    if record is None:
        out["file"] = "missing"
        out["detail"] = "gateway_state.json not written yet (still starting)"
        return out
    out["gateway_state"] = record.get("gateway_state")
    file_pid = record.get("pid")
    out["file_pid"] = file_pid if isinstance(file_pid, int) else None
    try:
        from gateway.status import runtime_status_pid_is_live
        live = bool(runtime_status_pid_is_live(record))
    except Exception:
        live = True  # cannot judge; the response itself is the liveness proof
    if not live:
        out["status"] = "degraded"
        out["detail"] = "state file describes a dead process (stale from an old gateway?)"
        out["remediation"] = "gateway_state.json 指向已死进程：疑似异常退出残留；重启容器可清理"
        return out
    if isinstance(file_pid, int) and file_pid != os.getpid():
        out["status"] = "degraded"
        out["detail"] = "state file owned by another live gateway"
        out["remediation"] = "另一个 gateway 进程占用同一 HERMES_HOME：检查是否双实例误部署"
    return out


def probe_loop(home: Path, *, now_epoch: Optional[float] = None,
               ttl_s: float = LOOP_HEARTBEAT_TTL_S) -> Dict[str, Any]:
    """Main-loop liveness from the unconditionally-written 30s heartbeat file.
    Stale past the TTL means the asyncio loop stopped dispatching — the exact
    'stuck' case this endpoint exists to report while it still can."""
    now = time.time() if now_epoch is None else now_epoch
    try:
        raw = json.loads((Path(home) / "state" / "gateway.heartbeat").read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ValueError("not an object")
        ts = _iso_to_epoch(raw.get("updated_at"))
    except Exception:
        return {"status": "unknown", "detail": "heartbeat file missing/unreadable (gateway still starting?)"}
    if ts is None:
        return {"status": "unknown", "detail": "heartbeat timestamp unparseable"}
    age = max(0.0, now - ts)
    out: Dict[str, Any] = {"status": "ok" if age <= ttl_s else "down",
                           "heartbeat_age_s": round(age, 1), "ttl_s": ttl_s}
    start = raw.get("start_time")
    if isinstance(start, (int, float)) and not isinstance(start, bool):
        out["uptime_s"] = int(max(0.0, now - float(start)))
    if out["status"] == "down":
        out["detail"] = f"loop heartbeat stale ({int(age)}s > {int(ttl_s)}s)"
        out["remediation"] = "主循环卡死：loop watchdog 应已强制退出并重启；若持续出现查 logs 下堆栈转储"
    return out


def probe_platform(home: Path, *, now_epoch: Optional[float] = None, platform: str = "wecom",
                   platform_down_minutes: float = 10.0) -> Dict[str, Any]:
    """Chat-platform connection state from gateway_state.json (adapters persist
    state/needs_attention/retrying_since via _mark_* helpers)."""
    now = time.time() if now_epoch is None else now_epoch
    try:
        record = _read_gateway_state(home)
    except Exception:
        return {"status": "unknown", "detail": "gateway state unreadable"}
    platforms = record.get("platforms") if isinstance(record, dict) else None
    entry = platforms.get(platform) if isinstance(platforms, dict) else None
    if not isinstance(entry, dict):
        return {"status": "unknown", "detail": f"platform {platform} not started"}
    state = str(entry.get("state") or "unknown").lower()
    needs_attention = bool(entry.get("needs_attention"))
    out: Dict[str, Any] = {"status": "ok", "platform": platform, "state": state,
                           "needs_attention": needs_attention,
                           "retrying_since": entry.get("retrying_since")}
    if state in _CONNECTED_STATES and not needs_attention:
        return out
    if needs_attention:
        out.update({"status": "down", "detail": "reconnect loop escalated (needs_attention)",
                    "remediation": "WeCom 连接持续重连失败：检查智能机器人凭证与网络出口"})
        return out
    retrying_since = _iso_to_epoch(entry.get("retrying_since"))
    if retrying_since is not None and (now - retrying_since) > platform_down_minutes * 60.0:
        minutes = int((now - retrying_since) // 60)
        out.update({"status": "down", "detail": f"disconnected and retrying for {minutes} min",
                    "remediation": "WeCom 断连超过阈值：检查智能机器人凭证与网络出口"})
        return out
    out.update({"status": "degraded", "detail": f"state={state} (reconnecting)",
                "remediation": "WeCom 连接异常重连中；持续断连检查凭证与网络"})
    return out


def _configured_model(home: Path) -> str:
    """model from config.yaml, mirroring gateway.run._resolve_gateway_model's
    read (string, or mapping's default/model key) without importing the runner."""
    try:
        import yaml
        data = yaml.safe_load((Path(home) / "config.yaml").read_text(encoding="utf-8"))
    except Exception:
        return ""
    model_cfg = data.get("model") if isinstance(data, dict) else None
    if isinstance(model_cfg, str):
        return model_cfg
    if isinstance(model_cfg, dict):
        return str(model_cfg.get("default") or model_cfg.get("model") or "")
    return ""


def _system_remediation(readiness: Dict[str, Any]) -> str:
    checks = readiness.get("checks") if isinstance(readiness.get("checks"), dict) else {}

    def bad(name: str) -> bool:
        entry = checks.get(name)
        return isinstance(entry, dict) and entry.get("status") != "ok"

    if bad("disk"):
        return "磁盘水位过高：清理 $HERMES_HOME（sessions/logs）"
    if bad("state_db"):
        return "state.db 不可读/损坏：查磁盘与容器重启历史，必要时按上游修复流程处理"
    if bad("model"):
        return "config.yaml 未配置模型：检查 model 配置"
    if bad("config"):
        return "config.yaml 解析失败：检查语法"
    return "系统检查异常：查看 /health/detail 的 system.checks 明细"


def probe_system(home: Path, *, runtime_status: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Shared readiness rollup (state.db ro-probe / config / disk / gateway /
    queues) plus memory pressure from the heartbeat file. Never raises."""
    out: Dict[str, Any] = {"status": "ok"}
    try:
        from gateway.readiness import collect_runtime_readiness
        record = runtime_status if runtime_status is not None else _read_gateway_state(home)
        readiness = collect_runtime_readiness(
            configured_model=_configured_model(Path(home)), runtime_status=record or {})
        out["checks"] = readiness.get("checks", {})
        if readiness.get("status") != "ok":
            out["status"] = "degraded"
            out["remediation"] = _system_remediation(readiness)
    except Exception as exc:
        out.update({"status": "unknown", "detail": f"readiness failed: {type(exc).__name__}"})
        return out
    try:
        from gateway.memory_status import collect_memory_status
        memory = collect_memory_status(Path(home))
        out["memory"] = memory
        if memory.get("pressure") == "critical":
            out["status"] = "degraded"
            out["remediation"] = "内存水位 critical：重启容器释放内存"
    except Exception:
        out["memory"] = {"pressure": "unknown"}
    return out


# ---------------------------------------------------------------------------
# Aggregation + payload assembly
# ---------------------------------------------------------------------------

_AGG_ORDER = {"down": 0, "degraded": 1, "ok": 2, "no_data": 2, "unknown": 2}


def aggregate(*statuses: str) -> str:
    """Worst non-neutral status wins; ok/no_data/unknown are neutral-positive."""
    worst = min(statuses, key=lambda s: _AGG_ORDER.get(s, 2), default="ok")
    return worst if worst in _AGG_ORDER else "ok"


def build_health_detail(snapshot: Dict[str, Any], *, home: Path, now_epoch: Optional[float] = None,
                        consecutive_down: int = 3, inflight_stale_s: float = 600.0,
                        stuck_minutes: float = 10.0, platform_down_minutes: float = 10.0,
                        ) -> Dict[str, Any]:
    """Full /health/detail payload: every check plus aggregated status and the
    collected Chinese remediation hints. All constituent probes never raise."""
    home = Path(home)
    process = probe_process(home)
    loop = probe_loop(home, now_epoch=now_epoch)
    platform = probe_platform(home, now_epoch=now_epoch, platform_down_minutes=platform_down_minutes)
    system = probe_system(home)
    model = model_check(snapshot, now_epoch=now_epoch, consecutive_down=consecutive_down,
                        inflight_stale_s=inflight_stale_s)
    stuck = stuck_check(snapshot, now_epoch=now_epoch, stuck_minutes=stuck_minutes)
    status = aggregate(process["status"], loop["status"], platform["status"],
                       system["status"], model["status"], stuck["status"])
    remediation = [block["remediation"] for block in (process, loop, platform, system, model, stuck)
                   if block.get("remediation")]
    detail: Dict[str, Any] = {
        "status": status, "checked_at": utc_now_iso(now_epoch if now_epoch is not None else time.time()),
        "process": process, "loop": loop, "platform": platform,
        "model": model, "stuck": stuck, "system": system, "remediation": remediation}
    if isinstance(loop.get("uptime_s"), int):
        detail["uptime_s"] = loop["uptime_s"]
    return detail


def summarize(detail: Dict[str, Any]) -> Dict[str, Any]:
    """The /health minimal body: what a monitor needs and nothing else."""
    return {"status": detail.get("status"), "checked_at": detail.get("checked_at")}
