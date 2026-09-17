"""(fork, wehermes) In-process passive health endpoint for the WeCom gateway.

Serves GET /health (aggregate) and /health/detail (per-check detail with
Chinese remediation hints) from a daemon thread off the main event loop, so a
wedged loop still answers — built from the file contract (gateway_state.json,
state/gateway.heartbeat, gateway.readiness/memory_status rollups) plus passive
model-call counters fed by hooks. Zero active LLM probes (no token cost).

Env: HEALTH_CHECK_ENABLED (default on), HEALTH_CHECK_HOST (127.0.0.1),
HEALTH_CHECK_PORT (7860); thresholds HEALTH_MODEL_WINDOW_S,
HEALTH_CONSECUTIVE_ERRORS, HEALTH_INFLIGHT_STALE_S, HEALTH_STUCK_MINUTES,
HEALTH_PLATFORM_DOWN_MINUTES.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Callable

from .health_lib import HealthCounters, build_health_detail, env_flag, env_float, env_int
from .server import start_server

logger = logging.getLogger(__name__)

_HOOKS = ("pre_api_request", "post_api_request", "api_request_error",
          "pre_gateway_dispatch", "post_tool_call")

# Module-level handles for tests/introspection; None until register().
_SERVER = None
_COUNTERS = None
_HOOK_SINK = None


class _HookSink:
    """Bind hook kwargs (shapes fixed by hermes_cli/hooks.py payloads) → counters."""

    def __init__(self, counters: HealthCounters) -> None:
        self._c = counters

    def pre_api_request(self, **kw: Any) -> None:
        self._c.on_pre(session_id=str(kw.get("session_id") or ""),
                       provider=str(kw.get("provider") or ""), model=str(kw.get("model") or ""))

    def post_api_request(self, **kw: Any) -> None:
        self._c.on_success(session_id=str(kw.get("session_id") or ""),
                           provider=str(kw.get("provider") or ""), model=str(kw.get("model") or ""),
                           api_duration=kw.get("api_duration"))

    def api_request_error(self, **kw: Any) -> None:
        err = kw.get("error") if isinstance(kw.get("error"), dict) else {}
        self._c.on_error(session_id=str(kw.get("session_id") or ""),
                         provider=str(kw.get("provider") or ""), model=str(kw.get("model") or ""),
                         reason=kw.get("reason"), status_code=kw.get("status_code"),
                         retryable=kw.get("retryable"), error_type=str(err.get("type") or ""))

    def pre_gateway_dispatch(self, **kw: Any) -> None:
        self._c.on_inbound()

    def post_tool_call(self, **kw: Any) -> None:
        self._c.on_tool(session_id=str(kw.get("session_id") or ""))


def _guard(name: str, fn: Callable) -> Callable:
    def wrapped(**kw: Any) -> None:
        try:
            fn(**kw)
        except Exception:
            logger.warning("health plugin: hook %s failed", name, exc_info=True)
    return wrapped


def _build_detail(counters: HealthCounters) -> Callable[[], dict]:
    def build() -> dict:
        from hermes_constants import get_hermes_home
        return build_health_detail(
            counters.snapshot(), home=get_hermes_home(),
            consecutive_down=env_int("HEALTH_CONSECUTIVE_ERRORS", 3),
            inflight_stale_s=env_float("HEALTH_INFLIGHT_STALE_S", 600.0),
            stuck_minutes=env_float("HEALTH_STUCK_MINUTES", 10.0),
            platform_down_minutes=env_float("HEALTH_PLATFORM_DOWN_MINUTES", 10.0))
    return build


def register(ctx) -> None:
    global _SERVER, _COUNTERS, _HOOK_SINK
    if not env_flag("HEALTH_CHECK_ENABLED", True):
        logger.info("health: disabled via HEALTH_CHECK_ENABLED")
        return
    counters = HealthCounters(window_s=env_float("HEALTH_MODEL_WINDOW_S", 900.0))
    sink = _HookSink(counters)
    for name in _HOOKS:
        ctx.register_hook(name, _guard(name, getattr(sink, name)))
    host = (os.environ.get("HEALTH_CHECK_HOST") or "127.0.0.1").strip() or "127.0.0.1"
    _SERVER = start_server(build_detail=_build_detail(counters), host=host,
                           port=env_int("HEALTH_CHECK_PORT", 7860), logger=logger)
    _COUNTERS, _HOOK_SINK = counters, sink
