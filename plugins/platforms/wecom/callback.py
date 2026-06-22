"""WeCom callback (self-built app HTTP) adapter — thin re-export wrapper.

Full implementation lives in ``gateway/platforms/wecom_callback.py``.
This module exists for plugin-discovery symmetry with
``plugins/platforms/wecom/adapter.py``.
"""

from __future__ import annotations

from gateway.platforms.wecom_callback import (  # noqa: F401
    ACCESS_TOKEN_TTL_SECONDS,
    DEFAULT_HOST,
    DEFAULT_PATH,
    DEFAULT_PORT,
    MESSAGE_DEDUP_TTL_SECONDS,
    WecomCallbackAdapter,
    check_wecom_callback_requirements,
)

__all__ = [
    "ACCESS_TOKEN_TTL_SECONDS",
    "DEFAULT_HOST",
    "DEFAULT_PATH",
    "DEFAULT_PORT",
    "MESSAGE_DEDUP_TTL_SECONDS",
    "WecomCallbackAdapter",
    "check_wecom_callback_requirements",
]