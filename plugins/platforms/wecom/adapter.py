"""WeCom AI Bot WebSocket adapter — thin re-export wrapper.

The full implementation lives in ``gateway/platforms/wecom.py`` and is
preserved there verbatim so the upstream gateway-runner code path,
``tools.send_message_tool._send_wecom``, and every other caller of
``from gateway.platforms.wecom import …`` keeps working without
modification.

This wrapper exists so the bundled-plugin slot at
``plugins/platforms/wecom/adapter.py`` has a real module to point at
when ``register(ctx)`` is called, and so the upstream plugin
discovery (``plugins/platforms/*/adapter.py``) can find the wecom
adapter alongside ``photon``, ``raft``, etc.

Production-tested fixes — preserved verbatim in
``gateway/platforms/wecom.py``:

* ``WeComAdapter.get_active`` / ``set_active`` classmethod singletons.
* ``asyncio.Lock`` on ``_reconnect_send``.
* ``_send_with_reconnect_retry`` retries on ``RuntimeError`` AND on
  ``errcode 846609`` (aibot websocket not subscribed).
* ``_flush_text_batch`` cancel-delivery race guard.
* ``Concurrent call to receive()`` transient-detection in ``_listen_loop``.
* dm_policy ``WECOM_ALLOWED_USERS`` env-var fallback.
"""

from __future__ import annotations

from gateway.platforms.wecom import (  # noqa: F401 — explicit re-exports
    APP_CMD_CALLBACK,
    APP_CMD_EVENT_CALLBACK,
    APP_CMD_LEGACY_CALLBACK,
    APP_CMD_PING,
    APP_CMD_RESPONSE,
    APP_CMD_SEND,
    APP_CMD_SUBSCRIBE,
    APP_CMD_UPLOAD_MEDIA_CHUNK,
    APP_CMD_UPLOAD_MEDIA_FINISH,
    APP_CMD_UPLOAD_MEDIA_INIT,
    CALLBACK_COMMANDS,
    CONNECT_TIMEOUT_SECONDS,
    DEFAULT_WS_URL,
    ERRCODE_NOT_SUBSCRIBED,
    HEARTBEAT_INTERVAL_SECONDS,
    MAX_MESSAGE_LENGTH,
    NON_RESPONSE_COMMANDS,
    RECONNECT_BACKOFF,
    REQUEST_TIMEOUT_SECONDS,
    VOICE_SUPPORTED_MIMES,
    WeComAdapter,
    check_wecom_requirements,
    get_active_adapter,
    qr_scan_for_bot_info,
)

__all__ = [
    "WeComAdapter",
    "check_wecom_requirements",
    "get_active_adapter",
    "qr_scan_for_bot_info",
    "APP_CMD_CALLBACK",
    "APP_CMD_EVENT_CALLBACK",
    "APP_CMD_LEGACY_CALLBACK",
    "APP_CMD_PING",
    "APP_CMD_RESPONSE",
    "APP_CMD_SEND",
    "APP_CMD_SUBSCRIBE",
    "APP_CMD_UPLOAD_MEDIA_CHUNK",
    "APP_CMD_UPLOAD_MEDIA_FINISH",
    "APP_CMD_UPLOAD_MEDIA_INIT",
    "CALLBACK_COMMANDS",
    "CONNECT_TIMEOUT_SECONDS",
    "DEFAULT_WS_URL",
    "ERRCODE_NOT_SUBSCRIBED",
    "HEARTBEAT_INTERVAL_SECONDS",
    "MAX_MESSAGE_LENGTH",
    "NON_RESPONSE_COMMANDS",
    "RECONNECT_BACKOFF",
    "REQUEST_TIMEOUT_SECONDS",
    "VOICE_SUPPORTED_MIMES",
]