"""WeCom (Enterprise WeChat) platform plugin.

Bundled-plugin entry point. Public adapter API is re-exported lazily
(PEP 562 ``__getattr__``) so callers can
``from plugins.platforms.wecom import WeComAdapter`` without forcing
optional dependencies (``cryptography``, ``defusedxml``) to import —
the original ``gateway/platforms/wecom.py`` doesn't need them either.

Production-tested wecom fixes live in
``gateway/platforms/wecom.py`` and are re-exported verbatim from
``plugins/platforms/wecom/adapter.py``:

* ``WeComAdapter.get_active`` / ``set_active`` classmethod singletons.
* ``asyncio.Lock`` on ``_reconnect_send``.
* ``_send_with_reconnect_retry`` retries on ``RuntimeError`` AND on
  ``errcode 846609``.
* ``_flush_text_batch`` cancel-delivery race guard.
* ``Concurrent call to receive()`` transient-detection in ``_listen_loop``.
* dm_policy ``WECOM_ALLOWED_USERS`` env-var fallback.
"""

from __future__ import annotations

from typing import Any, TYPE_CHECKING

__all__ = [
    "WeComAdapter",
    "check_wecom_requirements",
    "get_active_adapter",
    "qr_scan_for_bot_info",
    "WecomCallbackAdapter",
    "check_wecom_callback_requirements",
    "WXBizMsgCrypt",
    "PKCS7Encoder",
    "WeComCryptoError",
    "SignatureError",
    "DecryptError",
    "EncryptError",
    "register",
]

if TYPE_CHECKING:  # pragma: no cover — only for type checkers
    from plugins.platforms.wecom.adapter import (
        WeComAdapter,
        check_wecom_requirements,
        get_active_adapter,
        qr_scan_for_bot_info,
    )
    from plugins.platforms.wecom.callback import (
        WecomCallbackAdapter,
        check_wecom_callback_requirements,
    )
    from plugins.platforms.wecom.crypto import (
        PKCS7Encoder,
        WXBizMsgCrypt,
        WeComCryptoError,
        SignatureError,
        DecryptError,
        EncryptError,
    )


_LAZY_EXPORTS = {
    "WeComAdapter": ("plugins.platforms.wecom.adapter", "WeComAdapter"),
    "check_wecom_requirements": (
        "plugins.platforms.wecom.adapter",
        "check_wecom_requirements",
    ),
    "get_active_adapter": (
        "plugins.platforms.wecom.adapter",
        "get_active_adapter",
    ),
    "qr_scan_for_bot_info": (
        "plugins.platforms.wecom.adapter",
        "qr_scan_for_bot_info",
    ),
    "WecomCallbackAdapter": (
        "plugins.platforms.wecom.callback",
        "WecomCallbackAdapter",
    ),
    "check_wecom_callback_requirements": (
        "plugins.platforms.wecom.callback",
        "check_wecom_callback_requirements",
    ),
    "WXBizMsgCrypt": ("plugins.platforms.wecom.crypto", "WXBizMsgCrypt"),
    "PKCS7Encoder": ("plugins.platforms.wecom.crypto", "PKCS7Encoder"),
    "WeComCryptoError": (
        "plugins.platforms.wecom.crypto",
        "WeComCryptoError",
    ),
    "SignatureError": ("plugins.platforms.wecom.crypto", "SignatureError"),
    "DecryptError": ("plugins.platforms.wecom.crypto", "DecryptError"),
    "EncryptError": ("plugins.platforms.wecom.crypto", "EncryptError"),
}


def __getattr__(name: str) -> Any:
    """PEP 562 lazy attribute lookup — defer submodule import until use."""
    target = _LAZY_EXPORTS.get(name)
    if target is None:
        raise AttributeError(
            f"module 'plugins.platforms.wecom' has no attribute {name!r}"
        )
    import importlib

    module = importlib.import_module(target[0])
    value = getattr(module, target[1])
    globals()[name] = value  # cache for subsequent lookups
    return value


def register(ctx) -> None:
    """Bundled-plugin registration entry point.

    Registers both transport modes (``wecom`` AI-bot WebSocket and
    ``wecom_callback`` self-built HTTP callback) against the gateway.
    """
    from plugins.platforms.wecom.adapter import (
        WeComAdapter,
        check_wecom_requirements,
    )
    from plugins.platforms.wecom.callback import (
        WecomCallbackAdapter,
        check_wecom_callback_requirements,
    )

    ctx.register_platform(
        key="wecom",
        label="WeCom (Enterprise WeChat)",
        platform_enum_member="WECOM",
        adapter_factory=lambda config: WeComAdapter(config),
        requirements_check=check_wecom_requirements,
        cron_deliver_env_var="WECOM_HOME_CHANNEL",
        env_enablement_fn=_wecom_env_enablement,
        apply_yaml_config_fn=_wecom_apply_yaml_config,
    )

    ctx.register_platform(
        key="wecom_callback",
        label="WeCom Callback (self-built)",
        platform_enum_member="WECOM_CALLBACK",
        adapter_factory=lambda config: WecomCallbackAdapter(config),
        requirements_check=check_wecom_callback_requirements,
        cron_deliver_env_var="WECOM_CALLBACK_HOME_CHANNEL",
    )


def _wecom_env_enablement():
    """Seed ``PlatformConfig.extra`` from WECOM_* env vars (pre-construction)."""
    import os

    extra: dict = {}
    if os.getenv("WECOM_BOT_ID"):
        extra["bot_id"] = os.getenv("WECOM_BOT_ID")
    if os.getenv("WECOM_SECRET"):
        extra["secret"] = os.getenv("WECOM_SECRET")
    if os.getenv("WECOM_WEBSOCKET_URL"):
        extra["websocket_url"] = os.getenv("WECOM_WEBSOCKET_URL")
    if os.getenv("WECOM_DM_POLICY"):
        extra["dm_policy"] = os.getenv("WECOM_DM_POLICY")
    if os.getenv("WECOM_ALLOWED_USERS"):
        extra["allow_from"] = os.getenv("WECOM_ALLOWED_USERS")
    if os.getenv("WECOM_GROUP_POLICY"):
        extra["group_policy"] = os.getenv("WECOM_GROUP_POLICY")
    home_channel = os.getenv("WECOM_HOME_CHANNEL")
    return {"extra": extra, "home_channel": home_channel}


def _wecom_apply_yaml_config(yaml_cfg, platform_cfg):
    """Translate wecom ``config.yaml`` keys into ``extra`` / env."""
    import os

    extra: dict = {}
    extra_section = yaml_cfg.get("extra") if isinstance(yaml_cfg, dict) else None
    if isinstance(extra_section, dict):
        if extra_section.get("bot_id"):
            extra["bot_id"] = extra_section["bot_id"]
            if not os.getenv("WECOM_BOT_ID"):
                os.environ["WECOM_BOT_ID"] = str(extra_section["bot_id"])
        if extra_section.get("secret"):
            extra["secret"] = extra_section["secret"]
            if not os.getenv("WECOM_SECRET"):
                os.environ["WECOM_SECRET"] = str(extra_section["secret"])
        for k, v in extra_section.items():
            if k not in {"bot_id", "secret"} and v is not None:
                extra[k] = v
    return extra