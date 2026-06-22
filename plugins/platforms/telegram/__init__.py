"""telegram platform plugin — bundled-plugin entry point.

Thin wrapper that re-exports the canonical adapter from
``gateway.platforms.telegram`` (where every fork-specific production
fix lives verbatim) and registers it against the gateway via
``register(ctx)``.

PEP 562 lazy attribute lookup avoids pulling optional deps
(``aiohttp``, ``httpx``, ``slack-sdk``, ``matrix-client``, ...) at
``import plugins.platforms.telegram`` time.
"""

from __future__ import annotations

from typing import Any, TYPE_CHECKING

__all__ = [
    "TelegramAdapter",
    "check_telegram_requirements",
    "telegram_network"
    "register",
]

if TYPE_CHECKING:  # pragma: no cover — only for type checkers
    from gateway.platforms.telegram import TelegramAdapter, check_telegram_requirements
    from gateway.platforms.telegram import telegram_network  # type: ignore

_LAZY_EXPORTS = {
    "TelegramAdapter": ("gateway.platforms.telegram", "TelegramAdapter"),
    "check_telegram_requirements": ("gateway.platforms.telegram", "check_telegram_requirements"),
    "telegram_network": ("gateway.platforms.telegram_network", "*"),
}


def __getattr__(name: str) -> Any:
    """PEP 562 lazy attribute lookup."""
    target = _LAZY_EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module 'plugins.platforms.telegram' has no attribute {name!r}")
    module_path, attr_name = target
    import importlib
    if attr_name == "*":
        # Direct submodule import (e.g. gateway.platforms.feishu_comment).
        # This bypasses the gateway.platforms package __getattr__ (which
        # only exposes the canonical adapter classes, not helper modules).
        value = importlib.import_module(module_path)
    else:
        module = importlib.import_module(module_path)
        value = getattr(module, attr_name)
    globals()[name] = value
    return value


def register(ctx) -> None:
    """Bundled-plugin registration entry point."""
    from gateway.platforms.telegram import TelegramAdapter, check_telegram_requirements
    ctx.register_platform(
        key="telegram",
        label="Telegram",
        platform_enum_member="TELEGRAM",
        adapter_factory=lambda config: TelegramAdapter(config),
        requirements_check=check_telegram_requirements,
    )
