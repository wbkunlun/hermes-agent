"""whatsapp platform plugin — bundled-plugin entry point.

Thin wrapper that re-exports the canonical adapter from
``gateway.platforms.whatsapp`` (where every fork-specific production
fix lives verbatim) and registers it against the gateway via
``register(ctx)``.

PEP 562 lazy attribute lookup avoids pulling optional deps
(``aiohttp``, ``httpx``, ``slack-sdk``, ``matrix-client``, ...) at
``import plugins.platforms.whatsapp`` time.
"""

from __future__ import annotations

from typing import Any, TYPE_CHECKING

__all__ = [
    "WhatsAppAdapter",
    "check_whatsapp_requirements",
    
    "register",
]

if TYPE_CHECKING:  # pragma: no cover — only for type checkers
    from gateway.platforms.whatsapp import WhatsAppAdapter, check_whatsapp_requirements

_LAZY_EXPORTS = {
    "WhatsAppAdapter": ("gateway.platforms.whatsapp", "WhatsAppAdapter"),
    "check_whatsapp_requirements": ("gateway.platforms.whatsapp", "check_whatsapp_requirements"),
}


def __getattr__(name: str) -> Any:
    """PEP 562 lazy attribute lookup."""
    target = _LAZY_EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module 'plugins.platforms.whatsapp' has no attribute {name!r}")
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
    from gateway.platforms.whatsapp import WhatsAppAdapter, check_whatsapp_requirements
    ctx.register_platform(
        key="whatsapp",
        label="Whatsapp",
        platform_enum_member="WHATSAPP",
        adapter_factory=lambda config: WhatsAppAdapter(config),
        requirements_check=check_whatsapp_requirements,
    )
