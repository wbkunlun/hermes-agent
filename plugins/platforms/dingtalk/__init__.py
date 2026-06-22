"""dingtalk platform plugin — bundled-plugin entry point.

Thin wrapper that re-exports the canonical adapter from
``gateway.platforms.dingtalk`` (where every fork-specific production
fix lives verbatim) and registers it against the gateway via
``register(ctx)``.

PEP 562 lazy attribute lookup avoids pulling optional deps
(``aiohttp``, ``httpx``, ``slack-sdk``, ``matrix-client``, ...) at
``import plugins.platforms.dingtalk`` time.
"""

from __future__ import annotations

from typing import Any, TYPE_CHECKING

__all__ = [
    "DingTalkAdapter",
    "check_dingtalk_requirements",
    
    "register",
]

if TYPE_CHECKING:  # pragma: no cover — only for type checkers
    from gateway.platforms.dingtalk import DingTalkAdapter, check_dingtalk_requirements

_LAZY_EXPORTS = {
    "DingTalkAdapter": ("gateway.platforms.dingtalk", "DingTalkAdapter"),
    "check_dingtalk_requirements": ("gateway.platforms.dingtalk", "check_dingtalk_requirements"),
}


def __getattr__(name: str) -> Any:
    """PEP 562 lazy attribute lookup."""
    target = _LAZY_EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module 'plugins.platforms.dingtalk' has no attribute {name!r}")
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
    from gateway.platforms.dingtalk import DingTalkAdapter, check_dingtalk_requirements
    ctx.register_platform(
        key="dingtalk",
        label="Dingtalk",
        platform_enum_member="DINGTALK",
        adapter_factory=lambda config: DingTalkAdapter(config),
        requirements_check=check_dingtalk_requirements,
    )
