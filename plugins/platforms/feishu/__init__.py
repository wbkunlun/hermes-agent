"""feishu platform plugin — bundled-plugin entry point.

Thin wrapper that re-exports the canonical adapter from
``gateway.platforms.feishu`` (where every fork-specific production
fix lives verbatim) and registers it against the gateway via
``register(ctx)``.

PEP 562 lazy attribute lookup avoids pulling optional deps
(``aiohttp``, ``httpx``, ``slack-sdk``, ``matrix-client``, ...) at
``import plugins.platforms.feishu`` time.
"""

from __future__ import annotations

from typing import Any, TYPE_CHECKING

__all__ = [
    "FeishuAdapter",
    "check_feishu_requirements",
    "feishu_comment", "feishu_comment_rules", "feishu_meeting_invite"
    "register",
]

if TYPE_CHECKING:  # pragma: no cover — only for type checkers
    from gateway.platforms.feishu import FeishuAdapter, check_feishu_requirements
    from gateway.platforms.feishu import feishu_comment, feishu_comment_rules, feishu_meeting_invite  # type: ignore

_LAZY_EXPORTS = {
    "FeishuAdapter": ("gateway.platforms.feishu", "FeishuAdapter"),
    "check_feishu_requirements": ("gateway.platforms.feishu", "check_feishu_requirements"),
    "feishu_comment": ("gateway.platforms.feishu_comment", "*"),
    "feishu_comment_rules": ("gateway.platforms.feishu_comment_rules", "*"),
    "feishu_meeting_invite": ("gateway.platforms.feishu_meeting_invite", "*"),
}


def __getattr__(name: str) -> Any:
    """PEP 562 lazy attribute lookup."""
    target = _LAZY_EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module 'plugins.platforms.feishu' has no attribute {name!r}")
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
    from gateway.platforms.feishu import FeishuAdapter, check_feishu_requirements
    ctx.register_platform(
        key="feishu",
        label="Feishu",
        platform_enum_member="FEISHU",
        adapter_factory=lambda config: FeishuAdapter(config),
        requirements_check=check_feishu_requirements,
    )
