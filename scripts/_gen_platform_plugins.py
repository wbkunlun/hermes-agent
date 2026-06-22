#!/usr/bin/env python3
"""_gen_platform_plugins.py — Generate plugins/platforms/<name>/ for fork adapters.

Reads the canonical adapter class / requirement check / enum value from
each ``gateway/platforms/<name>.py`` and emits a thin bundled plugin
with:

  - ``__init__.py``     PEP 562 lazy import + ``register(ctx)`` hook
  - ``adapter.py``       thin re-export from ``gateway.platforms.<name>``
  - ``plugin.yaml``     manifest (kind: platform, label, requires_env)

Run once after each upstream merge. Idempotent (overwrites existing
plugin dirs by default; pass ``--check`` to only report missing ones).

Usage::

    python scripts/_gen_platform_plugins.py                  # generate all
    python scripts/_gen_platform_plugins.py --check          # dry-run
    python scripts/_gen_platform_plugins.py dingtalk feishu  # specific only
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
GW_PLATFORMS = REPO_ROOT / "gateway" / "platforms"
PLUGIN_PLATFORMS = REPO_ROOT / "plugins" / "platforms"

# 9 standalone fork adapters (the helper files feishu_comment* and
# telegram_network are imported by the main adapter and re-exported
# transitively, so they don't need their own plugin slot).
ADAPTERS = [
    ("dingtalk", "DingTalk", "DingTalk (DingTalk)", "dingtalk_bot"),
    ("email", "Email", "Email (SMTP/IMAP)", "smtp_host"),
    ("feishu", "Feishu", "Feishu (飞书)", "feishu_app_id"),
    ("matrix", "Matrix", "Matrix (Element)", "matrix_homeserver"),
    ("slack", "Slack", "Slack", "slack_bot_token"),
    ("sms", "SMS", "SMS (Twilio)", "twilio_account_sid"),
    ("telegram", "Telegram", "Telegram", "telegram_bot_token"),
    ("whatsapp", "WhatsApp", "WhatsApp (Baileys)", ""),
]

HELPER_MODULES = {
    # main adapter → list of helper modules under gateway.platforms
    "feishu": ["feishu_comment", "feishu_comment_rules", "feishu_meeting_invite"],
    "telegram": ["telegram_network"],
}

REQUIRED_ENV: dict[str, list[dict]] = {
    "dingtalk": [
        {"name": "DINGTALK_BOT_TOKEN", "description": "DingTalk AI Bot token", "password": True},
    ],
    "email": [
        {"name": "EMAIL_SMTP_HOST", "description": "SMTP host", "password": False},
        {"name": "EMAIL_SMTP_USER", "description": "SMTP user", "password": False},
        {"name": "EMAIL_SMTP_PASS", "description": "SMTP password", "password": True},
    ],
    "feishu": [
        {"name": "FEISHU_APP_ID", "description": "Feishu app ID", "password": False},
        {"name": "FEISHU_APP_SECRET", "description": "Feishu app secret", "password": True},
    ],
    "matrix": [
        {"name": "MATRIX_HOMESERVER", "description": "Matrix homeserver URL", "password": False},
        {"name": "MATRIX_ACCESS_TOKEN", "description": "Matrix access token", "password": True},
    ],
    "slack": [
        {"name": "SLACK_BOT_TOKEN", "description": "Slack bot OAuth token (xoxb-...)", "password": True},
        {"name": "SLACK_ALLOWED_USERS", "description": "Comma-separated allowlist", "password": False},
    ],
    "sms": [
        {"name": "TWILIO_ACCOUNT_SID", "description": "Twilio account SID", "password": False},
        {"name": "TWILIO_AUTH_TOKEN", "description": "Twilio auth token", "password": True},
        {"name": "TWILIO_FROM_NUMBER", "description": "Twilio sender number", "password": False},
    ],
    "telegram": [
        {"name": "TELEGRAM_BOT_TOKEN", "description": "Telegram bot token from @BotFather", "password": True},
        {"name": "TELEGRAM_ALLOWED_USERS", "description": "Comma-separated allowlist", "password": False},
    ],
    "whatsapp": [],
}


def parse_class_and_check(adapter_path: Path) -> tuple[str | None, str | None, str | None]:
    """Return (class_name, requirements_check_fn, platform_enum_member)."""
    src = adapter_path.read_text(encoding="utf-8", errors="replace")
    cls_match = re.search(r"^class (\w+Adapter)", src, re.M)
    req_match = re.search(r"^def (check_\w+_requirements)\(", src, re.M)
    enum_match = re.search(r"Platform\.(\w+)", src)
    return (
        cls_match.group(1) if cls_match else None,
        req_match.group(1) if req_match else None,
        enum_match.group(1) if enum_match else None,
    )


def render_init_py(name: str, class_name: str, req_fn: str, enum: str, helpers: list[str]) -> str:
    """Render plugins/platforms/<name>/__init__.py."""
    helpers_quoted = ", ".join(f'"{h}"' for h in helpers)
    helpers_lazy = "".join(
        f'\n    "{h}": ("gateway.platforms.{h}", "*"),'
        for h in helpers
    )
    helpers_typecheck_block = ""
    if helpers:
        helpers_typecheck_imports = ", ".join(helpers)
        helpers_typecheck_block = (
            f"    from gateway.platforms.{name} import {helpers_typecheck_imports}  # type: ignore\n"
        )
    return f'''"""{name} platform plugin — bundled-plugin entry point.

Thin wrapper that re-exports the canonical adapter from
``gateway.platforms.{name}`` (where every fork-specific production
fix lives verbatim) and registers it against the gateway via
``register(ctx)``.

PEP 562 lazy attribute lookup avoids pulling optional deps
(``aiohttp``, ``httpx``, ``slack-sdk``, ``matrix-client``, ...) at
``import plugins.platforms.{name}`` time.
"""

from __future__ import annotations

from typing import Any, TYPE_CHECKING

__all__ = [
    "{class_name}",
    "check_{name}_requirements",
    {helpers_quoted}
    "register",
]

if TYPE_CHECKING:  # pragma: no cover — only for type checkers
    from gateway.platforms.{name} import {class_name}, check_{name}_requirements
{helpers_typecheck_block}
_LAZY_EXPORTS = {{
    "{class_name}": ("gateway.platforms.{name}", "{class_name}"),
    "check_{name}_requirements": ("gateway.platforms.{name}", "check_{name}_requirements"),{helpers_lazy}
}}


def __getattr__(name: str) -> Any:
    """PEP 562 lazy attribute lookup."""
    target = _LAZY_EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module 'plugins.platforms.{name}' has no attribute {{name!r}}")
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
    from gateway.platforms.{name} import {class_name}, check_{name}_requirements
    ctx.register_platform(
        key="{name}",
        label="{name.capitalize()}",
        platform_enum_member="{enum}",
        adapter_factory=lambda config: {class_name}(config),
        requirements_check=check_{name}_requirements,
    )
'''


def render_adapter_py(name: str, helpers: list[str]) -> str:
    """Render plugins/platforms/<name>/adapter.py as thin re-export."""
    helper_imports = "\n".join(
        f"from gateway.platforms.{h} import *  # noqa: F401,F403\n"
        for h in helpers
    )
    return f'''"""{name} adapter — thin re-export wrapper.

Full implementation lives in ``gateway/platforms/{name}.py`` so all
fork-specific production fixes are preserved verbatim. This module
exists so the bundled-plugin slot at
``plugins/platforms/{name}/adapter.py`` has a real module to point at
when ``register(ctx)`` is called.
"""

from __future__ import annotations

from gateway.platforms.{name} import *  # noqa: F401,F403
{helper_imports}
'''


def render_plugin_yaml(name: str, label: str, requires_env: list[dict]) -> str:
    """Render plugins/platforms/<name>/plugin.yaml."""
    env_lines = []
    for e in requires_env:
        env_lines.append(f"  - name: {e['name']}")
        env_lines.append(f"    description: \"{e['description']}\"")
        env_lines.append(f"    prompt: \"{e['description']}\"")
        env_lines.append(f"    password: {'true' if e.get('password') else 'false'}")
    env_block = "\n".join(env_lines) if env_lines else "  []"
    return f'''name: {name}-platform
label: {label}
kind: platform
version: 1.0.0
description: >
  {label} platform adapter — bundled-plugin thin wrapper.

  Full implementation lives in ``gateway/platforms/{name}.py`` so all
  fork-specific production fixes are preserved verbatim.
author: wbkunlun (fork), NousResearch (upstream)
requires_env:
{env_block}
'''


def gen_for(name: str, label: str, _: str, dry_run: bool) -> bool:
    """Generate plugins/platforms/<name>/{__init__.py, adapter.py, plugin.yaml}."""
    src_path = GW_PLATFORMS / f"{name}.py"
    if not src_path.exists():
        print(f"  ✗ {name}: gateway/platforms/{name}.py missing — skipped", file=sys.stderr)
        return False
    class_name, req_fn, enum = parse_class_and_check(src_path)
    if not class_name or not req_fn or not enum:
        print(f"  ✗ {name}: could not parse class/req/enum from {src_path}", file=sys.stderr)
        return False

    helpers = HELPER_MODULES.get(name, [])
    plugin_dir = PLUGIN_PLATFORMS / name
    if dry_run:
        exists = "exists" if plugin_dir.exists() else "missing"
        print(f"  ✓ {name}: class={class_name} req={req_fn} enum={enum} helpers={helpers} ({exists})")
        return True

    plugin_dir.mkdir(parents=True, exist_ok=True)
    (plugin_dir / "__init__.py").write_text(
        render_init_py(name, class_name, req_fn, enum, helpers), encoding="utf-8",
    )
    (plugin_dir / "adapter.py").write_text(
        render_adapter_py(name, helpers), encoding="utf-8",
    )
    (plugin_dir / "plugin.yaml").write_text(
        render_plugin_yaml(name, label, REQUIRED_ENV.get(name, [])), encoding="utf-8",
    )
    print(f"  ✓ {name}: generated at {plugin_dir.relative_to(REPO_ROOT)}")
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("names", nargs="*", help="Adapter names to generate (default: all)")
    parser.add_argument("--check", action="store_true", help="Dry-run report only")
    args = parser.parse_args()

    if args.names:
        targets = [(n, n.capitalize(), "", "") for n in args.names]
    else:
        targets = ADAPTERS

    ok = 0
    for name, label, _, _ in targets:
        if gen_for(name, label, "", args.check):
            ok += 1

    print(f"\n{'checked' if args.check else 'generated'}: {ok}/{len(targets)}")
    return 0 if ok == len(targets) else 1


if __name__ == "__main__":
    sys.exit(main())