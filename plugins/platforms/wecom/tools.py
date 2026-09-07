"""WeCom business agent tools — thin wrappers over the official ``wecom-cli``.

Mirrors the a2a deferred client-tools pattern: ``plugin.yaml`` declares
``provides_tools`` so PluginManager imports this module and calls
``register_tools(ctx)``; the wecom ``register(ctx)`` in ``adapter.py`` calls
it too when the platform materialises.  Every tool lands in toolset
``"wecom"`` and therefore auto-joins the ``hermes-wecom`` /
``hermes-wecom-callback`` toolsets (tools/toolsets.py ``resolve_toolset``) —
zero core edits.

Tool surface (16):
  base    — wecom_cli_status / wecom_cli_schema / wecom_cli (generic passthrough)
  curated — 13 high-frequency conveniences (docs, sheets, calendar, todo,
            mail, contact search, message push)

Curated tools forward their ``args`` object verbatim as the CLI ``--json``
payload; exact keys are discovered via ``wecom_cli_schema`` so a changing
server-side command tree degrades gracefully (model self-corrects using the
schema tool) instead of rotting typed schemas here.
"""

from __future__ import annotations

import asyncio
import json
import shutil
import subprocess
import time
from typing import Any, Dict, List, Optional, Tuple

from plugins.platforms.wecom.cli import WecomCliError, cli_bin, run_cli
from tools.registry import tool_error, tool_result

# ---------------------------------------------------------------- probe

_PROBE_TTL_SECONDS = 60.0
_probe_state: Dict[str, Any] = {"ts": -1e9, "ok": False}


def reset_cli_probe_cache() -> None:
    """Test hook — drop the memoised availability probe."""
    _probe_state.update(ts=-1e9, ok=False)


def cli_tools_available() -> bool:
    """Shared ``check_fn`` for every wecom-cli tool.

    True iff the binary resolves on PATH *and* ``auth show --status`` reports
    ``authorized``.  Memoised at module level (60s) so the registry's
    per-tool check_fn cache — 16 tools would otherwise each spawn a
    subprocess — collapses to ~one probe per minute.  When this returns
    False the tools vanish from the model's definitions (graceful
    degradation on installs without the CLI or before ``auth init``).
    """
    now = time.monotonic()
    if now - _probe_state["ts"] < _PROBE_TTL_SECONDS:
        return bool(_probe_state["ok"])
    _probe_state["ts"] = now
    ok = False
    if shutil.which(cli_bin()):
        try:
            from plugins.platforms.wecom.cli import subprocess_env
            proc = subprocess.run(
                [cli_bin(), "auth", "show", "--status"],
                capture_output=True, text=True, timeout=15,
                env=subprocess_env(),
            )
            ok = proc.returncode == 0 and (proc.stdout or "").strip() == "authorized"
        except Exception:  # noqa: BLE001 — probe must never raise
            ok = False
    _probe_state["ok"] = ok
    return ok


# ---------------------------------------------------------------- helpers

async def _run_and_format(
    service_path: Tuple[str, ...],
    method: Optional[str] = None,
    args: Optional[Dict[str, Any]] = None,
    extra_argv: Optional[List[str]] = None,
    bare: bool = False,
) -> str:
    """Run wecom-cli off the event loop; format result for the model."""
    def _call() -> str:
        return run_cli(service_path, method, args, extra_argv=extra_argv, bare=bare)

    try:
        stdout = await asyncio.to_thread(_call)
    except WecomCliError as exc:
        return tool_error(str(exc))
    except ValueError as exc:
        return tool_error(str(exc))
    if not stdout:
        return tool_result({"ok": True, "result": None})
    try:
        parsed = json.loads(stdout)
    except json.JSONDecodeError:
        return tool_result({"ok": True, "raw": stdout[:4000]})
    if isinstance(parsed, dict) and "error" in parsed:
        return tool_error(json.dumps(parsed, ensure_ascii=False))
    return tool_result(parsed)


# ---------------------------------------------------------------- base tools

async def _handler_status(args: Dict[str, Any]) -> str:
    version = await _run_and_format((), extra_argv=["--version"], bare=True)
    auth = await _run_and_format(("auth",), "show")
    return tool_result({
        "binary": cli_bin(),
        "version": version.strip('"'),
        "auth": auth.strip(),
    })


async def _handler_schema(args: Dict[str, Any]) -> str:
    target = str(args.get("target") or "").strip()
    if target:
        return await _run_and_format(("schema",), "get", extra_argv=[target])
    return await _run_and_format(("schema",), "list")


async def _handler_generic(args: Dict[str, Any]) -> str:
    raw_path = args.get("service_path") or []
    if not isinstance(raw_path, list):
        return tool_error("service_path must be an array of segments")
    method = str(args.get("method") or "").strip() or None
    payload = args.get("args")
    if payload is not None and not isinstance(payload, dict):
        return tool_error("args must be an object")
    return await _run_and_format(tuple(raw_path), method, payload)


# ---------------------------------------------------------------- curated tools

_ARGS_OBJECT_SCHEMA = {
    "type": "object",
    "description": (
        "Arguments forwarded verbatim as the wecom-cli --json payload. "
        "Exact keys: call wecom_cli_schema with the matching target "
        "(e.g. 'doc.create') first if unsure."
    ),
    "additionalProperties": True,
}

CURATED_TOOLS: List[Dict[str, Any]] = [
    {"name": "wecom_doc_create", "cli": ("doc",), "method": "create",
     "description": "Create a WeCom online document (doc create)."},
    {"name": "wecom_doc_read", "cli": ("doc",), "method": "get",
     "description": "Read a WeCom document's content by URL/id (doc get)."},
    {"name": "wecom_doc_append", "cli": ("doc",), "method": "append",
     "description": "Append content to a WeCom document (doc append)."},
    {"name": "wecom_sheet_read", "cli": ("sheet",), "method": "get",
     "description": "Read rows/ranges of a WeCom online spreadsheet (sheet get)."},
    {"name": "wecom_sheet_append", "cli": ("sheet",), "method": "append",
     "description": "Append rows to a WeCom online spreadsheet (sheet append)."},
    {"name": "wecom_calendar_create", "cli": ("calendar",), "method": "create",
     "description": "Create a WeCom calendar event, optionally with attendees / meeting room (calendar create)."},
    {"name": "wecom_calendar_list", "cli": ("calendar",), "method": "list",
     "description": "List upcoming WeCom calendar events / free-busy (calendar list)."},
    {"name": "wecom_todo_create", "cli": ("todo",), "method": "create",
     "description": "Create a WeCom todo item, optionally assign participants (todo create)."},
    {"name": "wecom_todo_complete", "cli": ("todo",), "method": "complete",
     "description": "Mark a WeCom todo item complete (todo complete)."},
    {"name": "wecom_mail_send", "cli": ("mail",), "method": "send",
     "description": "Send (or reply/forward) WeCom email (mail send)."},
    {"name": "wecom_mail_search", "cli": ("mail",), "method": "search",
     "description": "Search WeCom mail and fetch details (mail search)."},
    {"name": "wecom_contact_search", "cli": ("contact",), "method": "search",
     "description": "Search WeCom members by name/pinyin/alias; basic member info (contact search)."},
    {"name": "wecom_message_push", "cli": ("message",), "method": "send",
     "description": "Push a markdown/media message to a chat the bot recently engaged with (message send)."},
]


def _curated_handler(cli_path: Tuple[str, ...], method: str):
    async def _handler(args: Dict[str, Any], **kwargs) -> str:
        payload = args.get("args")
        if payload is not None and not isinstance(payload, dict):
            return tool_error("args must be an object")
        return await _run_and_format(cli_path, method, payload)
    return _handler


_BASE_TOOL_SCHEMAS: Dict[str, Dict[str, Any]] = {
    "wecom_cli_status": {
        "name": "wecom_cli_status",
        "description": "WeCom CLI health check: binary version and auth status (authorized/unauthorized).",
        "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
    },
    "wecom_cli_schema": {
        "name": "wecom_cli_schema",
        "description": (
            "Discover wecom-cli commands: list service methods, or get one method's "
            "argument schema via target like 'doc.create'. Call this before wecom_cli "
            "or a curated tool when unsure about arguments."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "target": {"type": "string", "description": "e.g. 'doc.create' for schema get; empty lists all"},
            },
            "additionalProperties": False,
        },
    },
    "wecom_cli": {
        "name": "wecom_cli",
        "description": (
            "Generic passthrough to the official wecom-cli (13 service domains: "
            "message mail doc sheet smartsheet smartpage calendar meeting todo disk "
            "contact media identity). Prefer curated tools; use this for the long tail."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "service_path": {
                    "type": "array", "items": {"type": "string"},
                    "description": "CLI path segments, e.g. ['message','aibot','sessions']",
                },
                "method": {"type": "string", "description": "Final method segment, e.g. 'list'"},
                "args": _ARGS_OBJECT_SCHEMA,
            },
            "required": ["service_path", "method"],
            "additionalProperties": False,
        },
    },
}


def register_tools(ctx) -> None:
    """Register all 16 wecom-cli business tools into the ``wecom`` toolset."""
    ctx.register_tool(
        name="wecom_cli_status",
        toolset="wecom",
        schema=_BASE_TOOL_SCHEMAS["wecom_cli_status"],
        handler=_handler_status,
        description=_BASE_TOOL_SCHEMAS["wecom_cli_status"]["description"],
        emoji="💼",
        check_fn=cli_tools_available,
        is_async=True,
    )
    ctx.register_tool(
        name="wecom_cli_schema",
        toolset="wecom",
        schema=_BASE_TOOL_SCHEMAS["wecom_cli_schema"],
        handler=_handler_schema,
        description=_BASE_TOOL_SCHEMAS["wecom_cli_schema"]["description"],
        emoji="💼",
        check_fn=cli_tools_available,
        is_async=True,
    )
    ctx.register_tool(
        name="wecom_cli",
        toolset="wecom",
        schema=_BASE_TOOL_SCHEMAS["wecom_cli"],
        handler=_handler_generic,
        description=_BASE_TOOL_SCHEMAS["wecom_cli"]["description"],
        emoji="💼",
        check_fn=cli_tools_available,
        is_async=True,
    )
    for spec in CURATED_TOOLS:
        ctx.register_tool(
            name=spec["name"],
            toolset="wecom",
            schema={
                "name": spec["name"],
                "description": spec["description"],
                "parameters": {
                    "type": "object",
                    "properties": {"args": _ARGS_OBJECT_SCHEMA},
                    "required": ["args"],
                    "additionalProperties": False,
                },
            },
            handler=_curated_handler(spec["cli"], spec["method"]),
            description=spec["description"],
            emoji="💼",
            check_fn=cli_tools_available,
            is_async=True,
        )
