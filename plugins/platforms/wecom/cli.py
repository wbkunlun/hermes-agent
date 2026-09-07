"""Subprocess bridge to the official ``wecom-cli`` (WeCom business capabilities).

The WeCom team ships https://github.com/WecomTeam/wecom-cli — a Rust+Node CLI
covering messaging, mail, docs, sheets, smartsheets, calendar, meetings,
todos, WeDrive and contacts.  It has NO MCP server mode, so this module wraps
it as a typed subprocess boundary used by the plugin's agent tools
(``tools.py``).

Contract relied on here (wecom-cli docs/cli-reference.md):

* stdout is compact JSON; logs go to stderr only
* exit 0 success / 1 runtime error (stdout carries ``{"error": ...}``) /
  2 usage error
* ``auth show --status`` prints a single ``authorized``/``unauthorized`` line

Security: argv is built from validated segments — never a shell string.  The
subprocess env carries only PATH/HOME/WECOM_CLI_CONFIG_DIR, never the gateway
process's secrets.  Credentials live encrypted under the config dir (0600,
``credentials.enc``), which defaults to ``$HERMES_HOME/wecom-cli`` so they
persist on the Hermes Home volume across restarts.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

# Each path segment must look like a CLI service name.  This blocks anything
# shell-shaped or path-shaped from ever reaching argv, independent of the
# (server-discovered, evolving) service catalog.
_SERVICE_SEGMENT_RE = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")

DEFAULT_TIMEOUT_SECONDS = 60.0


class WecomCliError(RuntimeError):
    """wecom-cli exited non-zero or produced unusable output."""

    def __init__(self, message: str, *, stdout: str = "", stderr: str = ""):
        super().__init__(message)
        self.stdout = stdout
        self.stderr = stderr


def cli_bin() -> str:
    return os.getenv("WECOM_CLI_BIN", "wecom-cli")


def config_dir() -> Path:
    """Credentials dir — ``WECOM_CLI_CONFIG_DIR`` (image ENV) wins, else
    ``$HERMES_HOME/wecom-cli`` so a manual ``docker exec wecom-cli auth init``
    and the gateway subprocess see the same credentials."""
    override = os.getenv("WECOM_CLI_CONFIG_DIR", "").strip()
    if override:
        return Path(override)
    home = os.getenv("HERMES_HOME") or str(Path.home() / ".hermes")
    return Path(home) / "wecom-cli"


def subprocess_env() -> Dict[str, str]:
    try:
        config_dir().mkdir(parents=True, exist_ok=True)
    except OSError:
        pass
    return {
        "PATH": os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin"),
        "HOME": os.environ.get("HOME", "/tmp"),
        "WECOM_CLI_CONFIG_DIR": str(config_dir()),
    }


def validate_segments(service_path: Sequence[str]) -> List[str]:
    if not service_path:
        raise ValueError("service_path must not be empty")
    segments = [str(s).strip() for s in service_path]
    for segment in segments:
        if not _SERVICE_SEGMENT_RE.match(segment):
            raise ValueError(
                f"invalid service path segment {segment!r}: must match [a-z][a-z0-9_-]*"
            )
    return segments


def run_cli(
    service_path: Sequence[str],
    method: Optional[str] = None,
    args: Optional[Dict[str, Any]] = None,
    *,
    timeout: Optional[float] = None,
    extra_argv: Optional[List[str]] = None,
    bare: bool = False,
) -> str:
    """Run ``wecom-cli <service...> [method] [--json ARGS]``; return stdout.

    ``bare=True`` allows an empty *service_path* for global flags such as
    ``--version``.  Raises :class:`WecomCliError` on non-zero exit / timeout /
    missing binary; the CLI's structured ``{"error": ...}`` JSON (exit 1) is
    preserved verbatim in the exception message for the model to read.
    """
    argv = [cli_bin()] + ([] if bare else validate_segments(service_path))
    if method is not None:
        argv.append(method)
    if extra_argv:
        argv.extend(extra_argv)
    if args is not None:
        argv.extend(["--json", json.dumps(args, ensure_ascii=False)])

    if timeout is None:
        timeout = float(os.getenv("HERMES_WECOM_CLI_TIMEOUT", DEFAULT_TIMEOUT_SECONDS))
    try:
        proc = subprocess.run(  # noqa: S603 — argv list, no shell
            argv, capture_output=True, text=True, timeout=timeout,
            env=subprocess_env(),
        )
    except FileNotFoundError as exc:
        raise WecomCliError(f"wecom-cli binary not found: {cli_bin()}", stderr=str(exc)) from exc
    except subprocess.TimeoutExpired as exc:
        raise WecomCliError(f"wecom-cli timed out after {timeout}s: {' '.join(argv[:4])}") from exc

    stdout = (proc.stdout or "").strip()
    stderr = (proc.stderr or "").strip()
    if proc.returncode != 0:
        raise WecomCliError(
            f"wecom-cli exited {proc.returncode}: {stdout or stderr or 'no output'}",
            stdout=stdout,
            stderr=stderr,
        )
    return stdout
