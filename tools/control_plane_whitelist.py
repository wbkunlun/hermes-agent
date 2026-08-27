"""Control-plane dynamic whitelist (fork).

Pulls the sandbox whitelist (commands + users) from the control plane
(``GET ${CONTROL_PLANE_URL}/api/v1/agent/whitelist``) using the same
credential pair as audit reporting. ``CONTROL_PLANE_AUTH`` already carries
the ``Bearer `` prefix and is passed through verbatim — NEVER log its value.

Semantics (spec: wehermes docs/superpowers/specs/2026-08-27-platform-dynamic-whitelist-design.md):

* disabled — either env unset: consumers ignore this module entirely and
  keep their existing env/config behavior (zero behavior change).
* enabled — the platform lists REPLACE the env lists:
  - fetch succeeds   → fresh lists; an empty list = that class unrestricted
  - fetch fails      → last cached lists (memory, then /opt/data JSON)
  - no data at all   → deny everything (fail-closed) until first success
"""

from __future__ import annotations

import asyncio
import fnmatch
import json
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple

import httpx

logger = logging.getLogger(__name__)

DEFAULT_CACHE_PATH = Path("/opt/data/whitelist-cache.json")
_FETCH_TIMEOUT_S = 10.0
_MAX_ENTRIES = 200
_MAX_ENTRY_LEN = 512
_POLL_INTERVAL_S = 30.0
_BOOT_BACKOFF_S = (1.0, 5.0, 15.0)
_AUTH_ALERT_INTERVAL_S = 300.0


@dataclass(frozen=True)
class WhitelistSnapshot:
    """Immutable decision snapshot; swapped atomically by refresh()."""

    commands: Tuple[str, ...]
    users: Tuple[str, ...]
    updated_at: Optional[str]
    fetched_at: float


def _clean_list(raw) -> Optional[Tuple[str, ...]]:
    """Validate/normalize a platform list field. None = payload invalid."""
    if not isinstance(raw, list):
        return None
    seen = []
    for item in raw:
        if not isinstance(item, str):
            continue
        item = item.strip()
        if item and len(item) <= _MAX_ENTRY_LEN and item not in seen:
            seen.append(item)
        if len(seen) >= _MAX_ENTRIES:
            break
    return tuple(seen)


class WhitelistClient:
    """Shared singleton client; decisions are pure in-memory comparisons."""

    def __init__(self, *, url: str, auth: str, cache_path=DEFAULT_CACHE_PATH):
        self._url = url
        self._auth = auth
        self._cache_path = Path(cache_path)
        self._snapshot: Optional[WhitelistSnapshot] = None
        self._last_auth_alert = 0.0
        self._load_disk_cache()

    @property
    def snapshot(self) -> Optional[WhitelistSnapshot]:
        return self._snapshot

    # ---- decisions ------------------------------------------------------

    def user_allowed(self, sender_id: str = "", sender_name: str = "") -> bool:
        """Empty users = allow all; no snapshot = deny (fail-closed)."""
        snap = self._snapshot
        if snap is None:
            return False
        if not snap.users:
            return True
        return sender_id in snap.users or sender_name in snap.users

    def group_allowed(self, *, chat_id: str = "", chat_name: str = "") -> bool:
        """Same shared users list as DM; matches name or FULL chat id."""
        snap = self._snapshot
        if snap is None:
            return False
        if not snap.users:
            return True
        return chat_id in snap.users or chat_name in snap.users

    def command_gate(self, command: str) -> str:
        """Four-state verdict for approval.py: 'deny' | 'bypass' | 'normal'.

        'deny'    — hard block (no cached data, or non-empty list miss)
        'bypass'  — non-empty list hit, skip detection
        'normal'  — list empty, treat as unconfigured (normal pipeline)
        Callers handle the disabled case themselves via
        get_platform_whitelist() returning None.
        """
        snap = self._snapshot
        if snap is None:
            return "deny"
        if not snap.commands:
            return "normal"
        # Reuse approval.py's deobfuscation + segmenting so quoting tricks
        # and chained tails cannot ride in on an allowed first program.
        from tools.approval import (
            _REDIRECT_AMP_MASK,
            _REDIRECT_AMP_RE,
            _command_detection_variants,
            _iter_top_level_shell_segments,
            _shell_segment_tokens,
        )

        for variant in _command_detection_variants(command):
            masked = _REDIRECT_AMP_RE.sub(_REDIRECT_AMP_MASK, variant)
            segments = [
                s for s in (
                    seg.replace(_REDIRECT_AMP_MASK, "&").strip()
                    for seg in _iter_top_level_shell_segments(masked)
                )
                if s
            ]
            if segments and all(
                _platform_segment_allowed(seg, snap.commands, _shell_segment_tokens)
                for seg in segments
            ):
                return "bypass"
        return "deny"

    # ---- fetch -----------------------------------------------------------

    async def refresh(self) -> bool:
        """Fetch once (3 attempts on transient errors). Never raises.

        True = snapshot updated. Any failure keeps the previous snapshot.
        """
        delays = (0.5, 1.0)
        last_error = ""
        for attempt in range(3):
            transient = False
            try:
                async with httpx.AsyncClient() as client:
                    response = await client.get(
                        self._url,
                        headers={"Authorization": self._auth},
                        timeout=_FETCH_TIMEOUT_S,
                    )
            except httpx.HTTPError as exc:
                last_error = f"network error: {type(exc).__name__}"
                transient = True
            else:
                if response.status_code == 200:
                    return self._install_from_payload(response)
                if response.status_code in (401, 403):
                    self._alert_auth_problem(response.status_code)
                    return False  # credential problem: retrying cannot help
                if 500 <= response.status_code < 600:
                    last_error = f"HTTP {response.status_code}"
                    transient = True
                else:
                    logger.warning(
                        "control-plane whitelist fetch unexpected HTTP %s; keeping cache",
                        response.status_code,
                    )
                    return False
            if transient and attempt < 2:
                await asyncio.sleep(delays[attempt])
        logger.warning(
            "control-plane whitelist fetch failed after retries (%s); keeping cache",
            last_error,
        )
        return False

    def _install_from_payload(self, response) -> bool:
        """Validate the envelope, then swap in the new snapshot + persist.

        Any invalid shape keeps the previous snapshot (fail-safe).
        """
        try:
            payload = response.json()
        except ValueError:
            logger.warning("control-plane whitelist: non-JSON body; keeping cache")
            return False
        if not isinstance(payload, dict) or payload.get("success") is not True:
            logger.warning("control-plane whitelist: invalid envelope; keeping cache")
            return False
        data = payload.get("data")
        if not isinstance(data, dict):
            logger.warning("control-plane whitelist: invalid data field; keeping cache")
            return False
        commands = _clean_list(data.get("commands"))
        users = _clean_list(data.get("users"))
        if commands is None or users is None:
            logger.warning("control-plane whitelist: invalid list fields; keeping cache")
            return False
        updated_at = data.get("updated_at")
        self._snapshot = WhitelistSnapshot(
            commands=commands,
            users=users,
            updated_at=updated_at if isinstance(updated_at, str) else None,
            fetched_at=time.time(),
        )
        self._persist()
        return True

    def _alert_auth_problem(self, status_code: int) -> None:
        """401/403: rate-limited operator alert. 401 means the sandbox JWT
        expired — it is signed once at deploy time, so only a redeploy
        fixes it; meanwhile the cached whitelist keeps serving."""
        now = time.time()
        if now - self._last_auth_alert < _AUTH_ALERT_INTERVAL_S:
            return
        self._last_auth_alert = now
        logger.warning(
            "control-plane whitelist auth rejected (HTTP %d) — keeping cached "
            "whitelist. A 401 here usually means the sandbox JWT expired; it "
            "is only refreshed by redeploying the sandbox.",
            status_code,
        )

    # ---- cache persistence ----------------------------------------------

    def _persist(self) -> None:
        """Write the snapshot atomically (tmp + rename). Non-fatal."""
        snap = self._snapshot
        if snap is None:
            return
        try:
            self._cache_path.parent.mkdir(parents=True, exist_ok=True)
            record = {
                "commands": list(snap.commands),
                "users": list(snap.users),
                "updated_at": snap.updated_at,
                "fetched_at": snap.fetched_at,
            }
            tmp = self._cache_path.with_name(self._cache_path.name + ".tmp")
            tmp.write_text(json.dumps(record, ensure_ascii=False), encoding="utf-8")
            tmp.replace(self._cache_path)
        except OSError as exc:
            logger.warning("control-plane whitelist cache write failed: %s", exc)

    def _load_disk_cache(self) -> None:
        """Best-effort boot fallback when the platform is unreachable.
        Any anomaly (missing/corrupt/invalid shape) is ignored silently —
        no snapshot means fail-closed, which is the safe default."""
        try:
            raw = json.loads(self._cache_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return
        if not isinstance(raw, dict):
            return
        commands = _clean_list(raw.get("commands"))
        users = _clean_list(raw.get("users"))
        fetched_at = raw.get("fetched_at")
        if commands is None or users is None or not isinstance(fetched_at, (int, float)):
            return
        updated_at = raw.get("updated_at")
        self._snapshot = WhitelistSnapshot(
            commands=commands,
            users=users,
            updated_at=updated_at if isinstance(updated_at, str) else None,
            fetched_at=float(fetched_at),
        )
        logger.info(
            "control-plane whitelist: loaded disk cache (%d commands, %d users)",
            len(commands), len(users),
        )


def _platform_segment_allowed(segment: str, commands: Tuple[str, ...], tokenizer) -> bool:
    """Contract semantics: fnmatch (case-sensitive) over the whole segment.

    Unlike the env allowlist, a bare name matches ONLY itself (write
    ``ls*`` to allow arguments). Substitution / malformed quoting fail
    closed — a payload we cannot statically decompose is never matched.
    """
    tokens = tokenizer(segment, 0)
    if not tokens:
        return False
    if "$(" in segment or "`" in segment or "<(" in segment or ">(" in segment:
        return False
    candidate = " ".join(tokens)
    return any(fnmatch.fnmatchcase(candidate, pattern) for pattern in commands)


# ---- singleton -----------------------------------------------------------

_client: Optional[WhitelistClient] = None
_client_resolved = False


def get_platform_whitelist() -> Optional[WhitelistClient]:
    """Return the shared client, or None when the feature is disabled.

    None means consumers must use their existing env/config paths.
    """
    global _client, _client_resolved
    if _client_resolved:
        return _client
    _client_resolved = True
    base = (os.environ.get("CONTROL_PLANE_URL") or "").strip().rstrip("/")
    auth = (os.environ.get("CONTROL_PLANE_AUTH") or "").strip()
    if not base or not auth:
        return None
    _client = WhitelistClient(url=f"{base}/api/v1/agent/whitelist", auth=auth)
    return _client


def _reset_for_tests() -> None:
    global _client, _client_resolved
    _client = None
    _client_resolved = False
