"""Control-plane dynamic whitelist (fork).

Pulls the sandbox whitelist (commands + users) from the control plane
(``GET ${CONTROL_PLANE_URL}/api/v1/agent/whitelist``) using the same
credential pair as audit reporting. ``CONTROL_PLANE_AUTH`` already carries
the ``Bearer `` prefix and is passed through verbatim — NEVER log its value.

Semantics (spec: wehermes docs/superpowers/specs/2026-08-27-platform-dynamic-whitelist-design.md,
updated by the 2026-09-28 work order):

* disabled — either env unset: consumers ignore this module entirely and
  keep their existing env/config behavior (zero behavior change).
* enabled — the platform lists REPLACE the env lists:
  - fetch succeeds   → fresh lists; an empty list = that class unrestricted
  - fetch fails      → last cached lists (memory, then /opt/data JSON); the
                       degraded-state WARNING marks the served data STALE
                       with its cache age
  - no data at all   → DM admission FAILS OPEN with a per-message WARNING
                       naming the sender (fork work order 2026-09-28: a
                       control-plane outage silently dropping private
                       messages costs far more than admitting one DM);
                       group admission and command gating stay fail-closed
"""

from __future__ import annotations

import asyncio
import fnmatch
import json
import logging
import os
import random
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
# Failure backoff ladder (work order 2026-09-28 改动2): 5s → 30s → 2min →
# 5min cap, jittered ±20%, reset on the first success. A flat 30s retry
# against a down control plane produced ~600 failed-fetch log lines in 48h.
_FAILURE_BACKOFF_S = (5.0, 30.0, 120.0, 300.0)
_JITTER_RANGE = (0.8, 1.2)
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
        # Degraded-state tracking (work order 2026-09-28 改动2): failure /
        # recovery log lines are emitted on STATE CHANGE only, not per fetch.
        self._degraded = False
        self._last_error = ""
        self._load_disk_cache()

    @property
    def snapshot(self) -> Optional[WhitelistSnapshot]:
        return self._snapshot

    # ---- degraded-state logging -----------------------------------------

    def _mark_fetch_failure(self, reason: str) -> None:
        """Entering (or staying in) a fetch-failure state logs once."""
        self._last_error = reason
        if self._degraded:
            return
        self._degraded = True
        snap = self._snapshot
        if snap is not None:
            age = max(0.0, time.time() - snap.fetched_at)
            logger.warning(
                "control-plane whitelist unreachable (%s); serving STALE cache "
                "(fetched %.0fs ago, updated_at=%s) — group/command gates keep "
                "using this stale data",
                reason, age, snap.updated_at,
            )
        else:
            logger.warning(
                "control-plane whitelist unreachable (%s) and no cached data — "
                "DM admission fails open until the first successful fetch "
                "(groups and command gating stay fail-closed)",
                reason,
            )

    def _mark_fetch_success(self) -> None:
        """Recovering from a degraded state logs once."""
        if not self._degraded:
            return
        self._degraded = False
        snap = self._snapshot
        logger.info(
            "control-plane whitelist recovered; fresh lists (%d commands, %d users)",
            len(snap.commands) if snap else 0,
            len(snap.users) if snap else 0,
        )

    # ---- decisions ------------------------------------------------------

    def user_allowed(self, sender_id: str = "", sender_name: str = "") -> bool:
        """Empty users = allow all; no snapshot = fail-OPEN for DMs.

        Work order 2026-09-28 改动1: a control-plane outage must not silently
        drop private messages. No snapshot means either "still in the boot
        window" (a bounded startup fetch shrinks that window to seconds) or
        "fetch failing with no cache" — both admit the DM with a WARNING that
        names the sender and the reason, so the operator sees every
        fail-open admission in the logs.
        """
        snap = self._snapshot
        if snap is None:
            reason = (
                f"fetch failing: {self._last_error}" if self._degraded
                else "no successful fetch yet (boot window)"
            )
            logger.warning(
                "whitelist degraded: fail-open admitted dm sender=%r name=%r (%s)",
                sender_id, sender_name, reason,
            )
            return True
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
        # Reuse the approval allowlist leaf's deobfuscation + segmenting so quoting tricks
        # and chained tails cannot ride in on an allowed first program.
        from tools.approval_allowlist import _REDIRECT_AMP_MASK, _REDIRECT_AMP_RE
        from tools.approval_detection import (
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

        True = snapshot updated. Any failure keeps the previous snapshot and
        flips the client into (or keeps it in) the degraded state — logged on
        state change only (work order 2026-09-28 改动2).
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
            except (httpx.HTTPError, httpx.InvalidURL) as exc:
                last_error = f"network error: {type(exc).__name__}"
                transient = True
            else:
                if response.status_code == 200:
                    if self._install_from_payload(response):
                        self._mark_fetch_success()
                        return True
                    self._mark_fetch_failure("invalid payload from control plane")
                    return False
                if response.status_code in (401, 403):
                    self._alert_auth_problem(response.status_code)
                    self._mark_fetch_failure(f"auth rejected (HTTP {response.status_code})")
                    return False  # credential problem: retrying cannot help
                if 500 <= response.status_code < 600:
                    last_error = f"HTTP {response.status_code}"
                    transient = True
                else:
                    self._mark_fetch_failure(f"unexpected HTTP {response.status_code}")
                    return False
            if transient and attempt < 2:
                await asyncio.sleep(delays[attempt])
        self._mark_fetch_failure(last_error)
        return False

    def _install_from_payload(self, response) -> bool:
        """Validate the envelope, then swap in the new snapshot + persist.

        Any invalid shape keeps the previous snapshot (fail-safe); shape
        problems log at debug — the degraded-state WARNING on the caller
        already tells the operator the fetch produced no usable data.
        """
        try:
            payload = response.json()
        except ValueError:
            logger.debug("control-plane whitelist: non-JSON body; keeping cache")
            return False
        if not isinstance(payload, dict) or payload.get("success") is not True:
            logger.debug("control-plane whitelist: invalid envelope; keeping cache")
            return False
        data = payload.get("data")
        if not isinstance(data, dict):
            logger.debug("control-plane whitelist: invalid data field; keeping cache")
            return False
        commands = _clean_list(data.get("commands"))
        users = _clean_list(data.get("users"))
        if commands is None or users is None:
            logger.debug("control-plane whitelist: invalid list fields; keeping cache")
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


# ---- background poll -----------------------------------------------------

_poll_task: Optional["asyncio.Task[None]"] = None


def _jittered(base: float) -> float:
    """±20% so a fleet of sandboxes doesn't retry in lockstep."""
    return base * random.uniform(*_JITTER_RANGE)


async def _poll_loop(
    client: WhitelistClient, sleep=asyncio.sleep, jitter=_jittered
) -> None:
    """Refresh forever. Failures back off 5s→30s→2min→5min (capped,
    jittered); the first success resets to the flat 30s cadence (work order
    2026-09-28 改动2 — a flat retry against a down control plane flooded the
    logs)."""
    failures = 0
    while True:
        try:
            ok = await client.refresh()
        except Exception:
            logger.warning("control-plane whitelist poll error", exc_info=True)
            client._mark_fetch_failure("poll loop exception")
            ok = False
        if ok:
            failures = 0
            delay = _POLL_INTERVAL_S
        else:
            delay = _FAILURE_BACKOFF_S[min(failures, len(_FAILURE_BACKOFF_S) - 1)]
            failures += 1
        await sleep(jitter(delay))


async def ensure_startup_fetch(
    client: Optional[WhitelistClient] = None, *, rounds: int = 2, pause: float = 2.0
) -> bool:
    """Bounded blocking fetch before the gateway enters service (work order
    2026-09-28 改动1). Up to ``rounds`` refresh() calls (each already retries
    transient errors internally) with ``pause`` seconds between them.

    True when the feature is disabled or a fetch succeeded. False means the
    service enters degraded anyway — DM admission fails open (each admission
    logged), groups and command gating serve whatever the disk cache holds.
    """
    if client is None:
        client = get_platform_whitelist()
        if client is None:
            return True
    for round_index in range(rounds):
        if await client.refresh():
            return True
        if round_index < rounds - 1:
            await asyncio.sleep(pause)
    logger.warning(
        "control-plane whitelist: no data after %d startup attempts — entering "
        "service degraded (DM admission fails open; groups and command gating "
        "stay fail-closed)",
        rounds,
    )
    return False


def start_poll_task() -> Optional["asyncio.Task[None]"]:
    """Start the shared poll task (idempotent). None when disabled.

    Call once from gateway startup; the returned task can be registered
    with the gateway's background-task set for shutdown cancellation.
    """
    global _poll_task
    client = get_platform_whitelist()
    if client is None:
        return None
    if _poll_task is not None and not _poll_task.done():
        return _poll_task
    _poll_task = asyncio.create_task(_poll_loop(client))
    return _poll_task


def stop_poll_task() -> None:
    global _poll_task
    if _poll_task is not None and not _poll_task.done():
        _poll_task.cancel()
    _poll_task = None
