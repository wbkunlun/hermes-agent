"""Persistence for the parked-group dead-window queue.

fork 2026-10-26 (batch 4A): ``_pending_group_sends`` was in-memory by design (TTL/时效
semantics), but the queue holds UNDELIVERED cron reports — a gateway restart inside a
dead-window episode silently dropped every parked entry (the same shape as the
2026-10-08 learned-chats incident, where a restart wiped the learned set). Mirrors
learned_chats.py conventions exactly: lazy ``get_hermes_home()``, module-level test
override, 5s debounce, ``atomic_json_write`` via to_thread — PLUS an awaited flush on
disconnect, because a debounced write dropped at process exit loses content for good
(a re-learnable set loses nothing; parked content is gone).

The per-chat cron execution ids ride along with their park timestamp (so a
correction can never fire for content whose entries expired past the TTL): a
redelivery after the restart must still be able to correct their settled-failed
ledger rows (cron/delivery_correction.py).
"""

from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Explicit test override (tests patch this); None = current hermes home.
GROUP_PENDING_PATH: Optional[Path] = None

_PENDING_SAVE_DEBOUNCE_SECONDS = 5.0
_PERSIST_OFF_VALUES = {"0", "false", "off", "no"}


class GroupPendingMixin:
    """Persist the parked-group queue to HERMES_HOME (debounced, flushed on disconnect)."""

    _pending_save_delay = _PENDING_SAVE_DEBOUNCE_SECONDS

    def _pending_persist_enabled(self) -> bool:
        import os as _os

        return _os.getenv("HERMES_WECOM_GROUP_PENDING_PERSIST", "").strip().lower() not in _PERSIST_OFF_VALUES

    def _pending_queue_path(self) -> Path:
        if GROUP_PENDING_PATH is not None:
            return GROUP_PENDING_PATH
        from hermes_cli.config import get_hermes_home

        return get_hermes_home() / "wecom_group_pending.json"

    def _load_pending(self) -> None:
        """Seed the in-memory queue from disk; ANY surprise (missing, corrupt, wrong
        shape) degrades to empty. fork 2026-10-26 (release fix): the loader guards the
        whole body, not just the JSON parse — a valid-JSON file with a wrong-shaped
        ``chats``/``exec_ids`` value used to raise out of ``__init__`` and take the
        whole wecom platform down (the recoverable state file became an outage).
        Entries past the TTL are dropped on load (a restart does not extend them)."""
        pending = getattr(self, "_pending_group_sends", None)
        if pending is None or not self._pending_persist_enabled():
            return  # bare __new__ test stubs skip persistence
        path = self._pending_queue_path()
        if not path.exists():
            return
        try:
            restored = self._read_pending_file(path, pending)
        except Exception as exc:
            logger.warning(
                "[%s] group-pending file unreadable — starting empty (%s): %s",
                getattr(self, "name", "wecom"), path, exc,
            )
            return
        if restored:
            logger.info(
                "[%s] Restored %d parked group send(s) from %s",
                getattr(self, "name", "wecom"), restored, path.name,
            )

    def _read_pending_file(self, path: Path, pending: dict) -> int:
        import json

        data = json.loads(path.read_text(encoding="utf-8-sig"))
        if not isinstance(data, dict):
            return 0
        now = time.time()
        ttl = getattr(self, "_group_pending_ttl_seconds", 21600.0)
        restored = 0
        chats = data.get("chats")
        if isinstance(chats, dict):
            for chat_id, entries in chats.items():
                if not isinstance(entries, list):
                    continue
                live = [(float(ts), str(text)) for ts, text in entries
                        if isinstance(ts, (int, float)) and isinstance(text, str)
                        and now - float(ts) < ttl]
                if live:
                    pending[str(chat_id)] = live
                    restored += len(live)
        exec_ids = getattr(self, "_pending_group_exec_ids", None)
        if exec_ids is not None:
            stored = data.get("exec_ids")
            if isinstance(stored, dict):
                for chat_id, pairs in stored.items():
                    if not isinstance(pairs, list):
                        continue
                    live_ids = [(str(eid), float(ts)) for eid, ts in pairs
                                if isinstance(eid, str) and isinstance(ts, (int, float))
                                and (ttl <= 0 or now - float(ts) < ttl)]
                    if live_ids:
                        exec_ids[str(chat_id)] = live_ids
        return restored

    def _pending_payload(self) -> dict:
        return {
            "chats": {chat_id: [[ts, text] for ts, text in entries]
                      for chat_id, entries in (getattr(self, "_pending_group_sends", None) or {}).items()},
            "exec_ids": {chat_id: [[eid, ts] for eid, ts in pairs]
                         for chat_id, pairs in
                         (getattr(self, "_pending_group_exec_ids", None) or {}).items()},
        }

    def _schedule_pending_save(self) -> None:
        if not self._pending_persist_enabled():
            return
        if getattr(self, "_pending_save_handle", None) is not None:
            return  # one debounced write absorbs the burst
        loop = asyncio.get_running_loop()
        self._pending_save_handle = loop.call_later(self._pending_save_delay, self._pending_save_fire)

    def _pending_save_fire(self) -> None:
        self._pending_save_handle = None
        payload, path = self._pending_payload(), self._pending_queue_path()
        # fork 2026-10-26 (release fix): keep the task — an unreferenced task can be
        # garbage-collected mid-flight, silently skipping the persist this exists for;
        # the disconnect flush also needs it to serialise against.
        self._pending_write_task = asyncio.ensure_future(self._write_pending(payload, path))

    async def _write_pending(self, payload: dict, path: Path) -> None:
        try:
            from utils import atomic_json_write

            await asyncio.to_thread(atomic_json_write, path, payload)
        except Exception as exc:
            logger.warning("[%s] group-pending write failed: %s", getattr(self, "name", "wecom"), exc)

    def _cancel_pending_save(self) -> None:
        handle = getattr(self, "_pending_save_handle", None)
        if handle is not None:
            handle.cancel()
        self._pending_save_handle = None
