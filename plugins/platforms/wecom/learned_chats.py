"""Persistence for inbound-learned chat classification.

fork 2026-10-08: ``_group_chat_ids`` / ``_dm_userid_by_chat`` are rebuilt from inbound
traffic; before this mixin they were wiped on every restart (the 2026-09-29 module audit,
M1.7, left them in-memory by design). A restart between a group's last inbound and a
scheduled push misclassified the group as a DM — the send took the DM branch (no ⏰
redelivery parking) and the notification was lost outright (production 2026-10-08 17:31:
the 09:56 restart cleared the learned set; the 17:31 cron push hit the 846609 dead window
in the DM branch and vanished). Mirrors gateway/channel_directory.py conventions: lazy
``get_hermes_home()``, module-level test override, ``atomic_json_write`` via to_thread.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Optional

from gateway.platforms.helpers import bounded_put

logger = logging.getLogger(__name__)

# Explicit test override (tests patch this); None = current hermes home.
LEARNED_CHATS_PATH: Optional[Path] = None

_LEARNED_SAVE_DEBOUNCE_SECONDS = 5.0
_LEARNED_CAP = 1000  # same magnitude as DEDUP_MAX_SIZE; the maps are small in practice
_PERSIST_OFF_VALUES = {"0", "false", "off", "no"}


class LearnedChatsMixin:
    """Persist learned group ids + the DM chatid→userid map to HERMES_HOME (debounced)."""

    _learned_save_delay = _LEARNED_SAVE_DEBOUNCE_SECONDS

    def _learned_persist_enabled(self) -> bool:
        import os as _os

        return _os.getenv("HERMES_WECOM_LEARNED_CHATS_PERSIST", "").strip().lower() not in _PERSIST_OFF_VALUES

    def _learned_chats_path(self) -> Path:
        if LEARNED_CHATS_PATH is not None:
            return LEARNED_CHATS_PATH
        from hermes_cli.config import get_hermes_home

        return get_hermes_home() / "wecom_learned_chats.json"

    def _load_learned_chats(self) -> None:
        """Seed the in-memory maps from disk; corrupt/missing files degrade to empty."""
        groups = getattr(self, "_group_chat_ids", None)
        dm_map = getattr(self, "_dm_userid_by_chat", None)
        if groups is None or dm_map is None or not self._learned_persist_enabled():
            return  # bare __new__ test stubs skip persistence
        path = self._learned_chats_path()
        if not path.exists():
            return
        try:
            import json

            data = json.loads(path.read_text(encoding="utf-8-sig"))
        except Exception as exc:  # noqa: BLE001 — a bad sidecar degrades to defaults
            logger.warning(
                "[%s] learned-chats file unreadable — starting empty (%s): %s",
                getattr(self, "name", "wecom"), path, exc,
            )
            return
        if not isinstance(data, dict):
            return
        for chat_id in [g for g in data.get("groups", []) if isinstance(g, str) and g.strip()][-_LEARNED_CAP:]:
            groups.add(chat_id.strip())
        dm_entries = data.get("dm_userid")
        if isinstance(dm_entries, dict):
            pairs = [(str(k), str(v)) for k, v in dm_entries.items() if str(k).strip() and str(v).strip()]
            dm_map.update(dict(pairs[-_LEARNED_CAP:]))
        if groups or dm_map:
            logger.info(
                "[%s] Restored %d learned group chat(s) and %d DM userid mapping(s) from %s",
                getattr(self, "name", "wecom"), len(groups), len(dm_map), path.name,
            )

    def _note_learned_group(self, chat_id: str) -> None:
        """Classify AND persist. Called pre-policy in _admit_inbound — a policy-dropped
        group keeps its group semantics for the send path too."""
        groups = getattr(self, "_group_chat_ids", None)
        if groups is not None and chat_id not in groups:
            groups.add(chat_id)
            self._schedule_learned_save()

    def _remember_dm_userid(self, chat_id: str, userid: str) -> None:
        dm_map = getattr(self, "_dm_userid_by_chat", None)
        if dm_map is None:
            return
        changed = dm_map.get(chat_id) != userid
        bounded_put(dm_map, chat_id, userid, _LEARNED_CAP)
        if changed:
            self._schedule_learned_save()

    def _schedule_learned_save(self) -> None:
        if not self._learned_persist_enabled():
            return
        if getattr(self, "_learned_save_handle", None) is not None:
            return  # one debounced write absorbs the burst
        loop = asyncio.get_running_loop()
        self._learned_save_handle = loop.call_later(self._learned_save_delay, self._learned_save_fire)

    def _learned_save_fire(self) -> None:
        self._learned_save_handle = None
        payload = {
            "groups": sorted(getattr(self, "_group_chat_ids", None) or ()),
            "dm_userid": dict(getattr(self, "_dm_userid_by_chat", None) or {}),
        }
        asyncio.ensure_future(self._write_learned_chats(payload, self._learned_chats_path()))

    async def _write_learned_chats(self, payload: dict, path: Path) -> None:
        try:
            from utils import atomic_json_write

            await asyncio.to_thread(atomic_json_write, path, payload)
        except Exception as exc:  # noqa: BLE001 — observation only, the next learn retries
            logger.warning("[%s] learned-chats write failed: %s", getattr(self, "name", "wecom"), exc)

    def _cancel_learned_save(self) -> None:
        handle = getattr(self, "_learned_save_handle", None)
        if handle is not None:
            handle.cancel()
        self._learned_save_handle = None
