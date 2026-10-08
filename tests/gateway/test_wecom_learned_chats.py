"""Tests for WeCom learned-chat classification persistence.

fork 2026-10-08: learned group ids + the DM chatid→userid map persist to HERMES_HOME so a
restart no longer misclassifies a quiet group as a DM (production incident 2026-10-08
17:31 — the 09:56 restart cleared the learned set, the 17:31 cron push took the DM branch
in the 846609 dead window, no ⏰ parking, notification lost outright)."""

import asyncio
import json
from unittest.mock import AsyncMock, patch

import pytest


def _group_payload(chat_id="wrhR9", req_id="req-g", content="hi"):
    return {
        "cmd": "aibot_msg_callback",
        "headers": {"req_id": req_id},
        "body": {
            "msgid": f"msg-{req_id}",
            "chatid": chat_id,
            "chattype": "group",
            "msgtype": "text",
            "from": {"userid": "alice"},
            "text": {"content": content},
        },
    }


def _dm_payload(chat_id="wohR123", req_id="req-d"):
    return {
        "cmd": "aibot_msg_callback",
        "headers": {"req_id": req_id},
        "body": {
            "msgid": f"msg-{req_id}",
            "chatid": chat_id,
            "chattype": "single",
            "msgtype": "text",
            "from": {"userid": "zhangsan"},
            "text": {"content": "hi"},
        },
    }


def _adapter(**extra):
    from gateway.config import PlatformConfig
    from plugins.platforms.wecom.adapter import WeComAdapter

    adapter = WeComAdapter(PlatformConfig(enabled=True, extra=extra))
    adapter._text_batch_delay_seconds = 0
    adapter.handle_message = AsyncMock()
    adapter._extract_media = AsyncMock(return_value=([], []))
    adapter._learned_save_delay = 0.01  # snappy debounce for tests
    return adapter


async def _settle_debounce():
    """Timer fire + to_thread write + a few loop pumps."""
    await asyncio.sleep(0.05)
    for _ in range(4):
        await asyncio.sleep(0)


class TestLearnedChatsPersistence:

    @pytest.mark.asyncio
    async def test_group_learn_survives_restart(self, tmp_path):
        import plugins.platforms.wecom.learned_chats as learned_chats

        cache = tmp_path / "wecom_learned_chats.json"
        with patch.object(learned_chats, "LEARNED_CHATS_PATH", cache):
            adapter = _adapter()
            # Default pairing policy DROPS the group at intake — classification is
            # learned pre-policy so the send path still knows it is a group.
            await adapter._on_message(_group_payload("wrhR9"))
            assert "wrhR9" in adapter._group_chat_ids
            await _settle_debounce()
            assert cache.exists()
            assert "wrhR9" in json.loads(cache.read_text())["groups"]

            fresh = _adapter()  # restart: nothing in memory, file seeds the set
            assert fresh._is_group_chat("wrhR9") is True

    @pytest.mark.asyncio
    async def test_dm_userid_map_persists(self, tmp_path):
        import plugins.platforms.wecom.learned_chats as learned_chats

        cache = tmp_path / "wecom_learned_chats.json"
        with patch.object(learned_chats, "LEARNED_CHATS_PATH", cache):
            adapter = _adapter()
            adapter._is_dm_intake_allowed = lambda sender_id: True
            await adapter._on_message(_dm_payload("wohR123"))
            assert adapter._dm_userid_by_chat.get("wohR123") == "zhangsan"
            await _settle_debounce()
            assert cache.exists()

            fresh = _adapter()
            assert fresh._dm_userid_by_chat.get("wohR123") == "zhangsan"

    @pytest.mark.asyncio
    async def test_corrupt_file_degrades_to_empty(self, tmp_path):
        import plugins.platforms.wecom.learned_chats as learned_chats

        cache = tmp_path / "wecom_learned_chats.json"
        cache.write_text("{not json")
        with patch.object(learned_chats, "LEARNED_CHATS_PATH", cache):
            adapter = _adapter()  # must not raise
        assert adapter._group_chat_ids == set()
        assert adapter._dm_userid_by_chat == {}

    @pytest.mark.asyncio
    async def test_env_off_writes_nothing(self, tmp_path, monkeypatch):
        monkeypatch.setenv("HERMES_WECOM_LEARNED_CHATS_PERSIST", "off")
        adapter = _adapter()
        await adapter._on_message(_group_payload("wrhR9"))
        await _settle_debounce()
        assert not (tmp_path / "wecom_learned_chats.json").exists()

    @pytest.mark.asyncio
    async def test_persisted_group_routes_sends_as_group(self, tmp_path):
        """The point of persistence: a fresh instance must take the GROUP send branch
        (proactive chat_type=2), not the DM branch that lost the 17:31 notification."""
        import plugins.platforms.wecom.learned_chats as learned_chats

        cache = tmp_path / "wecom_learned_chats.json"
        cache.write_text(json.dumps({"groups": ["wrhR9"], "dm_userid": {}}))
        with patch.object(learned_chats, "LEARNED_CHATS_PATH", cache):
            adapter = _adapter()
        adapter._send_request = AsyncMock(return_value={"errcode": 0})

        result = await adapter.send("wrhR9", "restart-proof")

        assert result.success is True
        body = adapter._send_request.await_args.args[1]
        assert body["chat_type"] == 2
