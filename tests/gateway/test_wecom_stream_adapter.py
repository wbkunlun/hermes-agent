"""Tests for the WeCom adapter stream-frame / welcome / event additions."""

from unittest.mock import AsyncMock

import pytest

from gateway.config import PlatformConfig


def _new_adapter():
    from plugins.platforms.wecom.adapter import WeComAdapter

    adapter = WeComAdapter(PlatformConfig(enabled=True))
    adapter._send_reply_request = AsyncMock(return_value={"errcode": 0})
    adapter._send_request = AsyncMock(return_value={"errcode": 0})
    return adapter


class TestStreamCapability:
    def test_supports_stream_frames_flag(self):
        from plugins.platforms.wecom.adapter import WeComAdapter

        assert WeComAdapter.SUPPORTS_STREAM_FRAMES is True
        # still non-editable
        assert WeComAdapter.SUPPORTS_MESSAGE_EDITING is False


class TestSendStreamFrame:
    @pytest.mark.asyncio
    async def test_sends_stream_payload(self):
        adapter = _new_adapter()
        await adapter.send_stream_frame("req-1", "sid-1", "hello", False, chat_id="c1")

        adapter._send_reply_request.assert_awaited_once_with(
            "req-1",
            {
                "msgtype": "stream",
                "stream": {"id": "sid-1", "finish": False, "content": "hello"},
            },
        )
        # no fallback on success
        adapter._send_request.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_falls_back_on_stream_expired(self):
        adapter = _new_adapter()
        adapter._send_reply_request = AsyncMock(
            return_value={"errcode": 846608, "errmsg": "stream expired"}
        )

        await adapter.send_stream_frame("req-1", "sid-1", "hello", True, chat_id="c1")

        # stream marked expired
        assert "sid-1" in adapter._expired_stream_ids
        # fallback markdown sent via aibot_send_msg
        from plugins.platforms.wecom.adapter import APP_CMD_SEND

        adapter._send_request.assert_awaited_once()
        call = adapter._send_request.await_args
        assert call.args[0] == APP_CMD_SEND
        assert call.args[1]["msgtype"] == "markdown"
        assert call.args[1]["chatid"] == "c1"
        assert call.args[1]["markdown"]["content"] == "hello"

    @pytest.mark.asyncio
    async def test_already_expired_uses_fallback_without_stream(self):
        adapter = _new_adapter()
        adapter._expired_stream_ids.add("sid-1")

        await adapter.send_stream_frame("req-1", "sid-1", "again", False, chat_id="c1")

        # no stream frame attempt; direct markdown fallback
        adapter._send_reply_request.assert_not_awaited()
        adapter._send_request.assert_awaited_once()


class TestSendWelcome:
    @pytest.mark.asyncio
    async def test_sends_welcome_payload(self):
        adapter = _new_adapter()
        await adapter.send_welcome("req-9", "你好！")

        from plugins.platforms.wecom.adapter import APP_CMD_RESPONSE_WELCOME

        adapter._send_reply_request.assert_awaited_once_with(
            "req-9",
            {"msgtype": "text", "text": {"content": "你好！"}},
            cmd=APP_CMD_RESPONSE_WELCOME,
        )


class TestEnterChatEvent:
    @pytest.mark.asyncio
    async def test_enter_chat_sends_welcome(self):
        adapter = _new_adapter()
        adapter.send_welcome = AsyncMock()

        payload = {
            "headers": {"req_id": "evt-1"},
            "body": {
                "event": {"eventtype": "enter_chat"},
                "from": {"userid": "alice"},
            },
        }
        await adapter._on_event(payload)

        adapter.send_welcome.assert_awaited_once()
        args = adapter.send_welcome.await_args.args
        assert args[0] == "evt-1"
        assert "alice" in args[1]

    @pytest.mark.asyncio
    async def test_other_event_does_not_welcome(self):
        adapter = _new_adapter()
        adapter.send_welcome = AsyncMock()

        await adapter._on_event(
            {"headers": {"req_id": "evt-2"}, "body": {"event": {"eventtype": "feedback_event"}}}
        )
        adapter.send_welcome.assert_not_awaited()
