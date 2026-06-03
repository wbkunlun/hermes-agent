"""Tests for the standalone WeCom send path in send_message_tool."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from gateway.config import Platform
from gateway.platforms.base import SendResult


def _connected_adapter(*, message_id: str = "msg-1") -> MagicMock:
    adapter = MagicMock()
    adapter.is_connected = True
    adapter.send = AsyncMock(return_value=SendResult(success=True, message_id=message_id))
    return adapter


class TestSendWecomAdapterResolution:
    """_send_wecom must resolve adapters without hard-depending on get_active."""

    def test_reuses_gateway_runner_adapter(self) -> None:
        from tools.send_message_tool import _send_wecom

        adapter = _connected_adapter(message_id="runner-msg")
        runner = SimpleNamespace(adapters={Platform.WECOM: adapter})

        with patch("gateway.platforms.wecom.check_wecom_requirements", return_value=True), \
             patch("gateway.run._gateway_runner_ref", return_value=runner), \
             patch("gateway.platforms.wecom.get_active_adapter", return_value=None):
            result = asyncio.run(_send_wecom({"bot_id": "b", "secret": "s"}, "chat-1", "hello"))

        assert result == {
            "success": True,
            "platform": "wecom",
            "chat_id": "chat-1",
            "message_id": "runner-msg",
        }
        adapter.send.assert_awaited_once_with("chat-1", "hello")

    def test_reuses_get_active_adapter_when_runner_missing(self) -> None:
        from tools.send_message_tool import _send_wecom

        adapter = _connected_adapter(message_id="active-msg")

        with patch("gateway.platforms.wecom.check_wecom_requirements", return_value=True), \
             patch("gateway.run._gateway_runner_ref", return_value=None), \
             patch("gateway.platforms.wecom.get_active_adapter", return_value=adapter):
            result = asyncio.run(_send_wecom({"bot_id": "b", "secret": "s"}, "chat-2", "hi"))

        assert result["success"] is True
        assert result["message_id"] == "active-msg"
        adapter.send.assert_awaited_once()

    def test_no_attribute_error_when_get_active_missing(self) -> None:
        """Mixed installs: new send_message_tool + old wecom without get_active."""
        from gateway.platforms import wecom as wecom_mod
        from tools.send_message_tool import _send_wecom

        adapter = _connected_adapter(message_id="runner-only")
        runner = SimpleNamespace(adapters={Platform.WECOM: adapter})

        original_get_active = getattr(wecom_mod.WeComAdapter, "get_active", None)
        if hasattr(wecom_mod.WeComAdapter, "get_active"):
            delattr(wecom_mod.WeComAdapter, "get_active")

        try:
            with patch("gateway.platforms.wecom.check_wecom_requirements", return_value=True), \
                 patch("gateway.run._gateway_runner_ref", return_value=runner), \
                 patch(
                     "gateway.platforms.wecom.get_active_adapter",
                     side_effect=ImportError("no get_active_adapter"),
                 ):
                result = asyncio.run(
                    _send_wecom({"bot_id": "b", "secret": "s"}, "chat-3", "ping")
                )
        finally:
            if original_get_active is not None:
                wecom_mod.WeComAdapter.get_active = original_get_active

        assert result["success"] is True
        assert "get_active" not in str(result.get("error", ""))
        adapter.send.assert_awaited_once()

    def test_falls_back_to_temporary_adapter(self) -> None:
        from tools.send_message_tool import _send_wecom

        temp_adapter = _connected_adapter(message_id="temp-msg")
        temp_adapter.connect = AsyncMock(return_value=True)
        temp_adapter.disconnect = AsyncMock()

        with patch("gateway.platforms.wecom.check_wecom_requirements", return_value=True), \
             patch("gateway.run._gateway_runner_ref", return_value=None), \
             patch("gateway.platforms.wecom.get_active_adapter", return_value=None), \
             patch("gateway.platforms.wecom.WeComAdapter", return_value=temp_adapter) as adapter_cls:
            result = asyncio.run(_send_wecom({"bot_id": "b", "secret": "s"}, "chat-4", "temp"))

        assert result["success"] is True
        assert result["message_id"] == "temp-msg"
        adapter_cls.assert_called_once()
        temp_adapter.connect.assert_awaited_once()
        temp_adapter.disconnect.assert_awaited_once()

    def test_get_active_adapter_module_delegate(self) -> None:
        from gateway.platforms.wecom import WeComAdapter, get_active_adapter

        sentinel = object()
        with patch.object(WeComAdapter, "_active_instance", sentinel, create=True):
            assert get_active_adapter() is sentinel
