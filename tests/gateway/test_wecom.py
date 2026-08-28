"""Tests for the WeCom platform adapter."""

import asyncio
import base64
import os
import socket
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from gateway.config import PlatformConfig
from gateway.platforms.base import SendResult


class TestWeComRequirements:
    def test_returns_false_without_aiohttp(self, monkeypatch):
        monkeypatch.setattr("plugins.platforms.wecom.adapter.AIOHTTP_AVAILABLE", False)
        monkeypatch.setattr("plugins.platforms.wecom.adapter.HTTPX_AVAILABLE", True)
        from plugins.platforms.wecom.adapter import check_wecom_requirements

        assert check_wecom_requirements() is False


class TestWeComAdapterInit:
    def test_declares_stream_frames_capability(self):
        from plugins.platforms.wecom.adapter import WeComAdapter

        # WeCom streams via aibot_respond_msg msgtype:"stream" frames driven by
        # WeComStreamDelivery (clawrelay-style), not edit-based delivery.
        assert WeComAdapter.SUPPORTS_STREAM_FRAMES is True
        assert WeComAdapter.SUPPORTS_MESSAGE_EDITING is False
        assert WeComAdapter.REQUIRES_EDIT_FINALIZE is True


class TestWeComAdapterAuthzScope:
    """dm_policy/allowlist reads must honor the profile secret scope under
    multiplexing (#93522): a secondary profile's own scope is authoritative
    and must not inherit the default profile's process-env authorization."""

    @pytest.fixture()
    def multiplex_on(self):
        from agent import secret_scope

        previous = secret_scope.is_multiplex_active()
        secret_scope.set_multiplex_active(True)
        try:
            yield
        finally:
            secret_scope.set_multiplex_active(previous)

    def test_scoped_construction_reads_authz_from_scope_not_environ(self, multiplex_on, monkeypatch):
        from agent import secret_scope
        from plugins.platforms.wecom.adapter import WeComAdapter

        monkeypatch.setenv("WECOM_DM_POLICY", "pairing")
        monkeypatch.setenv("WECOM_ALLOWED_USERS", "default-user")
        token = secret_scope.set_secret_scope(
            {"WECOM_DM_POLICY": "allowlist", "WECOM_ALLOWED_USERS": "scoped-user"}
        )
        try:
            adapter = WeComAdapter(PlatformConfig(enabled=True))
        finally:
            secret_scope.reset_secret_scope(token)
        assert adapter._dm_policy == "allowlist"
        assert adapter._allow_from == ["scoped-user"]

    def test_scoped_miss_does_not_admit_default_profiles_allowlist(self, multiplex_on, monkeypatch):
        from agent import secret_scope
        from plugins.platforms.wecom.adapter import WeComAdapter

        monkeypatch.setenv("WECOM_DM_POLICY", "allowlist")
        monkeypatch.setenv("WECOM_ALLOWED_USERS", "default-user")
        token = secret_scope.set_secret_scope({"SOMETHING_ELSE": "x"})
        try:
            adapter = WeComAdapter(PlatformConfig(enabled=True))
        finally:
            secret_scope.reset_secret_scope(token)
        assert adapter._dm_policy == "pairing"
        assert adapter._allow_from == []


class TestWeComConnect:

    @pytest.mark.asyncio
    async def test_connect_records_handshake_failure_details(self, monkeypatch):
        import plugins.platforms.wecom.adapter as wecom_module
        from plugins.platforms.wecom.adapter import WeComAdapter

        class DummyClient:
            async def aclose(self):
                return None

        monkeypatch.setattr(wecom_module, "AIOHTTP_AVAILABLE", True)
        monkeypatch.setattr(wecom_module, "HTTPX_AVAILABLE", True)
        monkeypatch.setattr(
            wecom_module,
            "httpx",
            SimpleNamespace(AsyncClient=lambda **kwargs: DummyClient()),
        )

        adapter = WeComAdapter(
            PlatformConfig(enabled=True, extra={"bot_id": "bot-1", "secret": "secret-1"})
        )
        adapter._open_connection = AsyncMock(side_effect=RuntimeError("invalid secret (errcode=40013)"))

        success = await adapter.connect()

        assert success is False
        assert adapter.has_fatal_error is True
        assert adapter.fatal_error_code == "wecom_connect_error"
        assert "invalid secret" in (adapter.fatal_error_message or "")


class TestWeComQrScan:
    @patch("plugins.platforms.wecom.adapter.time")
    @patch("plugins.platforms.wecom.adapter.json.loads")
    @patch("plugins.platforms.wecom.adapter.logger")
    @patch("urllib.request.urlopen")
    @patch("urllib.request.Request")
    def test_qr_scan_timeout_uses_monotonic_clock(
        self,
        mock_request,
        mock_urlopen,
        _mock_logger,
        mock_json_loads,
        mock_time,
    ):
        from plugins.platforms.wecom.adapter import qr_scan_for_bot_info

        generate_resp = MagicMock()
        generate_resp.read.return_value = b'{"data":{"scode":"abc","auth_url":"https://example.com/qr"}}'
        generate_resp.__enter__.return_value = generate_resp
        generate_resp.__exit__.return_value = False

        poll_resp = MagicMock()
        poll_resp.read.return_value = b'{"data":{"status":"pending"}}'
        poll_resp.__enter__.return_value = poll_resp
        poll_resp.__exit__.return_value = False

        mock_urlopen.side_effect = [generate_resp, poll_resp]
        mock_json_loads.side_effect = [
            {"data": {"scode": "abc", "auth_url": "https://example.com/qr"}},
            {"data": {"status": "pending"}},
        ]
        mock_time.monotonic.side_effect = [1000, 1000.2, 1001.1]
        mock_time.time.side_effect = [1000, 900, 901, 902]
        mock_time.sleep = MagicMock()

        with patch("builtins.print"), patch.dict("sys.modules", {"qrcode": None}):
            result = qr_scan_for_bot_info(timeout_seconds=1)

        assert result is None
        assert mock_urlopen.call_count == 2


class TestWeComReplyMode:

    @pytest.mark.asyncio
    async def test_send_image_file_uses_passive_reply_media_when_reply_context_exists(self):
        from plugins.platforms.wecom.adapter import WeComAdapter

        adapter = WeComAdapter(PlatformConfig(enabled=True))
        adapter._reply_req_ids["msg-1"] = "req-1"
        adapter._prepare_outbound_media = AsyncMock(
            return_value={
                "data": b"image-bytes",
                "content_type": "image/png",
                "file_name": "demo.png",
                "detected_type": "image",
                "final_type": "image",
                "rejected": False,
                "reject_reason": None,
                "downgraded": False,
                "downgrade_note": None,
            }
        )
        adapter._upload_media_bytes = AsyncMock(return_value={"media_id": "media-1", "type": "image"})
        adapter._send_reply_request = AsyncMock(
            return_value={"headers": {"req_id": "req-1"}, "errcode": 0}
        )

        result = await adapter.send_image_file("chat-123", "/tmp/demo.png", reply_to="msg-1")

        assert result.success is True
        adapter._send_reply_request.assert_awaited_once()
        args = adapter._send_reply_request.await_args.args
        assert args[0] == "req-1"
        assert args[1] == {"msgtype": "image", "image": {"media_id": "media-1"}}


class TestExtractText:

    def test_extracts_mixed_text(self):
        from plugins.platforms.wecom.adapter import WeComAdapter

        body = {
            "msgtype": "mixed",
            "mixed": {
                "msg_item": [
                    {"msgtype": "text", "text": {"content": "part1"}},
                    {"msgtype": "image", "image": {"url": "https://example.com/x.png"}},
                    {"msgtype": "text", "text": {"content": "part2"}},
                ]
            },
        }
        text, _reply_text = WeComAdapter._extract_text(body)
        assert text == "part1\npart2"


class TestCallbackDispatch:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("cmd", ["aibot_msg_callback", "aibot_callback"])
    async def test_dispatch_accepts_new_and_legacy_callback_cmds(self, cmd):
        from plugins.platforms.wecom.adapter import WeComAdapter

        adapter = WeComAdapter(PlatformConfig(enabled=True))
        adapter._on_message = AsyncMock()

        await adapter._dispatch_payload({"cmd": cmd, "headers": {"req_id": "req-1"}, "body": {}})

        adapter._on_message.assert_awaited_once()


class TestPolicyHelpers:

    def test_dm_allowlist_honors_env_only_allowed_users(self, monkeypatch):
        """Env-only setup (WECOM_DM_POLICY + WECOM_ALLOWED_USERS, no config
        ``extra``) must populate the DM allowlist. Otherwise ``dm_policy:
        allowlist`` runs with an empty allowlist and drops every listed user
        at intake — the documented env vars become no-ops."""
        from plugins.platforms.wecom.adapter import WeComAdapter

        monkeypatch.setenv("WECOM_DM_POLICY", "allowlist")
        monkeypatch.setenv("WECOM_ALLOWED_USERS", "user-1, user-2")

        adapter = WeComAdapter(PlatformConfig(enabled=True))

        assert adapter._dm_policy == "allowlist"
        assert adapter._allow_from == ["user-1", "user-2"]
        assert adapter._is_dm_allowed("user-1") is True
        assert adapter._is_dm_allowed("user-2") is True
        assert adapter._is_dm_allowed("stranger") is False


    def test_pairing_group_policy_blocks_without_explicit_group_allow_from(self):
        from plugins.platforms.wecom.adapter import WeComAdapter

        adapter = WeComAdapter(
            PlatformConfig(enabled=True, extra={"group_policy": "pairing"})
        )

        assert adapter._is_group_allowed("group-1", "user-1") is False


class TestMediaHelpers:
    def test_detect_wecom_media_type(self):
        from plugins.platforms.wecom.adapter import WeComAdapter

        assert WeComAdapter._detect_wecom_media_type("image/png") == "image"
        assert WeComAdapter._detect_wecom_media_type("video/mp4") == "video"
        assert WeComAdapter._detect_wecom_media_type("audio/amr") == "voice"
        assert WeComAdapter._detect_wecom_media_type("application/pdf") == "file"

    def test_voice_non_amr_downgrades_to_file(self):
        from plugins.platforms.wecom.adapter import WeComAdapter

        result = WeComAdapter._apply_file_size_limits(128, "voice", "audio/mpeg")

        assert result["final_type"] == "file"
        assert result["downgraded"] is True
        assert "AMR" in (result["downgrade_note"] or "")


class TestMediaUpload:


    @pytest.mark.asyncio
    async def test_download_remote_bytes_blocks_connect_time_rebind(self, monkeypatch):
        import httpcore
        from httpcore._backends.auto import AutoBackend
        from plugins.platforms.wecom.adapter import WeComAdapter
        from tools.url_safety import SSRFConnectionBlocked

        for proxy_var in (
            "HTTP_PROXY",
            "HTTPS_PROXY",
            "ALL_PROXY",
            "http_proxy",
            "https_proxy",
            "all_proxy",
        ):
            monkeypatch.delenv(proxy_var, raising=False)

        answers = iter(("93.184.216.34", "169.254.169.254"))

        def fake_getaddrinfo(_host, port, *_args, **_kwargs):
            ip = next(answers)
            return [
                (socket.AF_INET, socket.SOCK_STREAM, 6, "", (ip, port or 0))
            ]

        connect_attempts = []

        async def fake_connect_tcp(
            _self,
            host,
            port,
            timeout=None,
            local_address=None,
            socket_options=None,
        ):
            connect_attempts.append((host, port))
            raise httpcore.ConnectError("stop before network")

        monkeypatch.setattr(socket, "getaddrinfo", fake_getaddrinfo)
        monkeypatch.setattr(AutoBackend, "connect_tcp", fake_connect_tcp)

        adapter = WeComAdapter(PlatformConfig(enabled=True))
        with pytest.raises(SSRFConnectionBlocked):
            await adapter._download_remote_bytes(
                "http://rebind.example/file.bin", max_bytes=1024
            )

        assert connect_attempts == []


class TestSend:


    @pytest.mark.asyncio
    async def test_send_voice_sends_caption_and_downgrade_note(self):
        from plugins.platforms.wecom.adapter import WeComAdapter

        adapter = WeComAdapter(PlatformConfig(enabled=True))
        adapter._prepare_outbound_media = AsyncMock(
            return_value={
                "data": b"voice-bytes",
                "content_type": "audio/mpeg",
                "file_name": "voice.mp3",
                "detected_type": "voice",
                "final_type": "file",
                "rejected": False,
                "reject_reason": None,
                "downgraded": True,
                "downgrade_note": "语音格式 audio/mpeg 不支持，企微仅支持 AMR 格式，已转为文件格式发送",
            }
        )
        adapter._upload_media_bytes = AsyncMock(return_value={"media_id": "media-1", "type": "file"})
        adapter._send_media_message = AsyncMock(return_value={"headers": {"req_id": "req-media"}, "errcode": 0})
        adapter.send = AsyncMock(return_value=SendResult(success=True, message_id="msg-1"))

        result = await adapter.send_voice("chat-123", "/tmp/voice.mp3", caption="listen")

        assert result.success is True
        adapter._send_media_message.assert_awaited_once_with("chat-123", "file", "media-1")
        assert adapter.send.await_count == 2
        adapter.send.assert_any_await(chat_id="chat-123", content="listen", reply_to=None)
        adapter.send.assert_any_await(
            chat_id="chat-123",
            content="ℹ️ 语音格式 audio/mpeg 不支持，企微仅支持 AMR 格式，已转为文件格式发送",
            reply_to=None,
        )


class TestInboundMessages:
    @pytest.mark.asyncio
    async def test_on_message_builds_event(self):
        from plugins.platforms.wecom.adapter import WeComAdapter

        adapter = WeComAdapter(
            PlatformConfig(
                enabled=True,
                extra={"group_policy": "allowlist", "group_allow_from": ["group-1"]},
            )
        )
        adapter._text_batch_delay_seconds = 0  # disable batching for tests
        adapter.handle_message = AsyncMock()
        adapter._extract_media = AsyncMock(return_value=(["/tmp/test.png"], ["image/png"]))

        payload = {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-1"},
            "body": {
                "msgid": "msg-1",
                "chatid": "group-1",
                "chattype": "group",
                "from": {"userid": "user-1"},
                "msgtype": "text",
                "text": {"content": "hello"},
            },
        }

        await adapter._on_message(payload)

        adapter.handle_message.assert_awaited_once()
        event = adapter.handle_message.await_args.args[0]
        assert event.text == "hello"
        assert event.source.chat_id == "group-1"
        assert event.source.user_id == "user-1"
        assert event.media_urls == ["/tmp/test.png"]
        assert event.media_types == ["image/png"]


class TestWeComZombieSessionFix:
    """Tests for PR #11572 — device_id, markdown reply, group req_id fallback."""

    def test_adapter_generates_stable_device_id_per_instance(self):
        from plugins.platforms.wecom.adapter import WeComAdapter

        adapter = WeComAdapter(PlatformConfig(enabled=True))
        assert isinstance(adapter._device_id, str)
        assert len(adapter._device_id) > 0
        # Second snapshot on the same adapter must be identical — only a fresh
        # adapter instance should get a new device_id (one-per-reconnect is the
        # zombie-session footgun we're fixing).
        assert adapter._device_id == adapter._device_id

    def test_different_adapter_instances_get_distinct_device_ids(self):
        from plugins.platforms.wecom.adapter import WeComAdapter

        a = WeComAdapter(PlatformConfig(enabled=True))
        b = WeComAdapter(PlatformConfig(enabled=True))
        assert a._device_id != b._device_id

    @pytest.mark.asyncio
    async def test_open_connection_includes_device_id_in_subscribe(self):
        from plugins.platforms.wecom.adapter import APP_CMD_SUBSCRIBE, WeComAdapter

        adapter = WeComAdapter(PlatformConfig(enabled=True))
        adapter._bot_id = "test-bot"
        adapter._secret = "test-secret"

        sent_payloads = []

        class _FakeWS:
            closed = False

            async def send_json(self, payload):
                sent_payloads.append(payload)

            async def close(self):
                return None

        class _FakeSession:
            def __init__(self, *args, **kwargs):
                pass

            async def ws_connect(self, *args, **kwargs):
                return _FakeWS()

            async def close(self):
                return None

        async def _fake_cleanup():
            return None

        async def _fake_handshake(req_id):
            return {"errcode": 0, "headers": {"req_id": req_id}}

        adapter._cleanup_ws = _fake_cleanup
        adapter._wait_for_handshake = _fake_handshake

        with patch("plugins.platforms.wecom.adapter.aiohttp.ClientSession", _FakeSession):
            await adapter._open_connection()

        assert len(sent_payloads) == 1
        subscribe = sent_payloads[0]
        assert subscribe["cmd"] == APP_CMD_SUBSCRIBE
        assert subscribe["body"]["bot_id"] == "test-bot"
        assert subscribe["body"]["secret"] == "test-secret"
        assert subscribe["body"]["device_id"] == adapter._device_id

    @pytest.mark.asyncio
    async def test_on_message_caches_last_req_id_per_chat(self):
        from plugins.platforms.wecom.adapter import WeComAdapter

        adapter = WeComAdapter(
            PlatformConfig(
                enabled=True,
                extra={"group_policy": "allowlist", "group_allow_from": ["group-1"]},
            )
        )
        adapter._text_batch_delay_seconds = 0
        adapter.handle_message = AsyncMock()
        adapter._extract_media = AsyncMock(return_value=([], []))

        payload = {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-abc"},
            "body": {
                "msgid": "msg-1",
                "chatid": "group-1",
                "chattype": "group",
                "from": {"userid": "user-1"},
                "msgtype": "text",
                "text": {"content": "hi"},
            },
        }

        await adapter._on_message(payload)
        assert adapter._last_chat_req_ids["group-1"][-1][0] == "req-abc"

    @pytest.mark.asyncio
    async def test_on_message_does_not_cache_blocked_sender_req_id(self):
        """Blocked chats shouldn't populate the proactive-send fallback cache."""
        from plugins.platforms.wecom.adapter import WeComAdapter

        adapter = WeComAdapter(
            PlatformConfig(
                enabled=True,
                extra={"group_policy": "allowlist", "group_allow_from": ["group-ok"]},
            )
        )
        adapter.handle_message = AsyncMock()
        adapter._extract_media = AsyncMock(return_value=([], []))

        payload = {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-abc"},
            "body": {
                "msgid": "msg-1",
                "chatid": "group-blocked",
                "chattype": "group",
                "from": {"userid": "user-1"},
                "msgtype": "text",
                "text": {"content": "hi"},
            },
        }

        await adapter._on_message(payload)
        adapter.handle_message.assert_not_awaited()
        assert "group-blocked" not in adapter._last_chat_req_ids

    def test_remember_chat_req_id_is_bounded(self):
        from plugins.platforms.wecom.adapter import DEDUP_MAX_SIZE, WeComAdapter

        adapter = WeComAdapter(PlatformConfig(enabled=True))
        for i in range(DEDUP_MAX_SIZE + 50):
            adapter._remember_chat_req_id(f"chat-{i}", f"req-{i}")
        assert len(adapter._last_chat_req_ids) <= DEDUP_MAX_SIZE
        # The most recently remembered chat must still be present.
        latest = f"chat-{DEDUP_MAX_SIZE + 49}"
        assert adapter._last_chat_req_ids[latest][-1][0] == f"req-{DEDUP_MAX_SIZE + 49}"


    @pytest.mark.asyncio
    async def test_proactive_group_send_falls_back_to_cached_req_id(self):
        """Sending into a group without reply_to should use the last cached
        req_id via APP_CMD_RESPONSE — WeCom AI Bots cannot initiate APP_CMD_SEND
        in group chats (errcode 600039)."""
        from plugins.platforms.wecom.adapter import WeComAdapter

        adapter = WeComAdapter(PlatformConfig(enabled=True))
        adapter._remember_chat_req_id("group-1", "inbound-req-42")
        adapter._send_reply_request = AsyncMock(
            return_value={"headers": {"req_id": "inbound-req-42"}, "errcode": 0}
        )
        adapter._send_request = AsyncMock(
            return_value={"headers": {"req_id": "new"}, "errcode": 0}
        )

        result = await adapter.send("group-1", "ping", reply_to=None)

        assert result.success is True
        # Must route through reply (APP_CMD_RESPONSE), not proactive send.
        adapter._send_reply_request.assert_awaited_once()
        adapter._send_request.assert_not_awaited()
        args = adapter._send_reply_request.await_args.args
        assert args[0] == "inbound-req-42"
        assert args[1]["msgtype"] == "markdown"
        assert args[1]["markdown"]["content"] == "ping"

    @pytest.mark.asyncio
    async def test_proactive_send_without_cached_req_id_uses_app_cmd_send(self):
        """When we have no prior req_id (fresh DM target), APP_CMD_SEND is used."""
        from plugins.platforms.wecom.adapter import APP_CMD_SEND, WeComAdapter

        adapter = WeComAdapter(PlatformConfig(enabled=True))
        adapter._send_request = AsyncMock(
            return_value={"headers": {"req_id": "new"}, "errcode": 0}
        )

        result = await adapter.send("fresh-dm-chat", "ping", reply_to=None)

        assert result.success is True
        adapter._send_request.assert_awaited_once()
        cmd = adapter._send_request.await_args.args[0]
        assert cmd == APP_CMD_SEND


class TestReplyReqIdExpiry:
    """Tests for TTL-based reply req_id caching and 846604 fallback.

    WeCom callback req_ids expire server-side ~60s after the inbound message.
    Using an expired one returns errcode 846604 ("websocket request expired").
    The adapter must (a) skip stale cache entries via TTL, and (b) fall back
    from aibot_respond_msg to aibot_send_msg when 846604 does slip through.
    """

    @pytest.mark.asyncio
    async def test_send_skips_expired_cached_req_id_and_uses_proactive(self):
        """A cached req_id older than REPLY_REQ_ID_TTL_SECONDS must not be
        used — falls through to aibot_send_msg instead of a doomed reply."""
        from plugins.platforms.wecom.adapter import APP_CMD_SEND, WeComAdapter

        adapter = WeComAdapter(PlatformConfig(enabled=True))
        # Inject a stale entry (2 minutes ago — well past the 50s TTL).
        adapter._last_chat_req_ids["chat-1"] = [
            ("req-old", time.monotonic() - 120),
        ]
        adapter._send_reply_request = AsyncMock()
        adapter._send_request = AsyncMock(
            return_value={"headers": {"req_id": "new"}, "errcode": 0}
        )

        result = await adapter.send("chat-1", "hello")

        assert result.success is True
        adapter._send_reply_request.assert_not_awaited()
        adapter._send_request.assert_awaited_once()
        assert adapter._send_request.await_args.args[0] == APP_CMD_SEND

    @pytest.mark.asyncio
    async def test_send_uses_fresh_cached_req_id_for_reply(self):
        """A cached req_id within TTL should use aibot_respond_msg."""
        from plugins.platforms.wecom.adapter import WeComAdapter

        adapter = WeComAdapter(PlatformConfig(enabled=True))
        adapter._remember_chat_req_id("chat-1", "req-fresh")
        adapter._send_reply_request = AsyncMock(return_value={"errcode": 0})
        adapter._send_request = AsyncMock()

        result = await adapter.send("chat-1", "hello")

        assert result.success is True
        adapter._send_reply_request.assert_awaited_once()
        adapter._send_request.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_send_falls_back_to_proactive_on_846604(self):
        """When reply returns 846604 (expired), retry the same content via
        aibot_send_msg so the message is still delivered."""
        from plugins.platforms.wecom.adapter import APP_CMD_SEND, WeComAdapter

        adapter = WeComAdapter(PlatformConfig(enabled=True))
        adapter._remember_chat_req_id("chat-1", "req-stale")

        reply_response = {
            "errcode": 846604,
            "errmsg": "websocket request expired, response is invalid",
        }
        proactive_response = {"errcode": 0, "headers": {"req_id": "new"}}
        adapter._send_reply_request = AsyncMock(return_value=reply_response)
        adapter._send_request = AsyncMock(return_value=proactive_response)

        result = await adapter.send("chat-1", "hello")

        assert result.success is True
        adapter._send_reply_request.assert_awaited_once()
        adapter._send_request.assert_awaited_once()
        assert adapter._send_request.await_args.args[0] == APP_CMD_SEND
        # The stale req_id must be evicted so the next send doesn't retry it.
        remaining = [rid for rid, _ in adapter._last_chat_req_ids.get("chat-1", [])]
        assert "req-stale" not in remaining

    @pytest.mark.asyncio
    async def test_send_846604_then_proactive_also_fails_returns_error(self):
        """If the proactive fallback ALSO fails, the error surfaces."""
        from plugins.platforms.wecom.adapter import WeComAdapter

        adapter = WeComAdapter(PlatformConfig(enabled=True))
        adapter._remember_chat_req_id("chat-1", "req-stale")
        adapter._send_reply_request = AsyncMock(
            return_value={"errcode": 846604, "errmsg": "expired"}
        )
        adapter._send_request = AsyncMock(
            return_value={"errcode": 600039, "errmsg": "cannot send to group"}
        )

        result = await adapter.send("chat-1", "hello")

        assert result.success is False
        assert "600039" in (result.error or "")

    @pytest.mark.asyncio
    async def test_cleanup_ws_preserves_req_id_caches(self):
        """_cleanup_ws must NOT clear req_id caches — WeCom groups can only
        receive via aibot_respond_msg (reply), and clearing on every ws
        teardown (heartbeat timeout, 846609) would destroy fresh req_ids
        that are still within TTL, making group replies impossible after
        any reconnect. The TTL in _cached_reply_req_id + the 846604
        fallback handle staleness without a blanket clear."""
        from plugins.platforms.wecom.adapter import WeComAdapter

        adapter = WeComAdapter(PlatformConfig(enabled=True))
        adapter._remember_chat_req_id("chat-1", "req-1")
        adapter._remember_reply_req_id("msg-1", "req-1")
        assert adapter._last_chat_req_ids
        assert adapter._reply_req_ids

        await adapter._cleanup_ws()

        # Caches must survive — the req_ids are still within TTL.
        assert adapter._last_chat_req_ids
        assert adapter._reply_req_ids

    def test_cached_reply_req_id_filters_by_ttl(self):
        """_cached_reply_req_id returns None when all entries are stale."""
        from plugins.platforms.wecom.adapter import WeComAdapter

        adapter = WeComAdapter(PlatformConfig(enabled=True))
        adapter._last_chat_req_ids["chat-1"] = [
            ("req-old", time.monotonic() - 200),
            ("req-stale", time.monotonic() - 100),
        ]
        assert adapter._cached_reply_req_id("chat-1") is None

        # Fresh entry should be returned.
        adapter._remember_chat_req_id("chat-1", "req-fresh")
        assert adapter._cached_reply_req_id("chat-1") == "req-fresh"


class TestTextBatchFlushRace:
    """Regression tests for the cancel-delivery race in _flush_text_batch.

    When asyncio.sleep() fires and Task.cancel() is called before the task
    runs, CPython sets _must_cancel but cannot cancel the already-done sleep
    future.  CancelledError is then delivered at the *next* await
    (handle_message), after the task has already popped the event — the
    superseding task sees an empty batch and silently drops the message.
    The fix adds a synchronous task-registry check between the sleep and
    the pop so a superseded task returns before touching the event.
    """

    @pytest.mark.asyncio
    async def test_superseded_task_does_not_pop_or_process_event(self):
        """A flush task that has been superseded must leave the event in the
        batch dict for the new task to handle."""
        from gateway.platforms.base import MessageEvent, MessageType
        from plugins.platforms.wecom.adapter import WeComAdapter

        adapter = WeComAdapter(PlatformConfig(enabled=True))
        adapter._text_batch_delay_seconds = 0

        key = "test-session"
        event = MessageEvent(text="hello", message_type=MessageType.TEXT)
        adapter._pending_text_batches[key] = event

        handle_calls = []

        async def fake_handle(evt):
            handle_calls.append(evt)

        adapter.handle_message = fake_handle

        # Create T1 and register it.
        t1 = asyncio.create_task(adapter._flush_text_batch(key))
        adapter._pending_text_batch_tasks[key] = t1

        # Simulate T2 superseding T1 before T1 wakes from sleep.
        t2 = asyncio.create_task(asyncio.sleep(0.2))
        adapter._pending_text_batch_tasks[key] = t2

        # Yield long enough for T1's sleep(0) to complete and T1 to run.
        await asyncio.sleep(0.05)

        t2.cancel()
        try:
            await t2
        except asyncio.CancelledError:
            pass

        # T1 must have returned without processing or removing the event.
        assert handle_calls == [], "superseded task must not call handle_message"
        assert adapter._pending_text_batches.get(key) is event, (
            "superseded task must not pop the event"
        )

    @pytest.mark.asyncio
    async def test_active_task_processes_event_normally(self):
        """When the task is not superseded it must still process the event."""
        from gateway.platforms.base import MessageEvent, MessageType
        from plugins.platforms.wecom.adapter import WeComAdapter

        adapter = WeComAdapter(PlatformConfig(enabled=True))
        adapter._text_batch_delay_seconds = 0

        key = "test-session"
        event = MessageEvent(text="world", message_type=MessageType.TEXT)
        adapter._pending_text_batches[key] = event

        handle_calls = []

        async def fake_handle(evt):
            handle_calls.append(evt)

        adapter.handle_message = fake_handle

        t1 = asyncio.create_task(adapter._flush_text_batch(key))
        adapter._pending_text_batch_tasks[key] = t1

        # No superseding task — T1 should process normally.
        await asyncio.sleep(0.05)

        assert handle_calls == [event], "active task must call handle_message"
        assert adapter._pending_text_batches.get(key) is None, (
            "active task must pop the event after processing"
        )


class TestSendRequestRetry:
    """Fix #1: _send_request retries through the reconnect window.

    A send that hits errcode 846609 (or the pre-send 'not connected' guard
    during the death window) must wait for the socket to come back and retry,
    instead of surfacing a failure to the user during the ~2-3s reconnect."""

    def _adapter(self):
        from plugins.platforms.wecom.adapter import WeComAdapter
        return WeComAdapter(PlatformConfig(enabled=True))

    def test_retries_on_subscription_death_then_succeeds(self):
        adapter = self._adapter()
        death = {"errcode": 846609, "errmsg": "aibot websocket not subscribed"}
        ok = {"errcode": 0, "msgid": "ok"}
        calls = []

        async def fake_once(cmd, body, timeout):
            calls.append(cmd)
            # the listen loop reconnects before the retry attempt fires
            adapter._ws_live.set()
            return death if len(calls) == 1 else ok

        adapter._send_request_once = fake_once
        from plugins.platforms.wecom.adapter import APP_CMD_SEND

        result = asyncio.run(adapter._send_request(APP_CMD_SEND, {"chatid": "u1"}))

        assert result == ok
        assert len(calls) == 2, "send must retry once after 846609"

    def test_returns_846609_when_socket_never_returns(self, monkeypatch):
        """If the socket never comes back within the budget, the 846609 is
        surfaced (no infinite retry loop)."""
        adapter = self._adapter()
        monkeypatch.setattr(
            "plugins.platforms.wecom.adapter.SEND_RETRY_BUDGET_SECONDS", 0.05
        )
        death = {"errcode": 846609, "errmsg": "dead"}
        calls = []

        async def fake_once(cmd, body, timeout):
            calls.append(cmd)
            return death  # _ws_live never set → no reconnect

        adapter._send_request_once = fake_once
        from plugins.platforms.wecom.adapter import APP_CMD_SEND

        result = asyncio.run(adapter._send_request(APP_CMD_SEND, {"chatid": "u1"}))

        assert result["errcode"] == 846609

    def test_retries_when_ws_not_connected(self):
        """The pre-send 'not connected' RuntimeError is also retried."""
        adapter = self._adapter()
        ok = {"errcode": 0, "msgid": "ok"}
        calls = []

        async def fake_once(cmd, body, timeout):
            calls.append(cmd)
            if len(calls) == 1:
                # socket recovers (listen loop reconnects) despite this attempt
                # hitting the pre-send "not connected" guard
                adapter._ws_live.set()
                raise RuntimeError("WeCom websocket is not connected")
            return ok

        adapter._send_request_once = fake_once
        from plugins.platforms.wecom.adapter import APP_CMD_SEND

        result = asyncio.run(adapter._send_request(APP_CMD_SEND, {"chatid": "u1"}))

        assert result == ok
        assert len(calls) == 2

    def test_ping_is_not_retried(self):
        """Heartbeat ping owns its own deadline + forced reconnect; it must
        bypass the retry wrapper (called exactly once)."""
        adapter = self._adapter()
        calls = []

        async def fake_once(cmd, body, timeout):
            calls.append(cmd)
            raise RuntimeError("ping should not retry")

        adapter._send_request_once = fake_once
        from plugins.platforms.wecom.adapter import APP_CMD_PING

        with pytest.raises(RuntimeError):
            asyncio.run(adapter._send_request(APP_CMD_PING, {}))

        assert len(calls) == 1, "APP_CMD_PING must not be retried"

    def test_cleanup_ws_clears_ws_live(self):
        adapter = self._adapter()
        adapter._ws_live.set()
        assert adapter._ws_live.is_set()
        asyncio.run(adapter._cleanup_ws())
        assert not adapter._ws_live.is_set()

    def test_subscription_death_clears_ws_live(self):
        adapter = self._adapter()
        adapter._ws_live.set()
        asyncio.run(adapter._handle_subscription_death({"errcode": 846609}))
        assert not adapter._ws_live.is_set()


class TestWeComStreaming:
    """Tests for progressive streaming via msgtype: ``stream``."""

    @pytest.fixture
    def adapter(self):
        from plugins.platforms.wecom.adapter import WeComAdapter

        return WeComAdapter(PlatformConfig(enabled=True))

    # ── send() — streaming first frame ──

    @pytest.mark.asyncio
    async def test_send_creates_stream_first_frame(self, adapter):
        """send() with expect_edits and valid reply_to must send
        msgtype: stream, finish: false, and return the stream id."""
        adapter._reply_req_ids["msg-1"] = "req-1"
        adapter._send_reply_request = AsyncMock(
            return_value={"headers": {"req_id": "req-1"}, "errcode": 0}
        )

        result = await adapter.send(
            "chat-123",
            "hello streaming",
            reply_to="msg-1",
            metadata={"expect_edits": True},
        )

        assert result.success is True
        assert result.message_id is not None
        stream_id = result.message_id
        assert len(stream_id) == 12  # uuid4.hex[:12]

        adapter._send_reply_request.assert_awaited_once()
        args = adapter._send_reply_request.await_args.args
        assert args[0] == "req-1"
        body = args[1]
        assert body["msgtype"] == "stream"
        assert body["stream"]["id"] == stream_id
        assert body["stream"]["finish"] is False
        assert body["stream"]["content"] == "hello streaming"

        # Stream state must be stored for edit_message()
        assert stream_id in adapter._stream_states
        assert adapter._stream_states[stream_id]["reply_req_id"] == "req-1"

    @pytest.mark.asyncio
    async def test_send_stream_falls_back_without_expect_edits(self, adapter):
        """send() without expect_edits must fall through to markdown."""
        adapter._reply_req_ids["msg-1"] = "req-1"
        adapter._send_reply_request = AsyncMock(
            return_value={"headers": {"req_id": "req-1"}, "errcode": 0}
        )

        result = await adapter.send(
            "chat-123",
            "hello markdown",
            reply_to="msg-1",
            metadata={"notify": True},
        )

        assert result.success is True
        adapter._send_reply_request.assert_awaited_once()
        args = adapter._send_reply_request.await_args.args
        assert args[1]["msgtype"] == "markdown"
        # Stream state must NOT be polluted by non-streaming sends
        assert adapter._stream_states == {}

    @pytest.mark.asyncio
    async def test_send_stream_skipped_without_metadata(self, adapter):
        """send() with metadata=None must use markdown path."""
        adapter._reply_req_ids["msg-1"] = "req-1"
        adapter._send_reply_request = AsyncMock(
            return_value={"headers": {"req_id": "req-1"}, "errcode": 0}
        )

        result = await adapter.send("chat-123", "hello", reply_to="msg-1")

        assert result.success is True
        adapter._send_reply_request.assert_awaited_once()
        args = adapter._send_reply_request.await_args.args
        assert args[1]["msgtype"] == "markdown"
        assert adapter._stream_states == {}

    @pytest.mark.asyncio
    async def test_send_stream_falls_back_without_reply_context(self, adapter):
        """send() with expect_edits but no reply_req_id must fall back
        to proactive markdown send — you cannot stream via aibot_respond_msg
        without a reply context."""
        adapter._send_request = AsyncMock(
            return_value={"headers": {"req_id": "req-1"}, "errcode": 0}
        )

        result = await adapter.send(
            "chat-123",
            "hello",
            reply_to=None,
            metadata={"expect_edits": True},
        )

        assert result.success is True
        adapter._send_request.assert_awaited_once()
        from plugins.platforms.wecom.adapter import APP_CMD_SEND

        cmd = adapter._send_request.await_args.args[0]
        assert cmd == APP_CMD_SEND
        assert adapter._stream_states == {}

    @pytest.mark.asyncio
    async def test_send_stream_error_cleans_up_state(self, adapter):
        """When the stream first frame returns an error, _stream_states must
        be cleaned up and SendResult(success=False) returned."""
        adapter._reply_req_ids["msg-1"] = "req-1"
        adapter._send_reply_request = AsyncMock(
            return_value={"errcode": 40001, "errmsg": "bad request"}
        )

        result = await adapter.send(
            "chat-123",
            "hello",
            reply_to="msg-1",
            metadata={"expect_edits": True},
        )

        assert result.success is False
        assert "40001" in (result.error or "")
        # Stream state must be cleaned up on failure
        assert adapter._stream_states == {}

    @pytest.mark.asyncio
    async def test_send_stream_uses_max_message_length_truncation(self, adapter):
        """Stream first frame must truncate content to MAX_MESSAGE_LENGTH."""
        adapter._reply_req_ids["msg-1"] = "req-1"
        adapter._send_reply_request = AsyncMock(
            return_value={"headers": {"req_id": "req-1"}, "errcode": 0}
        )
        from plugins.platforms.wecom.adapter import MAX_MESSAGE_LENGTH

        long = "x" * (MAX_MESSAGE_LENGTH + 100)

        result = await adapter.send(
            "chat-123",
            long,
            reply_to="msg-1",
            metadata={"expect_edits": True},
        )

        assert result.success is True
        args = adapter._send_reply_request.await_args.args
        assert len(args[1]["stream"]["content"]) == MAX_MESSAGE_LENGTH

    # ── send() — proactive non-streaming ──

    @pytest.mark.asyncio
    async def test_send_proactive_markdown_unchanged(self, adapter):
        """Proactive send (no reply_to, no metadata) must still use
        APP_CMD_SEND with markdown, exactly as before."""
        adapter._send_request = AsyncMock(
            return_value={"headers": {"req_id": "req-1"}, "errcode": 0}
        )
        from plugins.platforms.wecom.adapter import APP_CMD_SEND

        result = await adapter.send("chat-123", "hello proactive")

        assert result.success is True
        adapter._send_request.assert_awaited_once_with(
            APP_CMD_SEND,
            {
                "chatid": "chat-123",
                "msgtype": "markdown",
                "markdown": {"content": "hello proactive"},
            },
        )

    # ── edit_message() ──

    @pytest.mark.asyncio
    async def test_edit_message_update(self, adapter):
        """edit_message with finalize=False must send a stream update
        and preserve stream state."""
        stream_id = "abc123def456"
        adapter._stream_states[stream_id] = {
            "reply_req_id": "req-1",
            "created_at": 1000.0,
        }
        adapter._send_reply_request = AsyncMock(
            return_value={"headers": {"req_id": "req-1"}, "errcode": 0}
        )

        result = await adapter.edit_message(
            "chat-123", stream_id, "updated content", finalize=False
        )

        assert result.success is True
        assert result.message_id == stream_id
        adapter._send_reply_request.assert_awaited_once()
        args = adapter._send_reply_request.await_args.args
        assert args[0] == "req-1"
        body = args[1]
        assert body["msgtype"] == "stream"
        assert body["stream"]["id"] == stream_id
        assert body["stream"]["finish"] is False
        assert body["stream"]["content"] == "updated content"

        # State must survive non-finalize edit
        assert stream_id in adapter._stream_states

    @pytest.mark.asyncio
    async def test_edit_message_finalize(self, adapter):
        """edit_message with finalize=True must send finish: true and
        clean up stream state."""
        stream_id = "abc123def456"
        adapter._stream_states[stream_id] = {
            "reply_req_id": "req-1",
            "created_at": 1000.0,
        }
        adapter._send_reply_request = AsyncMock(
            return_value={"headers": {"req_id": "req-1"}, "errcode": 0}
        )

        result = await adapter.edit_message(
            "chat-123", stream_id, "final content", finalize=True
        )

        assert result.success is True
        args = adapter._send_reply_request.await_args.args
        assert args[1]["stream"]["finish"] is True
        # State must be cleaned up after finalize
        assert stream_id not in adapter._stream_states

    @pytest.mark.asyncio
    async def test_edit_message_unknown_stream(self, adapter):
        """edit_message with an unknown stream_id must return
        success=False so the consumer enters fallback mode."""
        result = await adapter.edit_message(
            "chat-123", "nonexistent-stream", "content", finalize=False
        )

        assert result.success is False
        assert "not found" in (result.error or "")

    @pytest.mark.asyncio
    async def test_edit_message_bad_state(self, adapter):
        """edit_message with a stream that has no reply_req_id must
        return success=False."""
        stream_id = "bad-stream"
        adapter._stream_states[stream_id] = {}  # missing reply_req_id

        result = await adapter.edit_message(
            "chat-123", stream_id, "content", finalize=False
        )

        assert result.success is False

    @pytest.mark.asyncio
    async def test_edit_message_timeout(self, adapter):
        """edit_message must report TimeoutError as success=False."""
        stream_id = "stream-1"
        adapter._stream_states[stream_id] = {
            "reply_req_id": "req-1",
            "created_at": 1000.0,
        }
        adapter._send_reply_request = AsyncMock(side_effect=asyncio.TimeoutError)

        result = await adapter.edit_message(
            "chat-123", stream_id, "content", finalize=False
        )

        assert result.success is False
        assert "Timeout" in (result.error or "")
        # State must NOT be cleaned up on timeout (no finalize)
        assert stream_id in adapter._stream_states

    @pytest.mark.asyncio
    async def test_edit_message_truncates_to_max_length(self, adapter):
        """edit_message must truncate content to MAX_MESSAGE_LENGTH."""
        from plugins.platforms.wecom.adapter import MAX_MESSAGE_LENGTH

        stream_id = "stream-1"
        adapter._stream_states[stream_id] = {
            "reply_req_id": "req-1",
            "created_at": 1000.0,
        }
        adapter._send_reply_request = AsyncMock(
            return_value={"headers": {"req_id": "req-1"}, "errcode": 0}
        )
        long = "x" * (MAX_MESSAGE_LENGTH + 100)

        result = await adapter.edit_message(
            "chat-123", stream_id, long, finalize=False
        )

        assert result.success is True
        args = adapter._send_reply_request.await_args.args
        assert len(args[1]["stream"]["content"]) == MAX_MESSAGE_LENGTH

    # ── Integration: send + edit_message cycle ──

    @pytest.mark.asyncio
    async def test_send_then_edit_cycle(self, adapter):
        """Verify a full send→edit→finalize cycle works correctly."""
        adapter._reply_req_ids["msg-1"] = "req-1"
        send_responses = iter(
            [
                {"headers": {"req_id": "req-1"}, "errcode": 0},
                {"headers": {"req_id": "req-1"}, "errcode": 0},
                {"headers": {"req_id": "req-1"}, "errcode": 0},
            ]
        )
        adapter._send_reply_request = AsyncMock(side_effect=lambda *a, **kw: next(send_responses))

        # Step 1: First frame
        r1 = await adapter.send(
            "chat-123", "part 1", reply_to="msg-1", metadata={"expect_edits": True}
        )
        assert r1.success is True
        sid = r1.message_id

        # Step 2: Update
        r2 = await adapter.edit_message("chat-123", sid, "part 1 part 2", finalize=False)
        assert r2.success is True
        assert sid in adapter._stream_states  # still active

        # Step 3: Finalize
        r3 = await adapter.edit_message("chat-123", sid, "part 1 part 2 part 3", finalize=True)
        assert r3.success is True
        assert sid not in adapter._stream_states  # cleaned up

        # All three requests used the same reply_req_id
        calls = adapter._send_reply_request.await_args_list
        assert len(calls) == 3
        for call in calls:
            assert call.args[0] == "req-1"
            assert call.args[1]["msgtype"] == "stream"
        # Stream IDs match
        assert calls[0].args[1]["stream"]["id"] == sid
        assert calls[1].args[1]["stream"]["id"] == sid
        assert calls[2].args[1]["stream"]["id"] == sid
        # Finish flags
        assert calls[0].args[1]["stream"]["finish"] is False
        assert calls[1].args[1]["stream"]["finish"] is False
        assert calls[2].args[1]["stream"]["finish"] is True


class TestListenLoopReconnectFix:
    """Regression tests for the post-auth-failure zombie-state deadlock.

    Symptom: when ``_open_connection`` raises mid-handshake (server closed the
    socket after the SUBSCRIBE frame), ``_ws`` is left set-but-closed. The
    outer ``_listen_loop`` calls ``_read_events`` which sees ``_ws.closed=True``
    and silently returns, then resets ``backoff_idx`` and loops again with no
    sleep — a CPU-spinning tight loop with no logs and no further reconnect
    attempts. All sends then fail with "WeCom websocket is not connected".
    """

    @pytest.mark.asyncio
    async def test_read_events_raises_when_ws_already_closed(self):
        """If ``_ws`` is set but already closed, ``_read_events`` must raise.

        Otherwise the outer ``_listen_loop`` would treat it as a clean exit,
        reset ``backoff_idx`` to 0, and spin forever without reconnecting.
        """
        from plugins.platforms.wecom.adapter import WeComAdapter

        adapter = WeComAdapter(PlatformConfig(enabled=True))
        adapter._ws = MagicMock()
        adapter._ws.closed = True

        with pytest.raises(RuntimeError, match=r"(?i)closed|not connected"):
            await adapter._read_events()

    @pytest.mark.asyncio
    async def test_listen_loop_retries_reconnect_after_open_connection_failure(self):
        """After ``_open_connection`` fails, ``_listen_loop`` must keep retrying
        instead of CPU-spinning in a tight no-op loop."""
        from plugins.platforms.wecom.adapter import WeComAdapter

        adapter = WeComAdapter(PlatformConfig(enabled=True))
        adapter._running = True

        # Simulate a closed-but-set _ws (the leftover state after a failed
        # handshake). _read_events should raise so the outer loop reconnects.
        closed_ws = MagicMock()
        closed_ws.closed = True
        adapter._ws = closed_ws

        # Stub _open_connection so we can count reconnect attempts. Each
        # attempt first calls _cleanup_ws (via _open_connection), which resets
        # _ws to None; we want to keep _ws in the broken state on the first
        # attempt to prove the retry loop observes the failure. Easier: have
        # _open_connection fail every time and assert the call count grows
        # over time, rather than assuming the loop's backoff sleep fires.
        attempt_counter = {"n": 0}

        async def always_fail_open_connection():
            attempt_counter["n"] += 1
            # Mimic the real failure: _ws is set to a closed socket, then we
            # raise before subscribe completes.
            closed = MagicMock()
            closed.closed = True
            adapter._ws = closed
            raise RuntimeError("WeCom websocket closed during authentication")

        adapter._open_connection = always_fail_open_connection
        # Avoid touching real mark_connected / cleanup branches.
        adapter._mark_connected = lambda: None

        # Run the listen loop in the background; cap with a short wall clock
        # and inspect call count. Without the fix the loop spins on a closed
        # _ws and never reaches _open_connection; with the fix each iteration
        # sleeps RECONNECT_BACKOFF[0]=2s before the next attempt. We give it
        # ~2.5s and expect >= 1 attempt to prove the retry path is reachable.
        task = asyncio.create_task(adapter._listen_loop())
        try:
            await asyncio.sleep(2.5)
        finally:
            adapter._running = False
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass

        assert attempt_counter["n"] >= 1, (
            f"_listen_loop made {attempt_counter['n']} reconnect attempts in "
            "2.5s — expected >= 1. If this is 0, the loop is CPU-spinning "
            "because _read_events returned silently on a closed _ws."
        )

    @pytest.mark.asyncio
    async def test_open_connection_resets_ws_state_on_mid_handshake_failure(self):
        """Defense-in-depth: when ``_open_connection`` raises after setting
        ``_ws`` (e.g. server closes mid-handshake), ``_ws`` must be reset to
        ``None`` so the next read attempts the "not connected" branch instead
        of operating on a stale closed-but-set socket.
        """
        from plugins.platforms.wecom.adapter import WeComAdapter

        adapter = WeComAdapter(PlatformConfig(enabled=True))
        adapter._bot_id = "test-bot"
        adapter._secret = "test-secret"

        class _ClosedDuringHandshakeWS:
            # Initially looks connected so _send_json (SUBSCRIBE) succeeds,
            # then server-side close fires during _wait_for_handshake — the
            # exact production symptom at 19:06:57.
            closed = False

            async def send_json(self, payload):
                # Flip to closed after SUBSCRIBE is sent, mirroring the
                # race where the server closes mid-handshake.
                self.closed = True
                return None

            async def close(self):
                return None

        class _FakeSession:
            def __init__(self, *args, **kwargs):
                pass

            async def ws_connect(self, *args, **kwargs):
                return _ClosedDuringHandshakeWS()

            async def close(self):
                return None

        async def _fake_handshake(req_id):
            # Server-side close during handshake — matches the prod symptom.
            raise RuntimeError("WeCom websocket closed during authentication")

        adapter._cleanup_ws = AsyncMock()
        adapter._wait_for_handshake = _fake_handshake

        with patch("plugins.platforms.wecom.adapter.aiohttp.ClientSession", _FakeSession):
            with pytest.raises(RuntimeError, match="during authentication"):
                await adapter._open_connection()

        # The failure must not leave _ws pointing at a stale closed socket.
        assert adapter._ws is None, (
            "_open_connection failed mid-handshake but _ws was not reset; "
            "next _read_events would see a closed-but-set _ws"
        )
        # Session must also be cleaned up to avoid the aiohttp connector leak.
        assert adapter._session is None, (
            "_open_connection failed but _session was not reset"
        )
