"""Tests for the WeCom platform adapter."""

import asyncio
import base64
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from gateway.config import PlatformConfig
from gateway.platforms.base import SendResult


class TestWeComRequirements:
    def test_returns_false_without_aiohttp(self, monkeypatch):
        monkeypatch.setattr("gateway.platforms.wecom.AIOHTTP_AVAILABLE", False)
        monkeypatch.setattr("gateway.platforms.wecom.HTTPX_AVAILABLE", True)
        from gateway.platforms.wecom import check_wecom_requirements

        assert check_wecom_requirements() is False

    def test_returns_false_without_httpx(self, monkeypatch):
        monkeypatch.setattr("gateway.platforms.wecom.AIOHTTP_AVAILABLE", True)
        monkeypatch.setattr("gateway.platforms.wecom.HTTPX_AVAILABLE", False)
        from gateway.platforms.wecom import check_wecom_requirements

        assert check_wecom_requirements() is False

    def test_returns_true_when_available(self, monkeypatch):
        monkeypatch.setattr("gateway.platforms.wecom.AIOHTTP_AVAILABLE", True)
        monkeypatch.setattr("gateway.platforms.wecom.HTTPX_AVAILABLE", True)
        from gateway.platforms.wecom import check_wecom_requirements

        assert check_wecom_requirements() is True


class TestWeComAdapterInit:
    def test_declares_non_editable_message_capability(self):
        from gateway.platforms.wecom import WeComAdapter

        assert WeComAdapter.SUPPORTS_MESSAGE_EDITING is False

    def test_reads_config_from_extra(self):
        from gateway.platforms.wecom import WeComAdapter

        config = PlatformConfig(
            enabled=True,
            extra={
                "bot_id": "cfg-bot",
                "secret": "cfg-secret",
                "websocket_url": "wss://custom.wecom.example/ws",
                "group_policy": "allowlist",
                "group_allow_from": ["group-1"],
            },
        )
        adapter = WeComAdapter(config)

        assert adapter._bot_id == "cfg-bot"
        assert adapter._secret == "cfg-secret"
        assert adapter._ws_url == "wss://custom.wecom.example/ws"
        assert adapter._group_policy == "allowlist"
        assert adapter._group_allow_from == ["group-1"]

    def test_falls_back_to_env_vars(self, monkeypatch):
        monkeypatch.setenv("WECOM_BOT_ID", "env-bot")
        monkeypatch.setenv("WECOM_SECRET", "env-secret")
        monkeypatch.setenv("WECOM_WEBSOCKET_URL", "wss://env.example/ws")
        from gateway.platforms.wecom import WeComAdapter

        adapter = WeComAdapter(PlatformConfig(enabled=True))
        assert adapter._bot_id == "env-bot"
        assert adapter._secret == "env-secret"
        assert adapter._ws_url == "wss://env.example/ws"


class TestWeComConnect:
    @pytest.mark.asyncio
    async def test_connect_records_missing_credentials(self, monkeypatch):
        import gateway.platforms.wecom as wecom_module
        from gateway.platforms.wecom import WeComAdapter

        monkeypatch.setattr(wecom_module, "AIOHTTP_AVAILABLE", True)
        monkeypatch.setattr(wecom_module, "HTTPX_AVAILABLE", True)

        adapter = WeComAdapter(PlatformConfig(enabled=True))

        success = await adapter.connect()

        assert success is False
        assert adapter.has_fatal_error is True
        assert adapter.fatal_error_code == "wecom_missing_credentials"
        assert "WECOM_BOT_ID" in (adapter.fatal_error_message or "")

    @pytest.mark.asyncio
    async def test_connect_records_handshake_failure_details(self, monkeypatch):
        import gateway.platforms.wecom as wecom_module
        from gateway.platforms.wecom import WeComAdapter

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
    @patch("gateway.platforms.wecom.time")
    @patch("gateway.platforms.wecom.json.loads")
    @patch("gateway.platforms.wecom.logger")
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
        from gateway.platforms.wecom import qr_scan_for_bot_info

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
    async def test_send_uses_passive_reply_markdown_when_reply_context_exists(self):
        from gateway.platforms.wecom import WeComAdapter

        adapter = WeComAdapter(PlatformConfig(enabled=True))
        # Mock a healthy WebSocket so the production-grade
        # _send_with_reconnect_retry wrapper (added in v0.16.0 for the
        # ercode-846609 auto-retry path) skips its pre-send liveness
        # reconnect. Without this mock the wrapper would attempt a
        # real websocket connect inside the test and time out.
        adapter._ws = AsyncMock(closed=False)
        adapter._reply_req_ids["msg-1"] = "req-1"
        adapter._send_reply_request = AsyncMock(
            return_value={"headers": {"req_id": "req-1"}, "errcode": 0}
        )

        result = await adapter.send("chat-123", "hello from reply", reply_to="msg-1")

        assert result.success is True
        adapter._send_reply_request.assert_awaited_once()
        args = adapter._send_reply_request.await_args.args
        assert args[0] == "req-1"
        # msgtype: stream triggers WeCom errcode 600039 on many mobile clients
        # (unsupported type). Markdown renders everywhere.
        assert args[1]["msgtype"] == "markdown"
        assert args[1]["markdown"]["content"] == "hello from reply"

    @pytest.mark.asyncio
    async def test_send_image_file_uses_passive_reply_media_when_reply_context_exists(self):
        from gateway.platforms.wecom import WeComAdapter

        adapter = WeComAdapter(PlatformConfig(enabled=True))
        # See comment in test_send_uses_passive_reply_markdown_… above —
        # the production _send_with_reconnect_retry wrapper triggers a
        # real reconnect on _ws=None, which times out the test.
        adapter._ws = AsyncMock(closed=False)
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
    def test_extracts_plain_text(self):
        from gateway.platforms.wecom import WeComAdapter

        body = {
            "msgtype": "text",
            "text": {"content": "  hello world  "},
        }
        text, reply_text = WeComAdapter._extract_text(body)
        assert text == "hello world"
        assert reply_text is None

    def test_extracts_mixed_text(self):
        from gateway.platforms.wecom import WeComAdapter

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

    def test_extracts_voice_and_quote(self):
        from gateway.platforms.wecom import WeComAdapter

        body = {
            "msgtype": "voice",
            "voice": {"content": "spoken text"},
            "quote": {"msgtype": "text", "text": {"content": "quoted"}},
        }
        text, reply_text = WeComAdapter._extract_text(body)
        assert text == "spoken text"
        assert reply_text == "quoted"


class TestCallbackDispatch:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("cmd", ["aibot_msg_callback", "aibot_callback"])
    async def test_dispatch_accepts_new_and_legacy_callback_cmds(self, cmd):
        from gateway.platforms.wecom import WeComAdapter

        adapter = WeComAdapter(PlatformConfig(enabled=True))
        adapter._on_message = AsyncMock()

        await adapter._dispatch_payload({"cmd": cmd, "headers": {"req_id": "req-1"}, "body": {}})

        adapter._on_message.assert_awaited_once()


class TestPolicyHelpers:
    def test_dm_allowlist(self):
        from gateway.platforms.wecom import WeComAdapter

        adapter = WeComAdapter(
            PlatformConfig(enabled=True, extra={"dm_policy": "allowlist", "allow_from": ["user-1"]})
        )
        assert adapter._is_dm_allowed("user-1") is True
        assert adapter._is_dm_allowed("user-2") is False

    def test_dm_allowlist_honors_env_only_allowed_users(self, monkeypatch):
        """Env-only setup (WECOM_DM_POLICY + WECOM_ALLOWED_USERS, no config
        ``extra``) must populate the DM allowlist. Otherwise ``dm_policy:
        allowlist`` runs with an empty allowlist and drops every listed user
        at intake — the documented env vars become no-ops."""
        from gateway.platforms.wecom import WeComAdapter

        monkeypatch.setenv("WECOM_DM_POLICY", "allowlist")
        monkeypatch.setenv("WECOM_ALLOWED_USERS", "user-1, user-2")

        adapter = WeComAdapter(PlatformConfig(enabled=True))

        assert adapter._dm_policy == "allowlist"
        assert adapter._allow_from == ["user-1", "user-2"]
        assert adapter._is_dm_allowed("user-1") is True
        assert adapter._is_dm_allowed("user-2") is True
        assert adapter._is_dm_allowed("stranger") is False

    def test_dm_allowlist_extra_takes_precedence_over_env(self, monkeypatch):
        """Config ``extra`` wins over the env fallback, so an explicit
        allowlist is never silently widened by a stray WECOM_ALLOWED_USERS."""
        from gateway.platforms.wecom import WeComAdapter

        monkeypatch.setenv("WECOM_ALLOWED_USERS", "env-user")

        adapter = WeComAdapter(
            PlatformConfig(enabled=True, extra={"dm_policy": "allowlist", "allow_from": ["cfg-user"]})
        )

        assert adapter._allow_from == ["cfg-user"]
        assert adapter._is_dm_allowed("cfg-user") is True
        assert adapter._is_dm_allowed("env-user") is False

    def test_group_allowlist_and_per_group_sender_allowlist(self):
        from gateway.platforms.wecom import WeComAdapter

        adapter = WeComAdapter(
            PlatformConfig(
                enabled=True,
                extra={
                    "group_policy": "allowlist",
                    "group_allow_from": ["group-1"],
                    "groups": {"group-1": {"allow_from": ["user-1"]}},
                },
            )
        )

        assert adapter._is_group_allowed("group-1", "user-1") is True
        assert adapter._is_group_allowed("group-1", "user-2") is False
        assert adapter._is_group_allowed("group-2", "user-1") is False


class TestMediaHelpers:
    def test_detect_wecom_media_type(self):
        from gateway.platforms.wecom import WeComAdapter

        assert WeComAdapter._detect_wecom_media_type("image/png") == "image"
        assert WeComAdapter._detect_wecom_media_type("video/mp4") == "video"
        assert WeComAdapter._detect_wecom_media_type("audio/amr") == "voice"
        assert WeComAdapter._detect_wecom_media_type("application/pdf") == "file"

    def test_voice_non_amr_downgrades_to_file(self):
        from gateway.platforms.wecom import WeComAdapter

        result = WeComAdapter._apply_file_size_limits(128, "voice", "audio/mpeg")

        assert result["final_type"] == "file"
        assert result["downgraded"] is True
        assert "AMR" in (result["downgrade_note"] or "")

    def test_oversized_file_is_rejected(self):
        from gateway.platforms.wecom import ABSOLUTE_MAX_BYTES, WeComAdapter

        result = WeComAdapter._apply_file_size_limits(ABSOLUTE_MAX_BYTES + 1, "file", "application/pdf")

        assert result["rejected"] is True
        assert "20MB" in (result["reject_reason"] or "")

    def test_decode_base64_pads_unpadded_wecom_payloads(self):
        """Regression test for wehermes issue 2026-06-24.

        WeCom strips trailing ``=`` from base64 strings in some
        callbacks, so ``image.base64`` arrives as 43 chars
        (``binascii.Error: ... number of data characters 3 cannot be 1
        more than a multiple of 4``). ``_decode_base64`` must pad to a
        multiple of 4 before decoding.
        """
        import base64
        from gateway.platforms.wecom import WeComAdapter

        # Round-trip: 32 random bytes → base64 → strip padding → re-decode
        raw = os.urandom(32)
        encoded = base64.b64encode(raw).decode()
        unpadded = encoded.rstrip("=")
        assert len(unpadded) % 4 != 0, "test precondition: stripped payload must be unpadded"

        decoded = WeComAdapter._decode_base64(unpadded)
        assert decoded == raw

        # The literal 43-char example from the issue.
        literal = "S4vLcShLQvWd1Y1G27IjGRrZtmKo2EEPql2ttk1thRA"
        assert len(literal) == 43
        decoded_literal = WeComAdapter._decode_base64(literal)
        assert len(decoded_literal) == 32

        # data: URI prefix still handled (only the post-comma segment is decoded)
        with_prefix = WeComAdapter._decode_base64("data:image/png;base64," + encoded)
        assert with_prefix == raw

    def test_url_safety_disabled_by_default(self):
        """Default behaviour must keep SSRF protection enabled.

        Opt-out is a per-deployment decision (Tencent Cloud COS,
        corporate proxy, VPN-routed egress). See
        ``docs/issues/hermes-agent-wecom-regressions-2026-06-24.md``.
        """
        from gateway.config import PlatformConfig
        from gateway.platforms.wecom import WeComAdapter

        adapter = WeComAdapter(PlatformConfig(enabled=True))
        assert adapter._url_safety_disabled() is False

    def test_url_safety_disabled_via_extra(self):
        from gateway.config import PlatformConfig
        from gateway.platforms.wecom import WeComAdapter

        adapter = WeComAdapter(
            PlatformConfig(enabled=True, extra={"disable_url_safety": True})
        )
        assert adapter._url_safety_disabled() is True

    def test_url_safety_disabled_via_env_var(self, monkeypatch):
        from gateway.config import PlatformConfig
        from gateway.platforms.wecom import WeComAdapter

        monkeypatch.setenv("WECOM_DISABLE_URL_SAFETY", "true")
        adapter = WeComAdapter(PlatformConfig(enabled=True))
        assert adapter._url_safety_disabled() is True

    def test_url_safety_env_var_truthy_parsing(self, monkeypatch):
        """``WECOM_DISABLE_URL_SAFETY`` accepts 1/true/yes/on (case-insensitive)."""
        from gateway.config import PlatformConfig
        from gateway.platforms.wecom import WeComAdapter

        for truthy in ("1", "true", "TRUE", "Yes", "on", "  yes  "):
            monkeypatch.setenv("WECOM_DISABLE_URL_SAFETY", truthy)
            adapter = WeComAdapter(PlatformConfig(enabled=True))
            assert adapter._url_safety_disabled() is True, f"failed for {truthy!r}"

        for falsy in ("", "0", "false", "no", "off", "anything-else"):
            monkeypatch.setenv("WECOM_DISABLE_URL_SAFETY", falsy)
            adapter = WeComAdapter(PlatformConfig(enabled=True))
            assert adapter._url_safety_disabled() is False, f"failed for {falsy!r}"

    def test_decrypt_file_bytes_round_trip(self):
        from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
        from gateway.platforms.wecom import WeComAdapter

        plaintext = b"wecom-secret"
        key = os.urandom(32)
        pad_len = 32 - (len(plaintext) % 32)
        padded = plaintext + bytes([pad_len]) * pad_len
        encryptor = Cipher(algorithms.AES(key), modes.CBC(key[:16])).encryptor()
        encrypted = encryptor.update(padded) + encryptor.finalize()

        decrypted = WeComAdapter._decrypt_file_bytes(encrypted, base64.b64encode(key).decode("ascii"))

        assert decrypted == plaintext

    @pytest.mark.asyncio
    async def test_load_outbound_media_rejects_placeholder_path(self):
        from gateway.platforms.wecom import WeComAdapter

        adapter = WeComAdapter(PlatformConfig(enabled=True))

        with pytest.raises(ValueError, match="placeholder was not replaced"):
            await adapter._load_outbound_media("<path>")


class TestMediaUpload:
    @pytest.mark.asyncio
    async def test_upload_media_bytes_uses_sdk_sequence(self, monkeypatch):
        import gateway.platforms.wecom as wecom_module
        from gateway.platforms.wecom import (
            APP_CMD_UPLOAD_MEDIA_CHUNK,
            APP_CMD_UPLOAD_MEDIA_FINISH,
            APP_CMD_UPLOAD_MEDIA_INIT,
            WeComAdapter,
        )

        adapter = WeComAdapter(PlatformConfig(enabled=True))
        calls = []

        async def fake_send_request(cmd, body, timeout=0):
            calls.append((cmd, body))
            if cmd == APP_CMD_UPLOAD_MEDIA_INIT:
                return {"errcode": 0, "body": {"upload_id": "upload-1"}}
            if cmd == APP_CMD_UPLOAD_MEDIA_CHUNK:
                return {"errcode": 0}
            if cmd == APP_CMD_UPLOAD_MEDIA_FINISH:
                return {
                    "errcode": 0,
                    "body": {
                        "media_id": "media-1",
                        "type": "file",
                        "created_at": "2026-03-18T00:00:00Z",
                    },
                }
            raise AssertionError(f"unexpected cmd {cmd}")

        monkeypatch.setattr(wecom_module, "UPLOAD_CHUNK_SIZE", 4)
        adapter._send_request = fake_send_request

        result = await adapter._upload_media_bytes(b"abcdefghij", "file", "demo.bin")

        assert result["media_id"] == "media-1"
        assert [cmd for cmd, _body in calls] == [
            APP_CMD_UPLOAD_MEDIA_INIT,
            APP_CMD_UPLOAD_MEDIA_CHUNK,
            APP_CMD_UPLOAD_MEDIA_CHUNK,
            APP_CMD_UPLOAD_MEDIA_CHUNK,
            APP_CMD_UPLOAD_MEDIA_FINISH,
        ]
        assert calls[1][1]["chunk_index"] == 0
        assert calls[2][1]["chunk_index"] == 1
        assert calls[3][1]["chunk_index"] == 2

    @pytest.mark.asyncio
    @patch("tools.url_safety.is_safe_url", return_value=True)
    async def test_download_remote_bytes_rejects_large_content_length(self, _mock_safe):
        from gateway.platforms.wecom import WeComAdapter

        class FakeResponse:
            headers = {"content-length": "10"}

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return None

            def raise_for_status(self):
                return None

            async def aiter_bytes(self):
                yield b"abc"

        class FakeClient:
            def stream(self, method, url, headers=None):
                return FakeResponse()

            async def aclose(self):
                return None

        adapter = WeComAdapter(PlatformConfig(enabled=True))

        # _download_remote_bytes builds its own proxy-free httpx.AsyncClient
        # (it does not reuse self._http_client), so patch the constructor.
        with patch("gateway.platforms.wecom.httpx.AsyncClient", return_value=FakeClient()):
            with pytest.raises(ValueError, match="exceeds WeCom limit"):
                await adapter._download_remote_bytes("https://example.com/file.bin", max_bytes=4)

    @pytest.mark.asyncio
    async def test_cache_media_decrypts_url_payload_before_writing(self):
        from gateway.platforms.wecom import WeComAdapter

        adapter = WeComAdapter(PlatformConfig(enabled=True))
        plaintext = b"secret document bytes"
        key = os.urandom(32)
        pad_len = 32 - (len(plaintext) % 32)
        padded = plaintext + bytes([pad_len]) * pad_len

        from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

        encryptor = Cipher(algorithms.AES(key), modes.CBC(key[:16])).encryptor()
        encrypted = encryptor.update(padded) + encryptor.finalize()
        adapter._download_remote_bytes = AsyncMock(
            return_value=(
                encrypted,
                {
                    "content-type": "application/octet-stream",
                    "content-disposition": 'attachment; filename="secret.bin"',
                },
            )
        )

        cached = await adapter._cache_media(
            "file",
            {
                "url": "https://example.com/secret.bin",
                "aeskey": base64.b64encode(key).decode("ascii"),
            },
        )

        assert cached is not None
        cached_path, content_type = cached
        assert Path(cached_path).read_bytes() == plaintext
        assert content_type == "application/octet-stream"


class TestSend:
    @pytest.mark.asyncio
    async def test_send_uses_proactive_payload(self):
        from gateway.platforms.wecom import APP_CMD_SEND, WeComAdapter

        adapter = WeComAdapter(PlatformConfig(enabled=True))
        # Mock a healthy WebSocket so the send() fast path goes straight to
        # the mocked _send_request (instead of enqueueing for the worker).
        adapter._ws = AsyncMock(closed=False)
        adapter._send_request = AsyncMock(return_value={"headers": {"req_id": "req-1"}, "errcode": 0})

        result = await adapter.send("chat-123", "Hello WeCom")

        assert result.success is True
        # Fast path calls _send_request directly with the default timeout
        # (no positional timeout arg) — it no longer routes through
        # _send_with_reconnect_retry.
        adapter._send_request.assert_awaited_once_with(
            APP_CMD_SEND,
            {
                "chatid": "chat-123",
                "msgtype": "markdown",
                "markdown": {"content": "Hello WeCom"},
            },
        )

    @pytest.mark.asyncio
    async def test_send_reports_wecom_errors(self):
        from gateway.platforms.wecom import WeComAdapter

        adapter = WeComAdapter(PlatformConfig(enabled=True))
        adapter._ws = AsyncMock(closed=False)
        adapter._send_request = AsyncMock(return_value={"errcode": 40001, "errmsg": "bad request"})

        result = await adapter.send("chat-123", "Hello WeCom")

        assert result.success is False
        assert "40001" in (result.error or "")

    @pytest.mark.asyncio
    async def test_send_image_falls_back_to_text_for_remote_url(self):
        from gateway.platforms.wecom import WeComAdapter

        adapter = WeComAdapter(PlatformConfig(enabled=True))
        adapter._send_media_source = AsyncMock(return_value=SendResult(success=False, error="upload failed"))
        adapter.send = AsyncMock(return_value=SendResult(success=True, message_id="msg-1"))

        result = await adapter.send_image("chat-123", "https://example.com/demo.png", caption="demo")

        assert result.success is True
        adapter.send.assert_awaited_once_with(chat_id="chat-123", content="demo\nhttps://example.com/demo.png", reply_to=None)

    @pytest.mark.asyncio
    async def test_send_voice_sends_caption_and_downgrade_note(self):
        from gateway.platforms.wecom import WeComAdapter

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
        from gateway.platforms.wecom import WeComAdapter

        adapter = WeComAdapter(PlatformConfig(enabled=True))
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

    @pytest.mark.asyncio
    async def test_on_message_preserves_quote_context(self):
        from gateway.platforms.wecom import WeComAdapter

        adapter = WeComAdapter(PlatformConfig(enabled=True))
        adapter._text_batch_delay_seconds = 0  # disable batching for tests
        adapter.handle_message = AsyncMock()
        adapter._extract_media = AsyncMock(return_value=([], []))

        payload = {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-1"},
            "body": {
                "msgid": "msg-1",
                "chatid": "group-1",
                "chattype": "group",
                "from": {"userid": "user-1"},
                "msgtype": "text",
                "text": {"content": "follow up"},
                "quote": {"msgtype": "text", "text": {"content": "quoted message"}},
            },
        }

        await adapter._on_message(payload)

        event = adapter.handle_message.await_args.args[0]
        assert event.reply_to_text == "quoted message"
        assert event.reply_to_message_id == "quote:msg-1"

    @pytest.mark.asyncio
    async def test_on_message_respects_group_policy(self):
        from gateway.platforms.wecom import WeComAdapter

        adapter = WeComAdapter(
            PlatformConfig(
                enabled=True,
                extra={"group_policy": "allowlist", "group_allow_from": ["group-allowed"]},
            )
        )
        adapter.handle_message = AsyncMock()
        adapter._extract_media = AsyncMock(return_value=([], []))

        payload = {
            "cmd": "aibot_callback",
            "headers": {"req_id": "req-1"},
            "body": {
                "msgid": "msg-1",
                "chatid": "group-blocked",
                "chattype": "group",
                "from": {"userid": "user-1"},
                "msgtype": "text",
                "text": {"content": "hello"},
            },
        }

        await adapter._on_message(payload)
        adapter.handle_message.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_on_message_injects_sender_identity_for_dm(self):
        """The model must know who it's talking to, even in a private chat.

        WeCom AI Bot callbacks only carry ``userid`` (no display name), and
        run.py's ``[user_name]`` prefix only fires for shared multi-user
        sessions — which DMs never are (``is_shared_multi_user_session``
        returns False for ``chat_type == "dm"``). Without an identity hint
        the LLM has no idea who the current user is. The adapter must inject
        the userid into ``channel_context``, which run.py prepends to the
        message before it reaches the model.
        """
        from gateway.platforms.wecom import WeComAdapter

        adapter = WeComAdapter(PlatformConfig(enabled=True))
        adapter._text_batch_delay_seconds = 0  # disable batching for tests
        adapter.handle_message = AsyncMock()
        adapter._extract_media = AsyncMock(return_value=([], []))

        payload = {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-1"},
            "body": {
                "msgid": "msg-1",
                "chatid": "dm-1",
                "chattype": "single",
                "from": {"userid": "zhangsan"},
                "msgtype": "text",
                "text": {"content": "hello"},
            },
        }

        await adapter._on_message(payload)

        adapter.handle_message.assert_awaited_once()
        event = adapter.handle_message.await_args.args[0]
        assert event.channel_context and "zhangsan" in event.channel_context

    @pytest.mark.asyncio
    async def test_on_message_injects_sender_identity_for_group(self):
        """Group chats must also carry sender identity.

        With ``group_sessions_per_user`` at its default (True), group
        sessions are per-user isolated, so run.py does not add the
        ``[user_name]`` prefix either. The ``channel_context`` hint is the
        model's only signal for who sent the message.
        """
        from gateway.platforms.wecom import WeComAdapter

        adapter = WeComAdapter(PlatformConfig(enabled=True))
        adapter._text_batch_delay_seconds = 0
        adapter.handle_message = AsyncMock()
        adapter._extract_media = AsyncMock(return_value=([], []))

        payload = {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-1"},
            "body": {
                "msgid": "msg-1",
                "chatid": "group-1",
                "chattype": "group",
                "from": {"userid": "lisi"},
                "msgtype": "text",
                "text": {"content": "hi all"},
            },
        }

        await adapter._on_message(payload)

        event = adapter.handle_message.await_args.args[0]
        assert event.channel_context and "lisi" in event.channel_context


class TestWeComZombieSessionFix:
    """Tests for PR #11572 — device_id, markdown reply, group req_id fallback."""

    def test_adapter_generates_stable_device_id_per_instance(self):
        from gateway.platforms.wecom import WeComAdapter

        adapter = WeComAdapter(PlatformConfig(enabled=True))
        assert isinstance(adapter._device_id, str)
        assert len(adapter._device_id) > 0
        # Second snapshot on the same adapter must be identical — only a fresh
        # adapter instance should get a new device_id (one-per-reconnect is the
        # zombie-session footgun we're fixing).
        assert adapter._device_id == adapter._device_id

    def test_different_adapter_instances_get_distinct_device_ids(self):
        from gateway.platforms.wecom import WeComAdapter

        a = WeComAdapter(PlatformConfig(enabled=True))
        b = WeComAdapter(PlatformConfig(enabled=True))
        assert a._device_id != b._device_id

    @pytest.mark.asyncio
    async def test_open_connection_includes_device_id_in_subscribe(self):
        from gateway.platforms.wecom import APP_CMD_SUBSCRIBE, WeComAdapter

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

        with patch("gateway.platforms.wecom.aiohttp.ClientSession", _FakeSession):
            await adapter._open_connection()

        assert len(sent_payloads) == 1
        subscribe = sent_payloads[0]
        assert subscribe["cmd"] == APP_CMD_SUBSCRIBE
        assert subscribe["body"]["bot_id"] == "test-bot"
        assert subscribe["body"]["secret"] == "test-secret"
        assert subscribe["body"]["device_id"] == adapter._device_id

    @pytest.mark.asyncio
    async def test_open_connection_routes_websocket_through_http_proxy(self, monkeypatch):
        """Regression (wehermes Tencent Cloud deploy): aiohttp's trust_env does
        an EXACT scheme match — ``wss://`` needs ``WSS_PROXY``, not
        ``HTTP_PROXY`` — so a deployment that only sets ``HTTP_PROXY`` gets no
        proxy for the WeCom WebSocket and the connect times out. When
        ``HTTP_PROXY`` is set we must pass ``proxy=`` to ``ws_connect``
        explicitly (via ``resolve_proxy_url``)."""
        from gateway.platforms.wecom import WeComAdapter

        monkeypatch.setenv("HTTP_PROXY", "http://proxy.example:8080")
        for var in (
            "HTTPS_PROXY", "WSS_PROXY", "ALL_PROXY",
            "https_proxy", "wss_proxy", "all_proxy",
            "NO_PROXY", "no_proxy",
        ):
            monkeypatch.delenv(var, raising=False)

        adapter = WeComAdapter(
            PlatformConfig(enabled=True, extra={"bot_id": "b", "secret": "s"})
        )

        captured: dict = {}

        class _FakeWS:
            closed = False

            async def send_json(self, payload):
                return None

            async def close(self):
                return None

        class _FakeSession:
            def __init__(self, **kwargs):
                captured["session_kw"] = kwargs

            async def ws_connect(self, *args, **kwargs):
                captured["ws_kw"] = kwargs
                return _FakeWS()

            async def close(self):
                return None

        adapter._cleanup_ws = AsyncMock()
        adapter._wait_for_handshake = AsyncMock(return_value={"errcode": 0})

        with patch("gateway.platforms.wecom.aiohttp.ClientSession", _FakeSession):
            await adapter._open_connection()

        # The proxy must reach the WebSocket — as ``proxy=`` (HTTP proxy when
        # aiohttp_socks is absent) or via a connector (SOCKS / aiohttp_socks).
        ws_proxy = captured["ws_kw"].get("proxy")
        has_connector = "connector" in captured.get("session_kw", {})
        assert ws_proxy == "http://proxy.example:8080" or has_connector, (
            "WebSocket not routed through HTTP_PROXY (would time out behind an "
            f"HTTP egress proxy): ws_kwargs={captured.get('ws_kw')}, "
            f"session_kwargs={captured.get('session_kw')}"
        )

    @pytest.mark.asyncio
    async def test_on_message_caches_last_req_id_per_chat(self):
        from gateway.platforms.wecom import WeComAdapter

        adapter = WeComAdapter(PlatformConfig(enabled=True))
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
        assert adapter._last_chat_req_ids["group-1"] == ["req-abc"]

    @pytest.mark.asyncio
    async def test_on_message_does_not_cache_blocked_sender_req_id(self):
        """Blocked chats shouldn't populate the proactive-send fallback cache."""
        from gateway.platforms.wecom import WeComAdapter

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
        from gateway.platforms.wecom import DEDUP_MAX_SIZE, WeComAdapter

        adapter = WeComAdapter(PlatformConfig(enabled=True))
        for i in range(DEDUP_MAX_SIZE + 50):
            adapter._remember_chat_req_id(f"chat-{i}", f"req-{i}")
        assert len(adapter._last_chat_req_ids) <= DEDUP_MAX_SIZE
        # The most recently remembered chat must still be present.
        latest = f"chat-{DEDUP_MAX_SIZE + 49}"
        assert adapter._last_chat_req_ids[latest] == [f"req-{DEDUP_MAX_SIZE + 49}"]

    def test_remember_chat_req_id_ignores_empty_values(self):
        from gateway.platforms.wecom import WeComAdapter

        adapter = WeComAdapter(PlatformConfig(enabled=True))
        adapter._remember_chat_req_id("", "req-1")
        adapter._remember_chat_req_id("chat-1", "")
        adapter._remember_chat_req_id("   ", "   ")
        assert adapter._last_chat_req_ids == {}

    @pytest.mark.asyncio
    async def test_proactive_group_send_falls_back_to_cached_req_id(self):
        """Sending into a group without reply_to should use the last cached
        req_id via APP_CMD_RESPONSE — WeCom AI Bots cannot initiate APP_CMD_SEND
        in group chats (errcode 600039)."""
        from gateway.platforms.wecom import WeComAdapter

        adapter = WeComAdapter(PlatformConfig(enabled=True))
        adapter._ws = AsyncMock(closed=False)
        adapter._last_chat_req_ids["group-1"] = ["inbound-req-42"]
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
        from gateway.platforms.wecom import APP_CMD_SEND, WeComAdapter

        adapter = WeComAdapter(PlatformConfig(enabled=True))
        adapter._ws = AsyncMock(closed=False)
        adapter._send_request = AsyncMock(
            return_value={"headers": {"req_id": "new"}, "errcode": 0}
        )

        result = await adapter.send("fresh-dm-chat", "ping", reply_to=None)

        assert result.success is True
        adapter._send_request.assert_awaited_once()
        cmd = adapter._send_request.await_args.args[0]
        assert cmd == APP_CMD_SEND



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
        from gateway.platforms.wecom import WeComAdapter

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
        t2 = asyncio.create_task(asyncio.sleep(9999))
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
        from gateway.platforms.wecom import WeComAdapter

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


class TestReliabilityBundle:
    """Send-reliability bundle: send-queue/worker, heartbeat-ack reconnect,
    subscription-death handling, req-id sliding window, inbound PING/PONG.

    These behaviors were ported from the wehermes overlay into the fork so
    transient WebSocket drops don't silently lose agent replies.
    """

    @pytest.mark.asyncio
    async def test_dispatch_payload_replies_pong_to_ping(self):
        from gateway.platforms.wecom import APP_CMD_PONG, WeComAdapter

        adapter = WeComAdapter(PlatformConfig(enabled=True))
        adapter._send_json = AsyncMock()

        await adapter._dispatch_payload({
            "cmd": "ping",
            "headers": {"req_id": "r1"},
            "body": {},
        })

        adapter._send_json.assert_awaited_once()
        sent = adapter._send_json.await_args.args[0]
        assert sent["cmd"] == APP_CMD_PONG
        assert sent["headers"]["req_id"] == "r1"
        assert sent["body"] == {}

    def test_remember_chat_req_id_keeps_sliding_window_of_3(self):
        from gateway.platforms.wecom import WeComAdapter

        adapter = WeComAdapter(PlatformConfig(enabled=True))
        adapter._remember_chat_req_id("c1", "r1")
        adapter._remember_chat_req_id("c1", "r2")
        adapter._remember_chat_req_id("c1", "r3")
        adapter._remember_chat_req_id("c1", "r4")

        assert adapter._last_chat_req_ids["c1"] == ["r2", "r3", "r4"]

    @pytest.mark.asyncio
    async def test_send_uses_last_req_id_in_sliding_window(self):
        from gateway.platforms.wecom import WeComAdapter

        adapter = WeComAdapter(PlatformConfig(enabled=True))
        adapter._ws = AsyncMock(closed=False)
        adapter._last_chat_req_ids["g1"] = ["old", "new"]
        adapter._send_reply_request = AsyncMock(
            return_value={"headers": {"req_id": "new"}, "errcode": 0}
        )

        result = await adapter.send("g1", "ping")

        assert result.success is True
        # Must use the most recent req_id in the window ([-1]), not the whole list.
        assert adapter._send_reply_request.await_args.args[0] == "new"

    @pytest.mark.asyncio
    async def test_send_enqueues_when_ws_down_returns_queued_id(self):
        from gateway.platforms.wecom import WeComAdapter

        adapter = WeComAdapter(PlatformConfig(enabled=True))
        adapter._ws = None  # WS unavailable

        result = await adapter.send("c1", "hello")

        assert result.success is True
        assert result.message_id.startswith("queued:")
        assert adapter._send_queue.qsize() == 1

    @pytest.mark.asyncio
    async def test_send_returns_failure_on_queue_full(self):
        from gateway.platforms.wecom import WeComAdapter

        adapter = WeComAdapter(PlatformConfig(enabled=True))
        adapter._ws = None
        # Pre-fill the queue to its maxsize (64).
        for _ in range(64):
            adapter._send_queue.put_nowait({"chat_id": "x", "content": "x", "reply_to": None})

        result = await adapter.send("c1", "hello")

        assert result.success is False
        assert "queue full" in (result.error or "").lower()

    @pytest.mark.asyncio
    async def test_send_enqueues_on_ws_not_connected_runtime_error(self):
        from gateway.platforms.wecom import WeComAdapter

        adapter = WeComAdapter(PlatformConfig(enabled=True))
        adapter._ws = AsyncMock(closed=False)  # looks healthy...
        adapter._send_request = AsyncMock(
            side_effect=RuntimeError("WeCom websocket is not connected")
        )

        result = await adapter.send("c1", "hello")

        assert result.success is True
        assert result.message_id.startswith("queued:")
        assert adapter._send_queue.qsize() == 1

    @pytest.mark.asyncio
    async def test_send_returns_failure_on_non_ws_runtime_error(self):
        from gateway.platforms.wecom import WeComAdapter

        adapter = WeComAdapter(PlatformConfig(enabled=True))
        adapter._ws = AsyncMock(closed=False)
        adapter._send_request = AsyncMock(side_effect=RuntimeError("something else"))

        result = await adapter.send("c1", "hello")

        assert result.success is False
        assert adapter._send_queue.qsize() == 0  # NOT enqueued — real failure

    @pytest.mark.asyncio
    async def test_send_worker_drains_queue_and_sends(self):
        import gateway.platforms.wecom as wecom_module
        from gateway.platforms.wecom import APP_CMD_SEND, WeComAdapter

        adapter = WeComAdapter(PlatformConfig(enabled=True))
        adapter._running = True
        adapter._ensure_ws_alive = AsyncMock(return_value=True)
        adapter._send_request = AsyncMock(return_value={"errcode": 0})

        adapter._send_queue.put_nowait({"chat_id": "c1", "content": "hello", "reply_to": None})
        adapter._send_queue.put_nowait(None)  # sentinel stops the loop after draining

        await adapter._send_worker_loop()

        adapter._send_request.assert_awaited_once_with(
            APP_CMD_SEND,
            {"chatid": "c1", "msgtype": "markdown", "markdown": {"content": "hello"}},
        )
        assert adapter._send_queue.empty()

    @pytest.mark.asyncio
    async def test_send_worker_drops_when_ws_stays_down(self, monkeypatch):
        import gateway.platforms.wecom as wecom_module
        from gateway.platforms.wecom import WeComAdapter

        adapter = WeComAdapter(PlatformConfig(enabled=True))
        adapter._running = True
        adapter._ensure_ws_alive = AsyncMock(return_value=False)
        adapter._send_request = AsyncMock()
        # Zero the retry backoff so the test doesn't sleep for seconds.
        monkeypatch.setattr(wecom_module, "SEND_RETRY_BACKOFF", [0.0, 0.0, 0.0], raising=False)

        adapter._send_queue.put_nowait({"chat_id": "c1", "content": "hello", "reply_to": None})
        adapter._send_queue.put_nowait(None)  # sentinel

        await adapter._send_worker_loop()

        adapter._send_request.assert_not_awaited()  # never sent — WS stayed down
        assert adapter._send_queue.empty()  # message dropped after retries

    @pytest.mark.asyncio
    async def test_heartbeat_ack_timeout_forces_reconnect_after_3_strikes(self, monkeypatch):
        import gateway.platforms.wecom as wecom_module
        from gateway.platforms.wecom import WeComAdapter

        adapter = WeComAdapter(PlatformConfig(enabled=True))
        adapter._running = True
        adapter._ws = AsyncMock(closed=False)
        monkeypatch.setattr(wecom_module, "HEARTBEAT_INTERVAL_SECONDS", 0.01)

        # 3 consecutive ack timeouts, then a success so the loop settles.
        calls = {"n": 0}

        async def _fake_send_request(*args, **kwargs):
            calls["n"] += 1
            if calls["n"] <= 3:
                raise asyncio.TimeoutError()
            return {"errcode": 0}

        adapter._send_request = _fake_send_request
        adapter._cleanup_ws = AsyncMock()

        task = asyncio.create_task(adapter._heartbeat_loop())
        await asyncio.sleep(0.15)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        adapter._cleanup_ws.assert_awaited_once()  # exactly one reconnect after 3 misses

    @pytest.mark.asyncio
    async def test_heartbeat_single_miss_does_not_reconnect(self, monkeypatch):
        import gateway.platforms.wecom as wecom_module
        from gateway.platforms.wecom import WeComAdapter

        adapter = WeComAdapter(PlatformConfig(enabled=True))
        adapter._running = True
        adapter._ws = AsyncMock(closed=False)
        monkeypatch.setattr(wecom_module, "HEARTBEAT_INTERVAL_SECONDS", 0.01)

        # 2 misses (below the 3-strike threshold), then a success.
        calls = {"n": 0}

        async def _fake_send_request(*args, **kwargs):
            calls["n"] += 1
            if calls["n"] <= 2:
                raise asyncio.TimeoutError()
            return {"errcode": 0}

        adapter._send_request = _fake_send_request
        adapter._cleanup_ws = AsyncMock()

        task = asyncio.create_task(adapter._heartbeat_loop())
        await asyncio.sleep(0.15)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        adapter._cleanup_ws.assert_not_awaited()  # transient misses must NOT tear down the WS

    @pytest.mark.asyncio
    async def test_subscription_death_hook_cleans_up_on_846609(self):
        from gateway.platforms.wecom import WeComAdapter

        adapter = WeComAdapter(PlatformConfig(enabled=True))
        adapter._ws = AsyncMock(closed=False)

        # Drive _send_request's correlated future to a 846609 response.
        async def _fake_send_json(payload):
            req_id = payload["headers"]["req_id"]
            fut = adapter._pending_responses.get(req_id)
            if fut and not fut.done():
                fut.set_result({"errcode": 846609, "errmsg": "aibot websocket not subscribed"})

        adapter._send_json = _fake_send_json
        adapter._fail_pending_responses = MagicMock()
        adapter._cleanup_ws = AsyncMock()

        response = await adapter._send_request("aibot_send_msg", {})

        assert response.get("errcode") == 846609
        adapter._fail_pending_responses.assert_called_once()
        adapter._cleanup_ws.assert_awaited_once()
