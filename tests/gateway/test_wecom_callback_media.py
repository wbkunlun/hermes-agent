"""Tests for wecom_callback markdown + media outbound (② Agent-fallback)."""

import pytest

from gateway.config import PlatformConfig
from plugins.platforms.wecom.callback_adapter import (
    WecomCallbackAdapter,
    _split_markdown_bytes,
)


def _app(name="test-app", corp_id="ww1234567890", agent_id="1000002"):
    return {
        "name": name, "corp_id": corp_id, "corp_secret": "s",
        "agent_id": agent_id, "token": "t",
        "encoding_aes_key": "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFG",
    }


def _adapter(apps=None):
    return WecomCallbackAdapter(PlatformConfig(
        enabled=True,
        extra={"mode": "callback", "host": "127.0.0.1", "port": 0, "apps": apps or [_app()]},
    ))


class _FakeResp:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


class _FakeClient:
    def __init__(self, responses):
        self.responses = list(responses)
        self.posts = []

    async def post(self, url, json=None, **kw):
        self.posts.append({"url": url, "json": json, "kw": kw})
        return _FakeResp(self.responses.pop(0))


class TestSplitMarkdownBytes:
    def test_short_content_single_segment(self):
        assert _split_markdown_bytes("hello") == ["hello"]

    def test_empty_content_no_segments(self):
        assert _split_markdown_bytes("") == []

    def test_splits_on_utf8_budget_preferring_lines(self):
        content = "短" * 3000  # 9000 bytes UTF-8 > 4096
        segments = _split_markdown_bytes(content, max_bytes=4096)
        assert len(segments) >= 3
        assert all(len(s.encode("utf-8")) <= 4096 for s in segments)
        assert "".join(segments) == content

    def test_pathological_single_long_line(self):
        content = "a" * 9000
        segments = _split_markdown_bytes(content, max_bytes=4096)
        assert all(len(s.encode("utf-8")) <= 4096 for s in segments)
        assert "".join(segments) == content


class TestSendMarkdown:
    @pytest.mark.asyncio
    async def test_send_markdown_under_limit_single_post(self):
        adapter = _adapter()
        adapter._access_tokens["test-app"] = {"token": "tok", "expires_at": 9999999999}
        adapter._user_app_map["ww1234567890:alice"] = "test-app"
        client = _FakeClient([{"errcode": 0, "msgid": "m1"}])
        adapter._http_client = client

        result = await adapter.send_markdown("ww1234567890:alice", "**done**")
        assert result.success is True
        assert client.posts[0]["json"]["msgtype"] == "markdown"
        assert client.posts[0]["json"]["markdown"]["content"] == "**done**"
        assert client.posts[0]["json"]["touser"] == "alice"

    @pytest.mark.asyncio
    async def test_send_markdown_segments_oversize_content(self):
        adapter = _adapter()
        adapter._access_tokens["test-app"] = {"token": "tok", "expires_at": 9999999999}
        adapter._user_app_map["ww1234567890:alice"] = "test-app"
        client = _FakeClient([{"errcode": 0, "msgid": "m1"}, {"errcode": 0, "msgid": "m2"}])
        adapter._http_client = client

        content = "x" * 5000  # 5000 bytes > 4096
        result = await adapter.send_markdown("ww1234567890:alice", content)
        assert result.success is True
        assert len(client.posts) == 2
        joined = "".join(p["json"]["markdown"]["content"] for p in client.posts)
        assert joined == content

    @pytest.mark.asyncio
    async def test_send_markdown_empty_content_fails_cleanly(self):
        adapter = _adapter()
        result = await adapter.send_markdown("ww1234567890:alice", "")
        assert result.success is False
