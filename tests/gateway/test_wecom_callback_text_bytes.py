"""Regression (audit 2026-09-29 module-1 H1): callback-channel TEXT sends must
respect WeCom's 2048 UTF-8 BYTE cap. The old char slice (content[:2048]) let
683..2048 CJK chars through as 2049..6144 bytes and the server rejected the
WHOLE reply — no segmentation, no retry, the DM was lost."""
import asyncio
import re

from plugins.platforms.wecom import callback_adapter as ca
from plugins.platforms.wecom.callback_adapter import SendResult

# 上游 truncate_message（resync 2026-10-07 换轨）给多块加 " (n/m)" 指示器后缀
_CHUNK_INDICATOR = re.compile(r" \(\d+/\d+\)$")


def _make_adapter():
    adapter = object.__new__(ca.WecomCallbackAdapter)

    async def fake_post(app, payload):
        fake_post.payloads.append(payload)
        return SendResult(success=True, message_id=f"m{len(fake_post.payloads)}")

    fake_post.payloads = []
    adapter._post_message = fake_post
    adapter._resolve_app_for_chat = lambda chat: {"agent_id": "1000002"}
    return adapter, fake_post


def test_text_send_segments_cjk_by_utf8_bytes():
    adapter, post = _make_adapter()
    text = "汉" * 700  # 2100 bytes > 2048
    result = asyncio.run(adapter.send("dm:u1", text))
    assert result.success, getattr(result, "error", None)
    assert len(post.payloads) == 2
    sizes = [len(p["text"]["content"].encode("utf-8")) for p in post.payloads]
    assert all(s <= 2048 for s in sizes), sizes
    assert "".join(_CHUNK_INDICATOR.sub("", p["text"]["content"]) for p in post.payloads) == text


def test_text_send_short_ascii_single_frame():
    adapter, post = _make_adapter()
    result = asyncio.run(adapter.send("dm:u1", "hello"))
    assert result.success
    assert len(post.payloads) == 1
    assert post.payloads[0]["text"]["content"] == "hello"


def test_text_send_stops_on_segment_failure():
    adapter, post = _make_adapter()
    calls = {"n": 0}

    async def flaky(app, payload):
        calls["n"] += 1
        return SendResult(success=False, error="boom") if calls["n"] > 1 else SendResult(success=True, message_id="m1")

    adapter._post_message = flaky
    result = asyncio.run(adapter.send("dm:u1", "汉" * 1400))
    assert not result.success
    assert calls["n"] == 2  # stopped at the failing segment (3 segments total — continue-semantics would be 3)


def test_split_prefers_newlines_and_never_splits_multibyte():
    text = "标题行\n" + "汉" * 700 + "\n结尾"
    segments = ca._split_markdown_bytes(text, max_bytes=2048)
    assert len(segments) >= 2
    assert segments[0] == "标题行\n"  # flush-first ordering — the old hard-cut put the 2046-byte block first
    assert all(len(s.encode("utf-8")) <= 2048 for s in segments)
    assert "".join(segments) == text
    for s in segments:  # no torn multi-byte chars
        s.encode("utf-8").decode("utf-8")
