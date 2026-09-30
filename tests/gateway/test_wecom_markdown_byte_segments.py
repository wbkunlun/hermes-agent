"""Regression (audit 2026-09-29 module-1 M1.8): the aibot markdown path sliced by
CHARS ([:4000]) while the streaming path enforces UTF-8 BYTES — a 4000-CJK-char
passive reply is 12KB and the server rejects the whole frame; group replies then
park in redelivery, re-fail and are dropped after TTL."""
import asyncio

import pytest

from plugins.platforms.wecom import adapter as adapter_mod
from plugins.platforms.wecom.adapter import WeComAdapter


def test_markdown_segments_byte_accurate():
    segments = adapter_mod._markdown_segments("汉" * 3000)  # 9000 bytes
    assert len(segments) >= 3
    assert all(len(s.encode("utf-8")) <= adapter_mod.AIBOT_MARKDOWN_MAX_BYTES for s in segments)
    assert "".join(segments) == "汉" * 3000


def _make_adapter():
    adapter = object.__new__(WeComAdapter)
    adapter.reply_frames, adapter.proactive_frames = [], []

    async def fake_reply(req_id, body, cmd=None, timeout=None):
        adapter.reply_frames.append((req_id, body))
        return {"errcode": 0}

    async def fake_request(cmd, body, timeout=None):
        adapter.proactive_frames.append((cmd, body))
        return {"errcode": 0}

    adapter._send_reply_request = fake_reply
    adapter._send_request = fake_request
    return adapter


def test_passive_markdown_sends_multiple_frames_same_req_id():
    adapter = _make_adapter()
    asyncio.run(adapter._send_reply_markdown("R1", "段" * 2500))  # 7500 bytes
    req_ids = [r for r, _ in adapter.reply_frames]
    assert len(adapter.reply_frames) >= 2
    assert set(req_ids) == {"R1"}  # sequential frames on ONE req_id (stream-frames precedent)
    assert all(len(b["markdown"]["content"].encode("utf-8")) <= adapter_mod.AIBOT_MARKDOWN_MAX_BYTES for _, b in adapter.reply_frames)


def test_proactive_markdown_sends_multiple_sends():
    adapter = _make_adapter()
    asyncio.run(adapter._send_proactive_markdown("wrChat", "段" * 2500))
    assert len(adapter.proactive_frames) >= 2
    assert all(f[0] == "aibot_send_msg" for f in adapter.proactive_frames)
    assert all(len(f[1]["markdown"]["content"].encode("utf-8")) <= adapter_mod.AIBOT_MARKDOWN_MAX_BYTES for f in adapter.proactive_frames)


def test_passive_markdown_stops_on_segment_failure():
    adapter = _make_adapter()
    calls = {"n": 0}

    async def flaky(req_id, body, cmd=None, timeout=None):
        calls["n"] += 1
        adapter.reply_frames.append((req_id, body))
        return {"errcode": 0} if calls["n"] == 1 else {"errcode": 84607, "errmsg": "rate limited"}

    adapter._send_reply_request = flaky
    with pytest.raises(RuntimeError, match=r"send reply markdown segment failed: WeCom errcode 84607"):
        asyncio.run(adapter._send_reply_markdown("R1", "段" * 2500))
    assert calls["n"] == 2  # stopped at the failing segment


def test_proactive_markdown_stops_on_segment_failure():
    # 质量评审 C-1 回归钉：非末段 errcode 曾被末段成功覆盖 → success=True + 段内容静默丢失
    adapter = _make_adapter()
    calls = {"n": 0}

    async def flaky(cmd, body, timeout=None):
        calls["n"] += 1
        adapter.proactive_frames.append((cmd, body))
        return {"errcode": 0} if calls["n"] == 1 else {"errcode": 84607, "errmsg": "rate limited"}

    adapter._send_request = flaky
    with pytest.raises(RuntimeError, match=r"send proactive markdown segment failed: WeCom errcode 84607"):
        asyncio.run(adapter._send_proactive_markdown("wrChat", "段" * 2500))
    assert calls["n"] == 2


def test_multi_segments_join_reconstructs_original():
    content = "段" * 2500
    adapter = _make_adapter()
    asyncio.run(adapter._send_reply_markdown("R1", content))
    assert "".join(b["markdown"]["content"] for _, b in adapter.reply_frames) == content
    adapter2 = _make_adapter()
    asyncio.run(adapter2._send_proactive_markdown("wrChat", content))
    assert "".join(b["markdown"]["content"] for _, b in adapter2.proactive_frames) == content


def test_empty_content_raises_without_sending():
    adapter = _make_adapter()
    with pytest.raises(RuntimeError, match="empty content"):
        asyncio.run(adapter._send_reply_markdown("R1", ""))
    with pytest.raises(RuntimeError, match="empty content"):
        asyncio.run(adapter._send_proactive_markdown("wrChat", ""))
    assert adapter.reply_frames == [] and adapter.proactive_frames == []
