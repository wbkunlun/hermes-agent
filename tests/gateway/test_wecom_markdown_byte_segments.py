"""Regression (audit 2026-09-29 module-1 M1.8): the aibot markdown path sliced by
CHARS ([:4000]) while the streaming path enforces UTF-8 BYTES — a 4000-CJK-char
passive reply is 12KB and the server rejects the whole frame; group replies then
park in redelivery, re-fail and are dropped after TTL."""
import asyncio

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
