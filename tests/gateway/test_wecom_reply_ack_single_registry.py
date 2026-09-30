"""Regression (audit 2026-09-29 module-1 H2): passive replies (markdown / replyMedia)
registered their ack future in _pending_responses while stream frames used
_reply_queues — the SAME inbound req_id could sit in both, and _dispatch_payload
resolved reply-queues first. A stream ack could "answer" a passive future (or the
passive wait could swallow a stream ack): the passive reply then timed out after
15s, looked failed, and the DM was re-delivered via proactive/fallback — a real
duplicate. One registry + per-req_id serialization removes the crosstalk."""
import asyncio
import logging

import pytest

from gateway.config import Platform
from plugins.platforms.wecom.adapter import WeComAdapter


class _FakeWS:
    closed = False


def _make_adapter():
    adapter = object.__new__(WeComAdapter)
    adapter.platform = Platform.WECOM  # name 是只读 property
    adapter._ws = _FakeWS()
    adapter._reply_queues = {}
    adapter._pending_responses = {}
    adapter.sent = []

    async def fake_send_json(payload):
        adapter.sent.append(payload)

    adapter._send_json = fake_send_json
    return adapter


def test_passive_reply_registers_in_reply_queue_not_pending_responses():
    adapter = _make_adapter()

    async def go():
        task = asyncio.create_task(adapter._send_reply_request("R1", {"msgtype": "markdown", "markdown": {"content": "x"}}))
        await asyncio.sleep(0)  # let it register + send
        assert "R1" in adapter._reply_queues, "passive reply must use the shared reply registry"
        assert "R1" not in adapter._pending_responses, "dual registry is the crosstalk root cause"
        # Deliver the ack through the normal dispatch path.
        await adapter._dispatch_payload({"cmd": "aibot_respond_msg_ack", "headers": {"req_id": "R1"}, "body": {"errcode": 0}})
        return await asyncio.wait_for(task, timeout=1.0)

    response = asyncio.run(go())
    assert response["body"]["errcode"] == 0
    assert adapter.sent[0]["headers"]["req_id"] == "R1"


def test_stream_ack_cannot_strand_a_passive_wait():
    """The original crosstalk: reply-queue pending frame + passive future on one req_id."""
    from plugins.platforms.wecom.streaming import ReplyFrame, ReplyQueue

    adapter = _make_adapter()

    async def go():
        # Simulate an in-flight intermediate stream frame on R1.
        queue = ReplyQueue("R1")
        stream_future = asyncio.get_running_loop().create_future()
        queue.pending_ack = ReplyFrame(body={"msgtype": "stream", "stream": {"id": "s1"}}, future=stream_future, is_final=False, sent_at=0.0)
        adapter._reply_queues["R1"] = queue
        passive = asyncio.create_task(adapter._send_reply_request("R1", {"msgtype": "markdown", "markdown": {"content": "x"}}))
        await asyncio.sleep(0)
        # The passive send must NOT proceed while the stream frame's ack is pending:
        assert adapter.sent == [], "passive reply must serialize behind the pending stream frame"

        async def slow_acks():
            await asyncio.sleep(0.05)
            # ack #1 resolves the PENDING STREAM frame; the passive send is parked behind it.
            await adapter._dispatch_payload({"cmd": "ack", "headers": {"req_id": "R1"}, "body": {"errcode": 0}})
            # wait until the passive markdown frame registers (drain wakeup takes a few loop hops)
            for _ in range(200):
                if adapter.sent:
                    break
                await asyncio.sleep(0.01)
            # ack #2 resolves the passive markdown frame itself — without it the test would
            # wait out the full 15s ack timeout.
            await adapter._dispatch_payload({"cmd": "ack", "headers": {"req_id": "R1"}, "body": {"errcode": 0}})

        await asyncio.gather(slow_acks(), passive, return_exceptions=False)
        assert stream_future.done() and not stream_future.cancelled()
        assert len(adapter.sent) == 1 and adapter.sent[0]["headers"]["req_id"] == "R1"

    asyncio.run(go())


def test_ack_timeout_raises_for_passive_reply():
    adapter = _make_adapter()

    async def go():
        with pytest.raises(asyncio.TimeoutError):
            await adapter._send_reply_correlated("R9", {"msgtype": "markdown", "markdown": {"content": "x"}}, timeout=0.05)

    asyncio.run(go())  # timeout must RAISE: the passive→proactive fallback (846604) depends on it


def test_drain_survives_cancelled_pending_future(monkeypatch, caplog):
    """C1: a pending final frame whose future was cancelled by its owner (final ack
    timeout / send-failure cleanup) must not kill the passive reply parked behind it —
    the shield-mirrored CancelledError used to escape drain, dead-end the per-chat
    worker and hang the lane forever."""
    from plugins.platforms.wecom.streaming import ReplyFrame, ReplyQueue, WeComStreamMixin

    adapter = _make_adapter()

    async def go():
        queue = ReplyQueue("R1")
        stream_future = asyncio.get_running_loop().create_future()
        queue.pending_ack = ReplyFrame(body={"msgtype": "stream", "stream": {"id": "s1", "finish": True}}, future=stream_future, is_final=True, sent_at=0.0)
        adapter._reply_queues["R1"] = queue
        monkeypatch.setattr(WeComStreamMixin, "_REPLY_ACK_TIMEOUT", 0.5)  # owner cancel (0.02s) must win the race against drain's own timeout

        async def cancel_owner_future():
            await asyncio.sleep(0.02)  # long before the 0.5s drain timeout: exercises the C1 cancel path, not TimeoutError
            stream_future.cancel()

        async def ack_passive_when_sent():
            for _ in range(200):
                if adapter.sent:
                    break
                await asyncio.sleep(0.01)
            await adapter._dispatch_payload({"cmd": "ack", "headers": {"req_id": "R1"}, "body": {"errcode": 0}})

        passive = asyncio.create_task(adapter._send_reply_request("R1", {"msgtype": "markdown", "markdown": {"content": "x"}}))
        # 修复前：passive 以 CancelledError 死亡、gather 直接抛；修复后：drain 吞掉镜像 cancel，帧照发
        await asyncio.gather(cancel_owner_future(), ack_passive_when_sent(), passive, return_exceptions=False)
        assert len(adapter.sent) == 1 and adapter.sent[0]["headers"]["req_id"] == "R1"

    with caplog.at_level(logging.INFO, logger="plugins.platforms.wecom.adapter"):
        asyncio.run(go())
    assert "cancelled by owner" in caplog.text  # 钉死走的是 C1 分支而非 drain 自身超时


def test_send_failure_releases_registration_and_raises():
    adapter = _make_adapter()

    async def boom(payload):
        raise RuntimeError("ws closed")

    adapter._send_json = boom

    async def go():
        with pytest.raises(RuntimeError, match="ws closed"):
            await adapter._send_reply_request("R1", {"msgtype": "markdown", "markdown": {"content": "x"}})
        assert "R1" not in adapter._reply_queues, "failed send must release the queue registration"

    asyncio.run(go())

