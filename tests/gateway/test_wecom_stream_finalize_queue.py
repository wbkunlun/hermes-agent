"""Regression (fork 2026-11-13, DPMS 561213): a finalize frame that sits in the control-lane
queue past the gateway's stream-flush budget detaches from its cancelled caller — the gateway
normal-sends the reply, then the queued frame lands afterwards and the user reads the same
reply twice (production 2026-11-13 17:49, hm-832fbfdf-68f).

Two layers: the send-queue drops items whose waiter is gone; the finalize path bounds its
queue wait below the gateway budget and declines cleanly past it.
"""
import asyncio
import logging
import time

from plugins.platforms.wecom.send_queue import ChatSendQueueMixin
from plugins.platforms.wecom.streaming import WeComStreamMixin


class _QueueHost(ChatSendQueueMixin):
    name = "test-adapter"

    def __init__(self):
        self._chat_queues = {}
        self._chat_workers = {}
        self._control_queues = {}
        self._control_workers = {}
        self._chat_token_usage = {}


class _SlowQueueStreamHost(WeComStreamMixin):
    name = "test-adapter"

    def __init__(self, queue_wait: float = 0.4):
        self._stream_expired_chats = set()
        self._queue_wait = queue_wait
        self.inner_ran = False

    async def _enqueue_chat_send(self, chat_id, coro_factory, is_control=False):
        await asyncio.sleep(self._queue_wait)  # 排队超预算（模拟 token bucket 限流等待）
        self.inner_ran = True
        return True


def test_send_queue_skips_an_item_whose_waiter_was_cancelled():
    async def scenario():
        host = _QueueHost()
        ran = {"a": False, "b": False}
        a_started = asyncio.Event()

        async def item_a():
            a_started.set()  # worker 已从队列取走本条并在执行
            ran["a"] = True
            await asyncio.sleep(0.3)  # 占住 worker，让 item_b 排在队列里

        async def item_b():
            ran["b"] = True

        first = asyncio.ensure_future(
            host._enqueue_chat_send("chat1", item_a, is_control=True)
        )
        await asyncio.wait_for(a_started.wait(), timeout=2.0)  # worker 忙于 item_a

        second = asyncio.ensure_future(
            host._enqueue_chat_send("chat1", item_b, is_control=True)
        )
        await asyncio.sleep(0.05)  # second 停在 await future 上
        second.cancel()  # 等待方放弃（网关冲刷预算到点）
        try:
            await second
        except asyncio.CancelledError:
            pass

        await first
        await asyncio.sleep(0.2)  # 给 worker 处理 item_b 的机会（应跳过）

        assert ran["a"] is True
        assert ran["b"] is False, "cancelled waiter's send must be dropped, not detached"

    asyncio.run(scenario())


def test_finalize_queue_wait_past_budget_declines_cleanly():
    async def scenario():
        host = _SlowQueueStreamHost(queue_wait=0.4)
        host._FINALIZE_QUEUE_WAIT_SECONDS = 0.05

        start = time.monotonic()
        result = await host.send_stream_frame("最终回复", finalize=True, chat_id="c1")
        elapsed = time.monotonic() - start

        assert result is False, "finalize past its queue bound must decline (gateway regular send delivers once)"
        assert elapsed < 0.35, f"finalize must give up promptly, took {elapsed:.2f}s"
        assert host.inner_ran is False, "the detached frame must never execute"

    asyncio.run(scenario())


def test_finalize_within_queue_wait_still_delivers(caplog):
    async def scenario():
        host = _SlowQueueStreamHost(queue_wait=0.02)
        host._FINALIZE_QUEUE_WAIT_SECONDS = 1.0
        return await host.send_stream_frame("最终回复", finalize=True, chat_id="c1")

    with caplog.at_level(logging.INFO, logger="plugins.platforms.wecom.adapter"):
        result = asyncio.run(scenario())

    assert result is True
    assert "declining" not in caplog.text
