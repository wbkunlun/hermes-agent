"""Regression (fork 2026-11-16, DPMS 561213): one WeCom reply is presented ONCE.

Production hm-5958d932-1f3: a reply too long for the stream frame left a half-drawn bubble on
screen and the gateway regular-sent the full reply beside it ("one reply, two messages"). A reply
too long for the frame now closes its bubble on the answer head (sealed) and continues in sealed
follow-up bubbles of the same stream, which the reply's own sends carry first; the gateway's
regular send stands cancelled. These tests drive the REAL delivery, adapter reply path and gateway
delivery decision against a fake transport — no network, no platform.
"""
import asyncio
import contextvars
import re

import pytest
import pytest_asyncio

from gateway.config import Platform
from gateway.platforms.base import SendResult
from gateway.platforms.helpers import StreamConsumerPublication
from gateway.run_turn_persistence import GatewayTurnPersistenceMixin
from gateway.run_turn_wecom_overflow import _wecom_overflow_claimed
from plugins.platforms.wecom import reply_lane as lane_mod
from plugins.platforms.wecom.adapter import WeComAdapter
from plugins.platforms.wecom.send_queue import ChatSendQueueMixin
from plugins.platforms.wecom.stream_delivery import WeComStreamDelivery
from plugins.platforms.wecom.streaming import (
    MAX_STREAM_CONTENT_LENGTH,
    STREAM_KEEPALIVE_ENABLED_DEFAULT,
    STREAM_SAFE_DURATION_SECONDS,
    StreamTurn,
    WeComStreamMixin,
)
from tests.waiting import async_wait_until

CHAT = "wrTESTchat"
REQ = "req-inbound-1"
FOOTER = "（本次用时 3.2s）"
PNG = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg=="
REPLY = "".join(f"第{i}段答案: x={i & 1023} (v{i % 7}).\n" for i in range(1500)) + f"\nMEDIA:data:image/png;base64,{PNG}"


async def _close_queues(fake):
    """Stop the per-chat send workers a fake transport spawned, and WAIT until they are dead.

    A worker task outlives its test otherwise: the delivery keeps the loop it was started on, so a
    sealed bubble the previous test left queued is sent through the NEXT test's transport — and
    this test's own finalize then queues behind that stale worker. Cancelling alone is not enough:
    the task has to run its cancellation through before the next test's loop exists, otherwise
    whichever task the loop schedules first decides the next test's outcome. Awaiting every worker
    makes each test's queues its own, whatever ran before it.
    """
    workers = [w for lanes in (fake._control_workers, fake._chat_workers) for w in lanes.values()]
    for lanes in (fake._control_workers, fake._chat_workers):
        lanes.clear()
    for worker in workers:
        worker.cancel()
    if workers:
        await asyncio.gather(*workers, return_exceptions=True)


class _FakeTransport(WeComStreamMixin, ChatSendQueueMixin):
    """A fake WeCom wire: records every stream frame and every content send in ONE journal."""

    name = "fake-wecom"

    def reset_chat(self):
        """A fresh conversation for CHAT — the chat-level state a new inbound leaves behind.

        Production discards the chat's expired mark and caches the new inbound's req_id
        (adapter.py ``_handle_inbound_frame``), and the turn registry holds only turns still
        streaming. Every test here drives CHAT, and a test that deliberately expires the chat or
        leaves no req_id (a refused bubble, a blowed-up finish, a long reply with no req_id)
        would otherwise decide the NEXT test's turn and its frame outcome. Each piece is REBOUND
        (fresh objects), never cleared in place: the stream mixin keeps them per chat, and a chat's
        state must never be recognised by the address of what an earlier chat left there.
        """
        self._stream_turns, self._stream_expired_chats, self._last_stream_turn_by_chat = {}, set(), {}
        self._last_chat_req_ids = {CHAT: REQ}
        self._chat_queues, self._chat_workers = {}, {}
        self._control_queues, self._control_workers = {}, {}
        self._chat_token_usage = {}
        self._stream_keepalive_enabled = STREAM_KEEPALIVE_ENABLED_DEFAULT
        self._stream_safe_duration_seconds = STREAM_SAFE_DURATION_SECONDS

    def __init__(self, *, refuse_bubble_at=None, finish_blows_up_at=None, drop_intermediate_at=None):
        self.refuse_bubble_at = refuse_bubble_at  # 1-based: this overflow bubble is refused
        self.bubble_refused = False
        self.bubbles_opened = 0
        self.bubble_finishes = 0
        self.finish_blows_up_at = finish_blows_up_at  # 1-based: this finish frame blows up
        # fork 2026-11-18 (DPMS 561213): 1-based CONTENT frame (seed excluded) that never lands —
        # the wire drops it; send_stream_frame still reports the stream alive.
        self.drop_intermediate_at = drop_intermediate_at
        self.intermediates_seen = 0
        self._stream_turns, self._stream_expired_chats, self._last_stream_turn_by_chat = {}, set(), {}
        self._last_chat_req_ids = {}
        self._chat_queues, self._chat_workers = {}, {}
        self._control_queues, self._control_workers = {}, {}
        self._chat_token_usage = {}
        self._reply_queues = {}
        self._stream_keepalive_enabled = STREAM_KEEPALIVE_ENABLED_DEFAULT
        self._stream_safe_duration_seconds = STREAM_SAFE_DURATION_SECONDS
        self.events = []  # (kind, text): "frame" / "finalize" / "bubble-open" / "bubble-closed" / "send"
        self.finish_frames = 0
        self.opened_streams = set()  # the main turns whose seed frame went out (the open bubble)

    def _record(self, kind, text):
        self.events.append((kind, text))

    async def send(self, chat_id, content, reply_to=None, metadata=None):
        # lands on the same journal: the seal's refused bubbles may regular-send through here
        # while the sends a refusal must not lose count as "send" too.
        self._record("send", content)
        return SendResult(success=True)

    async def _send_inner(self, chat_id, content, reply_to=None, **kw):
        return await self.send(chat_id, content)  # the regular send a refused bubble falls back to

    async def _send_overflow_bubble(self, chat_id, text):
        """One follow-up bubble through the REAL stream mixin door (open + finish)."""
        return await self.extend_stream_bubble(chat_id, text)


    async def _send_stream_reply(self, reply_req_id, stream_id, content, finish=False):
        return await _answer_frame(self, stream_id, content, finish)

    # the transport pieces the stream mixin asks for
    def _resolve_stream_req_id(self, chat_id, reply_to=None):
        return REQ

    def _reply_req_id_for_message(self, reply_to):
        return None

    # the transport mixins the adapter's reply path asks for
    async def _enqueue_chat_send(self, chat_id, coro_factory, is_control=False):
        return await coro_factory()

    async def _send_reply_markdown(self, req_id, content):
        self._record("send", content)  # the passive reply channel: a real content send
        return {"errcode": 0}

    async def _send_proactive_markdown(self, chat_id, content, chat_type=None):
        self._record("send", content)
        return {"errcode": 0}

    def _raise_for_wecom_error(self, response, what):
        return None

    def of(self, kind):
        return [text for k, text in self.events if k == kind]


class _SealSurface:
    """The seal pieces an adapter provides (the bubble draft door, the segmenter)."""

    name = "fake-wecom"

    async def extend_stream_bubble(self, chat_id, text):
        return await self.transport._send_overflow_bubble(chat_id, text)

    def _markdown_segments(self, content):
        return [content]  # the adapter's aibot segmenter, identity here


class _FakeAdapter(_SealSurface):
    """The delivery's adapter over the fake transport: its stream frames and the gateway's sends."""

    def __init__(self, transport):
        self.transport = transport

    async def send_stream_frame(self, text, *, finalize=False, chat_id=None, **kw):
        return await self.transport.send_stream_frame(text, finalize=finalize, chat_id=chat_id)

    def last_landed_stream_frame_content(self, chat_id):
        # fork 2026-11-18 (DPMS 561213): the durability probe, forwarded to the wire.
        return self.transport.last_landed_stream_frame_content(chat_id)

    async def send(self, chat_id, content, reply_to=None, metadata=None):
        return await self.transport.send(chat_id, content)

    async def _send_inner(self, chat_id, content, reply_to=None, **kw):
        return await self.transport.send(chat_id, content)

    async def _send_reply_markdown(self, req_id, content):
        return await self.transport._send_reply_markdown(req_id, content)

    async def _send_proactive_markdown(self, chat_id, content, chat_type=None):
        return await self.transport._send_proactive_markdown(chat_id, content)

    def _raise_for_wecom_error(self, response, what):
        return None

    def _is_group_chat(self, chat_id):
        return False

    send_overflow_bubbles = WeComAdapter.send_overflow_bubbles

    @property
    def events(self):
        return self.transport.events


class _BareAdapter(WeComAdapter):
    """The real adapter's send/reply path over the fake transport (no __init__: no live socket)."""

    async def extend_stream_bubble(self, chat_id, text):
        return await self.transport._send_overflow_bubble(chat_id, text)

    def _markdown_segments(self, content):
        return [content]  # the adapter's aibot segmenter, identity here

    def __init__(self, transport):
        self.transport = transport
        self.MAX_MESSAGE_LENGTH = 4000
        self.platform = Platform.WECOM
        self._group_chat_ids = set()
        self._last_chat_req_ids = {}
        self._chat_queues, self._chat_workers, self._control_queues, self._control_workers = {}, {}, {}, {}
        self._chat_token_usage = {}

    async def _send_reply_markdown(self, req_id, content):
        self.transport._record("send", content)
        return {"errcode": 0}

    async def _send_proactive_markdown(self, chat_id, content, chat_type=None):
        self.transport._record("send", content)
        return {"errcode": 0}

    def _raise_for_wecom_error(self, response, what):
        return None


@pytest_asyncio.fixture(autouse=True)
async def _transports():
    """Every transport a test builds starts on a clean chat, its send workers are stopped AND
    awaited when the test ends (a worker left running belongs to this test's loop, never the next
    test's — see _close_queues), and the process-global lane registry starts and ends empty."""
    lane_mod._LANES.clear()
    built = []
    original = _FakeTransport.__init__

    def recording(transport, *args, **kwargs):
        original(transport, *args, **kwargs)
        transport.reset_chat()
        built.append(transport)

    _FakeTransport.__init__ = recording
    try:
        yield
    finally:
        _FakeTransport.__init__ = original
        for transport in built:
            await _close_queues(transport)
        lane_mod._LANES.clear()


def _delivery(transport, text, payload=None):
    delivery = WeComStreamDelivery(adapter=_FakeAdapter(transport), chat_id=CHAT)
    delivery.stream_deltas_enabled = True  # set by run_turn_runner on every WeCom consumer
    delivery._disabled = False
    delivery.accumulated_text = text
    delivery.finished = True  # the reply is complete (as the consumer's finish() sets it)
    return delivery


async def _run_reply(transport, text, payload=None):
    delivery = _delivery(transport, text)
    await delivery._finalize()  # the reply finishes; the gateway then hands its payload over
    if payload is not None:
        delivery.attach_overflow(payload)
    return delivery


_FRAME_RE = re.compile("\uff08[0-9]+/[0-9]+\uff09\n|[ \uff08(][0-9]+/[0-9]+[\uff09)]$")


_FRAME_PREFIX_RE = re.compile("^（[0-9]+/[0-9]+）\n")


_LAYOUT_SUFFIX_RE = re.compile(r"(?: \([0-9]+/[0-9]+\)|\n\([0-9]+/[0-9]+\))")


def _plain(bubble: str) -> str:
    """A bubble's text without the framing THIS delivery adds (the leading（i/N）of an overflow
    bubble, the splitter's trailing " (i/N)"); markers the reply's own text carries stay."""
    return _LAYOUT_SUFFIX_RE.sub("", _FRAME_PREFIX_RE.sub("", bubble))


def _squash(text):
    return "".join(text.split())


async def _answer_frame(transport, stream_id, content, finish):
    """The wire's answer to ONE frame of the fake transport.

    An overflow bubble (``stream_<id>_c<n>``) is opened and closed in one call pair. The main
    turn opens on its seed frame: a finish frame for a turn whose seed never went out lands on the
    <think> placeholder bubble the server opened when the turn was created, not as a new finalize.
    One function, called the same way for every frame, so what a frame is answered with depends on
    the frame alone — never on how this coroutine was scheduled.

    The bubble/turn verdict is STRUCTURAL, never a substring: a turn id is "stream_" + hex and
    the hex alphabet has no underscore, so a main turn carries exactly one "_", while a bubble
    id carries the appended "_c<n>" — two. A "_c" substring test misread every turn whose random
    hex happened to start with 'c' (stream_cc…, 1/16 of turns) as a bubble, and its final frame
    never became the reply's finalize — the flake family this file exists to pin down (DPMS
    561213). A "_c<digits>$" suffix regex is not enough either: hex can be all digits after a
    leading 'c' (stream_c123…), and the suffix then matches a MAIN turn id.
    """
    if stream_id.count("_") > 1:  # an overflow bubble: opened and closed in one call pair
        if not finish:
            transport.bubbles_opened += 1
            if transport.refuse_bubble_at == transport.bubbles_opened:
                transport.bubble_refused = True
            transport._record("bubble-open", content)
            return {"errcode": 0}
        if transport.bubble_refused:
            raise RuntimeError("stream refused this overflow bubble")
        transport.bubble_finishes += 1
        transport._record("bubble-closed", content)
        return {"errcode": 0}
    if not finish:
        if content != "<think></think>":  # the seed is not a content frame
            transport.intermediates_seen += 1
            if transport.drop_intermediate_at == transport.intermediates_seen:
                raise RuntimeError("transport dropped this intermediate frame — the bytes never landed")
        transport.opened_streams.add(stream_id)
        transport._record("frame", content)
        return {"errcode": 0}
    if stream_id not in transport.opened_streams:
        # The placeholder bubble the socket opened for this turn, replaced by this finish frame.
        transport._record("bubble-closed", content)
        return {"errcode": 0}
    transport.finish_frames += 1
    if transport.finish_frames == transport.finish_blows_up_at:
        raise RuntimeError("transport blew up on this finish frame")
    assert len(content.encode("utf-8")) <= MAX_STREAM_CONTENT_LENGTH
    transport._record("finalize", content)
    return {"errcode": 0}


# ----------------------------------------------------------------------
# 1. A reply too long for the frame: sealed bubble + continuation, no second full reply
# ----------------------------------------------------------------------
@pytest.mark.asyncio
async def test_long_reply_is_sealed_and_continued_without_a_second_reply():
    transport = _FakeTransport()
    delivery = WeComStreamDelivery(adapter=_FakeAdapter(transport), chat_id=CHAT)
    delivery.accumulated_text = REPLY
    await delivery._finalize()  # the reply too long for the frame: the bubble is SEALED on its head
    head = delivery._overflow_head
    assert head.startswith("第0段答案") and _wecom_overflow_claimed(delivery)  # regular send cancelled

    # The gateway hands its payload over, then decides: the reply's close-out seals its bubbles.
    delivery.attach_overflow(REPLY + "\n\n" + FOOTER)
    assert delivery.overflow_pending
    await delivery.flush_pending_overflow()
    bubbles = transport.of("bubble-closed")
    assert len(bubbles) >= 2  # a long reply continues in several bubbles, the footer at the end
    assert _plain(bubbles[-1]).strip().endswith(FOOTER)
    assert all(len(b.encode("utf-8")) <= MAX_STREAM_CONTENT_LENGTH for b in bubbles)  # each fits a frame
    # The head in the bubble, then the bubbles: the reply exactly once, in order, footer last.
    assert _squash(REPLY).startswith(_FRAME_RE.sub("", _squash(head)))
    tail = _squash("".join(_plain(b) for b in bubbles))
    assert _squash(REPLY + "\n\n" + FOOTER).endswith(tail)
    assert _squash("".join(_plain(b) for b in bubbles)).count(_squash(FOOTER)) == 1
    # A second hand-over (the gateway's own regular send repeating the body) and a second
    # close-out add nothing: nothing is shown twice.
    opened = transport.of("bubble-open")
    assert delivery.attach_overflow(REPLY + "\n\n" + FOOTER) == []
    await delivery.flush_pending_overflow()
    assert delivery.overflow_parts == [] and transport.of("bubble-open") == opened
    assert transport.of("send") == []  # all of it went out as bubbles of the same reply
    finalizes = transport.of("finalize")  # ONE finalize: the reply has no second complete copy
    assert len(finalizes) == 1, f"one finalize frame, got {len(finalizes)}: {[f[:40] for f in finalizes]}"
    (finalize,) = finalizes
    assert finalize == head and finalize.startswith("第0段答案")  # the head alone, whole in its frame
    assert _squash("".join(bubbles)).count(_squash(FOOTER)) == 1  # the footer rides in once (tail only)


# ----------------------------------------------------------------------
# 2. A short reply: one finalize bubble, nothing queued
# ----------------------------------------------------------------------
@pytest.mark.asyncio
async def test_short_reply_is_one_bubble_and_nothing_queued():
    transport = _FakeTransport()
    delivery = await _run_reply(transport, "答案是 42", "答案是 42")
    assert transport.of("bubble-open") == []
    finalizes = transport.of("finalize")  # ONE bubble, finalized
    assert len(finalizes) == 1, f"one finalize frame, got {len(finalizes)}: {finalizes}"
    (finalize,) = finalizes
    assert "答案是 42" in finalize
    assert delivery.overflow_parts == []
    assert transport.of("send") == []


# ----------------------------------------------------------------------
# 3. Footer ordering
# ----------------------------------------------------------------------
@pytest.mark.asyncio
async def test_footer_rides_in_the_last_continuation_bubble():
    transport = _FakeTransport()
    delivery = WeComStreamDelivery(adapter=_FakeAdapter(transport), chat_id=CHAT)
    delivery.accumulated_text = REPLY
    await delivery._finalize()  # sealed: the reply continues in bubbles
    texts = [_plain(p.decode("utf-8")) for p in delivery.attach_overflow(REPLY + "\n\n" + FOOTER)]
    assert texts[-1].strip().endswith("\n" + FOOTER) or texts[-1].strip() == FOOTER
    assert "\n".join(texts).count(FOOTER) == 1  # once, at the very end


@pytest.mark.asyncio
async def test_a_short_replys_footer_comes_after_its_bubble():
    # A short reply is drawn in its bubble; the gateway's held-back footer is sent after it (the
    # already_sent + footer path), never before the bubble.
    transport = _FakeTransport()
    await _run_reply(transport, "答案是 42", "答案是 42")
    transport._record("send", FOOTER)  # gateway._hmwa_deliver_turn_response's trailing footer
    # The bubble's finalize is the completion point: wait for it to LAND (a pass must mean "it
    # arrived", not "I looked early"), then assert the order itself.
    await async_wait_until(lambda: transport.of("finalize"), timeout=2.0, what="the reply bubble's finalize")
    assert [kind for kind, _t in transport.events if kind in ("finalize", "send")] == ["finalize", "send"]


# ----------------------------------------------------------------------
# 4. Redundant regular send: the gateway decision stamps already_sent
# ----------------------------------------------------------------------
class _Gateway(GatewayTurnPersistenceMixin):
    def __init__(self, adapter):
        self.adapter = adapter
        self.regular_body_sends = []

    def _delivery_adapter_for(self, source):
        return self.adapter

    async def _hmwa_deliver_turn_response(self, *args, **kwargs):
        self.regular_body_sends.append(kwargs)

    def _note_bot_reply(self, source):
        pass


class _Source:
    chat_id = CHAT


class _TurnBinding:
    """The published turn context: the consumer once set up (helpers.StreamConsumerPublication)."""

    def __init__(self, consumer):
        self.stream_consumer_holder = [consumer]
        self.ctx = None


@pytest.mark.asyncio
async def test_gateway_decision_stamps_already_sent_and_sends_nothing():
    transport = _FakeTransport()
    adapter = _FakeAdapter(transport)
    delivery = WeComStreamDelivery(adapter=adapter, chat_id=CHAT)  # the adapter builds its consumer with itself
    delivery.accumulated_text = REPLY
    await delivery._finalize()  # the half-drawn bubble: the stream could not finish it in frame
    gateway = _Gateway(adapter)  # the gateway answers through the adapter the delivery streams on
    result = {"final_response": REPLY, "messages": []}
    with StreamConsumerPublication(_TurnBinding(delivery)):
        await gateway._hmwa_attach_wecom_overflow(result, _Source(), REPLY, FOOTER, False)
        await gateway._deliver_turn_response_then_limit_offer(
            None, _Source(), object(), "sk", 1, result, [], REPLY, FOOTER, False)
    assert result["already_sent"] is True  # the gateway's own regular send is cancelled
    assert _wecom_overflow_claimed(delivery)
    assert delivery.overflow_parts == [] and transport.of("bubble-closed")  # its bubbles went out
    assert transport.of("send") == []  # nothing was regular-sent
    assert gateway.regular_body_sends  # ...but the delivery path still ran


# ----------------------------------------------------------------------
# 5. A failed continuation bubble never swallows content
# ----------------------------------------------------------------------
@pytest.mark.asyncio
async def test_a_refused_bubble_sends_it_and_the_rest_as_messages():
    transport = _FakeTransport(refuse_bubble_at=1)  # the first overflow bubble is refused
    delivery = WeComStreamDelivery(adapter=_FakeAdapter(transport), chat_id=CHAT)
    delivery.accumulated_text = REPLY
    await delivery._finalize()  # sealed: the reply continues in bubbles
    queued = [p.decode("utf-8") for p in delivery.attach_overflow(REPLY + "\n\n" + FOOTER)]
    assert len(queued) >= 2, "the reply plus its footer continue in several bubbles"
    await delivery.flush_pending_overflow()
    assert delivery.overflow_parts == []
    fallback = transport.of("send")
    assert fallback, "the refused bubble is regular-sent, never dropped"
    got = _squash("".join(fallback))
    for bubble in queued:  # every bubble's text reached the chat, in order
        pos = got.find(_squash(_plain(bubble)))
        assert pos >= 0, bubble[:40]
        got = got[pos + len(_squash(_plain(bubble))):]
    assert FOOTER in "".join(fallback)


@pytest.mark.asyncio
async def test_an_exploding_bubble_also_falls_back_end_of_reply():
    transport = _FakeTransport(finish_blows_up_at=1)  # the finalize frame blows up: the bubble cannot close
    delivery = await _run_reply(transport, REPLY, REPLY + "\n\n" + FOOTER)
    assert delivery.overflow_parts
    await delivery.flush_pending_overflow()  # its adapter seals the bubbles (a plain one, then the rest)
    assert delivery.overflow_parts == []
    landed = "\n".join(t for k, t in transport.events if k in ("bubble-closed", "send"))
    assert FOOTER in landed  # the reply's tail went out, nothing silent


# ----------------------------------------------------------------------
# 6. Reply lane: the holder's own sends never wait on it, outsiders do — with a bound
# ----------------------------------------------------------------------
class _Probe(WeComStreamMixin, ChatSendQueueMixin):
    """The real streaming/send-queue mixins over a fake transport that journals each send."""

    name = "probe"

    def __init__(self):
        self._stream_turns, self._stream_expired_chats, self._last_stream_turn_by_chat = {}, set(), {}
        self._last_chat_req_ids = {CHAT: REQ}
        self._reply_queues = {}
        self._chat_queues, self._chat_workers = {}, {}
        self._control_queues, self._control_workers = {}, {}
        self._chat_token_usage = {}
        self._stream_keepalive_enabled, self._stream_safe_duration_seconds = True, 300.0
        self.journal = []

    async def _send_stream_reply(self, reply_req_id, stream_id, content, finish=False):
        if finish:
            self.journal.append("finalize")
        return {"errcode": 0}

    async def _send_proactive_markdown(self, chat_id, content, chat_type=None):
        self.journal.append("regular-in-progress")
        await asyncio.sleep(0.05)
        self.journal.append("regular")
        return {"errcode": 0}

    def _raise_for_wecom_error(self, response, what):
        return None

    def _stream_turn(self):
        for turn in self._stream_turns.values():
            return turn
        raise AssertionError("no turn")


@pytest.mark.asyncio
async def test_a_send_reaching_the_lane_mid_reply_queues_behind_the_frames():
    # The property the per-frame lane buys: a regular send issued while the reply still streams
    # cannot overtake the frames after it — it waits for the one in flight, then goes.
    delivery = WeComStreamDelivery(adapter=_FakeAdapter(_FakeTransport()), chat_id=CHAT)
    order = []

    async def stream_side():
        async with lane_mod.reply_lane(CHAT):  # a frame being sent
            await asyncio.sleep(0.05)
            order.append("frame")
        order.append("reply-done")

    frame = asyncio.create_task(stream_side(), context=contextvars.Context())
    await asyncio.sleep(0)  # the frame owns the lane
    send = asyncio.create_task(_outside_send(delivery, order), context=contextvars.Context())
    await asyncio.wait_for(asyncio.gather(frame, send), timeout=2.0)
    assert order == ["frame", "reply-done", "regular"]


async def _outside_send(delivery, order):
    async with lane_mod.reply_lane(delivery.chat_id):
        pass
    order.append("regular")


@pytest.mark.asyncio
async def test_the_replys_own_task_enters_without_waiting_and_outsiders_queue_fifo():
    order = []

    async def child(context=None):
        await asyncio.sleep(0)  # scheduled by the holder: it INHERITED the holder's context
        async with lane_mod.reply_lane(CHAT):
            order.append("child")

    async def outsider(name, context=None):
        await asyncio.sleep(0)  # an unrelated task: the test's own context, not the holder's
        async with lane_mod.reply_lane(CHAT):
            order.append(name)

    async with lane_mod.reply_lane(CHAT):
        await asyncio.wait_for(asyncio.create_task(child()), timeout=2.0)  # a task the holder started
        first = asyncio.create_task(outsider("outsider-1"), context=contextvars.Context())
        await asyncio.sleep(0.01)
        second = asyncio.create_task(outsider("outsider-2"), context=contextvars.Context())
        await asyncio.sleep(0.05)
        assert order == ["child"], "outside replies wait for the reply in front (FIFO)"
    await asyncio.wait_for(asyncio.gather(first, second), timeout=2.0)
    assert order == ["child", "outsider-1", "outsider-2"]


@pytest.mark.asyncio
async def test_reply_lane_outsider_times_out_instead_of_hanging(monkeypatch, caplog):
    monkeypatch.setattr(lane_mod, "_REPLY_LANE_WAIT_SECONDS", 0.1)
    entered = []

    async def outsider():
        async with lane_mod.reply_lane(CHAT):
            entered.append(True)

    with caplog.at_level("WARNING", logger=lane_mod.__name__):
        async with lane_mod.reply_lane(CHAT):
            task = asyncio.create_task(outsider(), context=contextvars.Context())  # not the holder's
            await asyncio.wait_for(task, timeout=5)  # bounded, never hangs
    assert entered == [True]
    assert "sending unordered" in caplog.text


# ----------------------------------------------------------------------
# 7. adapter.send() over the real path (B1: async lane, no TypeError)
# ----------------------------------------------------------------------
def test_adapter_send_runs_over_the_real_reply_path():
    async def scenario():
        transport = _FakeTransport()
        adapter = _BareAdapter(transport)
        result = await adapter.send(CHAT, "x")
        assert result.success
        assert transport.of("send") == ["x"]

    asyncio.run(asyncio.wait_for(scenario(), timeout=10))


# ----------------------------------------------------------------------
# 8. B6: a new inbound makes the chat's sealed turn stale (the overflow anchor is dropped)
# ----------------------------------------------------------------------
def test_new_inbound_clears_the_overflow_anchor():
    transport = _FakeTransport()
    sealed = StreamTurn(CHAT, REQ)
    sealed.finalized = True
    transport._last_stream_turn_by_chat[CHAT] = sealed
    assert transport._find_overflow_anchor(CHAT) is sealed
    sealed.req_id_fresh = False  # what adapter._handle_inbound_frame does for the next inbound
    assert transport._find_overflow_anchor(CHAT) is None


# ----------------------------------------------------------------------
# 9. Regression (DPMS 561213): one case's leftover chat state must not decide the next case
# ----------------------------------------------------------------------
@pytest.mark.asyncio
async def test_a_case_killed_mid_frame_leaves_a_locked_chat_lane():
    # The residue a case killed mid-frame leaves: its chat's lane entry stays LOCKED in the
    # process-global registry — the holder never got to release (its loop died under it). This
    # case reproduces that residue exactly (no task leak): it ends holding the chat's lane.
    lane = lane_mod._lane(CHAT)
    await lane.lock.acquire()
    assert lane_mod._LANES.get(CHAT) is lane and lane.lock.locked()


@pytest.mark.asyncio
async def test_the_case_after_a_killed_reply_still_finalizes_its_bubble():
    # The case above ended with CHAT's lane locked forever (what a killed reply leaves behind).
    # THIS case's reply must still stream on ITS OWN lane: the bubble finalizes exactly once and
    # nothing falls back to the regular send — the frames cannot inherit the dead lane.
    transport = _FakeTransport()
    await asyncio.wait_for(_run_reply(transport, "答案是 42", "答案是 42"), timeout=5.0)
    finalizes = transport.of("finalize")  # ONE bubble, finalized — the reply landed once
    assert len(finalizes) == 1, f"one finalize frame, got {len(finalizes)}: {finalizes}"
    assert "答案是 42" in finalizes[0]
    assert transport.of("send") == []  # the stream worked: no regular-send fallback


# ----------------------------------------------------------------------
# 10. Regression (DPMS 561213): the wire's bubble/turn verdict must not ride on a random id
# ----------------------------------------------------------------------
@pytest.mark.asyncio
async def test_a_turn_whose_random_stream_id_starts_with_c_is_still_a_turn():
    # The wire told an overflow bubble (``stream_<id>_c<n>``) from a MAIN turn by the "_c"
    # SUBSTRING — but a turn id is "stream_" + random hex, and hex starting with 'c' (1/16 of
    # turns: stream_cc…) contains "_c" too. That turn's seed then rode in as "bubble-open" and
    # its final frame NEVER became the reply's finalize — the whole flake family (a finalize
    # count of 0, a footer arriving "early", a reply "changed after the stream"). The verdict
    # must be structural (a bubble id carries the appended _c<n>, i.e. a SECOND underscore),
    # never a random middle. Both trap shapes are pinned: hex starting with 'c', and hex that
    # is ALL digits after it (a suffix regex would misread that one as a bubble too).
    for c_turn in ("stream_c" + "0" * 11, "stream_c12345678901"):
        transport = _FakeTransport()
        await _answer_frame(transport, c_turn, "<think></think>", finish=False)  # its seed
        await _answer_frame(transport, c_turn, "答案是 42", finish=True)  # the reply's own finalize
        assert transport.of("frame") == ["<think></think>"]  # a turn's seed is a frame, not a bubble
        assert transport.of("bubble-open") == [] and transport.of("bubble-closed") == []
        assert transport.of("finalize") == ["答案是 42"] and transport.finish_frames == 1
        # An overflow bubble of the SAME turn still answers as a bubble, open then close:
        await _answer_frame(transport, c_turn + "_c1", "续", finish=False)
        await _answer_frame(transport, c_turn + "_c1", "续", finish=True)
        assert transport.of("bubble-open") == ["续"] and transport.of("bubble-closed") == ["续"]
        assert transport.bubbles_opened == 1 and transport.bubble_finishes == 1
