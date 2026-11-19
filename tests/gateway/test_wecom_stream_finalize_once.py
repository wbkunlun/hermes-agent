"""Regression (fork 2026-11-18, DPMS 561213): a finalize that never lands must not re-send what
the bubble already drew — the WeCom long-reply double-send, treated at the root.

Production hm-5958d932-1f3 (2026-11-18 17:11→17:13): a 1190-char reply was drawn progressively in
the stream bubble, the finalize frame never landed (14s of silence in the decision window), and
the gateway regular-sent the SAME 1190 chars as a second message ("[Wecom] Sending response
(1190 chars)"). WeComStreamDelivery never implemented the gateway's stream-consumer durable-
delivery contract (``has_durably_delivered_text``, upstream #51828/#33793), so every finalize
decline/expiry/wire error left all suppression signals False. The fix: landed frames record what
they durably drew (``_durable_text``, snapshotted at display build); the gateway suppression
consults it; a partial draw hands ONLY the remainder to the overflow bubbles and claims the reply
so the regular send — the duplicate — stands cancelled. Keepalive ON throughout (production
``stream_keepalive_enabled: true``). These tests drive the REAL run loop, adapter frame path,
gateway suppression decision and persistence delivery against a fake transport.
"""
import asyncio
from types import SimpleNamespace

import pytest
import pytest_asyncio

from gateway.run_turn import GatewayTurnMixin
from gateway.run_turn_persistence import GatewayTurnPersistenceMixin
from gateway.run_turn_wecom_overflow import _wecom_overflow_claimed
from plugins.platforms.wecom import reply_lane as lane_mod
from plugins.platforms.wecom.adapter import WeComAdapter
from plugins.platforms.wecom.stream_delivery import WeComStreamDelivery
from plugins.platforms.wecom.streaming import STREAM_KEEPALIVE_INTERVAL_SECONDS
from tests.gateway.test_wecom_reply_once import (
    CHAT,
    FOOTER,
    REQ,
    _FakeAdapter,
    _FakeTransport,
    _close_queues,
)
from tests.waiting import async_wait_until

# The streamed answer, fed as progressive deltas — three visible pieces, none near the frame cap:
# the finalize is the NORMAL-size path and fails on the wire (finish frame blows up), the exact
# production shape (frames drew everything, the finish frame did not land).
P1 = "第一段：问题的根因是 finalize 从未落地。\n\n"
P2 = "第二段：渐进帧已把全文画进气泡，屏幕上有了完整答案。\n\n"
P3 = "第三段：封口只需 finish 帧，任何分支都不得重发全文。"
FULL = P1 + P2 + P3


@pytest_asyncio.fixture(autouse=True)
async def _clean_transports():
    """Every transport this test module builds has its send workers stopped AND awaited at the end
    (a worker left running belongs to this test's loop, never the next test's), and the
    process-global lane registry starts and ends empty."""
    lane_mod._LANES.clear()
    built = []
    original = _FakeTransport.__init__

    def recording(transport, *args, **kwargs):
        original(transport, *args, **kwargs)
        transport.reset_chat()
        transport._stream_keepalive_enabled = True  # production: stream_keepalive_enabled: true
        # the real adapter init reads stream_keepalive_interval_seconds (default 120s); without
        # it the seed's keepalive arm raises and the frame right after the seed is dropped.
        transport._stream_keepalive_interval_seconds = STREAM_KEEPALIVE_INTERVAL_SECONDS
        built.append(transport)

    _FakeTransport.__init__ = recording
    try:
        yield
    finally:
        _FakeTransport.__init__ = original
        for transport in built:
            await _close_queues(transport)
        lane_mod._LANES.clear()


async def _stream_progressively(transport, pieces, *, finish=True):
    """Drive the REAL run loop: wait until each piece landed in its own progressive frame (one
    throttle tick apart, as in production), then finish; the finalize finish frame blows up on the
    wire. With ``finish=False`` the run loop stays alive so the caller can end the turn itself.
    Returns ``(delivery, task)``."""
    transport.finish_blows_up_at = 1  # the finalize finish frame never lands
    delivery = WeComStreamDelivery(adapter=_FakeAdapter(transport), chat_id=CHAT)
    delivery.stream_deltas_enabled = True
    task = asyncio.create_task(delivery.run())
    # The run loop's OPENING push must land before the first delta: a delta fed while the loop
    # has not ticked yet rides the open frame itself (accumulated text is already in its display)
    # and that piece then never gets a frame of its own — the count-based waits below would hang.
    await async_wait_until(
        lambda: len(transport.of("frame")) >= 2, timeout=5.0,
        what="the open bubble frame (seed + open)",
    )
    landed = 2  # the seed frame and the open frame above; each fed piece lands one more
    for piece in pieces:
        delivery.on_delta(piece)
        landed += 1
        await async_wait_until(
            lambda n=landed: len(transport.of("frame")) >= n,
            timeout=5.0, what=f"progressive frame #{landed}",
        )
    await async_wait_until(
        lambda: transport.of("frame") and pieces[-1] in transport.of("frame")[-1],
        timeout=5.0, what="the last piece drawn in a landed frame",
    )
    if finish:
        delivery.finish()
        await asyncio.wait_for(task, timeout=5.0)
    return delivery, task


def _suppresses(delivery, final_text):
    """The exact gateway decision (run_turn._run_agent_stream_confirmed_final_delivery): True when
    the normal final send must NOT fire."""
    return GatewayTurnMixin._run_agent_stream_confirmed_final_delivery(delivery, final_text)


# ---------------------------------------------------------------------------
# (a) ≥3 progressive frames drew the WHOLE answer, the finalize never landed:
#     the gateway suppresses the regular send and the hand-over queues nothing —
#     the reply is on screen exactly once.
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_full_drawn_finalize_failed_gateway_suppresses_and_nothing_requeues():
    transport = _FakeTransport()
    delivery, _task = await _stream_progressively(transport, [P1, P2, P3])

    # The finalize frame blew up: nothing claimed the reply through it...
    assert delivery.final_response_sent is False
    assert not _wecom_overflow_claimed(delivery)
    # ...but every landed frame made its snapshot durable: the WHOLE answer is on screen.
    assert transport.of("finalize") == []  # the finish frame blew up: no finalize ever landed
    assert len(transport.of("frame")) >= 4  # open + one landed progressive frame per piece
    assert FULL in transport.of("frame")[-1]  # the last landed frame drew it whole

    # The gateway's suppression decision reads what the bubble durably drew — the hook that did
    # not exist before this fix (the production duplicate).
    assert _suppresses(delivery, FULL) is True
    assert delivery.has_durably_delivered_text(FULL) is True
    # A strict prefix of the drawn text does not flip it either — delivered means delivered.
    assert delivery.has_durably_delivered_text(P1 + P2) is True
    # A partial draw (a piece the bubble never showed) must NOT suppress.
    assert delivery.has_durably_delivered_text(FULL + "补一段") is False

    # The hand-over queues nothing (every character is already in the bubble) and the reply stays
    # unclaimed: the suppression above is what cancels the regular send.
    assert delivery.attach_overflow(FULL) == []
    assert not _wecom_overflow_claimed(delivery)
    await delivery.flush_pending_overflow()
    assert transport.of("send") == []  # no second copy of the reply anywhere
    assert transport.of("bubble-closed") == []  # no overflow bubble either: it was all on screen


# ---------------------------------------------------------------------------
# (b) partial draw (the last piece never landed), finalize failed: the hand-over queues ONLY the
#     remainder + footer, claims the reply — the regular send of the full text stands cancelled
#     and the drawn part is never repeated.
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_partial_draw_finalize_failed_only_the_remainder_is_queued_and_claimed():
    transport = _FakeTransport()
    delivery, task = await _stream_progressively(transport, [P1, P2], finish=False)
    # The third piece arrives and the turn ends in the same breath: no throttle tick passes, so
    # p3 is NEVER drawn (the finalize attempt carrying it blows up). The bubble shows P1+P2 only.
    delivery.on_delta(P3)
    delivery.finish()
    await asyncio.wait_for(task, timeout=5.0)

    assert delivery.final_response_sent is False
    assert delivery.has_durably_delivered_text(P1 + P2) is True
    assert delivery.has_durably_delivered_text(FULL) is False  # partial: must NOT suppress
    assert _suppresses(delivery, FULL) is False  # the regular send stays attach_overflow's to cancel
    assert P3 not in transport.of("frame")[-1]  # p3 was never drawn

    # The gateway hands over the completed reply (footer included): only what the bubble does not
    # show yet is queued, and the claim cancels the duplicate regular send.
    handed = FULL + "\n\n" + FOOTER
    queued = [p.decode("utf-8") for p in delivery.attach_overflow(handed)]
    assert _wecom_overflow_claimed(delivery), "the claim is what cancels the regular send"
    joined = "".join(queued)
    assert P1 not in joined and P2 not in joined, "the drawn part must never be queued again"
    assert P3 in joined and FOOTER in joined, "only the remainder rides the continuation"

    await delivery.flush_pending_overflow()
    # The blown finalize retired the turn: no stream is left to continue, so the queued remainder
    # falls back to the adapter's regular send — once, remainder only.
    assert transport.of("bubble-closed") == []
    sends = "".join(transport.of("send"))
    assert sends, "the remainder must still reach the user"
    assert P1 not in sends and P2 not in sends, "the drawn part is never sent again"
    assert P3 in sends and FOOTER in sends

    # Each piece is presented on exactly ONE surface: the drawn ones only in frames, the
    # remainder only in the fallback send — never the same text on both.
    frames = "".join(transport.of("frame"))
    for piece in (P1.strip(), P2.strip()):
        assert piece in frames and piece not in sends
    assert P3.strip() not in frames and P3.strip() in sends


# ---------------------------------------------------------------------------
# (b3) a CONTENT frame the wire dropped must never overcount durability: send_stream_frame
#      returns True for a dropped intermediate too (the stream stays alive), so the durability
#      record is confirmed against what actually landed — a later failed finalize then hands
#      over the TRUE remainder instead of suppressing a reply the bubble never finished.
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_dropped_intermediate_frame_never_overcounts_durability():
    transport = _FakeTransport(finish_blows_up_at=1, drop_intermediate_at=4)
    # the frame carrying P3 never lands AND the finalize blows up on the same flaky wire
    # (content frames: the open frame is #1, P1 #2, P2 #3 — the seed never counts)
    delivery = WeComStreamDelivery(adapter=_FakeAdapter(transport), chat_id=CHAT)
    delivery.stream_deltas_enabled = True
    task = asyncio.create_task(delivery.run())
    await async_wait_until(
        lambda: len(transport.of("frame")) >= 2, timeout=5.0,
        what="the open bubble frame (seed + open)",
    )
    delivery.on_delta(P1)
    await async_wait_until(lambda: len(transport.of("frame")) >= 3, timeout=5.0, what="the frame carrying P1")
    delivery.on_delta(P2)
    await async_wait_until(lambda: len(transport.of("frame")) >= 4, timeout=5.0, what="the frame carrying P2")
    # The wire drops the next content frame: the run loop pushes P3 (True) but it never lands.
    delivery.on_delta(P3)
    await async_wait_until(
        lambda: bool(delivery.last_pushed_display) and P3.strip() in delivery.last_pushed_display,
        timeout=5.0, what="the run loop's push of P3 (dropped by the wire)",
    )
    delivery.finish()
    await asyncio.wait_for(task, timeout=5.0)

    # P3 never reached the screen, and the finalize blew up on the same flaky wire...
    assert not any(P3 in f for f in transport.of("frame"))
    assert transport.of("finalize") == []
    assert delivery.final_response_sent is False
    # ...yet durability stayed honest: only the frames that landed are recorded.
    assert delivery.has_durably_delivered_text(P1 + P2) is True
    assert delivery.has_durably_delivered_text(FULL) is False
    assert _suppresses(delivery, FULL) is False  # no suppression: the reply is NOT fully shown

    # The hand-over queues the true remainder (the dropped piece + footer) and claims the reply,
    # so the regular send — which would have duplicated P1+P2 — stands cancelled.
    handed = FULL + "\n\n" + FOOTER
    queued = [p.decode("utf-8") for p in delivery.attach_overflow(handed)]
    assert _wecom_overflow_claimed(delivery)
    joined = "".join(queued)
    assert P1 not in joined and P2 not in joined, "the drawn part must never be queued again"
    assert P3 in joined and FOOTER in joined, "the dropped piece rides the continuation"

    await delivery.flush_pending_overflow()
    assert transport.of("bubble-closed") == []  # the turn died with the finalize: no bubbles
    sends = "".join(transport.of("send"))
    assert P1 not in sends and P2 not in sends, "the drawn part must never be queued again"
    assert P3 in sends and FOOTER in sends, "the dropped piece reaches the user exactly once"


# ---------------------------------------------------------------------------
# (b2) the same partial-draw reply through the REAL persistence delivery: the claim stamps
#      already_sent and the delivery path sends nothing.
# ---------------------------------------------------------------------------
class _Gateway(GatewayTurnPersistenceMixin):
    """The persistence delivery over the fake adapter. The delivery adapter IS the delivery's own
    adapter object (one delivery channel — the real gateway resolves the same instance), voice
    reply is off; ``_hmwa_deliver_turn_response``'s claimed-skip decision stays REAL."""

    def __init__(self, adapter):
        self.adapter = adapter
        self.bot_replies = 0

    def _delivery_adapter_for(self, source):
        return self.adapter

    def _should_send_voice_reply(self, *args, **kwargs):
        return False

    def _note_bot_reply(self, source):
        self.bot_replies += 1

    async def _deliver_media_from_response(self, response, event, adapter, thread_metadata=None):
        return None  # the harness carries no media pipeline


class _Source:
    chat_id = CHAT


class _TurnBinding:
    def __init__(self, consumer):
        self.stream_consumer_holder = [consumer]
        self.ctx = None


@pytest.mark.asyncio
async def test_partial_draw_through_gateway_delivery_stamps_already_sent_and_sends_nothing():
    from gateway.platforms.helpers import StreamConsumerPublication

    transport = _FakeTransport()
    delivery, task = await _stream_progressively(transport, [P1, P2], finish=False)
    delivery.on_delta(P3)
    delivery.finish()
    await asyncio.wait_for(task, timeout=5.0)

    gateway = _Gateway(delivery.adapter)
    result = {"final_response": FULL, "messages": []}
    event = SimpleNamespace()
    session_entry = SimpleNamespace(session_id="sess-finalize-once")
    with StreamConsumerPublication(_TurnBinding(delivery)):
        delivered = await gateway._deliver_turn_response_then_limit_offer(
            event, _Source(), session_entry, "sk", 1, result, [], FULL, FOOTER, False)
    assert result["already_sent"] is True  # the claim cancelled the regular send
    assert _wecom_overflow_claimed(delivery)
    assert delivered is None  # no body left to send: the 1190-char second message never happens
    assert gateway.bot_replies == 1  # the turn still closes out as replied
    sends = "".join(transport.of("send"))
    assert P1 not in sends and P2 not in sends  # the drawn part is never regular-sent
    assert P3 in sends and FOOTER in sends  # only the remainder rode the fallback


# ---------------------------------------------------------------------------
# (a2) the full-drawn reply through the REAL gateway mark: the suppression stamp lands on the
#      response, so the completion path never regular-sends.
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_full_drawn_through_gateway_mark_suppresses_the_normal_send():
    transport = _FakeTransport()
    delivery, _task = await _stream_progressively(transport, [P1, P2, P3])

    turn_ctx = SimpleNamespace(
        stream_consumer_holder=[delivery],
        source=SimpleNamespace(chat_id=CHAT),
        session_key="sk-stream-finalize",
    )
    gateway = GatewayTurnMixin.__new__(GatewayTurnMixin)  # the mixin method uses no other state
    response = {"final_response": FULL, "messages": []}
    await gateway._run_agent_mark_streamed_delivery(response, turn_ctx)
    assert response.get("already_sent") is True
    assert response.get("delivery_uncertain") is False


# ---------------------------------------------------------------------------
# (c) no streaming at all (the bubble never opened): existing behaviour unchanged — no
#     suppression, no claim, the regular send delivers as before.
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_no_streaming_keeps_the_regular_send():
    transport = _FakeTransport()
    delivery = WeComStreamDelivery(adapter=_FakeAdapter(transport), chat_id=CHAT)
    delivery.stream_deltas_enabled = True
    delivery._disabled = True  # the stream never opened (no cached req_id → gave up at once)

    assert delivery.has_durably_delivered_text(FULL) is False  # nothing was ever drawn
    assert _suppresses(delivery, FULL) is False
    assert delivery.attach_overflow(FULL) == []
    assert not _wecom_overflow_claimed(delivery)  # the regular send delivers, exactly as before
    assert transport.of("frame") == [] and transport.of("send") == []


# ---------------------------------------------------------------------------
# (f) fork 2026-11-19 (DPMS 561213): the TRANSFORMED final — a plugin footer appended to
#      the reply after the stream — whose body the bubble already shows must not go out
#      again as a plain message. Production hm-5958d932-1f3 (2026-11-19 09:08): the final
#      frame landed ("final_frame_delivered chars=229" — the 193-char reply inside the
#      bubble's own decoration) and the gateway sent the same 193 chars again ("[Wecom]
#      Sending response (193 chars)"). The reply is presented exactly once.
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_transformed_final_already_shown_is_presented_once():
    from gateway.platforms.helpers import StreamConsumerPublication

    transport = _FakeTransport()
    delivery = WeComStreamDelivery(adapter=_FakeAdapter(transport), chat_id=CHAT)
    delivery.stream_deltas_enabled = True
    task = asyncio.create_task(delivery.run())
    await async_wait_until(
        lambda: len(transport.of("frame")) >= 2, timeout=5.0,
        what="the open bubble frame (seed + open)",
    )
    delivery.on_delta(P1)
    await async_wait_until(
        lambda: bool(transport.of("frame")) and P1.strip() in transport.of("frame")[-1],
        timeout=5.0, what="the frame carrying the body",
    )
    # The plugin rewrote the reply (the footer) before the stream sealed: the final frame
    # draws the WHOLE transformed reply, so the bubble shows it whole.
    transformed = FULL + "\n\n" + FOOTER
    delivery.finish(transformed)
    await asyncio.wait_for(task, timeout=5.0)
    assert delivery.final_response_sent is True

    turn_ctx = SimpleNamespace(
        stream_consumer_holder=[delivery],
        source=SimpleNamespace(chat_id=CHAT),
        session_key="sk-transformed-once",
    )
    gateway = GatewayTurnMixin.__new__(GatewayTurnMixin)
    result = {"final_response": transformed, "messages": [], "response_transformed": True}
    await gateway._run_agent_mark_streamed_delivery(result, turn_ctx)
    assert result.get("already_sent") is True  # the plain send is cancelled

    gateway_p = _Gateway(delivery.adapter)
    event = SimpleNamespace()
    session_entry = SimpleNamespace(session_id="sess-transformed-once")
    with StreamConsumerPublication(_TurnBinding(delivery)):
        delivered = await gateway_p._deliver_turn_response_then_limit_offer(
            event, _Source(), session_entry, "sk", 1, result, [], transformed, "", False)
    assert delivered is None  # no second copy of the reply
    assert transport.of("send") == []  # the bubble is the reply's ONE presentation
    assert len(transport.of("finalize")) == 1  # one finalize frame
    assert transformed in transport.of("finalize")[0]
    # What the bubble proved it drew: the record the decision was made on.
    assert delivery.durable_answer_text == transformed


# ---------------------------------------------------------------------------
# (e) a passive reply whose ACK times out (bytes already on the wire) is treated as delivered:
#     no proactive fallback re-send, group or DM.
# ---------------------------------------------------------------------------
class _AckTimeoutAdapter(WeComAdapter):
    """The real adapter's send path over a bare surface; the passive reply's ack never comes."""

    def __init__(self):
        from gateway.config import Platform
        self.transport_journal = []
        self.platform = Platform.WECOM  # the base's `name` property reads platform.value
        self.MAX_MESSAGE_LENGTH = 4000
        self._group_chat_ids = set()
        self._last_chat_req_ids = {CHAT: REQ}
        self._chat_queues, self._chat_workers, self._control_queues, self._control_workers = {}, {}, {}, {}
        self._chat_token_usage = {}

    async def _send_reply_markdown(self, req_id, content):
        # _send_reply_correlated's TimeoutError: raised AFTER the bytes left (_send_json) —
        # the ack wait only.
        self.transport_journal.append(("passive-bytes-sent", content))
        raise TimeoutError()

    async def _send_proactive_markdown(self, chat_id, content, chat_type=None):
        self.transport_journal.append(("proactive", content))  # the duplicate this test forbids
        return {"errcode": 0}

    def _raise_for_wecom_error(self, response, what):
        return None


@pytest.mark.asyncio
async def test_passive_ack_timeout_dm_is_delivered_assumed_never_resent():
    adapter = _AckTimeoutAdapter()
    result = await adapter._send_inner(CHAT, "这条回复只出现一次")
    assert result.success is True
    assert result.raw_response.get("errmsg") == "ack_timeout_assumed_delivered"
    kinds = [k for k, _t in adapter.transport_journal]
    assert kinds == ["passive-bytes-sent"]  # the proactive fallback never fired


@pytest.mark.asyncio
async def test_passive_ack_timeout_group_is_delivered_assumed_never_resent():
    adapter = _AckTimeoutAdapter()
    adapter._group_chat_ids = {CHAT}  # the group branch used to fall through to aibot_send_msg
    result = await adapter._send_inner(CHAT, "这条群回复也只出现一次")
    assert result.success is True
    assert result.raw_response.get("errmsg") == "ack_timeout_assumed_delivered"
    assert [k for k, _t in adapter.transport_journal] == ["passive-bytes-sent"]
