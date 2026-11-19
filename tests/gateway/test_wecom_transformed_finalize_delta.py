"""Regression (fork 2026-11-19, DPMS 561213): a reply the bubble already shows must never go out
a second time as a plain message — the TRANSFORMED finalize branch.

Production hm-5958d932-1f3 (2026-11-19 09:08, instance kestrel4): the stream bubble drew the whole
reply — "final_frame_delivered chars=229" (the 193-char reply inside the bubble's own decoration:
the think block and the session link) — and the gateway sent the SAME 193 chars again as a plain
message ("[Wecom] Sending response (193 chars)"). The reply carried a plugin footer (the reply-footer
plugin rewrote it after the stream), so the finalize took the TRANSFORMED branch, and that branch
had nowhere to go on a platform that cannot edit a sent message (WeCom): with no editable message
id it neither edited nor cancelled, and the plain send became the reply's second copy.

The rule is now "what is on screen": the platform's durable record (what a landed frame actually
drew) answers it. Nothing left → the plain send is cancelled; a tail the bubble never drew (a
footer appended after the last frame) → only that tail goes out, never the body again.

These tests drive the REAL WeCom delivery (its final frame, its durable record), the REAL gateway
finalize decision and the REAL persistence delivery against a fake transport — no network, no
platform.
"""
import pytest
import pytest_asyncio

from types import SimpleNamespace

from gateway.platforms.helpers import StreamConsumerPublication
from gateway.run_turn import GatewayTurnMixin
from gateway.run_turn_persistence import GatewayTurnPersistenceMixin
from plugins.platforms.wecom import reply_lane as lane_mod
from plugins.platforms.wecom.stream_delivery import WeComStreamDelivery
from tests.gateway.test_wecom_reply_once import (
    CHAT,
    _FakeAdapter,
    _FakeTransport,
    _close_queues,
)

BODY = "我是 Hermes，你的运维 AI 助手。"
# What the reply-footer plugin appends after the stream (transform_llm_output): the transformed
# final reply is the body plus this footer.
PLUGIN_FOOTER = "\n\n---\n📊 Model: test · Input 12 · Output 34 · Duration 1.0s · Turns 1"
FULL = BODY + PLUGIN_FOOTER


@pytest_asyncio.fixture(autouse=True)
async def _clean_transports():
    """Every transport this module builds has its send workers stopped AND awaited at the end (a
    worker left running belongs to this test's loop, never the next test's), and the process-global
    lane registry starts and ends empty."""
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


async def _landed_bubble(transport, drawn_text):
    """The REAL delivery with its final frame LANDED: the bubble shows *drawn_text*."""
    delivery = WeComStreamDelivery(adapter=_FakeAdapter(transport), chat_id=CHAT)
    delivery.stream_deltas_enabled = True
    delivery.accumulated_text = drawn_text
    delivery.finished = True
    await delivery._finalize()
    assert delivery.final_response_sent is True  # the final frame landed
    return delivery


class _Gateway(GatewayTurnPersistenceMixin):
    """The persistence delivery over the fake adapter: the delivery adapter IS the delivery's own
    adapter object, voice reply is off; ``_hmwa_deliver_turn_response``'s decisions stay REAL."""

    def __init__(self, adapter):
        self.adapter = adapter
        self.bot_replies = 0

    def _delivery_adapter_for(self, source):
        return self.adapter

    def _event_thread_metadata(self, event, source):
        return None  # the harness carries no thread routing

    def _should_send_voice_reply(self, *args, **kwargs):
        return False

    def _note_bot_reply(self, source):
        self.bot_replies += 1

    async def _deliver_media_from_response(self, response, event, adapter, thread_metadata=None, footer_line=""):
        return None  # the harness carries no media pipeline


class _Source:
    chat_id = CHAT


class _TurnBinding:
    def __init__(self, consumer):
        self.stream_consumer_holder = [consumer]
        self.ctx = None


def _turn_ctx(consumer):
    return SimpleNamespace(
        stream_consumer_holder=[consumer],
        source=SimpleNamespace(chat_id=CHAT),
        session_key="sk-transformed-delta",
    )


async def _mark(gateway, consumer, result):
    await gateway._run_agent_mark_streamed_delivery(result, _turn_ctx(consumer))
    return result


async def _deliver(consumer, result, response_text, footer_line=""):
    """The completion path: the gateway's delivery decision over the fake adapter."""
    gateway = _Gateway(consumer.adapter)
    event = SimpleNamespace()
    session_entry = SimpleNamespace(session_id="sess-transformed-delta")
    with StreamConsumerPublication(_TurnBinding(consumer)):
        delivered = await gateway._deliver_turn_response_then_limit_offer(
            event, _Source(), session_entry, "sk", 1, result, [], response_text, footer_line, False,
        )
    return gateway, delivered


# ---------------------------------------------------------------------------
# (a) the transformed final, no editable message id, the final frame already drew
#     EVERYTHING: the plain send is cancelled — the reply is on screen exactly once.
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_transformed_final_already_on_screen_is_never_plain_sent_again():
    transport = _FakeTransport()
    delivery = await _landed_bubble(transport, FULL)  # the bubble drew the whole reply

    gateway = GatewayTurnMixin.__new__(GatewayTurnMixin)  # the mixin method uses no other state
    result = await _mark(gateway, delivery, {
        "final_response": FULL, "messages": [], "response_transformed": True,
    })

    assert result.get("already_sent") is True
    assert "_final_unshown_remainder" not in result  # nothing is left to send

    gateway_p, delivered = await _deliver(delivery, result, FULL)
    assert delivered is None  # the plain send never happens
    assert transport.of("send") == []  # no second copy of the reply anywhere
    finalizes = transport.of("finalize")
    assert len(finalizes) == 1 and FULL in finalizes[0]  # the bubble carries it, once
    assert gateway_p.bot_replies == 1  # the turn still closes out as replied


# ---------------------------------------------------------------------------
# (b) the transformed final, no editable message id, the footer appended AFTER the last
#     frame: only the tail the bubble never drew goes out — the body is never repeated.
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_transformed_final_sends_only_the_tail_the_bubble_never_drew():
    transport = _FakeTransport()
    delivery = await _landed_bubble(transport, BODY)  # the bubble drew the BODY only

    gateway = GatewayTurnMixin.__new__(GatewayTurnMixin)
    result = await _mark(gateway, delivery, {
        "final_response": FULL, "messages": [], "response_transformed": True,
    })

    # The body is on screen: it must not be sent again, and the tail is what is left.
    assert result.get("already_sent") is None
    assert result["_final_unshown_remainder"] == PLUGIN_FOOTER

    gateway_p, delivered = await _deliver(delivery, result, FULL)
    assert delivered == PLUGIN_FOOTER  # the tail alone
    assert gateway_p.bot_replies == 1

    # The platform sends what the delivery handed back (the gateway's own send path).
    if delivered:
        await delivery.adapter.send(CHAT, delivered)
    sends = "".join(transport.of("send"))
    assert BODY not in sends, "the body the bubble already shows is never sent again"
    assert PLUGIN_FOOTER.strip() in sends, "the tail reaches the user"
    assert len(transport.of("finalize")) == 1  # one bubble, one finalize frame


# ---------------------------------------------------------------------------
# (c) the transformed final WITH an editable message id: the in-place edit is unchanged.
# ---------------------------------------------------------------------------
class _EditableAdapter:
    """An adapter whose messages can be edited (the platforms that support it)."""

    def __init__(self):
        self.edits = []

    async def edit_message(self, chat_id, message_id, content, finalize=False, metadata=None):
        self.edits.append((message_id, content))
        return SimpleNamespace(success=True)


class _EditableConsumer:
    """A consumer holding ONE editable message — the in-place reconciliation target."""

    def __init__(self, adapter):
        self.adapter = adapter
        self.message_id = "msg-42"
        self.stream_deltas_enabled = True
        self.final_response_sent = False
        self._turn_split_delivery = False

    def delivered_final_matches(self, final_text):
        return None

    def has_durably_delivered_text(self, final_text):
        return False

    def _clean_for_display(self, text):
        return text

    @property
    def durable_answer_text(self):
        return ""


@pytest.mark.asyncio
async def test_transformed_final_with_an_editable_message_is_edited_in_place():
    adapter = _EditableAdapter()
    consumer = _EditableConsumer(adapter)

    gateway = GatewayTurnMixin.__new__(GatewayTurnMixin)
    result = await _mark(gateway, consumer, {
        "final_response": FULL, "messages": [], "response_transformed": True,
    })

    assert adapter.edits == [("msg-42", FULL)]  # the transformed text lands IN PLACE
    assert result.get("already_sent") is True
    assert "_final_unshown_remainder" not in result  # no tail decision on an editable message


# ---------------------------------------------------------------------------
# (d) the four NON-transformed branches keep their behaviour.
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_non_transformed_confirmed_delivery_still_suppresses_the_plain_send():
    transport = _FakeTransport()
    delivery = await _landed_bubble(transport, FULL)

    gateway = GatewayTurnMixin.__new__(GatewayTurnMixin)
    result = await _mark(gateway, delivery, {
        "final_response": FULL, "messages": [], "response_transformed": False,
    })

    assert result.get("already_sent") is True  # the streamed delivery confirmed it
    assert "_final_unshown_remainder" not in result


@pytest.mark.asyncio
async def test_non_transformed_stale_finalize_still_edits_the_streamed_message():
    class _StaleConsumer(_EditableConsumer):
        final_content_delivered = True

        def delivered_final_matches(self, final_text):
            return False  # the recorded payload is not the final text (#71643)

    adapter = _EditableAdapter()
    consumer = _StaleConsumer(adapter)

    gateway = GatewayTurnMixin.__new__(GatewayTurnMixin)
    result = await _mark(gateway, consumer, {
        "final_response": FULL, "messages": [], "response_transformed": False,
    })

    assert adapter.edits == [("msg-42", FULL)]  # the stale finalize is reconciled in place
    assert result.get("already_sent") is True


@pytest.mark.asyncio
async def test_non_transformed_nothing_on_screen_keeps_the_plain_send():
    transport = _FakeTransport()
    delivery = WeComStreamDelivery(adapter=_FakeAdapter(transport), chat_id=CHAT)
    delivery.stream_deltas_enabled = True
    delivery._disabled = True  # the stream never opened: nothing was ever drawn

    gateway = GatewayTurnMixin.__new__(GatewayTurnMixin)
    result = await _mark(gateway, delivery, {
        "final_response": FULL, "messages": [], "response_transformed": False,
    })

    assert "already_sent" not in result  # the plain send stays the delivery
    assert "_final_unshown_remainder" not in result

    gateway_p, delivered = await _deliver(delivery, result, FULL)
    assert delivered == FULL  # the reply goes out as the plain send, exactly as before
    assert gateway_p.bot_replies == 1


@pytest.mark.asyncio
async def test_non_transformed_preview_is_finished_in_place():
    """An unconfirmed final with ONE real preview on screen: the preview is edited up to the
    final text (never a second message)."""

    class _PreviewConsumer(_EditableConsumer):
        final_response_sent = False
        delivery_uncertain = False
        _delivery_ambiguous = False
        _use_native_streaming = False
        _preview_uncertain = False
        _egress_declined = False

    adapter = _EditableAdapter()
    adapter.MAX_MESSAGE_LENGTH = 4096
    consumer = _PreviewConsumer(adapter)

    gateway = GatewayTurnMixin.__new__(GatewayTurnMixin)
    result = await _mark(gateway, consumer, {
        "final_response": FULL, "messages": [], "response_transformed": False,
    })

    assert adapter.edits == [("msg-42", FULL)]  # finished in place
    assert result.get("already_sent") is True
