"""fork 2026-11-18 (DPMS 561213): a re-delivered WeCom callback runs the agent ONCE.

Production hm-5958d932-1f3 (2026-11-18): the platform re-delivers a callback it did not see
answered in time (the agent's turn took 48s+). The re-delivery carries NO msgid — the platform's
own id is absent — so the old dedup key (the msgid, else the random req_id) never matched: the
agent ran a second time and the user got the same answer twice.

The fix keys such a callback by its CONTENT and splits the re-delivery in two tiers: a replay
inside 30s is dropped silently (the user sees ONE reply), a later one replays the reply that was
already delivered instead of running the agent again.

The second half of the same DPMS: the EARLY ACK. A respond frame is the only answer the WS aibot
vocabulary can give a callback (there is no ack-only frame), so with ``stream_early_ack_enabled``
the chat's first ``send_typing`` of a turn opens the reply flow — an invisible <think></think>
seed on the inbound req_id at turn acceptance, before the model's first token. The turn's
StreamTurn adopts the seed's stream (ONE bubble per turn); a seed no turn ever adopts is sealed
with an empty finish frame at the first keep-alive interval. The switch is OFF by default (like
the keep-alive): the consumer's own seed at first output stays the previous behaviour.
"""
import logging
import time

import pytest
from unittest.mock import AsyncMock, MagicMock

from gateway.config import PlatformConfig
from gateway.platforms.base import SendResult
from gateway.platforms.helpers import carry_inbound_dedup
from plugins.platforms.wecom.adapter import WeComAdapter
from plugins.platforms.wecom.stream_delivery import WeComStreamDelivery
from plugins.platforms.wecom.streaming import StreamTurn
from tests.waiting import async_wait_until


class _Clock:
    """A controllable wall clock: the dedup ages are measured in seconds."""

    def __init__(self) -> None:
        self.now = 1_800_000_000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def clock(monkeypatch) -> _Clock:
    """Freeze the wall clock the dedup measures its ages with."""
    fake = _Clock()
    monkeypatch.setattr(time, "time", fake)
    return fake


def _payload(text: str, *, chatid: str | None = "chat-1", sender: str = "user-1",
             req_id: str = "req-1", msgid: str | None = None) -> dict:
    body = {
        "chattype": "single",
        "from": {"userid": sender},
        "msgtype": "text",
        "text": {"content": text},
    }
    if chatid is not None:
        body["chatid"] = chatid
    if msgid is not None:
        body["msgid"] = msgid
    return {"cmd": "aibot_msg_callback", "headers": {"req_id": req_id}, "body": body}


def _adapter() -> WeComAdapter:
    adapter = WeComAdapter(PlatformConfig(enabled=True))
    adapter._text_batch_delay_seconds = 0  # no client-side batching: the turn starts at once
    adapter._attachment_text_merge_delay_seconds = 0  # an attachment-only message is not held
    adapter.handle_message = AsyncMock()
    adapter._extract_media = AsyncMock(return_value=([], []))
    return adapter


def _capture_sends(adapter: WeComAdapter) -> list[tuple[str, str]]:
    """Replace the wire: every part the adapter sends is recorded, and the send succeeds."""
    sent: list[tuple[str, str]] = []

    async def _fake_send_reply_part(chat_id, content, reply_to, **kwargs) -> SendResult:
        sent.append((chat_id, content))
        return SendResult(success=True)

    adapter._send_reply_part = _fake_send_reply_part
    return sent


@pytest.mark.asyncio
async def test_redelivery_within_30s_runs_the_agent_once_and_sends_nothing(clock, caplog):
    """A replay inside 30s is dropped silently: one agent round, one reply, no second send."""
    adapter = _adapter()
    sent = _capture_sends(adapter)

    with caplog.at_level(logging.INFO, logger="plugins.platforms.wecom.adapter"):
        await adapter._on_message(_payload("你是谁", req_id="req-1"))
        clock.advance(1.8)
        await adapter._on_message(_payload("你是谁", req_id="req-2"))

    assert adapter.handle_message.await_count == 1, "the re-delivery must not run the agent again"
    assert sent == [], "a replay inside 30s sends nothing: the first reply is on its way"
    assert "Duplicate inbound dropped (content key, <30s)" in caplog.text
    assert "dropped=1" in caplog.text and "replayed=0" in caplog.text
    assert "age=1.8s" in caplog.text


@pytest.mark.asyncio
async def test_redelivery_after_30s_replays_the_delivered_reply_without_running_the_agent(clock):
    """A later re-delivery does NOT run the agent: the reply that was delivered goes out again."""
    adapter = _adapter()
    sent = _capture_sends(adapter)

    await adapter._on_message(_payload("你是谁", req_id="req-1"))
    # The agent's answer is delivered on the inbound's own request id (the gateway's reply anchor).
    await adapter.send(chat_id="chat-1", content="我是你的助手", reply_to="req-1")
    assert sent == [("chat-1", "我是你的助手")]

    clock.advance(64.0)
    await adapter._on_message(_payload("你是谁", req_id="req-2"))

    assert adapter.handle_message.await_count == 1, "the agent must not answer the same text twice"
    assert sent == [("chat-1", "我是你的助手"), ("chat-1", "我是你的助手")], "the reply is replayed"
    # The re-delivered callback carries a fresh req_id: the replay goes out on it.
    assert adapter._last_chat_req_ids["chat-1"] == "req-2"


@pytest.mark.asyncio
async def test_redelivery_after_30s_without_a_delivered_reply_is_dropped(clock, caplog):
    """No reply was delivered yet (the agent is still working): the re-delivery is dropped."""
    adapter = _adapter()
    sent = _capture_sends(adapter)

    with caplog.at_level(logging.INFO, logger="plugins.platforms.wecom.adapter"):
        await adapter._on_message(_payload("你是谁", req_id="req-1"))
        clock.advance(64.0)
        await adapter._on_message(_payload("你是谁", req_id="req-2"))

    assert adapter.handle_message.await_count == 1
    assert sent == []
    assert "no delivered reply to replay" in caplog.text
    assert "dropped=1" in caplog.text and "replayed=0" in caplog.text


@pytest.mark.asyncio
async def test_the_reply_is_replayed_only_once(clock):
    """A platform that re-delivers three times puts ONE copy of the answer in front of the user."""
    adapter = _adapter()
    sent = _capture_sends(adapter)

    await adapter._on_message(_payload("你是谁", req_id="req-1"))
    await adapter.send(chat_id="chat-1", content="我是你的助手", reply_to="req-1")

    clock.advance(64.0)
    await adapter._on_message(_payload("你是谁", req_id="req-2"))
    clock.advance(77.0)
    await adapter._on_message(_payload("你是谁", req_id="req-3"))

    assert adapter.handle_message.await_count == 1
    assert sent == [("chat-1", "我是你的助手"), ("chat-1", "我是你的助手")]


@pytest.mark.asyncio
async def test_different_texts_in_one_chat_are_separate_messages(clock):
    """The content key keys the TEXT: another question in the same chat is a new message."""
    adapter = _adapter()

    await adapter._on_message(_payload("你是谁", req_id="req-1"))
    clock.advance(2.0)
    await adapter._on_message(_payload("你的容器镜像是什么？", req_id="req-2"))

    assert adapter.handle_message.await_count == 2


@pytest.mark.asyncio
async def test_the_same_text_in_two_chats_is_separate_messages(clock):
    """The content key keys the CHAT: the same text elsewhere is a new message."""
    adapter = _adapter()

    await adapter._on_message(_payload("你是谁", chatid="chat-1", req_id="req-1"))
    clock.advance(2.0)
    await adapter._on_message(_payload("你是谁", chatid="chat-2", req_id="req-2"))

    assert adapter.handle_message.await_count == 2


@pytest.mark.asyncio
async def test_the_same_text_from_two_senders_is_separate_messages(clock):
    """The content key keys the SENDER: the same text from another user is a new message."""
    adapter = _adapter()

    await adapter._on_message(_payload("你是谁", sender="user-1", req_id="req-1"))
    clock.advance(2.0)
    await adapter._on_message(_payload("你是谁", sender="user-2", req_id="req-2"))

    assert adapter.handle_message.await_count == 2


@pytest.mark.asyncio
async def test_the_same_text_after_the_ttl_is_a_new_message(clock):
    """Past the TTL the user may repeat a question on purpose: it is answered again."""
    adapter = _adapter()

    await adapter._on_message(_payload("你是谁", req_id="req-1"))
    clock.advance(301.0)
    await adapter._on_message(_payload("你是谁", req_id="req-2"))

    assert adapter.handle_message.await_count == 2


@pytest.mark.asyncio
async def test_a_msgid_inbound_keeps_its_own_dedup(clock):
    """A callback WITH a msgid keeps the platform's id as its key: the old behaviour, unchanged."""
    adapter = _adapter()

    await adapter._on_message(_payload("你是谁", req_id="req-1", msgid="msg-1"))
    clock.advance(2.0)
    await adapter._on_message(_payload("你是谁", req_id="req-2", msgid="msg-1"))

    assert adapter.handle_message.await_count == 1, "the same msgid is the same message"

    clock.advance(2.0)
    await adapter._on_message(_payload("你是谁", req_id="req-3", msgid="msg-2"))
    assert adapter.handle_message.await_count == 2, "another msgid is another message"


@pytest.mark.asyncio
async def test_a_dm_without_a_chatid_is_keyed_by_its_sender(clock):
    """The production callback carries no chatid: the chat is then the sender."""
    adapter = _adapter()

    await adapter._on_message(_payload("你是谁", chatid=None, req_id="req-1"))
    clock.advance(1.8)
    await adapter._on_message(_payload("你是谁", chatid=None, req_id="req-2"))

    assert adapter.handle_message.await_count == 1


@pytest.mark.asyncio
async def test_a_media_only_inbound_keeps_the_old_key(clock):
    """A message with no text has no stable content: it keeps the old req_id key."""
    adapter = _adapter()
    payload = {
        "cmd": "aibot_msg_callback",
        "headers": {"req_id": "req-1"},
        "body": {
            "chattype": "single",
            "chatid": "chat-1",
            "from": {"userid": "user-1"},
            "msgtype": "image",
            "image": {"url": "https://example.invalid/a.png"},
        },
    }
    adapter._extract_media = AsyncMock(return_value=(["/tmp/a.png"], ["image/png"]))

    await adapter._on_message(payload)
    clock.advance(1.8)
    await adapter._on_message({**payload, "headers": {"req_id": "req-2"}})

    assert adapter.handle_message.await_count == 2, "a media message has no content key to dedup on"


@pytest.mark.asyncio
async def test_the_content_keyed_state_survives_a_reconnect(clock):
    """A rebuilt adapter inherits the content-key state: a reconnect is exactly when the
    platform re-pushes what went unanswered, so the re-delivery must stay a re-delivery."""
    predecessor = _adapter()
    _capture_sends(predecessor)
    await predecessor._on_message(_payload("你是谁", req_id="req-1"))
    await predecessor.send(chat_id="chat-1", content="我是你的助手", reply_to="req-1")

    successor = _adapter()
    carry_inbound_dedup(predecessor, successor)
    sent = _capture_sends(successor)

    # Inside 30s of the FIRST sighting (the age carried over): dropped, not re-answered.
    clock.advance(1.8)
    await successor._on_message(_payload("你是谁", req_id="req-2"))
    assert successor.handle_message.await_count == 0, "the re-delivery outlives the reconnect"
    assert sent == []

    # Past 30s the reply carried over is replayed — still without running the agent.
    clock.advance(64.0)
    await successor._on_message(_payload("你是谁", req_id="req-3"))
    assert successor.handle_message.await_count == 0
    assert sent == [("chat-1", "我是你的助手")]


@pytest.mark.asyncio
async def test_the_delivered_reply_is_filed_under_its_own_message(clock):
    """The send funnel files the reply under the inbound it answered; a newer reply overwrites
    it, a failed send files nothing."""
    adapter = _adapter()
    _capture_sends(adapter)
    await adapter._on_message(_payload("你是谁", req_id="req-1"))
    key = adapter._content_key_by_msg_id["req-1"]

    await adapter.send(chat_id="chat-1", content="我是你的助手", reply_to="req-1")
    assert adapter._delivered_reply_by_key[key][0] == "我是你的助手"

    await adapter.send(chat_id="chat-1", content="更好的回答", reply_to="req-1")
    assert adapter._delivered_reply_by_key[key][0] == "更好的回答"

    async def _failing(chat_id, content, reply_to, **kwargs) -> SendResult:
        return SendResult(success=False)

    adapter._send_reply_part = _failing
    await adapter.send(chat_id="chat-1", content="没有送出去的回答", reply_to="req-1")
    assert adapter._delivered_reply_by_key[key][0] == "更好的回答", "a failed send is not on file"


@pytest.mark.asyncio
async def test_overflow_fragments_and_replays_do_not_overwrite_the_filed_reply(clock):
    """An overflow bubble is a PART of a reply already on file, and a replay sends text that is
    already on file: neither may overwrite what a later re-delivery would replay."""
    adapter = _adapter()
    _capture_sends(adapter)
    await adapter._on_message(_payload("你是谁", req_id="req-1"))
    key = adapter._content_key_by_msg_id["req-1"]

    await adapter.send(chat_id="chat-1", content="我是你的助手", reply_to="req-1")
    await adapter.send(chat_id="chat-1", content="（2/2）的助手", metadata={"is_reply_overflow": True})
    await adapter.send(chat_id="chat-1", content="我是你的助手", metadata={"is_redelivery": True})

    assert adapter._delivered_reply_by_key[key][0] == "我是你的助手"


@pytest.mark.asyncio
async def test_a_streamed_final_reply_is_recorded_for_a_later_replay(clock):
    """The streamed turn's finalize records the full reply under the inbound it answered (the
    streamed path never traverses send()), and a re-delivery replays that text."""
    adapter = _adapter()
    sent = _capture_sends(adapter)
    await adapter._on_message(_payload("你是谁", req_id="req-1"))

    # The streamed turn is sealed: streaming.py files it as the chat's last turn at finalize.
    turn = StreamTurn(chat_id="chat-1", req_id="req-1")
    turn.finalized = True
    adapter._last_stream_turn_by_chat["chat-1"] = turn
    delivery = WeComStreamDelivery(adapter, chat_id="chat-1")
    delivery.accumulated_text = "我是你的助手"
    delivery._record_delivered_reply()

    clock.advance(64.0)
    await adapter._on_message(_payload("你是谁", req_id="req-2"))

    assert adapter.handle_message.await_count == 1
    assert sent == [("chat-1", "我是你的助手")], "the streamed reply is what the re-delivery replays"


# ----------------------------------------------------------------------
# The early ack (fork 2026-11-18, DPMS 561213): send_typing seeds the turn's
# reply flow; the StreamTurn adopts it; an orphan seed is sealed.
# ----------------------------------------------------------------------

def _early_ack_adapter() -> WeComAdapter:
    """An adapter with the early ack ON (the switch is off by default, like the keep-alive)."""
    adapter = _adapter()
    adapter._stream_early_ack_enabled = True
    return adapter


def _journal_stream_wire(adapter: WeComAdapter) -> list[dict]:
    """Record every respond frame on the wire; the ack comes back the way the server sends it
    (the real ``_resolve_reply_ack`` path), so intermediates are never skipped on a phantom ack."""
    frames: list[dict] = []

    async def _record_send_json(payload: dict) -> None:
        frames.append(payload)
        req_id = (payload.get("headers") or {}).get("req_id")
        if req_id:
            adapter._resolve_reply_ack(req_id, {"cmd": "aibot_respond_msg", "headers": {"req_id": req_id}, "body": {"errcode": 0}})

    adapter._ws = MagicMock(closed=False)
    adapter._send_json = _record_send_json
    adapter._REPLY_ACK_TIMEOUT = 0.05  # belt: an ack that never comes settles as ack-timeout-delivered
    return frames


def _stream_of_frame(frame: dict) -> dict:
    return frame["body"]["stream"]


@pytest.mark.asyncio
async def test_send_typing_seeds_the_turn_and_the_turn_adopts_its_stream():
    """The typing call IS the early ack: one invisible seed, and the turn's StreamTurn
    continues on the very same stream — no second seed, one bubble for the whole turn."""
    adapter = _early_ack_adapter()
    frames = _journal_stream_wire(adapter)
    adapter._last_chat_req_ids["chat-1"] = "req-1"

    await adapter.send_typing("chat-1")

    # The seed: an invisible <think></think>, finish=false, on the inbound's own req_id —
    # and typing itself creates NO turn (the slot waits for the consumer's StreamTurn).
    assert len(frames) == 1
    seed = _stream_of_frame(frames[0])
    assert seed["content"] == "<think></think>" and seed["finish"] is False
    assert frames[0]["headers"]["req_id"] == "req-1"
    assert len(adapter._stream_turns) == 0
    stream_id = seed["id"]
    slot = adapter._pending_stream_seeds["chat-1"]
    assert slot.state == "pending" and slot.stream_id == stream_id

    # The typing refresh (every 2s) does NOT seed twice within one turn.
    await adapter.send_typing("chat-1")
    assert len(frames) == 1

    # The consumer's first partial (with its turn_id) adopts the slot: same stream.
    assert await adapter.send_stream_frame("思考中", chat_id="chat-1", turn_id="t1")
    turn = adapter._stream_turns["chat-1:t1"]
    assert turn.stream_id == stream_id, "the turn continues the seed's stream: ONE bubble"
    assert turn.seeded is True, "the seed is already out — no second <think></think>"

    # Seal cancelled at adoption (the handle is cleared): the bubble belongs to the turn now.
    assert slot.state == "adopted"
    assert slot.seal_handle is None

    # Finalize closes the very same stream; the wire shows seed → intermediate → final.
    assert await adapter.send_stream_frame("思考中", chat_id="chat-1", turn_id="t1", finalize=True)
    assert [(_stream_of_frame(f)["content"], _stream_of_frame(f)["finish"]) for f in frames] == [
        ("<think></think>", False), ("思考中", False), ("思考中" + "\u200b", True),
    ]
    assert all(_stream_of_frame(f)["id"] == stream_id for f in frames)


@pytest.mark.asyncio
async def test_an_unadopted_seed_is_sealed_with_an_empty_finish_frame():
    """No StreamTurn ever adopts the seed: an empty finish frame closes the thinking
    bubble at the first keep-alive interval — no orphan bubble, no visible text."""
    adapter = _early_ack_adapter()
    adapter._stream_keepalive_interval_seconds = 0.05  # the seal fires almost at once
    frames = _journal_stream_wire(adapter)
    adapter._last_chat_req_ids["chat-1"] = "req-1"

    await adapter.send_typing("chat-1")
    assert len(frames) == 1  # the seed, and nothing else yet
    await adapter.send_typing("chat-1")
    assert len(frames) == 1, "the refresh does not seed twice"

    await async_wait_until(
        lambda: any(_stream_of_frame(f)["finish"] for f in frames), what="the seal frame"
    )

    seal = next(f for f in frames if _stream_of_frame(f)["finish"])
    assert _stream_of_frame(seal)["content"] == "", "the seal is an EMPTY finish frame"
    assert _stream_of_frame(seal)["id"] == _stream_of_frame(frames[0])["id"], "same bubble"
    assert seal["headers"]["req_id"] == "req-1"
    assert all(_stream_of_frame(f)["content"] in ("<think></think>", "") for f in frames), \
        "an orphan seed shows the user no visible text"
    assert adapter._pending_stream_seeds["chat-1"].state == "sealed"


@pytest.mark.asyncio
async def test_send_typing_without_the_switch_stays_a_noop():
    """The switch is OFF by default (like the keep-alive): typing sends nothing, keeps the
    consumer's own seed as the only opener — the previous behaviour, unchanged."""
    adapter = _adapter()
    assert adapter._stream_early_ack_enabled is False, "the default is off"
    frames = _journal_stream_wire(adapter)
    adapter._last_chat_req_ids["chat-1"] = "req-1"

    await adapter.send_typing("chat-1")

    assert frames == []
    assert adapter._pending_stream_seeds == {}
    assert len(adapter._stream_turns) == 0


@pytest.mark.asyncio
async def test_a_seed_is_neither_sent_on_an_expired_nor_a_busy_chat():
    """An expired chat has no reply flow to open; a chat with a live turn is a busy
    follow-up — never a second bubble mid-turn."""
    adapter = _early_ack_adapter()
    frames = _journal_stream_wire(adapter)
    adapter._last_chat_req_ids["chat-1"] = "req-1"

    adapter._stream_expired_chats.add("chat-1")
    await adapter.send_typing("chat-1")
    assert frames == [] and adapter._pending_stream_seeds == {}

    adapter._stream_expired_chats.clear()
    adapter._stream_turns["chat-1:req-1"] = StreamTurn("chat-1", "req-1")  # a live turn
    await adapter.send_typing("chat-1")
    assert frames == [] and adapter._pending_stream_seeds == {}


@pytest.mark.asyncio
async def test_a_new_turn_reseeds_on_its_own_req_id():
    """The chat's next inbound starts a new turn: the previous seed (even adopted) clears
    and the typing call seeds on the NEW req_id."""
    adapter = _early_ack_adapter()
    frames = _journal_stream_wire(adapter)
    adapter._last_chat_req_ids["chat-1"] = "req-1"
    await adapter.send_typing("chat-1")
    first_stream = _stream_of_frame(frames[0])["id"]

    adapter._last_chat_req_ids["chat-1"] = "req-2"  # the next inbound's own req_id
    await adapter.send_typing("chat-1")

    assert len(frames) == 2
    second = _stream_of_frame(frames[1])
    assert second["content"] == "<think></think>"
    assert frames[1]["headers"]["req_id"] == "req-2"
    assert second["id"] != first_stream, "a new turn opens its own stream"
    slot = adapter._pending_stream_seeds["chat-1"]
    assert slot.req_id == "req-2" and slot.stream_id == second["id"]
