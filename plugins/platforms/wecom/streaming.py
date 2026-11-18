"""WeCom native streaming mixin (``msgtype: stream`` via aibot_respond_msg): per-turn state, per-req_id
ack tracking (official replyStreamNonBlocking semantics), keep-alive heartbeat, finalize clock fallback."""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass
from typing import Any, Optional

logger = logging.getLogger("plugins.platforms.wecom.adapter")

APP_CMD_RESPONSE = "aibot_respond_msg"

# Each reply stream lives ~6 minutes (the connection ping does NOT refresh it); afterwards
# 846608 (stream window) / 846604 (req_id window) mean the reply flow is dead. 846609 = ws
# lost its subscription. 6000 = finalize raced a newer frame (bubble already replaced: benign).
STREAM_EXPIRED_ERRCODE = 846608
STREAM_REQUEST_EXPIRED_ERRCODE = 846604
STREAM_NOT_SUBSCRIBED_ERRCODE = 846609
STREAM_VERSION_CONFLICT_ERRCODE = 6000
MAX_STREAM_CONTENT_LENGTH = 20480  # WeCom server-enforced byte limit per frame
# SDK queue is 100 frames per reqId; cap intermediates (openclaw uses 85) so finalize has room.
MAX_INTERMEDIATE_FRAMES = 85

# Two defences against the 6-min window (docs/wecom-stream-keepalive-*.md): Layer 2 clock
# fallback (always on) declines finish=true past STREAM_SAFE_DURATION_SECONDS so the consumer's
# send() delivers; Layer 1 keep-alive (OFF by default) re-sends accumulated text as finish=false
# every interval — off because an extra frame widens the ack race double-send relies on.
STREAM_SAFE_DURATION_SECONDS = 330.0
STREAM_KEEPALIVE_INTERVAL_SECONDS = 120.0
STREAM_KEEPALIVE_ENABLED_DEFAULT = False
# fork 2026-11-18 (DPMS 561213): with this on, ``send_typing`` opens the chat's reply flow at turn
# acceptance — an invisible <think></think> respond frame on the inbound req_id. A respond frame is
# the ONLY answer the WS aibot vocabulary can give a callback (there is no ack-only frame), so this
# is the earliest possible answer: the platform's re-delivery clock sees the callback answered
# before the model's first token, instead of at the consumer's first output. Off by default like
# the keep-alive: the consumer's own seed at first output is the previous behaviour, and one extra
# frame per turn is a deliberate choice.
STREAM_EARLY_ACK_ENABLED_DEFAULT = False


class WeComStreamExpiredError(RuntimeError):
    """Raised on errcode 846608/846604: the stream/req_id reply flow is dead; fall back to ``aibot_send_msg``."""

    def __init__(self, errcode: int = STREAM_EXPIRED_ERRCODE, errmsg: str = ""):
        super().__init__(f"WeCom stream expired (errcode={errcode}): {errmsg or 'no detail'}")
        self.errcode, self.errmsg = errcode, errmsg


@dataclass
class ReplyFrame:
    """A reply frame awaiting its aibot_respond_msg ack (FIFO per req_id)."""
    body: dict[str, Any]
    future: asyncio.Future
    is_final: bool = False
    sent_at: Optional[float] = None


class ReplyQueue:
    """Per-req_id pending-ack tracker: intermediates skip while an ack is pending, finals wait."""
    def __init__(self, req_id: str):
        self.req_id, self.pending_ack = req_id, None  # pending_ack: Optional[ReplyFrame]


class StreamTurn:
    """Per-turn stream state so concurrent messages never share a stream."""
    def __init__(self, chat_id: str, req_id: str):
        self.chat_id, self.req_id, self.stream_id = chat_id, req_id, f"stream_{uuid.uuid4().hex[:12]}"
        self.accumulated_text = ""
        self.finalized = self.seeded = self.expired = False  # seeded prevents a double seed (errcode 6000)
        self.start_time = time.monotonic()
        self.last_sent_content: str = ""  # content ACTUALLY sent; final frame must differ or WeCom drops it
        self._intermediate_frames_sent: int = 0
        # fork 2026-11-16 (DPMS 561213): the req_id is still the LATEST inbound of the chat. The
        # adapter clears it when the next inbound arrives by plain assignment (no shared object
        # mutation — this turn may be written from the send path's thread). A STALE (False) marker
        # never becomes fresh again: the frame would open a bubble on an already-answered req_id.
        self.req_id_fresh: bool = True
        # fork 2026-11-16 (DPMS 561213): index of the follow-up bubble this turn's OVERFLOW delivery
        # continues in (extend_stream_bubble). Only overflow follow-ups advance it; the original
        # turn's intermediates/finalize always address stream_id.
        self.continuation: int = 0
        self.keepalive_handle: Optional[asyncio.TimerHandle] = None  # cancel on EVERY turn-exit path


@dataclass
class PendingStreamSeed:
    """fork 2026-11-18 (DPMS 561213): the chat's ``send_typing`` seed until a StreamTurn adopts it.

    The seed IS a respond frame on the inbound req_id (the early callback ack), so the slot holds
    what the eventual StreamTurn must inherit to keep ONE bubble per turn: the req_id and the
    stream_id the seed went out on. ``state`` moves one way: ``pending`` → ``adopted`` (the turn's
    StreamTurn took the stream over) or ``sealed`` (no turn ever came; the thinking bubble was
    closed with an empty finish frame). An adopted slot STAYS in the registry as the turn's
    one-seed-per-turn latch — it clears when the chat's NEXT inbound starts a new turn."""
    chat_id: str
    req_id: str
    stream_id: str
    created_at: float  # time.monotonic() domain, like StreamTurn.start_time
    seal_handle: Optional[asyncio.TimerHandle] = None  # the orphan seal; cancelled on adoption
    state: str = "pending"


def _stream_of(body: dict[str, Any]) -> dict[str, Any]:
    return body.get("stream", {}) if isinstance(body.get("stream"), dict) else {}


def _stream_desc(body: dict[str, Any]) -> tuple:
    stream = _stream_of(body)
    return stream.get("id", "N/A"), stream.get("finish", "N/A")


def _elapsed(since: Optional[float]) -> float:
    return time.monotonic() - (since or time.monotonic())


class WeComStreamMixin:
    """Native streaming mixed into WeComAdapter (uses its ws transport, registries and ``_stream_*`` config)."""

    MAX_STREAM_CONTENT_LENGTH = MAX_STREAM_CONTENT_LENGTH
    _REPLY_ACK_TIMEOUT = 15.0  # official REPLY_SEND_TIMEOUT_MS; shorter widened the double-send race
    # fork 2026-11-13 (DPMS 561213): finalize frames must settle before the gateway's stream
    # flush budget (17s) cancels the consumer, else the flag stays unset and the gateway
    # normal-sends on top of the (later-landing) bubble final. The send-queue skips the
    # cancelled item, so past this bound the finalize declines cleanly and the reply is
    # delivered exactly once by the gateway's regular send.
    _FINALIZE_QUEUE_WAIT_SECONDS = 12.0

    async def _send_reply_queued(self, reply_req_id: str, body: dict[str, Any], *, is_final: bool = False, skip_if_pending: bool = False) -> dict[str, Any]:
        """aibot_respond_msg with per-req_id ack tracking: is_final drains the pending ack then awaits its own;
        skip_if_pending returns ``{"skipped": True}`` while a prior ack is pending."""
        self._require_ws()
        normalized = self._require_reply_req_id(reply_req_id)
        queue = self._reply_queues.setdefault(normalized, ReplyQueue(normalized))
        if skip_if_pending and queue.pending_ack is not None:
            return {"skipped": True, "errcode": 0, "errmsg": "pending_ack"}
        if is_final and queue.pending_ack is not None:
            await self._drain_pending_ack(queue, normalized)
        future: asyncio.Future = asyncio.get_running_loop().create_future()
        frame = ReplyFrame(body=body, future=future, is_final=is_final, sent_at=time.monotonic())
        # Register BEFORE sending so a mid-send ack routes; re-attach `queue` because the drain
        # above may have let the intermediate ack pop it out of _reply_queues (orphan → timeout).
        self._reply_queues[normalized] = queue
        queue.pending_ack = frame
        logger.debug(
            "[%s] _send_reply_queued: req_id=%s is_final=%s skip_if_pending=%s stream_id=%s finish=%s content_len=%d", self.name, normalized, is_final, skip_if_pending, *_stream_desc(body), len(_stream_of(body).get("content", "") or ""),
        )
        try:
            await self._send_json({"cmd": APP_CMD_RESPONSE, "headers": {"req_id": normalized}, "body": body})
        except Exception:
            # Nobody awaits the future here — cancel it rather than log "exception never retrieved".
            self._release_pending(queue, normalized, frame)
            if not future.done():
                future.cancel()
            raise
        if not is_final:  # fire-and-forget; pending_ack stays registered so later frames can skip
            return {"errcode": 0, "errmsg": "sent_nonblocking"}
        try:
            return await asyncio.wait_for(future, timeout=self._REPLY_ACK_TIMEOUT)
        except TimeoutError:
            # Bytes went out, ack is late — WeCom already rendered it; raising caused duplicates.
            logger.warning("[%s] Final frame ack timeout (req_id=%s) — treating as delivered (matches official wecom-openclaw-plugin behaviour). No fallback send.", self.name, normalized)
            return {"errcode": 0, "errmsg": "ack_timeout_assumed_delivered", "ack_pending": True}
        finally:
            self._release_pending(queue, normalized, frame)

    async def _send_reply_correlated(self, req_id: str, body: dict[str, Any], timeout: float) -> dict[str, Any]:
        """Send a NON-stream reply frame (passive markdown / replyMedia) on the SHARED
        per-req_id registry. fork 2026-09-30 (audit 2026-09-29 module-1 H2): passive replies used
        to register in ``_pending_responses`` while stream frames used ``_reply_queues``;
        the same inbound req_id could sit in both and ``_dispatch_payload`` resolved
        reply-queues first, so a stream ack could "answer" a passive future (or vice
        versa) — the passive reply then timed out, looked failed, and the DM was
        re-delivered via proactive/fallback as a duplicate. One registry serializes both
        frame kinds per req_id. Unlike final stream frames, an ack timeout RAISES so the
        caller's passive→proactive fallback (846604 recovery) still fires.
        The ack timeout is the caller's REQUEST_TIMEOUT_SECONDS (not _REPLY_ACK_TIMEOUT,
        which is a single-point knob tuned for the stream double-send race)."""
        self._require_ws()
        req_id = self._require_reply_req_id(req_id)
        queue = self._reply_queues.setdefault(req_id, ReplyQueue(req_id))
        if queue.pending_ack is not None:
            await self._drain_pending_ack(queue, req_id)
        future: asyncio.Future = asyncio.get_running_loop().create_future()
        # is_final=True borrows the stream-final drain semantics (await own ack); it only affects logging here.
        frame = ReplyFrame(body=body, future=future, is_final=True, sent_at=time.monotonic())
        # Re-attach: the drain above may have popped the queue (orphan → timeout).
        self._reply_queues[req_id] = queue
        queue.pending_ack = frame
        try:
            await self._send_json({"cmd": APP_CMD_RESPONSE, "headers": {"req_id": req_id}, "body": body})
        except Exception:
            self._release_pending(queue, req_id, frame)
            if not future.done():
                future.cancel()
            raise
        try:
            return await asyncio.wait_for(future, timeout=timeout)
        finally:
            self._release_pending(queue, req_id, frame)

    async def _drain_pending_ack(self, queue: ReplyQueue, req_id: str) -> None:
        """Before a final frame: wait (bounded) for the pending intermediate's ack, then clear it."""
        pending_frame = queue.pending_ack
        pending_desc = (self.name, req_id, *_stream_desc(pending_frame.body))
        logger.debug("[%s] _send_reply_queued: final waiting for pending ack drain — req_id=%s pending_stream_id=%s pending_finish=%s pending_sent_at=%.1fs_ago", *pending_desc, _elapsed(pending_frame.sent_at))
        try:
            await asyncio.wait_for(asyncio.shield(pending_frame.future), timeout=self._REPLY_ACK_TIMEOUT)
        except TimeoutError:
            logger.warning(
                "[%s] Reply ack timeout waiting for pending (req_id=%s) — pending_stream_id=%s pending_finish=%s elapsed=%.1fs. Possible causes: ack cmd filtered, ack req_id mismatch, or WeCom did not ack.",
                *pending_desc, _elapsed(pending_frame.sent_at),
            )
        except asyncio.CancelledError:
            task = asyncio.current_task()
            if not pending_frame.future.cancelled() or (task is not None and task.cancelling() > 0):
                raise  # genuine cancellation of THIS task must propagate
            # pending future was cancelled by its owner (final ack timeout / send-failure
            # cleanup) — shield mirrors the cancel out here; treat as drain-failed, keep sending.
            logger.info("[%s] Reply ack drain: pending frame future cancelled by owner (req_id=%s) — treating as drain failure, continuing", self.name, req_id)
        except Exception:
            pass
        queue.pending_ack = None  # resolved or timed out either way

    def _release_pending(self, queue: ReplyQueue, req_id: str, frame: ReplyFrame) -> None:
        """Clear ``frame`` if it is still the pending ack; drop the queue once empty."""
        if queue.pending_ack is frame:
            queue.pending_ack = None
        if queue.pending_ack is None:
            self._reply_queues.pop(req_id, None)

    def _resolve_reply_ack(self, req_id: str, payload: dict[str, Any]) -> bool:
        """Resolve a pending reply ack. Returns True if handled."""
        queue = self._reply_queues.get(req_id)
        if queue is None or queue.pending_ack is None:
            return False
        frame = queue.pending_ack
        if not frame.future.done():
            _body = payload.get("body", {}) if isinstance(payload.get("body"), dict) else {}
            logger.debug("[%s] _resolve_reply_ack: resolved req_id=%s is_final=%s elapsed=%.2fs errcode=%s", self.name, req_id, frame.is_final, _elapsed(frame.sent_at), _body.get("errcode", "N/A"))
            frame.future.set_result(payload)
        self._release_pending(queue, req_id, frame)
        return True

    def _fail_reply_queues(self, error: Exception) -> None:
        for queue in list(self._reply_queues.values()):
            if queue.pending_ack and not queue.pending_ack.future.done():
                queue.pending_ack.future.set_exception(error)
        self._reply_queues.clear()

    def _resolve_stream_req_id(self, chat_id: str, reply_to: Optional[str]) -> Optional[str]:
        """Explicit ``reply_to`` (cached message id) → last inbound req_id for the chat → None."""
        return self._reply_req_id_for_message(reply_to) or self._last_chat_req_ids.get(str(chat_id or "").strip()) or None

    @staticmethod
    def _cancel_keepalive(turn: StreamTurn) -> None:
        handle, turn.keepalive_handle = turn.keepalive_handle, None
        if handle is not None:
            try:
                handle.cancel()
            except Exception:
                pass

    def _retire_turn(self, turn: StreamTurn, turn_id: Optional[str]) -> None:
        """Single choke point for "turn is dead": cancel the timer, then drop it from the registry."""
        self._cancel_keepalive(turn)
        self._stream_turns.pop(f"{turn.chat_id}:{turn_id or turn.req_id}", None)

    def _expire_turn(self, turn: StreamTurn, turn_id: Optional[str]) -> None:
        turn.expired = True
        self._retire_turn(turn, turn_id)
        self._stream_expired_chats.add(turn.chat_id)

    def _find_active_turn_for_chat(self, chat_id: str) -> Optional[StreamTurn]:
        return next((t for t in self._stream_turns.values() if t.chat_id == chat_id and not t.finalized), None)

    def last_landed_stream_frame_content(self, chat_id: str) -> str:
        """fork 2026-11-18 (DPMS 561213): what the chat's active stream turn last actually LANDED
        on the wire, or "" when no live turn exists. ``send_stream_frame`` returns True for a
        dropped intermediate too (a wire error keeps the stream alive), so a consumer's
        durability bookkeeping — what the bubble provably shows the user — reads this instead of
        trusting the return value; ``last_sent_content`` is set only after a frame went out."""
        turn = self._find_active_turn_for_chat(chat_id)
        return turn.last_sent_content if turn is not None else ""

    def _find_overflow_anchor(self, chat_id: str) -> Optional[StreamTurn]:
        """fork 2026-11-16 (DPMS 561213): the turn an overflow bubble may continue — the chat's
        sealed turn whose req_id is still the latest inbound, else a live unfinalized turn."""
        anchor = getattr(self, "_last_stream_turn_by_chat", {}).get(chat_id)
        if anchor is not None and anchor.req_id_fresh and not anchor.expired:
            return anchor
        turn = self._find_active_turn_for_chat(chat_id)
        return turn if turn is not None and turn.req_id_fresh else None

    async def extend_stream_bubble(self, chat_id: str, text: str) -> bool:
        """fork 2026-11-16 (DPMS 561213): deliver one follow-up bubble of an OVERFLOW reply as a
        sealed ``msgtype: stream`` frame opened and finished in a single call.

        Reason: finalize declines once the final display exceeds MAX_STREAM_CONTENT_LENGTH (the
        adapter byte-truncates frames), and the reply then went out through the regular send —
        leaving the half-drawn bubble on screen next to a second, complete message. Sending the
        full reply as ordered bubbles keeps ONE channel: bubble N is sealed before bubble N+1
        opens, so nothing is duplicated and nothing is lost. ``text`` must fit the frame budget.

        *text* is str (the frames' unit): callers holding bytes decode UTF-8 first. Returns False
        (the caller regular-sends the bubble, and every bubble after it, in order) when there is
        no usable turn to continue, the stream is unavailable, or the frame does not land.
        The turn's ``continuation`` advances only when the FIRST frame landed — a bubble the wire
        refused must not leave a half-opened stream id behind for the next one to reuse."""
        chat = (chat_id or "").strip()
        if not chat or not text:
            return False
        if not isinstance(text, str):
            text = bytes(text).decode("utf-8", errors="replace")
        turn = self._find_overflow_anchor(chat)
        if turn is None:
            return False
        # The overflow steps of one reply arrive in order, so a per-turn counter is enough.
        stream_id = f"{turn.stream_id}_c{turn.continuation + 1}"

        async def send() -> bool:
            await self._send_stream_reply(turn.req_id, stream_id, text, finish=False)
            turn.continuation += 1
            await self._send_stream_reply(turn.req_id, stream_id, text, finish=True)
            return True

        try:
            return await asyncio.wait_for(
                self._enqueue_chat_send(chat, send, is_control=True),
                timeout=self._FINALIZE_QUEUE_WAIT_SECONDS,
            )
        except TimeoutError:
            logger.info("[%s] overflow bubble %s queued >%.0fs for chat %s — falling back for the remainder",
                        self.name, stream_id, self._FINALIZE_QUEUE_WAIT_SECONDS, chat)
            return False
        except WeComStreamExpiredError:
            self._stream_expired_chats.add(chat)
            logger.info("[%s] overflow bubble %s: stream expired for chat %s — falling back for the remainder",
                        self.name, stream_id, chat)
            return False
        except Exception as exc:
            logger.info("[%s] overflow bubble %s failed for chat %s (%s) — falling back for the remainder",
                        self.name, stream_id, chat, exc)
            return False

    def _arm_keepalive(self, turn: StreamTurn, *, turn_id: Optional[str]) -> None:
        """Arm the keep-alive timer if enabled and not already armed (idempotent)."""
        if not self._stream_keepalive_enabled or turn.finalized or turn.expired or turn.keepalive_handle is not None:
            return
        try:
            turn.keepalive_handle = asyncio.get_running_loop().call_later(self._stream_keepalive_interval_seconds, self._on_keepalive_fire, turn, turn_id)
        except RuntimeError:
            pass

    def _on_keepalive_fire(self, turn: StreamTurn, turn_id: Optional[str]) -> None:
        turn.keepalive_handle = None
        if not (turn.finalized or turn.expired):
            try:
                asyncio.ensure_future(self._keepalive_send(turn, turn_id))
            except RuntimeError:
                pass

    async def _keepalive_send(self, turn: StreamTurn, turn_id: Optional[str]) -> None:
        """Re-send accumulated text as finish=false to refresh the window, then re-arm. Never a placeholder
        (empty text skips); on 846604/846608 the turn is retired for Layer 2."""
        if turn.finalized or turn.expired or turn._intermediate_frames_sent >= MAX_INTERMEDIATE_FRAMES:
            return  # cap reached: no room for intermediates; let finalize / Layer 2 run
        content = turn.accumulated_text or ""
        if not content.strip():
            self._arm_keepalive(turn, turn_id=turn_id)
            return
        try:
            await self._send_stream_reply(turn.req_id, turn.stream_id, content, finish=False)
        except WeComStreamExpiredError:
            self._expire_turn(turn, turn_id)
            return
        except Exception as exc:
            logger.debug("[%s] keep-alive send failed (chat=%s, turn=%s): %s", self.name, turn.chat_id, turn.stream_id, exc)
            self._arm_keepalive(turn, turn_id=turn_id)  # transient — retry next interval
            return
        turn.last_sent_content = content
        self._arm_keepalive(turn, turn_id=turn_id)

    @staticmethod
    def _truncate_stream_content(content: str, limit: int) -> str:
        """Truncate to ``limit`` UTF-8 bytes (WeCom caps frames by bytes, not codepoints)."""
        encoded = content.encode("utf-8")
        return content if len(encoded) <= limit else encoded[:limit].decode("utf-8", errors="ignore")

    async def _send_stream_reply(self, reply_req_id: str, stream_id: str, content: str, finish: bool = False) -> dict[str, Any]:
        """Send one ``msgtype: "stream"`` frame: intermediates non-blocking/skip-if-pending, the final frame awaits
        its ack so 846608/6000 are detected. Raises WeComStreamExpiredError on expiry."""
        truncated = self._truncate_stream_content(content or "", self.MAX_STREAM_CONTENT_LENGTH)
        if len(content or "") != len(truncated):
            logger.warning("[%s] Stream content truncated for stream_id=%s", self.name, stream_id)
        body: dict[str, Any] = {"msgtype": "stream", "stream": {"id": stream_id, "finish": bool(finish), "content": truncated}}
        if not finish:
            return await self._send_reply_queued(reply_req_id, body, is_final=False, skip_if_pending=True)
        response = await self._send_reply_queued(reply_req_id, body, is_final=True, skip_if_pending=False)
        errcode = response.get("errcode", 0)
        if errcode in (STREAM_EXPIRED_ERRCODE, STREAM_REQUEST_EXPIRED_ERRCODE):
            raise WeComStreamExpiredError(errcode=errcode, errmsg=str(response.get("errmsg") or ""))
        if errcode == STREAM_VERSION_CONFLICT_ERRCODE:
            # Content is already on screen; raising would pop the turn and duplicate via send().
            logger.info("[%s] finalize hit errcode 6000 (version conflict) — bubble already replaced by a newer frame; treating as delivered.", self.name)
            return response
        self._raise_for_wecom_error(response, "send stream reply")
        return response

    async def send_stream_frame(self, text: str, *, finalize: bool = False, chat_id: Optional[str] = None, reply_to: Optional[str] = None, **kwargs) -> bool:
        """Gateway streaming entry point: first call seeds the turn, later calls push cumulative text, ``finalize=True``
        closes it; ``turn_id`` kwarg keys concurrent turns. Returns False when unavailable — caller falls back to send()."""
        chat = (chat_id or "").strip()
        if not chat:
            logger.warning("[%s] send_stream_frame: chat_id required", self.name)
            return False
        turn_id = kwargs.get("turn_id")
        # Chat-level expiry only blocks NEW turn creation; a known turn_id may still finalize.
        if not turn_id and chat in self._stream_expired_chats:
            return False
        inner = lambda: self._send_stream_frame_inner(text, chat=chat, reply_to=reply_to, finalize=finalize, turn_id=turn_id)
        # Finalize counts toward 30/min → control lane; intermediates are unmetered (no queue).
        if not finalize:
            return await inner()
        try:
            # fork 2026-11-13 (DPMS 561213): a finalize frame sitting in the control-lane
            # queue past the gateway's stream-flush budget detaches from its caller — the
            # gateway stops waiting (final_response_sent stays unset) and delivers the
            # reply via the normal send, then the queued frame lands afterwards and the
            # user reads the same reply twice (production 2026-11-13 17:49,
            # hm-832fbfdf-68f: the frame sat behind the token bucket's rate-limit sleep).
            # Bound the wait BELOW the gateway's flush budget and decline cleanly past it;
            # the send-queue drops the cancelled item so the detached frame never lands.
            return await asyncio.wait_for(
                self._enqueue_chat_send(chat, inner, is_control=True),
                timeout=self._FINALIZE_QUEUE_WAIT_SECONDS,
            )
        except TimeoutError:
            # fork 2026-11-18 (DPMS 561213): the finish frame never left the queue — but the
            # progressive frames may already have drawn the whole answer in the bubble. The
            # gateway decides from what is durably on screen (has_durably_delivered_text):
            # fully drawn → the regular send is suppressed; partially drawn → only the
            # remainder goes out. "The regular send delivers the reply" is no longer the
            # contract — re-sending what the bubble shows is the duplicate this fork fixes.
            logger.info(
                "[%s] finalize_decision=finalize_queue_decline chat=%s wait>%.0fs — finish frame not sent; gateway suppresses/trim the regular send from what the bubble durably drew",
                self.name, chat, self._FINALIZE_QUEUE_WAIT_SECONDS,
            )
            return False

    def _locate_turn(self, chat: str, reply_to: Optional[str], finalize: bool, turn_id: Optional[str]) -> Optional[StreamTurn]:
        """Find or create the StreamTurn (None = unavailable); a turn locks to its creation req_id."""
        if turn_id:
            turn = self._stream_turns.get(f"{chat}:{turn_id}")
            if turn:
                return turn
            if finalize:  # never create a turn on finalize: caller must fall back, not seed+finish
                logger.debug("[%s] send_stream_frame: cannot finalize non-existent turn (turn_id=%s, chat=%s)", self.name, turn_id, chat)
                return None
        elif existing_turn := self._find_active_turn_for_chat(chat):  # direct callers without turn_id reuse the chat's active (unfinalized) turn
            logger.debug("[%s] send_stream_frame: reusing existing turn %s for chat %s", self.name, existing_turn.stream_id, chat)
            return existing_turn
        suffix = f" (turn_id={turn_id})" if turn_id else ""
        req_id = None if chat in self._stream_expired_chats else self._resolve_stream_req_id(chat, reply_to)
        if not req_id:
            why = "chat %s is expired, cannot create new turn%s" if chat in self._stream_expired_chats else "no req_id available for chat %s%s"
            logger.debug("[%s] send_stream_frame: " + why, self.name, chat, suffix)
            return None
        key = f"{chat}:{turn_id or req_id}"
        turn = None if turn_id else self._stream_turns.get(key)
        if turn is None:
            turn = StreamTurn(chat, req_id)
            # fork 2026-11-18 (DPMS 561213): the turn the seed was waiting for — it inherits the
            # seed's stream (ONE bubble for the whole turn) and its seeded flag, so the inner
            # path sends no second <think></think> (a double seed is errcode 6000).
            self._adopt_pending_stream_seed(turn, chat, req_id, turn_id)
        self._stream_turns[key] = turn
        logger.debug("[%s] send_stream_frame: created new turn %s (%s) for chat %s", self.name, turn.stream_id, f"turn_id={turn_id}, req_id={req_id}" if turn_id else f"req_id={req_id}", chat)
        return turn

    async def _finalize_turn(self, turn: StreamTurn, text: str, chat: str, turn_id: Optional[str]) -> bool:
        """Send the finish=true frame (or decline via the Layer 2 clock fallback)."""
        # Layer 2: an old stream would hit 846604/846608 on finish=true, so decline up front and let
        # send() deliver once. Skipped with Layer 1 on: the heartbeat refreshed the window.
        if not self._stream_keepalive_enabled:
            stream_age = time.monotonic() - turn.start_time
            if stream_age >= self._stream_safe_duration_seconds:
                logger.info(
                    "[%s] Stream age %.0fs >= safe duration %.0fs for chat %s — declining finalize frame, falling back to proactive send (Layer 2 clock fallback).",
                    self.name, stream_age, self._stream_safe_duration_seconds, chat,
                )
                self._expire_turn(turn, turn_id)
                return False
        self._cancel_keepalive(turn)
        # A final frame identical to the last intermediate is silently dropped — differ via ZWSP.
        final_text = text + "\u200b" if text and text == turn.last_sent_content else text
        await self._send_stream_reply(turn.req_id, turn.stream_id, final_text, finish=True)
        # fork 2026-11-16 (DPMS 561213): keep the sealed turn as the chat's overflow anchor —
        # the run loop's overflow delivery continues this reply in further bubbles.
        getattr(self, "_last_stream_turn_by_chat", {})[chat] = turn
        turn.finalized = True
        self._stream_turns.pop(f"{chat}:{turn_id or turn.req_id}", None)
        return True

    async def _send_stream_frame_inner(self, text: str, *, chat: str, reply_to: Optional[str] = None, finalize: bool = False, turn_id: Optional[str] = None) -> bool:
        turn: Optional[StreamTurn] = None
        try:
            turn = self._locate_turn(chat, reply_to, finalize, turn_id)
            if turn is None or turn.expired:
                return False
            if not turn.seeded and not turn.finalized:
                # Official THINKING_MESSAGE seed; `seeded` prevents a double seed (6000).
                await self._send_stream_reply(turn.req_id, turn.stream_id, "<think></think>", finish=False)
                turn.seeded = True
                self._arm_keepalive(turn, turn_id=turn_id)
                if not text and not finalize:
                    return True  # consumer's explicit seed call — nothing more to send
            if finalize:
                return await self._finalize_turn(turn, text, chat, turn_id)
            # Fire-and-forget: the gateway decides when to push (identity dedup in stream_consumer.py).
            turn.accumulated_text = text
            if turn._intermediate_frames_sent >= MAX_INTERMEDIATE_FRAMES or text == turn.last_sent_content:
                return True  # cap reached (finalize drains the rest) or nothing new
            await self._send_stream_reply(turn.req_id, turn.stream_id, text, finish=False)
            turn._intermediate_frames_sent += 1
            turn.last_sent_content = text
            return True
        except WeComStreamExpiredError:
            # Intermediates are overwritten by the next frame anyway; expiring here would duplicate.
            if not finalize:
                logger.info("[%s] Intermediate stream frame expired (errcode=%d) for chat %s — dropping frame, stream stays live", self.name, STREAM_EXPIRED_ERRCODE, chat)
                return True
            logger.info("[%s] Stream expired (errcode=%d) for chat %s — switching to proactive send", self.name, STREAM_EXPIRED_ERRCODE, chat)
            if turn is None:
                self._stream_expired_chats.add(chat)
            else:
                self._expire_turn(turn, turn_id)
        except Exception as exc:
            if not finalize:  # same intermediate/final split as above
                logger.info("[%s] Intermediate stream frame failed (chat=%s): %s — dropping frame, stream stays live", self.name, chat, exc)
                return True
            logger.warning("[%s] Stream frame failed (chat=%s): %s", self.name, chat, exc)
            if turn is not None:
                self._retire_turn(turn, turn_id)
        return False

    def supports_native_streaming(self, chat_type: Optional[str] = None, metadata: Optional[dict[str, Any]] = None) -> bool:
        """Stream frames work in DMs and groups alike (groups just need a cached inbound req_id)."""
        del chat_type, metadata
        return True

    # ------------------------------------------------------------------
    # The pending seed slot (fork 2026-11-18, DPMS 561213)
    # ------------------------------------------------------------------
    # send_typing opens the turn's reply flow with an invisible <think></think> seed (the early
    # callback ack); the slot below holds what the turn's StreamTurn must inherit until it is
    # created — and what gets sealed when no turn ever comes.

    def _adopt_pending_stream_seed(self, turn: StreamTurn, chat: str, req_id: str, turn_id: Optional[str]) -> None:
        """A fresh StreamTurn takes over the chat's typing seed: same stream, no second seed."""
        slots = getattr(self, "_pending_stream_seeds", None)
        if not isinstance(slots, dict):
            return  # a bare stream mixin (no adapter init) has no seed registry: nothing to adopt
        slot = slots.get(chat)
        if slot is None or slot.state != "pending" or slot.req_id != req_id:
            return
        if time.monotonic() - slot.created_at > self._stream_keepalive_interval_seconds:
            return  # stale: its seal either fired already or is about to
        if slot.seal_handle is not None:
            try:
                slot.seal_handle.cancel()
            except Exception:
                pass
            slot.seal_handle = None
        slot.state = "adopted"  # stays as the latch: ONE seed per turn until the chat's next inbound
        turn.stream_id = slot.stream_id
        turn.seeded = True  # the <think></think> is already out — a second one is a 6000 double seed
        # The consumer path arms the keep-alive when it seeds; adoption replaces that seed, so it
        # arms here (idempotent, and a no-op while the keep-alive is disabled).
        self._arm_keepalive(turn, turn_id=turn_id)
        logger.debug("[%s] send_typing seed %s adopted by the turn of chat %s", self.name, slot.stream_id, chat)

    def _clear_pending_stream_seed(self, chat_id: str) -> None:
        """Drop the chat's seed slot: a newer inbound supersedes it, or the seed never landed."""
        slots = getattr(self, "_pending_stream_seeds", None)
        if not isinstance(slots, dict):
            return
        slot = slots.pop(chat_id, None)
        if slot is not None and slot.seal_handle is not None:
            try:
                slot.seal_handle.cancel()
            except Exception:
                pass

    def _arm_pending_seed_seal(self, slot: PendingStreamSeed) -> None:
        """An un-adopted seed closes at the first keep-alive interval (an empty finish frame, the
        ``_close_empty_native_bubble`` precedent): a turn that never streams leaves no orphan
        “thinking” bubble behind."""
        try:
            slot.seal_handle = asyncio.get_running_loop().call_later(
                self._stream_keepalive_interval_seconds, self._on_pending_seed_seal_fire, slot.chat_id
            )
        except RuntimeError:
            pass  # no running loop — nothing can keep the adapter alive past this call anyway

    def _on_pending_seed_seal_fire(self, chat_id: str) -> None:
        slot = self._pending_stream_seeds.get(chat_id)
        if slot is not None:
            slot.seal_handle = None
        if slot is None or slot.state != "pending":
            return  # adopted while the timer lagged: the bubble belongs to the turn now
        try:
            asyncio.ensure_future(self._seal_pending_stream_seed(chat_id))
        except RuntimeError:
            pass

    async def _seal_pending_stream_seed(self, chat_id: str) -> None:
        """Close an un-adopted seed's bubble with an empty finish frame; the slot is spent."""
        slot = self._pending_stream_seeds.get(chat_id)
        if slot is None or slot.state != "pending":
            return
        # Before the await: an adoption racing this seal must never take a finished stream.
        slot.state = "sealed"
        try:
            # A finish frame rides the control lane like every finalize (order before the next reply).
            await asyncio.wait_for(
                self._enqueue_chat_send(
                    chat_id,
                    lambda: self._send_stream_reply(slot.req_id, slot.stream_id, "", finish=True),
                    is_control=True,
                ),
                timeout=self._FINALIZE_QUEUE_WAIT_SECONDS,
            )
            logger.debug("[%s] Sealed the un-adopted typing seed of chat %s (stream %s)", self.name, chat_id, slot.stream_id)
        except WeComStreamExpiredError:
            logger.info("[%s] Pending seed seal for chat %s: stream already expired — bubble is gone server-side", self.name, chat_id)
        except TimeoutError:
            logger.info("[%s] Pending seed seal queued >%.0fs for chat %s — giving up (best-effort close)", self.name, self._FINALIZE_QUEUE_WAIT_SECONDS, chat_id)
        except Exception as exc:
            logger.info("[%s] Pending seed seal failed for chat %s: %s", self.name, chat_id, exc)

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        """fork 2026-11-18 (DPMS 561213): with ``stream_early_ack_enabled``, the chat's FIRST typing
        call of a turn opens the reply flow — an invisible ``<think></think>`` seed on the chat's
        inbound req_id. A respond frame is the only answer the WS aibot vocabulary can give a
        callback (there is no ack-only frame; ping→pong answers pings only), and the gateway types
        at EVERY turn start (``base._keep_typing`` → here) long before the stream consumer seeds —
        so the platform's re-delivery clock sees the callback answered at turn acceptance, not at
        the model's first token. ONE seed per turn: the slot is adopted by the turn's StreamTurn
        (same bubble), or sealed with an empty finish frame at the first keep-alive interval.
        Heartbeat turns never reach here (``base._start_typing_refresh`` excludes them). Off by
        default: the consumer's own seed at first output keeps serving — the previous behaviour."""
        del metadata
        if not getattr(self, "_stream_early_ack_enabled", False):
            return  # the legacy no-op: typing is handled by the stream consumer's seed
        chat = (chat_id or "").strip()
        if not chat:
            return
        try:
            current_req_id = self._resolve_stream_req_id(chat, None)
            slot = self._pending_stream_seeds.get(chat)
            if slot is not None and current_req_id:
                if slot.req_id == current_req_id:
                    return  # this turn already holds a seed (pending, adopted, or sealed)
                self._clear_pending_stream_seed(chat)  # a newer inbound: its turn seeds afresh
            if not current_req_id or chat in self._stream_expired_chats:
                return
            if self._find_active_turn_for_chat(chat) is not None:
                return  # busy follow-up: never a second bubble mid-turn
            slot = PendingStreamSeed(
                chat_id=chat, req_id=current_req_id,
                stream_id=f"stream_{uuid.uuid4().hex[:12]}", created_at=time.monotonic(),
            )
            # Registered BEFORE the await: the consumer's StreamTurn may be created while the frame
            # is in flight, and it must find the slot to adopt (one bubble for the whole turn).
            self._pending_stream_seeds[chat] = slot
            response = await self._send_stream_reply(slot.req_id, slot.stream_id, "<think></think>", finish=False)
            if slot.state != "pending":
                return  # adopted mid-send — the seal (if armed later) would close the turn's bubble
            if response.get("skipped"):
                # An ack is pending on this req_id; the seed did not land. Retry on the next
                # typing refresh (2s) instead of sealing a bubble that never opened.
                self._clear_pending_stream_seed(chat)
                return
            self._arm_pending_seed_seal(slot)
        except Exception as exc:
            # Typing must never break the turn (base._keep_typing bounds each call anyway).
            logger.debug("[%s] send_typing early-ack seed failed (chat=%s): %s", self.name, chat, exc)
            slot = getattr(self, "_pending_stream_seeds", {}).get(chat)
            if slot is not None and slot.state == "pending":
                self._clear_pending_stream_seed(chat)
