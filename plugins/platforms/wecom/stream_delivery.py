"""WeCom stream-frame delivery — clawrelay-style streaming for the WeCom smart bot.

The WeCom smart-bot protocol cannot edit sent messages, but it supports a
*stream-frame* reply: ``aibot_respond_msg`` with ``msgtype: "stream"``. The first
frame opens a chat bubble, later frames update it by ``stream.id``, and a frame
with ``finish=True`` closes it. This module drives that protocol to reproduce the
interaction style of `clawrelay-wecom-server
<https://github.com/wxkingstar/clawrelay-wecom-server>`_:

- a single growing bubble (no separate tool-progress bubbles);
- a ``<think>`` block that stays open while the model is "thinking" and collapses
  once answer text begins (WeCom renders an unclosed ``<think>`` as an animated
  "正在思考" indicator);
- thinking markers ``🤔 正在思考中...`` / tool lines / ``💭 {preview}`` /
  ``✨ 回复完成``;
- 300ms throttled pushes with a running indicator ``⏳ 正在运行中…`` that switches
  to ``⏳ 正在运行中，完成后会通知您🔔`` after 60s.

Restored 2026-09-02 (dropped during the 2026-08-31 v0.21.0 resync along with the
run.py wiring — see the ``WECOM_STREAM_DELIVERY`` branches in gateway/run.py).
Adapted to the v0.21.0 adapter surface: frames go through
``adapter.send_stream_frame(text, finalize=..., chat_id=...)`` — the adapter's
StreamTurn state resolves req_id/stream_id per turn — and tool progress arrives
via ``on_tool_progress(line)`` (gateway-formatted lines with their own emoji).

Threading model mirrors :class:`gateway.stream_consumer.GatewayStreamConsumer`:
the input methods (``on_delta`` / ``on_commentary`` / ``on_tool_progress`` /
``finish``) are synchronous and called from the agent worker thread — they mutate
display state and enqueue a signal on a thread-safe :class:`queue.Queue`. The
async :meth:`WeComStreamDelivery.run` task drains that queue in the event loop,
builds the display, throttles, and sends stream frames.
"""

from __future__ import annotations

import asyncio
import logging
import queue
import re
import time
from typing import Any, Optional

from plugins.platforms.wecom.reply_lane import reply_lane
from plugins.platforms.wecom.streaming import MAX_STREAM_CONTENT_LENGTH
logger = logging.getLogger(__name__)

# Push at most one stream frame every STREAM_THROTTLE_INTERVAL seconds.
STREAM_THROTTLE_INTERVAL = 0.3
# After this many seconds the running indicator switches to a "we'll notify you"
# message, signalling that the task is long-running.
LONG_RUNNING_THRESHOLD = 60
# Give up streaming after this many consecutive failed frame sends (e.g. no
# cached req_id for the chat). The final reply then falls back to plain send().
MAX_CONSECUTIVE_FRAME_FAILURES = 10
# fork 2026-09-30 (audit 2026-09-29 module-2 H4): fold the think block in the FINAL frame;
# intermediates keep every line. Unbounded think + answer text could exceed the
# 20480-byte frame cap and byte-truncate the ANSWER tail.
MAX_FINAL_THINK_LINES = 30

# Queue signals (state is mutated synchronously by the input methods; these only
# wake the run loop).
_DELTA = object()
_COMMENTARY = object()
_TOOL = object()
_DONE = object()

_EMPTY_REPLY_FALLBACK = "AI 已完成处理，但未生成文本回复。请尝试换个方式描述您的需求。"

# fork 2026-11-16 (DPMS 561213): overflow delivery (a reply too long for the final stream frame).
# The bubble is sealed on the answer head (that frame holds no more than the head) and the follow-up
# bubbles continue the reply — one channel instead of a bubble next to a separately regular-sent reply.
_OVERFLOW_SEAL = "……（回复较长，完整内容见下方消息）"
# Bytes kept free in the sealed bubble so the reply continues in a bubble of its own.
_OVERFLOW_MIN_CONTINUATION = 256
# Below this piece size the gateway chunker's output is just its " (n/m)" of single lines.


def overflow_indicator(index: int, total: int) -> str:
    """The ``（index/total）`` prefix of an overflow bubble batch."""
    return f"（{index}/{total}）\n"


def _split_overflow_marker(text: str) -> tuple[str, str]:
    """Strip the at most two ``（n/m）`` prefixes a queued bubble carries (see _frame_overflow_reply)."""
    marker = ""
    while len(marker) < 2 and _OVERFLOW_MARKER_RE.match(text):
        head = _OVERFLOW_MARKER_RE.match(text)
        marker += head.group(0)
        text = text[head.end():]
    return marker, text


_OVERFLOW_MARKER_RE = re.compile("^（\\d+/\\d+）\\n")
# The gateway splitter's per-piece " (i/N)" suffix (layout, not text).
_FRAME_SUFFIX_RE = re.compile(r"(?: \(\d+/\d+\)|\n\(\d+/\d+\)(?=\n|$))")



def _split_bytes(text: str, limit: int) -> list[str]:
    """Split *text* into ≤limit UTF-8 byte pieces, preferring line breaks.

    Every piece is a SUBSTRING of *text*, and joining the pieces gives *text* back: the sealed
    bubble's head and its continuation are two cuts of one reply, so a splitter that inserts or
    re-numbers text (the gateway's markdown chunker does, for pieces it splits) would draw the
    same line twice. A line longer than the budget is cut at a code-point boundary."""
    if not text:
        return []
    pieces: list[str] = []
    piece = ""  # the piece being filled, its line breaks included
    for line in text.splitlines(keepends=True):
        if len((piece + line).encode("utf-8")) <= limit:
            piece += line
            continue
        if piece:
            pieces.append(piece)
            piece = ""
        data = line.encode("utf-8")
        while len(data) > limit:  # a line longer than a piece: cut it at a code-point boundary
            cut = limit
            while cut > 0 and (0x80 <= data[cut] < 0xC0):
                cut -= 1
            cut = cut or limit
            pieces.append(data[:cut].decode("utf-8"))
            data = data[cut:]
        piece = data.decode("utf-8")
    if piece:
        pieces.append(piece)
    return pieces


_MEDIA_LINE_RE = re.compile(r"(?:^|\s)MEDIA:\s*\S+")


def _segments_and_images(text: str) -> str:
    """The trailing part of a reply that is not its markdown body: reply segments
    (``\\x00SEGMENT\\x00``-joined), MEDIA: tags, and what follows the last one up to the end (the
    completion footer) — the lines an overflow bubble shows after the body it continues."""
    if "\x00SEGMENT\x00" in (text or ""):
        return text.rsplit("\x00SEGMENT\x00", 1)[1]
    lines = (text or "").splitlines()
    hits = [i for i, line in enumerate(lines) if _MEDIA_LINE_RE.search(line)]
    if hits:
        last = max(hits)
        start = last
        while start > 0 and not lines[start].strip():  # a blank line above it belongs to the body
            start -= 1
        return "\n".join(lines[start:])
    return ""


_WS_RE = re.compile(r"\s+")


def _norm_for_match(text: str) -> str:
    return _WS_RE.sub(" ", (text or "").replace("\u200b", "")).strip()


def _friendly_error(exc: BaseException) -> str:
    """Convert an internal exception into a user-friendly WeCom message."""
    msg = str(exc).lower()
    if "timeout" in msg or "timed out" in msg:
        return "⏱️ 处理超时，请稍后重试。"
    if "connection" in msg or "connect" in msg:
        return (
            "AI 服务暂时无法连接，请联系管理员检查服务状态后重试。"
        )
    return "抱歉，处理出错，请稍后重试。如问题持续，请联系管理员。"


def _build_running_indicator(start_time: float) -> str:
    """Build the running indicator; ellipsis cycles (. → .. → ...).

    After ``LONG_RUNNING_THRESHOLD`` seconds it switches to a "we'll notify you"
    message so the user knows the task is still alive but long-running.
    """
    elapsed = time.monotonic() - start_time
    if elapsed >= LONG_RUNNING_THRESHOLD:
        return "\n\n⏳ 正在运行中，完成后会通知您🔔"
    dots = "." * (int(time.time()) % 3 + 1)
    return f"\n\n⏳ 正在运行中{dots}"


def _build_display_content(
    thinking_lines: list,
    thinking_buf: str,
    session_link: str,
    text: str,
    finished: bool = False,
    fold_think: bool = False,
) -> str:
    """Build the combined display: ``<think>`` block + session link + answer text.

    While thinking (no answer text and not finished) the ``<think>`` tag is left
    *unclosed* so WeCom shows an animated "正在思考" indicator instead of a
    collapsed "已完成思考" block. Ported from clawrelay's orchestrator.

    With ``fold_think`` (final frame only) the oldest think steps beyond
    ``MAX_FINAL_THINK_LINES`` collapse into a single marker line, so the answer
    text is never pushed past the frame's byte cap by a tool-heavy think block.
    """
    parts: list = []
    if thinking_lines or thinking_buf:
        lines = list(thinking_lines)
        if thinking_buf:
            preview = thinking_buf[-200:]
            prefix = "..." if len(thinking_buf) > 200 else ""
            lines.append(f"💭 {prefix}{preview}")
        if fold_think and len(lines) > MAX_FINAL_THINK_LINES:
            dropped = len(lines) - MAX_FINAL_THINK_LINES
            lines = [f"...（已折叠前 {dropped} 行）"] + lines[-MAX_FINAL_THINK_LINES:]
        think_content = "<think>\n" + "\n".join(lines)
        # Close the think block once answer text arrives (or on finish);
        # otherwise keep it open so WeCom renders "正在思考".
        if text or finished:
            think_content += "\n</think>"
        parts.append(think_content)
    if session_link:
        parts.append(session_link)
    if text:
        parts.append(text)
    return "\n\n".join(parts)


class WeComStreamDelivery:
    """Drives one WeCom stream bubble for a single assistant reply.

    Construct one per inbound user message. Call the sync input methods from the
    gateway/agent callbacks, then ``await run()`` as a background task.
    """

    def __init__(
        self,
        adapter: Any,
        chat_id: Optional[str] = None,
        chat_record_url: str = "",
    ) -> None:
        self.adapter = adapter
        self.chat_id = chat_id
        self.chat_record_url = chat_record_url or ""

        # Display state — mutated by the sync input methods (agent thread),
        # read by run() (event loop). Safe because each input method performs a
        # single atomic mutation with no await in between.
        self.thinking_lines: list[str] = ["🤔 正在思考中..."]
        self.thinking_buf = ""
        self.accumulated_text = ""
        self.tool_names_seen: set[str] = set()
        self.after_tool_use = False
        self.finished = False
        self._error_mode = False
        self._disabled = False

        # Thread-safe signal queue + loop binding.
        self._queue: queue.Queue = queue.Queue()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._wakeup: Optional[asyncio.Event] = None

        # Throttle / push state — owned by run().
        self.start_time = 0.0
        self.last_pushed_display = ""
        self.last_push_time = 0.0
        self._consecutive_failures = 0
        self._already_sent = False
        self._final_response_sent = False
        # fork 2026-11-16 (DPMS 561213): the overflow delivery — the full reply as sealed follow-up
        # bubbles when it cannot fit the final frame (see _finalize / attach_overflow).
        self.overflow_parts: list[bytes] = []
        self._overflow_head = ""  # answer head already shown by the sealed bubble
        self.overflow_text = ""  # what the overflow bubbles continue (a sealed reply: minus its head)
        self._overflow_full = ""  # the reply as streamed, head included
        self._overflow_source = ""  # what the sealed bubble's continuation owes (it drew none of it)

        self.overflow_body = ""  # what the bubbles still carry, in send order
        self.overflow_tail = ""  # segment / MEDIA: tail the bubbles are sent right before
        self._overflow_handed = ""  # the last payload the gateway handed over (attach_overflow)
        self._overflow_queued_for = ""  # the payload the queued bubbles carry, as handed over
        self._displayed_reply = ""  # the reply as the last final frame drew it in its bubble
        self._overflow_shown = ""  # the reply the last framing draws: sealed head + this hand-over
        self._delivery_claimed = False
        # One seal at a time: once one seal path has sent this reply's bubbles the other leaves
        # them alone, so a bubble is never sealed twice (and no reply is shown twice).
        self._seal_lock = asyncio.Lock()
        # A send of this reply carries the bubbles queued BEFORE it (adapter._send_reply_part);
        # whatever a send queues after it is the reply's close-out flush (run()).
        self._overflow_owned = False
        # fork 2026-11-18 (DPMS 561213): the answer text a frame actually LANDED on screen — the
        # cumulative progressive draws (intermediate push / final frame / sealed head), snapshotted
        # at display-build so deltas arriving mid-send never count before they show. The gateway's
        # final-send suppression consults it (has_durably_delivered_text): a bubble that drew the
        # whole answer must never be followed by the same answer as a second message.
        self._durable_text = ""


    # ------------------------------------------------------------------
    # Properties (GatewayStreamConsumer-compatible surface)
    # ------------------------------------------------------------------
    @property
    def already_sent(self) -> bool:
        """True once at least one frame has been pushed."""
        return self._already_sent

    @property
    def final_response_sent(self) -> bool:
        """True once the closing finish frame has been delivered."""
        return self._final_response_sent

    @property
    def accepts_tool_progress(self) -> bool:
        """Tool-progress lines are folded into the think block."""
        return True

    def attach_overflow(self, final_text: str) -> list[bytes]:
        """fork 2026-11-16 (DPMS 561213): the reply is delivered — publish the overflow delivery.

        Called by the gateway once its collection for this chat is complete (the footer included).
        The bubbles carry the regular-send payload — the reply's body followed by its tail (the
        MEDIA:/segment lines and footer it cannot render) — so the gateway's own regular send is
        provably redundant; until the last bubble is sealed it is also deferrable, because a send
        of this reply carries the queued bubbles first (adapter._send_reply_part). A short
        continuation goes WITHOUT numbering (it reads as part of the bubble); only a multi-bubble
        continuation is numbered ``（n/m）``.

        * sealed: the reply is queued NOW as one numbered run, and claimed — the gateway's send of
          it would be that reply a second time.
        * finalized: nothing is queued until the run loop ends (the reply bubble already shows
          the body); when the gateway's text is not what was streamed, only the tail's bubbles go
          out and the regular send keeps the body — a second hand-over of that text claims it.

        Returns the newly queued bubbles."""
        body = final_text or ""
        if body == self._overflow_handed:
            return []  # the same payload handed over again (the gateway's own re-send)
        self._overflow_handed = body
        if self._delivery_claimed:
            if self._overflow_source and not self.overflow_parts:
                # A sealed bubble drew no answer text: the whole reply is framed here, one run,
                # and the bubble shows its first piece — the rest continues from the same cut.
                if not body:
                    return []
                if body.startswith(self._overflow_source):  # what was streamed, plus (at most) a footer
                    # Whatever follows the streamed reply (its footer) is the bubbles' tail.
                    extra = body[len(self._overflow_source):]
                    self.overflow_body = self._overflow_source
                    self.overflow_tail = extra.strip("\n")
                else:  # the gateway's text is not what was streamed: the bubbles carry that text
                    self.overflow_body, self.overflow_tail = "", body
                self._overflow_queued_for = body
                return self._frame_overflow_reply()
            # Only text the bubbles do not show yet is appended as one more bubble. The tail was
            # queued, not drawn: text appended later may well start with it.
            if not body or body == self._overflow_queued_for:
                return []  # the payload already handed over (the gateway repeating its own send)
            shown = self.overflow_text
            if self._overflow_head:
                shown = "".join(_split_bytes(self.overflow_text, self._head_budget())[1:])
            fresh = body.removeprefix(shown).lstrip("\n")
            if not fresh.strip():
                return []
            parts = self._frame_overflow_reply(fresh, head="")
            self._overflow_queued_for = body
            logger.info("[wecom-stream] reply text changed after the stream — appended in %d follow-up bubble(s)", len(parts))
            return parts
        # Not claimed: no bubble was sealed short, so nothing is owed beyond what is on screen —
        # only a final frame that normally drew the whole reply leaves nothing to queue here.
        # fork 2026-11-18 (DPMS 561213): except when the bubble drew the answer progressively and
        # the finalize frame never landed (queue-budget decline, stream expiry, wire error, flush
        # cancel). What the bubble already shows must not go out again inside the regular send's
        # full text: only the part past the drawn snapshot is queued as this reply's continuation,
        # and the reply is CLAIMED so the gateway's regular send — which would repeat the drawn
        # part — stands cancelled (production hm-5958d932-1f3: 1190 chars on screen, then
        # "Sending response (1190 chars)" again 14s later).
        if not self._delivery_claimed and self._durable_text and not self._displayed_reply:
            shown = self._durable_text
            if _norm_for_match(body) == _norm_for_match(shown):
                # Every character the hand-over carries is already in the bubble — queue nothing.
                # The gateway's own suppression (has_durably_delivered_text) cancels the send.
                logger.info("[wecom-stream] finalize_decision=bubble_shows_all_no_overflow body_len=%d — nothing to queue, regular send suppressed by the gateway", len(body))
                return []
            if body.startswith(shown):
                fresh = body[len(shown):].lstrip("\n")
                if not fresh.strip():
                    logger.info("[wecom-stream] finalize_decision=bubble_shows_all_no_overflow body_len=%d — nothing to queue, regular send suppressed by the gateway", len(body))
                    return []
                parts = self._frame_overflow_reply(fresh, head="")
                self._delivery_claimed = True  # the send of the drawn part would be a duplicate
                self._overflow_queued_for = body
                logger.info("[wecom-stream] finalize_decision=remainder_as_overflow drawn_len=%d body_len=%d bubbles=%d — regular send cancelled", len(shown), len(body), len(parts))
                return parts
            # The hand-over is not what the bubble drew (transformed reply): fall through — the
            # legacy branches below deliver it as the regular send's own copy plus a tail.
        if body == self._displayed_reply:
            return []
        # fork 2026-11-19 (DPMS 561213): the final frame landed and drew the body; the hand-over
        # adds a tail the bubble never showed (a plugin footer appended after the last frame).
        # The gateway sends ONLY that tail — its delta — so queueing it here would deliver it
        # twice (the bubble's copy plus the gateway's). Nothing is queued and the reply stays
        # unclaimed: the body is on screen, the tail is the gateway's to send.
        if self._displayed_reply and body.startswith(self._displayed_reply):
            fresh = body[len(self._displayed_reply):].lstrip("\n")
            if not fresh.strip():
                return []
            logger.info(
                "[wecom-stream] finalize_decision=delta_tail_left_to_the_gateway drawn_len=%d body_len=%d "
                "tail_len=%d — the gateway's delta send carries it",
                len(self._displayed_reply), len(body), len(fresh),
            )
            return []
        transformed = bool(body) and body != self.overflow_text
        if transformed:
            self._displayed_reply = ""  # not what is on screen: this delivery is its only copy
        self.overflow_body = "" if transformed else self.overflow_text
        self.overflow_tail = _segments_and_images(body) if (transformed or not self.overflow_body) else ""
        if not (transformed or self.overflow_body or self.overflow_tail):
            return []
        self._delivery_claimed = not transformed
        return self._frame_overflow_reply()

    @property
    def delivery_claimed(self) -> bool:
        """fork 2026-11-16 (DPMS 561213): the full reply is the overflow delivery's — no more
        regular send of the body (see attach_overflow)."""
        return self._delivery_claimed

    @property
    def overflow_pending(self) -> bool:
        return bool(self.overflow_parts)

    def _claim_delivery(self) -> None:
        """The reply bubble was sealed and the reply is the overflow bubbles' to deliver — sealed
        on its head (only the head is drawn in the bubble) or without any text at all. Held ONLY
        here and on attach_overflow's twin claim (its overflow_body is the whole reply); a final
        frame that drew the whole reply never claims, so the gateway's send is not cancelled.
        Nothing is framed here: attach_overflow frames the payload the gateway hands over."""
        self._delivery_claimed = True

    def delivered_final_matches(self, final_text: str) -> Optional[bool]:
        """fork 2026-11-16 (DPMS 561213): does the overflow delivery carry exactly *final_text*?

        The gateway's stale-finalize reconciliation asks this before a regular send (upstream
        #71643). ``None`` = no claim: without an overflow, WeCom frames are cumulative displays,
        never content-identical to the bare reply."""
        if not self._delivery_claimed:
            return None
        shown = f"{self.overflow_body}\n\n{self.overflow_tail}" if self.overflow_tail else self.overflow_body
        return _norm_for_match(shown) == _norm_for_match(final_text)

    def has_durably_delivered_text(self, final_text: str) -> bool:
        """fork 2026-11-18 (DPMS 561213): the gateway stream-consumer contract — is *final_text*
        already on screen whole? WeCom frames are cumulative, so the last landed frame drew every
        answer character up to its snapshot; equality (or the final being a subset of it) answers
        "delivered". A partial draw answers False: the remainder is attach_overflow's to deliver
        (queue the continuation + claim), and the regular send stays the safety net should that
        hand-over fail — a prefix must never suppress and then lose the tail.

        The upstream GatewayStreamConsumer got this hook for the failed-finalize family
        (#51828 / #33793); without it every finalize decline/expiry/wire error re-sent what the
        bubble had progressively drawn (production hm-5958d932-1f3: 1190 chars, twice)."""
        wanted = _norm_for_match(final_text or "")
        shown = _norm_for_match(self._durable_text)
        return bool(wanted) and bool(shown) and (shown == wanted or wanted in shown)

    @property
    def durable_answer_text(self) -> str:
        """fork 2026-11-19 (DPMS 561213): the ANSWER text this bubble proved it drew — the
        gateway's "what is already on screen" record for a platform that cannot edit a sent
        message (WeCom). A reply rewritten after the stream (a plugin footer appended to it) must
        send only what the bubble never showed; this is what that difference is measured
        against. The frame's own decoration (the think block, the session link) is NOT part of it
        — it would make every reply look like a different one."""
        return self._durable_text

    def detach_overflow(self) -> None:
        """fork 2026-11-16 (DPMS 561213): the regular send takes over — the overflow delivery gives up.

        The caller's send carries the whole payload (the tail the short bubble cannot hold
        included), so the claim drops and the queue clears: a finished reply leaves nothing behind."""
        self.overflow_parts.clear()
        self.overflow_body = self.overflow_tail = ""
        self._delivery_claimed = False

    @property
    def shows_running_indicator(self) -> bool:
        """True while this delivery renders its own running indicator in the bubble.

        The gateway's long-running heartbeat (``_run_agent_notify_long_running``)
        checks this before posting a "⏳ Working" message: WeCom cannot edit
        messages, so on this platform every heartbeat arrives as a NEW message
        duplicating the bubble's own "⏳ 正在运行中…" indicator — one extra
        message per interval on long turns. Once the delivery gave up
        (``_disabled``) the indicator is gone and the heartbeat resumes.
        """
        return not self._disabled

    @property
    def message_id(self) -> Optional[str]:
        """Upstream consumer contract (``run_turn.py`` reads it bare on the
        transformed/stale-finalize reconciliation paths): the id of the streamed
        bubble an in-place edit would target. WeCom cannot edit sent messages,
        so this is always ``None`` — the edit paths short-circuit and the
        complete response goes out via the normal final send."""
        return None

    # ------------------------------------------------------------------
    # Sync input API — called from the agent worker thread
    # ------------------------------------------------------------------
    def _ping(self) -> None:
        """Wake the run loop (thread-safe)."""
        loop = self._loop
        wakeup = self._wakeup
        if loop is None or wakeup is None:
            return
        try:
            loop.call_soon_threadsafe(wakeup.set)
        except RuntimeError:
            # Loop closed between checks — delivery is tearing down.
            pass

    def on_delta(self, text: str) -> None:
        """Accumulate an answer-text delta."""
        if self.finished or not text:
            return
        if (
            self.after_tool_use
            and self.accumulated_text
            and not self.accumulated_text.endswith("\n\n")
        ):
            self.accumulated_text += "\n\n"
        self.after_tool_use = False
        self.accumulated_text += text
        self._queue.put(_DELTA)
        self._ping()

    def on_commentary(self, text: str) -> None:
        """Accumulate an interim thinking/commentary snippet."""
        if self.finished or not text:
            return
        self.thinking_buf += text
        self._queue.put(_COMMENTARY)
        self._ping()

    def on_tool_progress(self, line: str) -> None:
        """Record a gateway-formatted tool-progress line (e.g. ``🔍 Searching …``).

        The gateway already dedups consecutive identical lines (folding repeats
        into ``(×N)``), so each call is a fresh event; skip only an exact tail
        duplicate to stay robust against interleaved callers.
        """
        if self.finished or self._disabled or not line:
            return
        self.after_tool_use = True
        if not self.thinking_lines or self.thinking_lines[-1] != line:
            self.thinking_lines.append(line)
        self._queue.put(_TOOL)
        self._ping()

    def on_tool_start(self, tool_name: str) -> None:
        """Record a tool invocation as a ``🔧 **{tool}**`` thinking line."""
        if self.finished or not tool_name:
            return
        self.after_tool_use = True
        if tool_name not in self.tool_names_seen:
            self.tool_names_seen.add(tool_name)
            self.thinking_lines.append(f"🔧 **{tool_name}**")
            self._queue.put(_TOOL)
            self._ping()

    def on_segment_break(self) -> None:
        """Tool/segment boundary — no-op for WeCom's single-bubble model.

        Tool markers arrive via :meth:`on_tool_progress`; we keep accumulating in
        the same bubble rather than starting a new one.
        """

    def on_error(self, exc: BaseException) -> None:
        """Surface a friendly error message as the reply, then finish."""
        if self.finished:
            return
        self._error_mode = True
        self.accumulated_text = _friendly_error(exc)
        self.finish()

    def finish(self, final_text: str = "") -> None:
        """Signal that the stream is complete; the run loop sends the final frame.

        ``final_text``, when non-empty, replaces the accumulated answer text —
        the gateway passes the authoritative final response here in case late
        deltas were dropped.
        """
        if final_text and final_text.strip() and final_text != "(empty)":
            self.accumulated_text = final_text
        if self.finished:
            return
        self.finished = True
        self._queue.put(_DONE)
        self._ping()

    # ------------------------------------------------------------------
    # Async run loop — drains the queue and pushes stream frames
    # ------------------------------------------------------------------
    async def run(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._wakeup = asyncio.Event()
        self.start_time = time.monotonic()

        # fork 2026-11-16 (DPMS 561213): one order per reply — every frame takes the reply lane for
        # its own send (a send arriving meanwhile queues behind it). The run loop does NOT hold the
        # lane for its lifetime: its holder's sends landing inside that window would wait out the
        # lane's bound before every reply (see plugins/platforms/wecom/reply_lane.py).
        await self._run_frames()
        # The reply is over: whatever a send of this reply did not take goes out now, before the
        # consumer task ends and the gateway's stream flush takes over.
        if not self._overflow_owned:
            await self.flush_pending_overflow()

    async def _run_frames(self) -> None:
        # Initial frame: open <think> showing "正在思考".
        await self._push_intermediate(self._display())

        while True:
            # Always yield between iterations: wake early on new input, or on a
            # throttle tick. Without this await the loop would busy-spin while
            # idle (display unchanged), starving the agent thread's callbacks.
            self._wakeup.clear()
            try:
                await asyncio.wait_for(
                    self._wakeup.wait(), timeout=STREAM_THROTTLE_INTERVAL
                )
            except TimeoutError:
                pass
            except asyncio.CancelledError:
                return

            if self._disabled:
                return

            if self._drain_done():
                await self._finalize()
                return

            # Throttle elapsed — push the latest display if it changed.
            if time.monotonic() - self.last_push_time >= STREAM_THROTTLE_INTERVAL:
                # fork 2026-11-18 (DPMS 561213): snapshot the answer the display is built from; a
                # landed frame makes exactly that snapshot durable (see _push_intermediate).
                answer = self.accumulated_text
                display = self._display(answer=answer)
                if display != self.last_pushed_display:
                    await self._push_intermediate(display, answer)

    def _drain_done(self) -> bool:
        """Drain all queued signals; return True if a finish signal was seen."""
        saw_done = False
        while True:
            try:
                item = self._queue.get_nowait()
            except queue.Empty:
                break
            if item is _DONE:
                saw_done = True
        return saw_done

    async def _finalize(self) -> None:
        if self._disabled:
            return
        if not self.accumulated_text.strip():
            self.accumulated_text = _EMPTY_REPLY_FALLBACK
        # fork 2026-11-16 (DPMS 561213): the overflow bubbles carry what the regular send would
        # (the gateway's final response, segments and footer included), so a long reply stays ONE
        # channel with nothing sent twice.
        # fork 2026-11-18 (DPMS 561213): snapshot BEFORE building the display — every "what did
        # the final frame draw" record (overflow anchor, displayed reply, durable text) describes
        # the snapshot, never the accumulated text deltas that land while the frame is on the wire.
        answer = self.accumulated_text
        self._overflow_full = self.overflow_text = answer
        if not self._error_mode:
            self.thinking_lines.append("✨ 回复完成")
        display = self._display(finished=True, fold_think=True, answer=answer)
        # fork 2026-09-30 (audit 2026-09-29 module-2 H4): if even the folded display exceeds the
        # frame budget the adapter byte-truncates it (keep-head) and marking the turn
        # delivered would silently lose the tail.
        # fork 2026-11-16 (DPMS 561213): it is no longer declined outright — declining left the
        # half-drawn bubble on screen and the gateway regular-sent the full reply as a SECOND
        # message ("one reply, two messages", production hm-5958d932-1f3). The full reply now
        # goes out as sealed overflow bubbles (bubble N is closed before N+1 opens), and only if
        # that cannot start does the gateway's regular send take over (final_response_sent unset).
        size = len(display.encode("utf-8"))
        if size > MAX_STREAM_CONTENT_LENGTH:
            logger.info(
                "[wecom-stream] final display %d bytes exceeds frame budget %d — closing the bubble and delivering the full reply as overflow bubbles",
                size, MAX_STREAM_CONTENT_LENGTH,
            )
            # The seal closes the bubble NOW (the gateway withholds the regular send for a claimed
            # reply, so nothing else would finish a stream left open); it claims the reply too.
            sealed = await self._close_bubble()
            if sealed is None:
                logger.info("[wecom-stream] finalize_decision=seal_failed_unclaimed chars=%d — no seal frame landed; the gateway's regular send remains the fallback", size)
            # fork 2026-11-18 (DPMS 561213): the sealed bubbles carry the whole reply — file it,
            # so a re-delivery of the same inbound replays it instead of running the agent again.
            self._record_delivered_reply()
            return
        # Set the flag ONLY when the finalize frame actually went through. The
        # adapter's Layer 2 clock fallback (stream age >= 330s, keep-alive off
        # by default) declines finish=True and returns False precisely so the
        # gateway's normal send() delivers the complete reply once — an
        # unconditional True here suppressed that fallback and silently dropped
        # the tail of every long turn (expired-at-finalize turns lost it all).
        if await self._send_frame(display, finish=True):
            self._final_response_sent = True
            # fork 2026-11-16 (DPMS 561213): the bubble shows the reply. The segment / MEDIA: tail
            # it cannot render goes out as one follow-up bubble, queued by the gateway's hand-over
            # (attach_overflow) and sealed by the reply's close-out — or by a send of this reply,
            # which carries it first. Only the tail may be queued: the reply it drew is on screen.
            self._displayed_reply = answer
            # fork 2026-11-18 (DPMS 561213): the bubble carries the reply — remember it, so a
            # re-delivery of THIS inbound replays the same text instead of running the agent again.
            self._durable_text = answer
            self._record_delivered_reply()
            logger.info("[wecom-stream] finalize_decision=final_frame_delivered chars=%d", len(display))
        else:
            # fork 2026-11-18 (DPMS 561213): the finish frame did not land (decline / expiry /
            # wire error) but the progressive frames may already have drawn the whole answer —
            # the gateway decides from what is durably on screen (has_durably_delivered_text),
            # never blindly re-sends.
            logger.info("[wecom-stream] finalize_decision=final_frame_not_delivered chars=%d durable_chars=%d", len(display), len(self._durable_text))

    def _record_delivered_reply(self) -> None:
        """fork 2026-11-18 (DPMS 561213): tell the adapter what this bubble delivered.

        The adapter files it under the content key of the inbound this reply answers, so a
        re-delivery of that inbound replays this text instead of running the agent again. The
        adapter resolves the key from the stream turn the reply was drawn in.
        """
        recorder = getattr(self.adapter, "_record_delivered_reply", None)
        if callable(recorder):
            recorder(self.chat_id, self.accumulated_text)

    async def flush_pending_overflow(self) -> None:
        """Seal the overflow bubbles this reply still has queued — the reply's ONE close-out door.

        Called when a send of the reply would carry them (adapter._send_reply_part), when the
        reply's frames are over (run), and by the gateway once the reply is declared. The first
        caller takes the queue; the others find it empty. A bubble the stream refuses — and every
        bubble after it — goes out through the adapter's regular send (segmented, numbered) and
        is counted as sealed: nothing queued is dropped silently."""
        async with self._seal_lock:
            if not self.overflow_parts:
                return
            pending = list(self.overflow_parts)
            self.overflow_parts.clear()
            # A send of this reply goes out right after the bubbles it carried: the queued ones
            # that follow a bubble the send is about to regular-send must not be sealed AFTER it.
            self._overflow_owned = True
            await self._seal_bubbles(pending)

    async def harvest_overflow(self) -> list[bytes]:
        """Take the bubbles a send queued BEFORE it — it sends them ahead of itself
        (adapter._send_reply_part), so they are never left for the reply's close-out flush."""
        async with self._seal_lock:
            pending = list(self.overflow_parts)
            self.overflow_parts.clear()
            return pending

    async def _seal_bubbles(self, pending: list[bytes]) -> None:
        chat = str(self.chat_id or "").strip()
        adapter = self.adapter
        fallback = getattr(adapter, "send_overflow_bubbles", None)
        if not chat or not pending:
            return
        if callable(fallback):
            await fallback(chat, pending)  # the adapter's send path: bubbles, then segmented sends
            return
        # An adapter with no seal of its own: a bubble the stream cannot take goes through its
        # regular send, together with the bubbles after it (order kept, numbering added here).
        streamable = callable(getattr(adapter, "extend_stream_bubble", None))
        for offset, part in enumerate(pending):
            text = part.decode("utf-8") if isinstance(part, (bytes, bytearray)) else str(part)
            _, text = _split_overflow_marker(text)
            if streamable:
                async with reply_lane(chat):  # one lane entry per bubble: never overtakes a frame
                    if await adapter.extend_stream_bubble(chat, text):
                        continue
            logger.warning("[wecom-stream] overflow bubble %d/%d refused for chat %s — sending it and the rest as messages",
                           offset + 1, len(pending), chat)
            rest = pending[offset:]
            for left, remaining in enumerate(rest):
                body = remaining.decode("utf-8") if isinstance(remaining, (bytes, bytearray)) else str(remaining)
                _, body = _split_overflow_marker(body)
                marker = overflow_indicator(left + 1, len(rest)) if len(rest) > 1 else ""
                try:
                    # fork 2026-11-18 (DPMS 561213): is_reply_overflow — a bubble of a reply
                    # already on file; never recorded, or the fragment would overwrite the full
                    # reply the re-delivery replay waits for.
                    await adapter.send(chat, marker + body, metadata={"force_proactive_send": True, "is_reply_overflow": True})
                except Exception as exc:
                    logger.warning("[wecom-stream] overflow fallback send failed for chat %s: %s", chat, exc)
            return

    def _head_budget(self) -> int:
        """Bytes of the reply a sealed bubble may show: the frame's own budget less the room for one
        continuation bubble of its own (counted in bytes: the answer is CJK-heavy in practice)."""
        return max(0, MAX_STREAM_CONTENT_LENGTH - _OVERFLOW_MIN_CONTINUATION)

    def _sealed_head(self) -> str:
        """What the sealed bubble shows: the reply's first piece at the head budget, which is also
        the continuation's cut (`_continuation_after`) — the bubble frame sends it whole, and no
        line is drawn twice."""
        pieces = _split_bytes(self.accumulated_text, self._head_budget()) if self._head_budget() else []
        return pieces[0] if len(pieces) > 1 else ""

    def _continuation_after(self) -> str:
        """The reply after the sealed head: from the same piece boundary (no line drawn twice)."""
        pieces = _split_bytes(self.accumulated_text, self._head_budget()) if self._head_budget() else []
        return "".join(pieces[1:]) if len(pieces) > 1 else ""

    def _set_sealed_source(self, text: str) -> None:
        """What the overflow bubbles continue: *text* with the sealed head cut off — the text the
        hand-over frames (so it must be framed from here, not from the full reply again)."""
        self.overflow_text = text

    async def _close_bubble(self) -> Optional[bool]:
        """fork 2026-11-16 (DPMS 561213): seal the half-drawn bubble with the answer head.

        WeCom cannot edit a sent frame, but a finish frame replaces the bubble's content — so
        the bubble ends on the first part of the answer instead of the mid-turn running
        indicator, and the overflow bubbles continue where it stops. A plain seal (no display)
        is the second choice; ``None`` when neither landed (the gateway's regular send then
        carries the whole reply)."""
        head = self._sealed_head()
        if head:
            # The head alone, as one FINAL frame: the display's <think> block and session link
            # cannot fit next to it, or the answer would show in the bubble twice (head + start).
            # The content is cut to the frame it is sent in: only a frame the wire holds whole lets
            # the continuation start where the bubble stops (a frame cut after the head loses lines).
            if await self._send_frame(head[:MAX_STREAM_CONTENT_LENGTH], finish=True):
                self._overflow_head = head  # the answer text the bubble ends on
                self._overflow_source = self.accumulated_text  # what the continuation owes, from the top
                # What follows the head, cut at the head's own length — not by re-splitting at the
                # display budget, whose line-break preference may cut a line earlier (drawn twice).
                self._set_sealed_source(self.accumulated_text[len(head):])
                self._claim_delivery()
                # fork 2026-11-18 (DPMS 561213): the seal frame LANDED with the head — exactly the
                # head is on screen (the continuation owes the rest).
                self._durable_text = head
                return True
        if await self._send_frame(_OVERFLOW_SEAL, finish=True):
            logger.info("[wecom-stream] bubble sealed without content; the full reply follows in overflow bubbles")
            self._overflow_head = ""
            self._claim_delivery()
            return True
        return None

    def _frame_overflow_reply(self, text: Optional[str] = None, *, head: Optional[str] = None) -> list[bytes]:
        """fork 2026-11-16 (DPMS 561213): queue the reply as bubbles that each fit one frame.

        When the sealed bubble already shows the reply head, the tail continues in one bubble
        that reads as part of its bubble (no numbering); only a multi-bubble continuation is
        numbered ``（n/m）`` so the reader can follow it across messages. Defaults: what the
        overflow owed (body + tail) minus what the sealed bubble showed."""
        if text is None:
            text = self.overflow_body
            if self.overflow_tail:
                text = f"{text}\n\n{self.overflow_tail}" if text else self.overflow_tail
        budget = MAX_STREAM_CONTENT_LENGTH
        if head is None:
            head = self._sealed_head()
        if head and text.startswith(head):
            text = text[len(head):].lstrip("\n")  # the sealed bubble already shows this
        self._overflow_shown = f"{head}{text}" if head else ""  # the bubble's head + this hand-over
        body_parts = [p for p in _split_bytes(text, budget) if p.strip()]
        if len(body_parts) > 1:
            width = min(budget - 1, max(32, budget // 4))
            while True:
                # (n/m) prefixes eat the budget: reframe within a narrower one until every piece
                # plus its own marker fits (the width shrinks every round, at most `budget` rounds).
                if all(len(p.encode("utf-8")) + 24 <= budget for p in body_parts):
                    break
                body_parts = [p for p in _split_bytes(text, width) if p.strip()] or [text[:width]]
                width = max(1, width - max(8, width // 8))
        if len(body_parts) > 1:  # a continuation the reader follows across messages: numbered
            total = len(body_parts)
            markers = [overflow_indicator(i, total) for i in range(1, total + 1)]
        else:  # none, or one bubble that fits the frame: a plain continuation
            markers = [""] * len(body_parts)
        framed = [(marker + part).encode("utf-8") for part, marker in zip(body_parts, markers)]
        self.overflow_parts.extend(framed)
        return framed

    async def _push_intermediate(self, display: str, answer: Optional[str] = None) -> None:
        """Send an intermediate frame (open/updated bubble) with the running indicator."""
        if self._disabled:
            return
        text = display + _build_running_indicator(self.start_time)
        if await self._send_frame(text, finish=False):
            # Only record on success so a failed frame is retried on the next
            # tick (the adapter's next cumulative frame overwrites it anyway).
            self.last_pushed_display = display
            # fork 2026-11-18 (DPMS 561213): the frame LANDED — the answer it drew is on screen
            # whole (frames are cumulative), which is what the gateway's final-send suppression
            # reads (has_durably_delivered_text). Snapshot, never live accumulated text: deltas
            # arriving while this frame was on the wire are the NEXT frame's durability.
            # send_stream_frame returns True for a dropped intermediate as well (a wire error
            # keeps the stream alive), so the record is confirmed against the turn's actual
            # last-landed content: a dropped frame leaves durability at the last landed frame
            # and the finalize hand-over covers the difference — it must never overcount.
            landed = getattr(self.adapter, "last_landed_stream_frame_content", None)
            if landed is not None and landed(self.chat_id) != text:
                logger.info(
                    "[wecom-stream] finalize_decision=frame_dropped_not_durable chat=%s drawn_chars=%d — the send was dropped by the adapter, durability stays at the last landed frame",
                    self.chat_id,
                    len(answer if answer is not None else self.accumulated_text),
                )
            else:
                self._durable_text = self.accumulated_text if answer is None else answer
        self.last_push_time = time.monotonic()

    async def _send_frame(self, text: str, finish: bool) -> bool:
        """Send one frame; True when the adapter delivered it (or assumed
        delivery — the adapter treats a final-frame ack timeout as delivered)."""
        try:
            async with reply_lane(self.chat_id):
                ok = await self.adapter.send_stream_frame(
                    text,
                    finalize=finish,
                    chat_id=self.chat_id,
                )
            if ok is False:
                # send_stream_frame reports "stream unavailable" as False
                # (no req_id / expired / transport down) — count it like an
                # exception so the circuit breaker below can trip.
                raise RuntimeError("send_stream_frame returned False")
            self._consecutive_failures = 0
            self._already_sent = True
            return True
        except Exception as exc:  # never let a frame failure kill the stream
            self._consecutive_failures += 1
            logger.debug("[wecom-stream] frame send failed: %s", exc)
            if (
                not self._already_sent
                and self._consecutive_failures >= MAX_CONSECUTIVE_FRAME_FAILURES
            ):
                # The stream never opened (e.g. no cached req_id for this chat).
                # Stop pushing — the turn's final reply is delivered by the
                # gateway's normal send path.
                self._disabled = True
                logger.info(
                    "[wecom-stream] giving up after %d failed frames; "
                    "falling back to plain send",
                    self._consecutive_failures,
                )
            return False

    # ------------------------------------------------------------------
    # Display helper
    # ------------------------------------------------------------------
    def _display(self, finished: bool = False, fold_think: bool = False, answer: Optional[str] = None) -> str:
        # fork 2026-11-18 (DPMS 561213): *answer* pins the reply text the display is built from —
        # the caller snapshots it before building so the frame it sends and the durability record
        # describe the same characters (deltas landing mid-send belong to the NEXT frame).
        return _build_display_content(
            self.thinking_lines,
            self.thinking_buf,
            self.chat_record_url,
            self.accumulated_text if answer is None else answer,
            finished=finished,
            fold_think=fold_think,
        )
