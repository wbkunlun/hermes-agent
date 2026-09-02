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
import time
from typing import Any, Optional

logger = logging.getLogger(__name__)

# Push at most one stream frame every STREAM_THROTTLE_INTERVAL seconds.
STREAM_THROTTLE_INTERVAL = 0.3
# After this many seconds the running indicator switches to a "we'll notify you"
# message, signalling that the task is long-running.
LONG_RUNNING_THRESHOLD = 60
# Give up streaming after this many consecutive failed frame sends (e.g. no
# cached req_id for the chat). The final reply then falls back to plain send().
MAX_CONSECUTIVE_FRAME_FAILURES = 10

# Queue signals (state is mutated synchronously by the input methods; these only
# wake the run loop).
_DELTA = object()
_COMMENTARY = object()
_TOOL = object()
_DONE = object()

_EMPTY_REPLY_FALLBACK = "AI 已完成处理，但未生成文本回复。请尝试换个方式描述您的需求。"


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
) -> str:
    """Build the combined display: ``<think>`` block + session link + answer text.

    While thinking (no answer text and not finished) the ``<think>`` tag is left
    *unclosed* so WeCom shows an animated "正在思考" indicator instead of a
    collapsed "已完成思考" block. Ported from clawrelay's orchestrator.
    """
    parts: list = []
    if thinking_lines or thinking_buf:
        lines = list(thinking_lines)
        if thinking_buf:
            preview = thinking_buf[-200:]
            prefix = "..." if len(thinking_buf) > 200 else ""
            lines.append(f"💭 {prefix}{preview}")
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
        self._queue: "queue.Queue" = queue.Queue()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._wakeup: Optional[asyncio.Event] = None

        # Throttle / push state — owned by run().
        self.start_time = 0.0
        self.last_pushed_display = ""
        self.last_push_time = 0.0
        self._consecutive_failures = 0
        self._already_sent = False
        self._final_response_sent = False

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
            except asyncio.TimeoutError:
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
                display = self._display()
                if display != self.last_pushed_display:
                    await self._push_intermediate(display)

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
        if not self._error_mode:
            self.thinking_lines.append("✨ 回复完成")
        display = self._display(finished=True)
        await self._send_frame(display, finish=True)
        self._final_response_sent = True

    async def _push_intermediate(self, display: str) -> None:
        """Send an intermediate frame (open/updated bubble) with the running indicator."""
        if self._disabled:
            return
        text = display + _build_running_indicator(self.start_time)
        await self._send_frame(text, finish=False)
        self.last_pushed_display = display
        self.last_push_time = time.monotonic()

    async def _send_frame(self, text: str, finish: bool) -> None:
        try:
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

    # ------------------------------------------------------------------
    # Display helper
    # ------------------------------------------------------------------
    def _display(self, finished: bool = False) -> str:
        return _build_display_content(
            self.thinking_lines,
            self.thinking_buf,
            self.chat_record_url,
            self.accumulated_text,
            finished=finished,
        )
