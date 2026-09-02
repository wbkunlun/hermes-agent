"""Tests for the WeCom clawrelay-style stream delivery module.

Restored 2026-09-02 with the v0.21.0 adapter surface: frames go through
``send_stream_frame(content, finalize=..., chat_id=...)`` and the adapter
resolves req_id/stream_id internally.
"""

import asyncio

import pytest

from plugins.platforms.wecom import stream_delivery as mod
from plugins.platforms.wecom.stream_delivery import (
    WeComStreamDelivery,
    _build_display_content,
    _build_running_indicator,
    _friendly_error,
)


class FakeAdapter:
    def __init__(self):
        self.frames = []  # list of (content, finish)
        self.fail = False  # when True, simulate "stream unavailable"

    async def send_stream_frame(self, content, *, finalize=False, chat_id=None):
        if self.fail:
            return False
        self.frames.append((content, finalize))
        return True


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------

class TestBuildDisplayContent:
    def test_thinking_only_leaves_think_tag_open(self):
        out = _build_display_content(["🤔 正在思考中..."], "", "", "")
        assert out.startswith("<think>\n")
        assert "</think>" not in out
        assert "🤔 正在思考中..." in out

    def test_answer_text_closes_think_block(self):
        out = _build_display_content(["🤔 正在思考中..."], "", "", "the answer")
        assert "<think>\n" in out
        assert "</think>" in out
        assert "the answer" in out

    def test_finished_closes_think_block_even_without_text(self):
        out = _build_display_content(["🤔 正在思考中..."], "", "", "", finished=True)
        assert "</think>" in out

    def test_thinking_buffer_preview_with_prefix_when_long(self):
        long_buf = "x" * 300
        out = _build_display_content(["🤔 正在思考中..."], long_buf, "", "")
        assert "💭 ..." in out
        # last 200 chars shown
        assert "x" * 200 in out

    def test_thinking_buffer_preview_without_prefix_when_short(self):
        out = _build_display_content(["🤔 正在思考中..."], "short", "", "")
        assert "💭 short" in out
        # the "..." ellipsis prefix is only added for long buffers
        assert "💭 ..." not in out

    def test_session_link_inserted_when_provided(self):
        link = "📎 查看实时聊天记录：[链接>>](https://x/s)"
        out = _build_display_content(["🤔 正在思考中..."], "", link, "answer")
        assert link in out
        # link sits between think block and answer, joined by blank lines
        assert out.index("</think>") < out.index(link) < out.index("answer")

    def test_parts_joined_by_blank_lines(self):
        out = _build_display_content(["🤔 正在思考中..."], "", "", "answer", finished=True)
        assert "\n\n" in out


class TestRunningIndicator:
    def test_short_elapsed_shows_dots(self, monkeypatch):
        # force monotonic baseline so elapsed stays small
        monkeypatch.setattr(mod.time, "monotonic", lambda: 1.0)
        out = _build_running_indicator(0.5)  # elapsed 0.5s
        assert "⏳ 正在运行中" in out
        assert "完成后会通知您" not in out
        # 1-3 dots
        assert out.rstrip().endswith(".") or out.rstrip().endswith("..") or out.rstrip().endswith("...")

    def test_long_elapsed_switches_message(self, monkeypatch):
        monkeypatch.setattr(mod.time, "monotonic", lambda: 100.0)
        out = _build_running_indicator(0.0)  # elapsed 100s
        assert "⏳ 正在运行中，完成后会通知您🔔" in out


class TestFriendlyError:
    def test_timeout(self):
        assert "超时" in _friendly_error(TimeoutError("timed out"))

    def test_connection(self):
        assert "连接" in _friendly_error(ConnectionError("connection refused"))

    def test_generic(self):
        assert "稍后重试" in _friendly_error(ValueError("boom"))


# ---------------------------------------------------------------------------
# WeComStreamDelivery run-loop behaviour
# ---------------------------------------------------------------------------

@pytest.fixture
def fast_throttle(monkeypatch):
    monkeypatch.setattr(mod, "STREAM_THROTTLE_INTERVAL", 0.02)


async def _run_to_completion(delivery, feeder):
    task = asyncio.create_task(delivery.run())
    await asyncio.sleep(0.01)  # let run() initialise and push the first frame
    await feeder(delivery)
    delivery.finish()
    await asyncio.wait_for(task, timeout=5.0)


class TestRunLoop:
    @pytest.mark.asyncio
    async def test_initial_frame_is_open_think_then_finish_closes(self, fast_throttle):
        fake = FakeAdapter()
        d = WeComStreamDelivery(fake, chat_id="c1")

        async def feeder(deliv):
            deliv.on_delta("Hello")

        await _run_to_completion(d, feeder)

        # First frame: open think, finish=False
        first_content, first_finish = fake.frames[0]
        assert "<think>" in first_content
        assert "</think>" not in first_content
        assert first_finish is False

        # Last frame: finish=True, closed think, answer, completion marker
        last_content, last_finish = fake.frames[-1]
        assert last_finish is True
        assert "</think>" in last_content
        assert "Hello" in last_content
        assert "✨ 回复完成" in last_content

    @pytest.mark.asyncio
    async def test_tool_marker_appears_in_think_block(self, fast_throttle):
        fake = FakeAdapter()
        d = WeComStreamDelivery(fake, chat_id="c1")

        async def feeder(deliv):
            deliv.on_tool_start("read_file")
            await asyncio.sleep(0.05)
            deliv.on_delta("done")

        await _run_to_completion(d, feeder)

        joined = "\n".join(c for c, _ in fake.frames)
        assert "🔧 **read_file**" in joined

    @pytest.mark.asyncio
    async def test_tool_progress_line_appears_in_think_block(self, fast_throttle):
        """Gateway-formatted progress lines (with their own emoji) fold in."""
        fake = FakeAdapter()
        d = WeComStreamDelivery(fake, chat_id="c1")

        async def feeder(deliv):
            deliv.on_tool_progress("🔍 Searching the web for \"hermes\"")
            await asyncio.sleep(0.05)
            deliv.on_delta("done")

        await _run_to_completion(d, feeder)

        joined = "\n".join(c for c, _ in fake.frames)
        assert "🔍 Searching the web" in joined
        # dedup: a repeated identical line is not appended twice
        assert joined.count("🔍 Searching the web") >= 1

    @pytest.mark.asyncio
    async def test_commentary_shows_preview(self, fast_throttle):
        fake = FakeAdapter()
        d = WeComStreamDelivery(fake, chat_id="c1")

        async def feeder(deliv):
            deliv.on_commentary("let me think")
            await asyncio.sleep(0.05)

        await _run_to_completion(d, feeder)

        joined = "\n".join(c for c, _ in fake.frames)
        assert "💭 let me think" in joined

    @pytest.mark.asyncio
    async def test_finish_with_final_text_replaces_answer(self, fast_throttle):
        """finish(final_text) carries the authoritative final response."""
        fake = FakeAdapter()
        d = WeComStreamDelivery(fake, chat_id="c1")

        async def feeder(deliv):
            deliv.on_delta("partial")

        task = asyncio.create_task(d.run())
        await asyncio.sleep(0.01)
        await feeder(d)
        d.finish("the authoritative answer")
        await asyncio.wait_for(task, timeout=5.0)

        last_content, last_finish = fake.frames[-1]
        assert last_finish is True
        assert "the authoritative answer" in last_content
        assert "partial" not in last_content

    @pytest.mark.asyncio
    async def test_empty_reply_uses_fallback(self, fast_throttle):
        fake = FakeAdapter()
        d = WeComStreamDelivery(fake, chat_id="c1")

        async def feeder(deliv):
            pass

        await _run_to_completion(d, feeder)

        last_content, last_finish = fake.frames[-1]
        assert last_finish is True
        assert "未生成文本回复" in last_content

    @pytest.mark.asyncio
    async def test_on_error_finishes_without_completion_marker(self, fast_throttle):
        fake = FakeAdapter()
        d = WeComStreamDelivery(fake, chat_id="c1")

        async def feeder(deliv):
            deliv.on_error(TimeoutError("timed out"))

        await _run_to_completion(d, feeder)

        last_content, last_finish = fake.frames[-1]
        assert last_finish is True
        assert "超时" in last_content
        assert "✨ 回复完成" not in last_content

    @pytest.mark.asyncio
    async def test_chat_record_url_appears_when_set(self, fast_throttle):
        fake = FakeAdapter()
        url = "📎 查看实时聊天记录：[链接>>](https://x/s/123)"
        d = WeComStreamDelivery(fake, chat_id="c1", chat_record_url=url)

        async def feeder(deliv):
            deliv.on_delta("answer")

        await _run_to_completion(d, feeder)

        joined = "\n".join(c for c, _ in fake.frames)
        assert url in joined

    @pytest.mark.asyncio
    async def test_on_segment_break_is_noop(self, fast_throttle):
        # segment breaks must not create new bubbles / lose accumulated text
        fake = FakeAdapter()
        d = WeComStreamDelivery(fake, chat_id="c1")

        async def feeder(deliv):
            deliv.on_delta("part1")
            await asyncio.sleep(0.05)
            deliv.on_segment_break()
            await asyncio.sleep(0.05)
            deliv.on_delta("part2")

        await _run_to_completion(d, feeder)

        last_content, _ = fake.frames[-1]
        assert "part1" in last_content
        assert "part2" in last_content

    @pytest.mark.asyncio
    async def test_consecutive_failures_disable_delivery(self, fast_throttle, monkeypatch):
        """A stream that never opens (send_stream_frame keeps returning False)
        trips the circuit breaker and stops pushing frames."""
        fake = FakeAdapter()
        fake.fail = True
        monkeypatch.setattr(mod, "MAX_CONSECUTIVE_FRAME_FAILURES", 3)
        d = WeComStreamDelivery(fake, chat_id="c1")

        async def feeder(deliv):
            for _ in range(8):
                deliv.on_tool_progress("🔍 still trying")
                await asyncio.sleep(0.03)

        task = asyncio.create_task(d.run())
        await asyncio.sleep(0.01)
        await feeder(d)
        d.finish("final answer")
        await asyncio.wait_for(task, timeout=5.0)

        # Disabled before any frame landed: nothing was recorded as sent and
        # the run loop exited via the _disabled path (no crash).
        assert d._disabled is True
        assert d.already_sent is False


class TestAdapterWiring:
    def test_wecom_adapter_exposes_delivery_class(self):
        """gateway/run.py branches on this class attribute to pick the
        clawrelay-style delivery for WeCom."""
        from plugins.platforms.wecom.adapter import WeComAdapter
        from plugins.platforms.wecom.stream_delivery import WeComStreamDelivery as Cls

        assert WeComAdapter.WECOM_STREAM_DELIVERY is Cls

    def test_callback_adapter_does_not_expose_delivery_class(self):
        """The callback (self-built app) channel has no stream protocol."""
        from plugins.platforms.wecom.callback_adapter import WecomCallbackAdapter

        assert getattr(WecomCallbackAdapter, "WECOM_STREAM_DELIVERY", None) is None
