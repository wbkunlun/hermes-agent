"""Regression (audit 2026-09-29 module-2 H4): thinking_lines grew unbounded and the
think block sat FIRST in every cumulative frame, so in tool-heavy turns the ANSWER
text was pushed past the 20480-byte frame cap and byte-truncated (keep-head). The
finalize frame then succeeded and set _final_response_sent, which suppresses the
gateway's regular send — the tail was silently lost forever."""
import asyncio
import logging

from plugins.platforms.wecom.stream_delivery import (
    MAX_FINAL_THINK_LINES,
    WeComStreamDelivery,
)
from plugins.platforms.wecom.streaming import MAX_STREAM_CONTENT_LENGTH


class _FakeAdapter:
    def __init__(self):
        self.calls = []

    async def send_stream_frame(self, text, *, finalize=False, chat_id=None, **kw):
        self.calls.append((finalize, text))
        return True


def _delivery():
    d = WeComStreamDelivery(adapter=_FakeAdapter(), chat_id="c1")
    d._disabled = False
    d._error_mode = False
    return d


def test_finalize_folds_think_block():
    d = _delivery()
    d.thinking_lines = [f"🔧 step {i}" for i in range(500)]
    d.accumulated_text = "答" * 2000  # 6000 bytes — fits once think is folded
    asyncio.run(d._finalize())
    finalize_calls = [t for f, t in d.adapter.calls if f]
    assert len(finalize_calls) == 1
    text = finalize_calls[0]
    assert "已折叠" in text
    assert "step 499" in text and "step 0" not in text  # keeps the tail, drops the head
    assert len(text.encode("utf-8")) <= MAX_STREAM_CONTENT_LENGTH
    assert d._final_response_sent is True


def test_finalize_declines_when_answer_alone_exceeds_budget(caplog):
    d = _delivery()
    d.thinking_lines = ["🔧 step 1"]
    d.accumulated_text = "答" * 9000  # 27000 bytes > 20480 even with no think block
    with caplog.at_level(logging.INFO, logger="plugins.platforms.wecom.stream_delivery"):
        asyncio.run(d._finalize())
    assert not [1 for f, _ in d.adapter.calls if f], "finalize frame must NOT be sent"
    assert d._final_response_sent is False  # gateway regular send delivers the full reply instead
    assert "declining finalize" in caplog.text


def test_short_turn_unchanged():
    d = _delivery()
    d.thinking_lines = ["🤔 正在思考中..."]
    d.accumulated_text = "答案是 42"
    asyncio.run(d._finalize())
    (finalize, text), = [c for c in d.adapter.calls if c[0]]
    assert "🤔 正在思考中..." in text and "答案是 42" in text  # small turns keep full think block
    assert d._final_response_sent is True


def test_finalize_at_exact_budget_still_sends(monkeypatch):
    # 边界钉：decline 条件是严格大于——恰好等于预算的帧必须发送（对照 streaming 截断的 <= 放行）
    from plugins.platforms.wecom import stream_delivery as sd
    d = _delivery()
    d._error_mode = True  # _finalize 否则会在此测量之后追加 "✨ 回复完成" 行，display 将比 exact 大一行
    d.thinking_lines = ["🔧 step 1"]
    d.accumulated_text = "答" * 100
    exact = len(d._display(finished=True, fold_think=True).encode("utf-8"))
    monkeypatch.setattr(sd, "MAX_STREAM_CONTENT_LENGTH", exact)
    asyncio.run(d._finalize())
    assert [1 for f, _ in d.adapter.calls if f], "exact-budget frame must be sent"
    assert d._final_response_sent is True


def test_fold_threshold_constant_reasonable():
    assert 10 <= MAX_FINAL_THINK_LINES <= 100
