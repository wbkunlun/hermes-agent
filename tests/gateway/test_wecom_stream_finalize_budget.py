"""Regression (audit 2026-09-29 module-2 H4): thinking_lines grew unbounded and the
think block sat FIRST in every cumulative frame, so in tool-heavy turns the ANSWER
text was pushed past the 20480-byte frame cap and byte-truncated (keep-head). The
finalize frame then succeeded and set _final_response_sent, which suppresses the
gateway's regular send — the tail was silently lost forever."""
import asyncio
import logging
import re

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


def _strip_overflow_frame(bubble: str) -> str:
    """A numbered overflow bubble's text: its（i/N）frame and the splitter's " (i/N)" suffix removed
    (string work on purpose: a regex over the CJK parentheses reads badly)."""
    markers = list(re.findall("\uff08[0-9]+/[0-9]+\uff09", bubble)) + list(re.findall(r"\([0-9]+/[0-9]+\)", bubble))
    lines = bubble.split("\n")
    if lines and lines[0] in markers:
        lines = lines[1:]
    text = "\n".join(lines)
    tail = text.rsplit(" ", 1)
    if len(tail) == 2 and tail[1] in markers + [" (" + m for m in markers]:
        text = tail[0]
    return text


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
    # fork 2026-11-16 (DPMS 561213): the reply no longer declines outright (that left the
    # half-drawn bubble on screen next to a separate regular-sent full reply — production
    # hm-5958d932-1f3). The bubble is SEALED with the answer head inside the frame budget and the
    # full reply continues as overflow bubbles of the same stream; the gateway's regular send of
    # the body is cancelled by the claim.
    d = _delivery()
    d.thinking_lines = ["🔧 step 1"]
    d.accumulated_text = "答" * 9000  # 27000 bytes > 20480 even with no think block
    with caplog.at_level(logging.INFO, logger="plugins.platforms.wecom.stream_delivery"):
        asyncio.run(d._finalize())
    (_finish, seal), = [c for c in d.adapter.calls if c[0] and "答" in c[1]]  # the head seal, not the bare seal
    assert len(seal.encode("utf-8")) <= MAX_STREAM_CONTENT_LENGTH
    assert seal.startswith("答") and seal.endswith("答")  # the answer head alone, nothing after it
    assert "declining finalize" not in caplog.text
    assert d.delivery_claimed is True  # the full body goes out as bubbles; no regular send
    # fork 2026-11-16 close-out: the gateway now hands its payload over BEFORE the delivery
    # decision, so the bubbles are framed straight away (was: an unchanged attach framed nothing
    # and a later attach with the footer text re-framed the whole body a second time).
    # The gateway hands over the reply it is about to regular-send: the bubbles carry that payload.
    payload = "答" * 9000
    d.attach_overflow(payload + "\n\n（本次用时 3.2s）")
    assert d.attach_overflow(payload + "\n\n（本次用时 3.2s）") == []  # same hand-over again: nothing queued
    bubbles = [p.decode("utf-8") for p in d.overflow_parts]
    assert bubbles  # whatever the head did not show continues in follow-up bubbles
    assert all(len(b.encode("utf-8")) <= MAX_STREAM_CONTENT_LENGTH for b in bubbles)
    # fork 2026-11-16 close-out: reworked from "exactly one （n/m） per bubble" — the markers are
    # now asserted against the new framing contract（（i/N）on every numbered bubble, footer plain）.
    markers = [re.findall("（(\\d+)/(\\d+)）", b) for b in bubbles]
    numbered = [ms[0] for ms in markers if ms]  # (i, N) of each numbered continuation bubble
    # fork 2026-11-16 close-out: numbering is the framing's choice per run (an unnumbered bubble
    # that fits reads as part of the reply) — when numbered, ONE (i/N) run in order; the content
    # checks below are what the test is for.
    assert {total for _i, total in numbered} == ({str(len(numbered))} if numbered else set())
    assert [int(i) for i, _total in numbered] == list(range(1, len(numbered) + 1))
    bodies = [_strip_overflow_frame(b) for b in bubbles]
    # fork 2026-11-16 close-out: per-piece check instead of "every bubble is a contiguous slice"
    # (the splitter prefers line breaks, so a piece may be a line run rather than a plain substring
    # run): the reply's characters are exactly split across the seal and the bubbles, the head is
    # not repeated.
    footer = bodies[-1]
    reply_bodies = [body for body in bodies[:-1] if "答" in body]
    assert all(set(body) <= set(d.accumulated_text) for body in reply_bodies)  # nothing foreign
    # fork 2026-11-16 close-out: the original assertion summed 答 over the seal and the bubbles.
    # The pieces now also absorb the splitter's per-piece " (1/2)" numbering, so the seal holds a
    # head plus numbering rather than the bare head; the check is that the two hand-overs tile the
    # reply — the seal's 答 followed by the bubbles' 答 total exactly one copy.
    assert seal.count("答") + sum(body.count("答") for body in bodies) == 9000  # every character, once
    assert d._overflow_shown == payload + "\n\n（本次用时 3.2s）"  # the head and the bubbles draw that payload
    assert footer.endswith("（本次用时 3.2s）")  # the footer rides in the last bubble


def test_finalize_declines_completely_when_even_the_seal_fails(caplog):
    # fork 2026-11-16 (DPMS 561213): if the stream cannot take the seal either, nothing of the
    # reply claimed it — the gateway's regular send still delivers the full reply (exactly once).
    class _Sealless(_FakeAdapter):
        async def send_stream_frame(self, text, *, finalize=False, chat_id=None, **kw):
            self.calls.append((finalize, text))
            return finalize is False  # every FINISH frame is refused; intermediates land

    d = _delivery()
    d.adapter = _Sealless()
    d.accumulated_text = "答" * 9000
    asyncio.run(d._finalize())
    assert [text for finish, text in d.adapter.calls if finish], "both seals must be attempted"
    assert d.delivery_claimed is False
    assert d.overflow_parts == []


def test_short_turn_unchanged():
    d = _delivery()
    d.thinking_lines = ["🤔 正在思考中..."]
    d.accumulated_text = "答案是 42"
    asyncio.run(d._finalize())
    (_finalize_flag, text), = [c for c in d.adapter.calls if c[0]]
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
