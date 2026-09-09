"""fork: the gateway long-running heartbeat must not spam WeCom chats.

``_run_agent_notify_long_running`` posts a "⏳ Working — N min" heartbeat every
interval and edits it in place where the platform supports editing. WeCom
cannot edit messages (``SUPPORTS_MESSAGE_EDITING = False``, base
``edit_message`` returns success=False), so on WeCom EVERY heartbeat arrives as
a NEW message — on a long turn (agent timeout default 1800s, interval default
180s) that is up to ~10 duplicate "still working" messages per turn, while the
WeCom stream bubble already renders its own "⏳ 正在运行中…" indicator.

The fork gate: while the turn's stream consumer reports
``shows_running_indicator`` (WeComStreamDelivery while live), the heartbeat
skips sending entirely; consumers without the property keep the upstream
behaviour, and a delivery that gave up (``_disabled``) drops the property so
the heartbeat resumes as the fallback progress signal.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from gateway import run_turn as gateway_run_turn
from gateway.run import GatewayRunner
from plugins.platforms.wecom.stream_delivery import WeComStreamDelivery


class _FakeAdapter:
    """Records sends; edit always fails like the real WeCom adapter."""

    def __init__(self):
        self.sent = []

    async def send(self, chat_id, content, metadata=None):
        self.sent.append(content)
        return SimpleNamespace(success=True, message_id=f"m{len(self.sent)}")

    async def edit_message(self, chat_id, message_id, content, **kwargs):
        return SimpleNamespace(success=False, error="Not supported")


class _Disp:
    user_config = {}
    platform_key = "wecom"

    def _display_surface_mode(self, key, default=True, allow_generic=False):
        return default

    def resolve_display_setting(self, config, platform_key, name, default=True):
        return False

    def _generic_status_phrase(self, kind):
        return "Working"


def _turn_ctx(consumer, source):
    return SimpleNamespace(
        source=source,
        session_key="",
        agent_holder=[object()],
        _status_thread_metadata=None,
        stream_consumer_holder=[consumer],
        _cleanup_progress=False,
        _cleanup_msg_ids=[],
    )


def _make_runner(fake_adapter):
    runner = object.__new__(GatewayRunner)
    runner._adapter_for_source = lambda source: fake_adapter
    runner._agent_activity_summary = lambda agent: {}
    return runner


async def _run_heartbeat(runner, disp, turn_ctx):
    task = asyncio.create_task(
        runner._run_agent_notify_long_running(disp, turn_ctx, [None])
    )
    await asyncio.sleep(0.12)  # a few 20ms ticks
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


@pytest.fixture
def fast_interval(monkeypatch):
    monkeypatch.setenv("HERMES_AGENT_NOTIFY_INTERVAL", "0.02")


class TestStreamConsumerShowsRunningIndicator:
    def test_wecom_delivery_live_reports_indicator(self):
        from tests.gateway.test_wecom_stream_delivery import FakeAdapter

        delivery = WeComStreamDelivery(FakeAdapter(), chat_id="c1")
        turn_ctx = _turn_ctx(delivery, source=None)
        assert GatewayRunner._stream_consumer_shows_running_indicator(turn_ctx) is True

    def test_no_consumer_reports_false(self):
        turn_ctx = _turn_ctx(None, source=None)
        assert GatewayRunner._stream_consumer_shows_running_indicator(turn_ctx) is False

    def test_consumer_without_property_reports_false(self):
        turn_ctx = _turn_ctx(object(), source=None)
        assert GatewayRunner._stream_consumer_shows_running_indicator(turn_ctx) is False


class TestHeartbeatSuppression:
    @pytest.mark.asyncio
    async def test_live_wecom_delivery_suppresses_heartbeat(self, fast_interval):
        from tests.gateway.test_wecom_stream_delivery import FakeAdapter

        fake = _FakeAdapter()
        runner = _make_runner(fake)
        source = SimpleNamespace(chat_id="c1", platform=None)
        delivery = WeComStreamDelivery(FakeAdapter(), chat_id="c1")
        turn_ctx = _turn_ctx(delivery, source=source)

        await _run_heartbeat(runner, _Disp(), turn_ctx)
        assert fake.sent == []  # no heartbeat message posted

    @pytest.mark.asyncio
    async def test_without_consumer_heartbeat_still_posts(self, fast_interval):
        fake = _FakeAdapter()
        runner = _make_runner(fake)
        source = SimpleNamespace(chat_id="c1", platform=None)
        turn_ctx = _turn_ctx(None, source=source)

        await _run_heartbeat(runner, _Disp(), turn_ctx)
        assert len(fake.sent) >= 1  # upstream behaviour preserved

    @pytest.mark.asyncio
    async def test_disabled_delivery_restores_heartbeat(self, fast_interval):
        """The delivery gave up (no bubble) — its indicator is gone, so the
        gateway heartbeat must resume as the progress signal."""
        from tests.gateway.test_wecom_stream_delivery import FakeAdapter

        fake = _FakeAdapter()
        runner = _make_runner(fake)
        source = SimpleNamespace(chat_id="c1", platform=None)
        delivery = WeComStreamDelivery(FakeAdapter(), chat_id="c1")
        delivery._disabled = True
        turn_ctx = _turn_ctx(delivery, source=source)

        await _run_heartbeat(runner, _Disp(), turn_ctx)
        assert len(fake.sent) >= 1
