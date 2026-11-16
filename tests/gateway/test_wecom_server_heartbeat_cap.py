"""fork 2026-11-16 (DPMS 561213): the WeCom server closes the WS at a 600s heartbeat cap
(258 of 265 production closes landed on ~600s). That close carries the same
``disconnected_event`` as a genuine displacement, so the adapter treated it as a kick:
placed the 20s kicked floor AND purged ``_last_chat_req_ids`` / ``_reply_req_ids``. On a
long turn the maps were wiped every 10 minutes, reply routing lost its associations and
users saw 20-minute-class delivery delays.

Contract: a connection that held up to the heartbeat cap (≥90% of it) reconnects fast,
keeps the maps and does not raise the kicked floor; every shorter-lived connection —
including one with no uptime stamp — keeps the displacement path unchanged.
"""

from __future__ import annotations

import asyncio
import logging
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from gateway.config import PlatformConfig
from plugins.platforms.wecom.adapter import RECONNECT_BACKOFF, WeComAdapter


def _kick_payload():
    return {
        "cmd": "aibot_event_callback",
        "headers": {"req_id": "req-hb"},
        "body": {"msgtype": "event", "event": {"eventtype": "disconnected_event"}},
    }


def _adapter(backoff=20.0):
    adapter = WeComAdapter(PlatformConfig(enabled=True))
    adapter._kicked_backoff_seconds = backoff
    adapter._running = True
    adapter._ws = SimpleNamespace(closed=False, close=AsyncMock())
    adapter._last_chat_req_ids["chat-1"] = "req-1"
    adapter._reply_req_ids["msg-1"] = "req-1"
    return adapter


def _stamp_uptime(monkeypatch, adapter, uptime):
    now = 10_000.0
    monkeypatch.setattr(time, "monotonic", lambda: now)
    adapter._connected_at_monotonic = now - uptime


class TestServerHeartbeatCap:
    @pytest.mark.asyncio
    async def test_short_lived_connection_still_treated_as_displacement(self, monkeypatch, caplog):
        adapter = _adapter()
        _stamp_uptime(monkeypatch, adapter, 45.0)

        with caplog.at_level(logging.DEBUG, logger="plugins.platforms.wecom.adapter"):
            await adapter._dispatch_payload(_kick_payload())

        assert adapter._kicked_at_monotonic is not None
        assert not adapter._last_chat_req_ids and not adapter._reply_req_ids
        assert "Displaced by a newer WeCom WS subscription" in caplog.text

    @pytest.mark.asyncio
    async def test_heartbeat_cap_close_keeps_req_id_maps_and_skips_the_kicked_floor(self, monkeypatch, caplog):
        adapter = _adapter()
        _stamp_uptime(monkeypatch, adapter, 590.0)

        with caplog.at_level(logging.DEBUG, logger="plugins.platforms.wecom.adapter"):
            await adapter._dispatch_payload(_kick_payload())
            await asyncio.sleep(0)  # pump the ensure_future(_close_ws_for_recycle)

        assert adapter._kicked_at_monotonic is None
        assert adapter._last_chat_req_ids == {"chat-1": "req-1"}
        assert adapter._reply_req_ids == {"msg-1": "req-1"}
        assert adapter._resubscribe_pending is True
        adapter._ws.close.assert_awaited_once()
        assert "normal fast reconnect, keeping req_id maps" in caplog.text
        assert "Displaced by a newer WeCom WS subscription" not in caplog.text

    @pytest.mark.asyncio
    async def test_missing_uptime_stamp_falls_back_to_displacement(self, monkeypatch):
        adapter = _adapter()
        adapter._connected_at_monotonic = None

        await adapter._dispatch_payload(_kick_payload())

        assert adapter._kicked_at_monotonic is not None
        assert not adapter._last_chat_req_ids and not adapter._reply_req_ids

    @pytest.mark.asyncio
    async def test_heartbeat_close_reconnects_on_the_fast_tier(self, monkeypatch):
        import random as random_module

        adapter = _adapter(backoff=20.0)
        _stamp_uptime(monkeypatch, adapter, 590.0)
        adapter._fast_reconnect_granted = False
        monkeypatch.setattr(random_module, "random", lambda: 0.0)

        await adapter._dispatch_payload(_kick_payload())
        adapter._refill_fast_reconnect_budget()  # what the listen loop does before its sleep

        delay = adapter._reconnect_delay(0)
        assert delay == adapter.FAST_RECONNECT_DELAY
        assert delay < RECONNECT_BACKOFF[0]
        assert delay < adapter._kicked_backoff_seconds
