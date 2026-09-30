"""Regression (audit 2026-09-29 module-1 M1.7): group-ness had two truth sources.
_send_inner used inbound-learned ∪ operator-configured groups, _try_agent_fallback
only the learned set — a CONFIGURED group (learned set wiped on restart) leaked
into the agent-fallback channel whose touser the self-built-app API rejects."""
import asyncio

from gateway.config import Platform
from plugins.platforms.wecom import adapter as adapter_mod
from plugins.platforms.wecom.adapter import WeComAdapter


def _make_adapter():
    adapter = object.__new__(WeComAdapter)
    # `name` is a read-only property (platform.value.title()) — set platform instead.
    adapter.platform = Platform.WECOM
    adapter._group_chat_ids = set()          # learned set empty (fresh restart)
    adapter._groups = {"wrCONFIGURED": {"name": "ops"}}  # operator-configured group
    return adapter


def test_configured_group_never_reaches_agent_fallback(monkeypatch):
    adapter = _make_adapter()

    def _boom():
        raise AssertionError("configured group must not reach agent fallback")

    monkeypatch.setattr(adapter_mod, "_agent_fallback_client", _boom)
    result = asyncio.run(adapter._try_agent_fallback("wrCONFIGURED", "x", "test"))
    assert result is None


def test_learned_group_also_blocked(monkeypatch):
    adapter = _make_adapter()
    adapter._group_chat_ids = {"wrLEARNED"}

    def _boom():
        raise AssertionError("learned group must not reach agent fallback")

    monkeypatch.setattr(adapter_mod, "_agent_fallback_client", _boom)
    assert asyncio.run(adapter._try_agent_fallback("wrLEARNED", "x", "test")) is None


def test_dm_still_eligible_for_fallback(monkeypatch):
    adapter = _make_adapter()

    class _StubClient:
        async def send_markdown(self, touser, content):
            return True, None

    sentinel = _StubClient()
    monkeypatch.setattr(adapter_mod, "_agent_fallback_client", lambda: sentinel)
    # DM passes the group gate (client is consulted; stub returns non-None client)
    result = asyncio.run(adapter._try_agent_fallback("dm:u1", "x", "test"))
    assert result is not None and result.success is True
