"""Tests for WeCom send_message routing.

Ensures a WeCom send goes through the gateway's live in-process adapter
(no competing WebSocket → no errcode 846609 displacement) rather than the
standalone ephemeral path that opens a second WS to the same bot_id.

Kept in a separate file from test_send_message_tool.py so it is not gated
on python-telegram-bot being installed, and so it can stub ``gateway.run``
via sys.modules (the real module pulls in the full agent stack).
"""

import asyncio
import sys
import threading
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock, patch

from gateway.config import Platform
from tools.send_message_tool import _send_to_platform
from plugins.platforms.wecom.adapter import _standalone_send


def _stub_gateway_run(live_adapter=None, runner=None):
    """Build a fake gateway.run module exposing _gateway_runner_ref.

    ``live_adapter`` is shorthand for a runner whose adapters dict holds the
    given WeCom adapter; ``runner`` overrides the whole runner object.
    Returns (module, runner)."""
    if runner is None:
        runner = SimpleNamespace(adapters={Platform.WECOM: live_adapter} if live_adapter else {})
    mod = ModuleType("gateway.run")
    mod._gateway_runner_ref = lambda: runner
    return mod, runner


class TestSendToPlatformWecom:
    def test_wecom_send_routes_through_live_adapter(self):
        """A live in-process WeCom adapter must receive the send directly.
        The standalone ephemeral path (which opens a second WS and displaces
        the gateway session → 846609) must NOT be used when a live adapter
        is available."""
        live_send = AsyncMock(return_value=SimpleNamespace(success=True, message_id="m1"))
        fake_run, _ = _stub_gateway_run(live_adapter=SimpleNamespace(send=live_send))

        with patch.dict(sys.modules, {"gateway.run": fake_run}), \
             patch(
                 "tools.send_message_tool._registry_standalone_send",
                 new_callable=AsyncMock,
             ) as standalone:
            result = asyncio.run(
                _send_to_platform(Platform.WECOM, SimpleNamespace(extra={}), "brycehuang", "hello")
            )

        assert result["success"] is True
        assert result["message_id"] == "m1"
        live_send.assert_awaited_once()
        assert live_send.call_args.kwargs.get("chat_id") == "brycehuang"
        # CRITICAL regression guard: standalone ephemeral send (the
        # displacement fingerprint: fresh adapter + connect) was NOT used.
        standalone.assert_not_awaited()

    def test_wecom_send_falls_back_to_standalone_when_no_runner(self):
        """When no live runner is available (true out-of-process caller),
        _send_via_adapter falls back to the registry's standalone_sender_fn
        (wecom's _standalone_send), exactly as before this change."""
        fake_run, _ = _stub_gateway_run(runner=None)
        standalone_fn = AsyncMock(
            return_value={"success": True, "platform": "wecom", "message_id": "s1"}
        )
        fake_entry = SimpleNamespace(standalone_sender_fn=standalone_fn)
        fake_registry_module = ModuleType("gateway.platform_registry")
        fake_registry_module.platform_registry = SimpleNamespace(get=lambda name: fake_entry)

        with patch.dict(
            sys.modules,
            {"gateway.run": fake_run, "gateway.platform_registry": fake_registry_module},
        ):
            result = asyncio.run(
                _send_to_platform(Platform.WECOM, SimpleNamespace(extra={}), "brycehuang", "hello")
            )

        assert result["success"] is True
        standalone_fn.assert_awaited_once()


class TestWecomStandaloneSend:
    """Defense-in-depth: _standalone_send itself reuses a live in-process
    adapter instead of opening a competing WS, mirroring _send_via_adapter."""

    def test_reuses_live_adapter_when_runner_present(self):
        """When a live wecom adapter is reachable via _gateway_runner_ref,
        _standalone_send must reuse it (no fresh adapter / connect / WS)."""
        live_send = AsyncMock(return_value=SimpleNamespace(success=True, message_id="m1"))
        live_adapter = SimpleNamespace(send=live_send)
        runner = SimpleNamespace(adapters={Platform.WECOM: live_adapter})
        fake_run = ModuleType("gateway.run")
        fake_run._gateway_runner_ref = lambda: runner

        with patch.dict(sys.modules, {"gateway.run": fake_run}):
            result = asyncio.run(
                _standalone_send(SimpleNamespace(extra={}), "brycehuang", "hello")
            )

        assert result["success"] is True
        assert result["message_id"] == "m1"
        live_send.assert_awaited_once()
        assert live_send.call_args.args[:2] == ("brycehuang", "hello")

    def test_ephemeral_when_no_runner(self):
        """With no runner (true out-of-process caller), the ephemeral path
        (fresh adapter + connect + send + disconnect) is used unchanged."""
        fake_run = ModuleType("gateway.run")
        fake_run._gateway_runner_ref = lambda: None
        ephemeral = SimpleNamespace(
            connect=AsyncMock(return_value=True),
            send=AsyncMock(return_value=SimpleNamespace(success=True, message_id="e1")),
            disconnect=AsyncMock(),
        )

        with patch.dict(sys.modules, {"gateway.run": fake_run}), \
             patch(
                 "plugins.platforms.wecom.adapter.check_wecom_requirements",
                 return_value=True,
             ), \
             patch(
                 "plugins.platforms.wecom.adapter.WeComAdapter",
                 return_value=ephemeral,
             ):
            result = asyncio.run(
                _standalone_send(SimpleNamespace(extra={}), "brycehuang", "hello")
            )

        assert result["success"] is True
        ephemeral.connect.assert_awaited_once()
        ephemeral.send.assert_awaited_once()
        ephemeral.disconnect.assert_awaited_once()

    def test_cross_loop_falls_back_to_gateway_loop_schedule_not_ephemeral(self):
        """A live adapter whose send fails with a "different event loop" error
        (cron's asyncio.run fallback creates a new loop, but the adapter's
        websocket futures are bound to the gateway loop) must be re-scheduled
        onto the gateway loop — NOT re-sent via an ephemeral WS, which would
        displace the main subscription (errcode 846609)."""
        # Gateway loop runs in a background thread (as the real gateway does);
        # asyncio.run_coroutine_threadsafe requires a running loop.
        gateway_loop = asyncio.new_event_loop()
        thread = threading.Thread(target=gateway_loop.run_forever, daemon=True)
        thread.start()
        try:
            send_calls = []

            async def _send(*args, **kwargs):
                send_calls.append(len(send_calls))
                if len(send_calls) == 1:
                    raise RuntimeError("Future attached to a different loop")
                return SimpleNamespace(success=True, message_id="cross-m1")

            live_adapter = SimpleNamespace(send=_send)
            runner = SimpleNamespace(
                adapters={Platform.WECOM: live_adapter},
                _gateway_loop=gateway_loop,
            )
            fake_run = ModuleType("gateway.run")
            fake_run._gateway_runner_ref = lambda: runner

            ephemeral = SimpleNamespace(
                connect=AsyncMock(return_value=True),
                send=AsyncMock(),
                disconnect=AsyncMock(),
            )

            with patch.dict(sys.modules, {"gateway.run": fake_run}), \
                 patch(
                     "plugins.platforms.wecom.adapter.check_wecom_requirements",
                     return_value=True,
                 ), \
                 patch(
                     "plugins.platforms.wecom.adapter.WeComAdapter",
                     return_value=ephemeral,
                 ):
                result = asyncio.run(
                    _standalone_send(SimpleNamespace(extra={}), "brycehuang", "hello")
                )

            assert result["success"] is True
            assert result["message_id"] == "cross-m1"
            assert len(send_calls) == 2, "must retry on the gateway loop"
            # CRITICAL: no ephemeral WS was opened (would displace subscription).
            ephemeral.connect.assert_not_awaited()
            ephemeral.send.assert_not_awaited()
            ephemeral.disconnect.assert_not_awaited()
        finally:
            gateway_loop.call_soon_threadsafe(gateway_loop.stop)
            thread.join(timeout=5)
            gateway_loop.close()

    def test_cross_loop_without_gateway_loop_returns_error_not_ephemeral(self):
        """If the live adapter fails cross-loop but the runner exposes no
        reachable _gateway_loop, the send must return an error rather than
        open an ephemeral WS — an ephemeral connection would displace the
        gateway's sole subscription (errcode 846609)."""
        async def _send(*args, **kwargs):
            raise RuntimeError("Future attached to a different loop")

        live_adapter = SimpleNamespace(send=_send)
        runner = SimpleNamespace(
            adapters={Platform.WECOM: live_adapter},
            _gateway_loop=None,
        )
        fake_run = ModuleType("gateway.run")
        fake_run._gateway_runner_ref = lambda: runner

        ephemeral = SimpleNamespace(
            connect=AsyncMock(return_value=True),
            send=AsyncMock(),
            disconnect=AsyncMock(),
        )

        with patch.dict(sys.modules, {"gateway.run": fake_run}), \
             patch(
                 "plugins.platforms.wecom.adapter.check_wecom_requirements",
                 return_value=True,
             ), \
             patch(
                 "plugins.platforms.wecom.adapter.WeComAdapter",
                 return_value=ephemeral,
             ):
            result = asyncio.run(
                _standalone_send(SimpleNamespace(extra={}), "brycehuang", "hello")
            )

        assert result.get("success") is not True
        ephemeral.connect.assert_not_awaited()
        ephemeral.send.assert_not_awaited()
        ephemeral.disconnect.assert_not_awaited()
