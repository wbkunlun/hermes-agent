"""Tests for Bot→Agent delivery fallback (② seam 1)."""

import pytest

from gateway.config import Platform
from plugins.platforms.wecom import adapter as wecom_adapter
from plugins.platforms.wecom.callback_adapter import WecomAgentFallbackClient


class TestFallbackClientEnabled:
    def test_disabled_by_env_flag(self, monkeypatch):
        monkeypatch.setenv("WECOM_CALLBACK_CORP_ID", "ww1")
        monkeypatch.setenv("WECOM_CALLBACK_CORP_SECRET", "s")
        monkeypatch.setenv("WECOM_CALLBACK_AGENT_ID", "1000002")
        for off in ("0", "false", "off", "no", "FALSE", " Off "):
            monkeypatch.setenv("WECOM_AGENT_FALLBACK", off)
            assert wecom_adapter._agent_fallback_client() is None

    def test_enabled_when_three_envs_present(self, monkeypatch):
        monkeypatch.delenv("WECOM_AGENT_FALLBACK", raising=False)
        monkeypatch.setenv("WECOM_CALLBACK_CORP_ID", "ww1")
        monkeypatch.setenv("WECOM_CALLBACK_CORP_SECRET", "s")
        monkeypatch.setenv("WECOM_CALLBACK_AGENT_ID", "1000002")
        client = wecom_adapter._agent_fallback_client()
        assert isinstance(client, WecomAgentFallbackClient)
        # 缓存：同 env 二次调用拿同一实例
        assert wecom_adapter._agent_fallback_client() is client

    def test_env_change_rebuilds_client(self, monkeypatch):
        monkeypatch.delenv("WECOM_AGENT_FALLBACK", raising=False)
        monkeypatch.setenv("WECOM_CALLBACK_CORP_ID", "ww1")
        monkeypatch.setenv("WECOM_CALLBACK_CORP_SECRET", "s")
        monkeypatch.setenv("WECOM_CALLBACK_AGENT_ID", "1000002")
        first = wecom_adapter._agent_fallback_client()
        monkeypatch.setenv("WECOM_CALLBACK_AGENT_ID", "1000009")
        second = wecom_adapter._agent_fallback_client()
        assert first is not second and second is not None

    def test_absent_envs_disable(self, monkeypatch):
        monkeypatch.delenv("WECOM_CALLBACK_CORP_ID", raising=False)
        monkeypatch.delenv("WECOM_CALLBACK_CORP_SECRET", raising=False)
        monkeypatch.delenv("WECOM_CALLBACK_AGENT_ID", raising=False)
        monkeypatch.delenv("WECOM_AGENT_FALLBACK", raising=False)
        assert wecom_adapter._agent_fallback_client() is None


class TestFallbackClientSend:
    @pytest.mark.asyncio
    async def test_send_markdown_posts_with_token(self, monkeypatch):
        client = WecomAgentFallbackClient("ww1", "secret", "1000002")
        posts = []

        class FakeResp:
            def __init__(self, payload):
                self._p = payload

            def json(self):
                return self._p

        class FakeAsyncClient:
            def __init__(self, **kw):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, *a):
                return False

            async def get(self, url, params=None):
                posts.append(("get", url, params))
                return FakeResp({"errcode": 0, "access_token": "T", "expires_in": 7200})

            async def post(self, url, json=None):
                posts.append(("post", url, json))
                return FakeResp({"errcode": 0, "msgid": "fb1"})

        import plugins.platforms.wecom.callback_adapter as ca
        monkeypatch.setattr(ca.httpx, "AsyncClient", FakeAsyncClient)

        ok, err = await client.send_markdown("zhangsan", "**hi**")
        assert ok is True and err is None
        assert posts[0][0] == "get" and "gettoken" in posts[0][1]
        assert posts[1][2]["msgtype"] == "markdown"
        assert posts[1][2]["markdown"]["content"] == "**hi**"
        assert posts[1][2]["touser"] == "zhangsan"

    @pytest.mark.asyncio
    async def test_send_markdown_token_failure_returns_error(self, monkeypatch):
        client = WecomAgentFallbackClient("ww1", "secret", "1000002")

        class FakeResp:
            def __init__(self, payload):
                self._p = payload

            def json(self):
                return self._p

        class FakeAsyncClient:
            def __init__(self, **kw):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, *a):
                return False

            async def get(self, url, params=None):
                return FakeResp({"errcode": 40001, "errmsg": "bad credential"})

        import plugins.platforms.wecom.callback_adapter as ca
        monkeypatch.setattr(ca.httpx, "AsyncClient", FakeAsyncClient)

        ok, err = await client.send_markdown("zhangsan", "x")
        assert ok is False and err and "40001" in err


class TestSendInnerFallbackSeam:
    @pytest.mark.asyncio
    async def test_proactive_failure_falls_back_to_agent_channel(self, monkeypatch):
        """主动发送失败 → 自建应用回退成功 → SendResult.success=True。"""
        adapter = wecom_adapter.WeComAdapter.__new__(wecom_adapter.WeComAdapter)
        adapter.platform = Platform.WECOM
        adapter._group_chat_ids = set()
        adapter._last_chat_req_ids = {}
        sent = []

        class FakeClient:
            async def send_markdown(self, touser, content):
                sent.append((touser, content))
                return True, None

        monkeypatch.setattr(wecom_adapter, "_agent_fallback_client", lambda: FakeClient())

        async def failing_send_request(cmd, payload):
            raise RuntimeError("proactive send exploded")

        adapter._send_request = failing_send_request
        # 无缓存 req_id → 直接走主动路径 → 失败 → 回退
        adapter._reply_req_id_for_message = lambda reply_to: None

        result = await adapter._send_inner("zhangsan", "hello bot")
        assert result.success is True
        assert result.raw_response == {"agent_fallback": True, "reason": "bot send failed: proactive send exploded"}
        assert sent == [("zhangsan", "hello bot")]

    @pytest.mark.asyncio
    async def test_group_chat_never_falls_back(self, monkeypatch):
        adapter = wecom_adapter.WeComAdapter.__new__(wecom_adapter.WeComAdapter)
        adapter.platform = Platform.WECOM
        adapter._group_chat_ids = {"wr_group_1"}
        called = []

        class FakeClient:
            async def send_markdown(self, touser, content):
                called.append(touser)
                return True, None

        monkeypatch.setattr(wecom_adapter, "_agent_fallback_client", lambda: FakeClient())
        result = await adapter._try_agent_fallback("wr_group_1", "hi", "group no req_id")
        assert result is None
        assert called == []

    @pytest.mark.asyncio
    async def test_fallback_disabled_returns_none(self, monkeypatch):
        adapter = wecom_adapter.WeComAdapter.__new__(wecom_adapter.WeComAdapter)
        adapter.platform = Platform.WECOM
        adapter._group_chat_ids = set()
        monkeypatch.setattr(wecom_adapter, "_agent_fallback_client", lambda: None)
        assert await adapter._try_agent_fallback("zhangsan", "hi", "any") is None

    @pytest.mark.asyncio
    async def test_fallback_failure_returns_none(self, monkeypatch):
        adapter = wecom_adapter.WeComAdapter.__new__(wecom_adapter.WeComAdapter)
        adapter.platform = Platform.WECOM
        adapter._group_chat_ids = set()

        class FakeClient:
            async def send_markdown(self, touser, content):
                return False, "server rejected"

        monkeypatch.setattr(wecom_adapter, "_agent_fallback_client", lambda: FakeClient())
        assert await adapter._try_agent_fallback("zhangsan", "hi", "any") is None

    @pytest.mark.asyncio
    async def test_no_fallback_when_bot_succeeds(self, monkeypatch):
        """主动路径成功时绝不触发回退（raw_response 无 agent_fallback）。"""
        adapter = wecom_adapter.WeComAdapter.__new__(wecom_adapter.WeComAdapter)
        adapter.platform = Platform.WECOM
        adapter._group_chat_ids = set()
        adapter._last_chat_req_ids = {}

        class FakeClient:
            async def send_markdown(self, touser, content):
                raise AssertionError("fallback must not fire on success")

        monkeypatch.setattr(wecom_adapter, "_agent_fallback_client", lambda: FakeClient())

        async def ok_send_request(cmd, payload):
            return {"errcode": 0, "msgid": "direct-1"}

        adapter._send_request = ok_send_request
        adapter._reply_req_id_for_message = lambda reply_to: None

        result = await adapter._send_inner("zhangsan", "hello")
        assert result.success is True
        assert "agent_fallback" not in (result.raw_response or {})


class TestStandaloneFallback:
    @pytest.mark.asyncio
    async def test_standalone_prefers_agent_fallback_over_ephemeral(self, monkeypatch):
        from gateway.config import PlatformConfig

        class FakeClient:
            async def send_markdown(self, touser, content):
                return True, None

        monkeypatch.setattr(wecom_adapter, "_agent_fallback_client", lambda: FakeClient())
        # 若走了 ephemeral 路径会触发 check_wecom_requirements → 使其炸掉以证明没走到
        def boom():
            raise AssertionError("ephemeral path must not be reached when fallback succeeds")

        monkeypatch.setattr(wecom_adapter, "check_wecom_requirements", boom)

        result = await wecom_adapter._standalone_send(
            PlatformConfig(enabled=True), "zhangsan", "cron hello",
        )
        assert result.get("success") is True
        assert result.get("via") == "agent_fallback"

    @pytest.mark.asyncio
    async def test_standalone_falls_through_when_fallback_fails(self, monkeypatch):
        from gateway.config import PlatformConfig

        class FakeClient:
            async def send_markdown(self, touser, content):
                return False, "server said no"

        monkeypatch.setattr(wecom_adapter, "_agent_fallback_client", lambda: FakeClient())

        class FakeAdapter:
            def __init__(self, cfg):
                pass

            async def connect(self):
                return True

            async def send(self, chat_id, message):
                class R:
                    success = True
                    message_id = "eph-1"
                return R()

            async def disconnect(self):
                pass

        monkeypatch.setattr(wecom_adapter, "WeComAdapter", FakeAdapter)
        monkeypatch.setattr(wecom_adapter, "check_wecom_requirements", lambda: True)

        result = await wecom_adapter._standalone_send(
            PlatformConfig(enabled=True), "zhangsan", "cron hello",
        )
        assert result.get("success") is True
        assert result.get("message_id") == "eph-1"

    @pytest.mark.asyncio
    async def test_standalone_fallback_raises_still_falls_through(self, monkeypatch):
        from gateway.config import PlatformConfig

        class FakeClient:
            async def send_markdown(self, touser, content):
                raise RuntimeError("network down")

        monkeypatch.setattr(wecom_adapter, "_agent_fallback_client", lambda: FakeClient())

        class FakeAdapter:
            def __init__(self, cfg):
                pass

            async def connect(self):
                return True

            async def send(self, chat_id, message):
                class R:
                    success = True
                    message_id = "eph-2"
                return R()

            async def disconnect(self):
                pass

        monkeypatch.setattr(wecom_adapter, "WeComAdapter", FakeAdapter)
        monkeypatch.setattr(wecom_adapter, "check_wecom_requirements", lambda: True)

        result = await wecom_adapter._standalone_send(
            PlatformConfig(enabled=True), "zhangsan", "cron hello",
        )
        assert result.get("success") is True
        assert result.get("message_id") == "eph-2"

    def test_dead_block_removed(self):
        """死代码块删除后，_standalone_send 内 check_wecom_requirements 只出现一次。"""
        import inspect

        source = inspect.getsource(wecom_adapter._standalone_send)
        assert source.count("check_wecom_requirements()") == 1

    @pytest.mark.asyncio
    async def test_standalone_skips_direct_live_send_off_gateway_loop(self, monkeypatch):
        """fork 跨 loop 修复：caller 不在 gateway loop 上时必须跳过对 live adapter 的直调（异 loop
        await 会把 per-chat 队列 future 停在本 loop、gateway worker 跨线程唤醒不了——生产 60s 卡顿
        根因），直接走 threadsafe 派发。"""
        import asyncio
        import concurrent.futures
        import threading
        from types import SimpleNamespace
        from unittest.mock import AsyncMock
        from gateway.config import PlatformConfig
        import tools.send_message_senders as senders
        import agent.async_utils as async_utils

        gateway_loop = asyncio.new_event_loop()
        thread = threading.Thread(target=gateway_loop.run_forever, daemon=True)
        thread.start()

        fake_live = SimpleNamespace(send=AsyncMock())
        scheduled = []

        def fake_schedule(coro, loop):
            scheduled.append(loop)
            coro.close()  # 结果由桩直接给出，真实协程不执行
            fut = concurrent.futures.Future()
            fut.set_result(SimpleNamespace(success=True, message_id="cross-1"))
            return fut

        monkeypatch.setattr(async_utils, "safe_schedule_threadsafe", fake_schedule)
        monkeypatch.setattr(
            senders, "_live_adapter",
            lambda platform: (SimpleNamespace(_gateway_loop=gateway_loop), fake_live),
        )
        monkeypatch.setattr(wecom_adapter, "_agent_fallback_client", lambda: None)

        try:
            result = await wecom_adapter._standalone_send(
                PlatformConfig(enabled=True), "zhangsan", "cron hello",
            )
            assert result.get("success") is True
            assert result.get("message_id") == "cross-1"
            fake_live.send.assert_not_awaited()  # 异 loop 直调被跳过
            assert scheduled == [gateway_loop]  # 走的是 threadsafe 派发
        finally:
            gateway_loop.call_soon_threadsafe(gateway_loop.stop)
            thread.join(timeout=2)
            gateway_loop.close()

    @pytest.mark.asyncio
    async def test_standalone_uses_direct_send_on_gateway_loop(self, monkeypatch):
        """caller 就在 gateway loop 上时保留直调（无跨线程派发开销）。"""
        import asyncio
        from types import SimpleNamespace
        from unittest.mock import AsyncMock
        from gateway.config import PlatformConfig
        import tools.send_message_senders as senders

        fake_live = SimpleNamespace(
            send=AsyncMock(return_value=SimpleNamespace(success=True, message_id="direct-1"))
        )
        runner = SimpleNamespace(_gateway_loop=asyncio.get_running_loop())
        monkeypatch.setattr(senders, "_live_adapter", lambda platform: (runner, fake_live))
        monkeypatch.setattr(wecom_adapter, "_agent_fallback_client", lambda: None)

        result = await wecom_adapter._standalone_send(
            PlatformConfig(enabled=True), "zhangsan", "cron hello",
        )
        assert result.get("success") is True
        assert result.get("message_id") == "direct-1"
        fake_live.send.assert_awaited_once_with("zhangsan", "cron hello")

    @pytest.mark.asyncio
    async def test_standalone_cross_loop_result_flows_via_wrap_future(self, monkeypatch):
        """跨 loop 结果经 wrap_future 回流到本次 await，且等待期间本 loop 保持可调度
        （旧的阻塞 .result() 会把 loop 卡死，并发任务全部饿死）。"""
        import asyncio
        import concurrent.futures
        import threading
        import time
        from types import SimpleNamespace
        from unittest.mock import AsyncMock
        from gateway.config import PlatformConfig
        import tools.send_message_senders as senders
        import agent.async_utils as async_utils

        gateway_loop = asyncio.new_event_loop()
        thread = threading.Thread(target=gateway_loop.run_forever, daemon=True)
        thread.start()
        fake_live = SimpleNamespace(send=AsyncMock())

        def fake_schedule(coro, loop):
            coro.close()
            fut = concurrent.futures.Future()

            def complete():
                time.sleep(0.05)  # 50ms 后另一线程才给出结果
                fut.set_result(SimpleNamespace(success=True, message_id="delayed-1"))

            threading.Thread(target=complete, daemon=True).start()
            return fut

        monkeypatch.setattr(async_utils, "safe_schedule_threadsafe", fake_schedule)
        monkeypatch.setattr(
            senders, "_live_adapter",
            lambda platform: (SimpleNamespace(_gateway_loop=gateway_loop), fake_live),
        )
        monkeypatch.setattr(wecom_adapter, "_agent_fallback_client", lambda: None)

        ticks = []

        async def ticker():
            for _ in range(10):
                ticks.append(1)
                await asyncio.sleep(0.01)

        ticker_task = asyncio.create_task(ticker())
        try:
            result = await wecom_adapter._standalone_send(
                PlatformConfig(enabled=True), "zhangsan", "cron hello",
            )
            assert result.get("success") is True
            assert result.get("message_id") == "delayed-1"
            # 等待期间本 loop 保持可调度：旧的阻塞 .result() 此刻 ticks 应为 0
            assert len(ticks) >= 2, f"loop starved during cross-loop send: only {len(ticks)} ticks"
        finally:
            gateway_loop.call_soon_threadsafe(gateway_loop.stop)
            thread.join(timeout=2)
            gateway_loop.close()
            ticker_task.cancel()
            try:
                await ticker_task
            except asyncio.CancelledError:
                pass
