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
        assert result.raw_response == {"agent_fallback": True, "reason": "bot send error: proactive send exploded"}
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
