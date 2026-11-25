"""数字员工平台 —— 适配器端到端行为（新契约）。

走真实 aiohttp ``TestServer``，只把"网关把消息交给 agent"替换成可控的假处理器，
验证我们自己的请求-响应语义：同步返回、超时降级、迟到结果主动推送、
鉴权先于解析、并发汇入、会话隔离。
"""

from __future__ import annotations

import asyncio
import json

import httpx
import pytest

aiohttp = pytest.importorskip("aiohttp")
from aiohttp.test_utils import TestClient, TestServer

from gateway.config import PlatformConfig
from plugins.platforms.digital_employee.adapter import DigitalEmployeeAdapter

BODY = {
    "user_name": "zhangsan",
    "group_name": "运维群",
    "question": "你好",
    "service_name": "vivian",
    "reference_info": "",
    "user_type": "0",
    "conversation_type": "GROUP",
    "conversation_id": "conv-1",
    "conversation_name": "运维群(3284275877)",
    "session_id": "sess-1",
    "query_info": [{"message_id": "m1", "type": "text", "content": "你好"}],
    "history_list": [],
}


class _Harness:
    """适配器 + 可控 agent 行为。"""

    def __init__(self, extra: dict | None = None) -> None:
        self.adapter = DigitalEmployeeAdapter(PlatformConfig(enabled=True, extra=extra or {}))
        self.seen_events: list = []
        self.reply = "好的"
        self.started = asyncio.Event()
        self.finished = asyncio.Event()
        self.release: asyncio.Event | None = None
        self.arrivals = 0
        self._arrival_events: dict[int, asyncio.Event] = {}

    async def on_message(self, event) -> None:
        self.seen_events.append(event)
        self.started.set()
        if self.release is not None:
            await self.release.wait()
        await self.adapter.send(chat_id=event.source.chat_id, content=self.reply)
        self.finished.set()

    def note_arrival(self) -> None:
        self.arrivals += 1
        for n, event in self._arrival_events.items():
            if self.arrivals >= n:
                event.set()

    def arrival_event(self, count: int) -> asyncio.Event:
        event = self._arrival_events.setdefault(count, asyncio.Event())
        if self.arrivals >= count:
            event.set()
        return event

    def install(self) -> None:
        self.adapter._message_handler = self.on_message

    def client(self) -> TestClient:
        app = aiohttp.web.Application()

        async def _chat(request):
            self.note_arrival()
            return await self.adapter._handle_chat(request)

        app.router.add_get("/health", self.adapter._handle_health)
        app.router.add_post(self.adapter.settings.webhook_path, _chat)
        self.adapter._app = app
        return TestClient(TestServer(app))


@pytest.mark.asyncio
async def test_sync_reply_is_returned_in_the_same_response():
    h = _Harness()
    h.install()
    client = h.client()
    await client.start_server()
    try:
        resp = await client.post("/chat", json=BODY)
        assert resp.status == 200
        assert await resp.json() == {"message_list": [{"type": "txt", "content": "好的"}]}
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_event_is_built_from_the_new_contract():
    h = _Harness()
    h.install()
    client = h.client()
    await client.start_server()
    try:
        await client.post("/chat", json=BODY)
        event = h.seen_events[0]
        assert event.text == "你好"
        assert event.source.chat_id == "conv-1"
        assert event.source.chat_type == "group"
        assert event.source.user_id == "zhangsan"
        assert event.raw_message["service_name"] == "vivian"
        assert event.raw_message["group_name"] == "运维群"
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_conversations_are_isolated_per_service_name():
    """同一会话、不同数字员工 → 不同 hermes 会话键，不能串台。"""
    from plugins.platforms.digital_employee.inbound import parse_inbound

    a = parse_inbound(BODY)
    b = parse_inbound(dict(BODY, service_name="other"))
    from plugins.platforms.digital_employee.adapter import session_key

    assert session_key(a) != session_key(b)


@pytest.mark.asyncio
async def test_bearer_token_is_checked_before_parsing():
    h = _Harness({"apiKey": "secret"})
    h.install()
    client = h.client()
    await client.start_server()
    try:
        bad = await client.post("/chat", data=b"{not json", headers={"Authorization": "Bearer wrong"})
        assert bad.status == 401
        ok = await client.post("/chat", json=BODY, headers={"Authorization": "Bearer secret"})
        assert ok.status == 200
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_custom_auth_header_is_supported():
    h = _Harness({"apiKey": "secret", "authHeader": "X-Api-Key"})
    h.install()
    client = h.client()
    await client.start_server()
    try:
        wrong = await client.post("/chat", json=BODY, headers={"Authorization": "Bearer secret"})
        assert wrong.status == 401
        ok = await client.post("/chat", json=BODY, headers={"X-Api-Key": "secret"})
        assert ok.status == 200
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_no_api_key_configured_means_open():
    h = _Harness()
    h.install()
    client = h.client()
    await client.start_server()
    try:
        assert (await client.post("/chat", json=BODY)).status == 200
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_timeout_replies_with_notice():
    h = _Harness({"responseTimeout": "0.2", "timeoutNotice": "稍等哈"})
    h.release = asyncio.Event()
    h.install()
    client = h.client()
    await client.start_server()
    try:
        resp = await client.post("/chat", json=BODY, timeout=aiohttp.ClientTimeout(total=5))
        assert resp.status == 200
        assert await resp.json() == {"message_list": [{"type": "txt", "content": "稍等哈"}]}
        h.release.set()
        await asyncio.wait_for(h.finished.wait(), timeout=5)
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_late_reply_is_pushed_to_the_group_by_name():
    """超时后的迟到答复：走企微发消息 API，群聊按 groupName 寻址。"""
    calls: list[dict] = []
    pushed = asyncio.Event()

    def push_handler(request: httpx.Request) -> httpx.Response:
        calls.append(json.loads(request.content))
        pushed.set()
        return httpx.Response(200, json={"code": "0", "message": "执行成功", "result": "mid-1"})

    h = _Harness({
        "responseTimeout": "0.2",
        "pushUrl": "http://push.example/api",
        "pushKeyId": "KID",
    })
    h.reply = "迟到的答案"
    h.release = asyncio.Event()
    h.install()
    h.adapter._client = httpx.AsyncClient(transport=httpx.MockTransport(push_handler))
    client = h.client()
    await client.start_server()
    try:
        await client.post("/chat", json=BODY, timeout=aiohttp.ClientTimeout(total=5))
        h.release.set()
        await asyncio.wait_for(pushed.wait(), timeout=5)
        assert calls[0]["keyId"] == "KID"
        assert calls[0]["groupName"] == "运维群"
        assert calls[0]["sendContent"] == "迟到的答案"
    finally:
        await client.close()
        await h.adapter._client.aclose()


@pytest.mark.asyncio
async def test_late_reply_for_a_direct_chat_is_pushed_by_user_name():
    calls: list[dict] = []
    pushed = asyncio.Event()

    def push_handler(request: httpx.Request) -> httpx.Response:
        calls.append(json.loads(request.content))
        pushed.set()
        return httpx.Response(200, json={"code": "0", "message": "执行成功", "result": "mid-1"})

    h = _Harness({"responseTimeout": "0.2", "pushUrl": "http://push.example/api", "pushKeyId": "KID"})
    h.release = asyncio.Event()
    h.install()
    h.adapter._client = httpx.AsyncClient(transport=httpx.MockTransport(push_handler))
    client = h.client()
    await client.start_server()
    try:
        await client.post("/chat", json=dict(BODY, group_name=""), timeout=aiohttp.ClientTimeout(total=5))
        h.release.set()
        await asyncio.wait_for(pushed.wait(), timeout=5)
        assert calls[0]["sendToUser"] == "zhangsan"
        assert "groupName" not in calls[0]
    finally:
        await client.close()
        await h.adapter._client.aclose()


@pytest.mark.asyncio
async def test_concurrent_waiters_share_one_agent_run():
    """同一会话的并发请求汇入同一轮 agent（平台重投/并发是常态）。"""
    h = _Harness({"responseTimeout": "5"})
    h.release = asyncio.Event()
    h.install()
    client = h.client()
    await client.start_server()
    try:
        first_task = asyncio.create_task(client.post("/chat", json=BODY))
        await asyncio.wait_for(h.started.wait(), timeout=5)
        second_task = asyncio.create_task(client.post("/chat", json=dict(BODY, query_info=[])))
        await asyncio.wait_for(h.arrival_event(2).wait(), timeout=5)
        h.release.set()
        first, second = await asyncio.wait_for(asyncio.gather(first_task, second_task), timeout=5)
        assert first.status == second.status == 200
        assert len(h.seen_events) == 1
        assert await second.json() == await first.json()
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_health_endpoint():
    h = _Harness()
    client = h.client()
    await client.start_server()
    try:
        resp = await client.get("/health")
        assert resp.status == 200
        assert (await resp.json())["platform"] == "digital_employee"
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_missing_conversation_id_is_400():
    h = _Harness()
    h.install()
    client = h.client()
    await client.start_server()
    try:
        body = dict(BODY)
        body.pop("conversation_id")
        assert (await client.post("/chat", json=body)).status == 400
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_long_reply_is_chunked():
    h = _Harness({"textChunkLimit": "50"})
    h.reply = "句子。" * 80
    h.install()
    client = h.client()
    await client.start_server()
    try:
        messages = (await (await client.post("/chat", json=BODY)).json())["message_list"]
        assert len(messages) > 1
        assert all(m["type"] == "txt" for m in messages)
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_outbound_attachment_degrades_to_a_text_notice():
    """文件暂不支持：不能静默丢弃，要告诉用户。"""
    h = _Harness()
    client = h.client()
    await client.start_server()
    try:
        result = await h.adapter.send_document(chat_id="conv-1", file_path="/tmp/report.pdf")
        assert result.success is True
        # 不带等待方 → 走推送路径；未配置 push 时至少不能抛异常
        assert result.message_id is not None
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_inbound_attachment_is_answered_with_a_note():
    """入站图片不带下载能力：agent 收到说明并按文字回答。"""
    h = _Harness()
    h.install()
    client = h.client()
    await client.start_server()
    try:
        body = dict(BODY, query_info=[{"message_id": "m9", "type": "image", "content": "dep/a.jpg"}])
        assert (await client.post("/chat", json=body)).status == 200
        assert "无法读取附件" in h.seen_events[0].text
    finally:
        await client.close()
