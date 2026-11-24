"""数字员工平台 —— 主动发消息（企微发消息 API）。

协议照 ``wechat_group.py``：``POST {url}``，body 带 ``keyId``，只看 HTTP 200，
不解析业务错误码（参考脚本也不解析）。群聊按 ``groupName`` 寻址，私聊按
``sendToUser`` 寻址。
"""

from __future__ import annotations

import json

import httpx
import pytest

from plugins.platforms.digital_employee.push import WeChatPushClient


def _client(handler, url: str = "http://push.example/api", key: str = "KID") -> WeChatPushClient:
    return WeChatPushClient(
        url=url, key_id=key, client=httpx.AsyncClient(transport=httpx.MockTransport(handler))
    )


def test_disabled_without_url_or_key():
    assert WeChatPushClient(url="", key_id="K").enabled is False
    assert WeChatPushClient(url="http://x", key_id="").enabled is False
    assert WeChatPushClient(url="http://x", key_id="K").enabled is True


@pytest.mark.asyncio
async def test_group_message_matches_the_reference_payload():
    seen: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen.update(json.loads(request.content))
        return httpx.Response(200, text="ok")

    result = await _client(handler).send_group(group_name="运维群", content="hello", at_users="a,b")
    assert result.ok is True
    assert seen == {
        "keyId": "KID",
        "sendContent": "hello",
        "groupName": "运维群",
        "atUserList": ["a", "b"],
    }


@pytest.mark.asyncio
async def test_private_message_uses_send_to_user():
    seen: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen.update(json.loads(request.content))
        return httpx.Response(200)

    result = await _client(handler).send_private(user_name="zhangsan(张三)", content="hi")
    assert result.ok is True
    assert seen == {"keyId": "KID", "sendContent": "hi", "sendToUser": "zhangsan(张三)"}


@pytest.mark.asyncio
async def test_empty_at_list_is_sent_as_empty_array():
    """参考脚本对无 @ 的群消息也传 ``[]``，接口按此契约。"""
    seen: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen.update(json.loads(request.content))
        return httpx.Response(200)

    await _client(handler).send_group(group_name="g", content="x")
    assert seen["atUserList"] == []


@pytest.mark.asyncio
async def test_only_http_200_counts_as_success():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="boom")

    result = await _client(handler).send_group(group_name="g", content="x")
    assert result.ok is False
    assert "500" in result.error


@pytest.mark.asyncio
async def test_network_error_is_returned_not_raised():
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused")

    result = await _client(handler).send_group(group_name="g", content="x")
    assert result.ok is False
    assert "failed" in result.error


@pytest.mark.asyncio
async def test_disabled_client_never_calls_network():
    def handler(request: httpx.Request) -> httpx.Response:  # pragma: no cover
        raise AssertionError("must not be called")

    client = WeChatPushClient(
        url="", key_id="", client=httpx.AsyncClient(transport=httpx.MockTransport(handler))
    )
    result = await client.send_group(group_name="g", content="x")
    assert result.ok is False
    assert "disabled" in result.error
