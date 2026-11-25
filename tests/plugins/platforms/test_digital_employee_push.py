"""数字员工平台 —— 主动发消息（虚拟消息发送接口 virtualSendMsg）。

协议按官方接口文档（2026-11-25）：Header 带 ``Authorization: <keyId>``（裸值）；
响应 ``{"code","message","result"}`` **code=="0" 才算成功**；5xx / token 过期(code="5")
/ 网络异常重试 1 次；``sendContent`` ≤ 20000 字符（超长切分按序发多条）。
群聊按 ``groupName`` 寻址，私聊按 ``sendToUser`` 寻址。
"""

from __future__ import annotations

import json

import httpx
import pytest

from plugins.platforms.digital_employee.push import (
    MAX_SEND_CONTENT,
    WeChatPushClient,
)


def _client(handler, url: str = "http://push.example/api", key: str = "KID") -> WeChatPushClient:
    return WeChatPushClient(
        url=url, key_id=key, client=httpx.AsyncClient(transport=httpx.MockTransport(handler))
    )


def _ok(result: str = "mid-1") -> httpx.Response:
    return httpx.Response(200, json={"code": "0", "message": "执行成功", "result": result})


def test_disabled_without_url_or_key():
    assert WeChatPushClient(url="", key_id="K").enabled is False
    assert WeChatPushClient(url="http://x", key_id="").enabled is False
    assert WeChatPushClient(url="http://x", key_id="K").enabled is True


@pytest.mark.asyncio
async def test_group_message_matches_the_reference_payload():
    seen: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen.update(json.loads(request.content))
        return _ok()

    result = await _client(handler).send_group(group_name="运维群", content="hello", at_users="a,b")
    assert result.ok is True
    assert seen == {
        "keyId": "KID",
        "sendContent": "hello",
        "groupName": "运维群",
        "atUserList": ["a", "b"],
    }


@pytest.mark.asyncio
async def test_authorization_header_carries_the_raw_key():
    """接口规范：Header 带 Authorization；平台侧口径 = 裸 keyId，无 Bearer 前缀。"""
    seen: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["auth"] = request.headers.get("Authorization")
        return _ok()

    result = await _client(handler, key="my-key-123").send_private(user_name="zhangsan(张三)", content="hi")
    assert result.ok is True
    assert seen["auth"] == "my-key-123"
    assert "Bearer" not in str(seen["auth"])


@pytest.mark.asyncio
async def test_private_message_uses_send_to_user():
    seen: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen.update(json.loads(request.content))
        return _ok()

    result = await _client(handler).send_private(user_name="zhangsan(张三)", content="hi")
    assert result.ok is True
    assert seen == {"keyId": "KID", "sendContent": "hi", "sendToUser": "zhangsan(张三)"}


@pytest.mark.asyncio
async def test_empty_at_list_is_sent_as_empty_array():
    """参考脚本对无 @ 的群消息也传 ``[]``，接口按此契约。"""
    seen: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen.update(json.loads(request.content))
        return _ok()

    await _client(handler).send_group(group_name="g", content="x")
    assert seen["atUserList"] == []


@pytest.mark.asyncio
async def test_business_code_zero_is_success_with_result_message_id():
    def handler(request: httpx.Request) -> httpx.Response:
        return _ok(result="d7d7b908e17a")

    result = await _client(handler).send_group(group_name="g", content="x")
    assert result.ok is True
    assert result.message_id == "d7d7b908e17a"


@pytest.mark.asyncio
async def test_success_without_result_yields_empty_message_id():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"code": "0", "message": "执行成功", "result": None})

    result = await _client(handler).send_group(group_name="g", content="x")
    assert result.ok is True
    assert result.message_id == ""


@pytest.mark.asyncio
async def test_invalid_key_code_nine_fails_without_retry():
    """code=9（当前key无效）是业务/凭证错误：不重试。"""
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(200, json={"code": "9", "message": "当前key无效", "result": None})

    result = await _client(handler).send_group(group_name="g", content="x")
    assert result.ok is False
    assert calls["n"] == 1
    assert "9" in result.error and "当前key无效" in result.error


@pytest.mark.asyncio
async def test_missing_param_code_three_fails_without_retry():
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(200, json={"code": "3", "message": "参数缺失", "result": None})

    result = await _client(handler).send_group(group_name="g", content="x")
    assert result.ok is False
    assert calls["n"] == 1
    assert "3" in result.error


@pytest.mark.asyncio
async def test_token_expired_code_five_retries_once():
    """code=5（token过期）按文档可重试：恰好重试 1 次（共 2 次请求）。"""
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(200, json={"code": "5", "message": "token过期", "result": None})

    result = await _client(handler).send_group(group_name="g", content="x")
    assert result.ok is False
    assert calls["n"] == 2


@pytest.mark.asyncio
async def test_token_expired_retry_can_succeed():
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(200, json={"code": "5", "message": "token过期", "result": None})
        return _ok(result="after-retry")

    result = await _client(handler).send_group(group_name="g", content="x")
    assert result.ok is True
    assert result.message_id == "after-retry"
    assert calls["n"] == 2


@pytest.mark.asyncio
async def test_http_5xx_is_retried_then_reported():
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(500, text="boom")

    result = await _client(handler).send_group(group_name="g", content="x")
    assert result.ok is False
    assert calls["n"] == 2
    assert "500" in result.error


@pytest.mark.asyncio
async def test_http_4xx_fails_without_retry():
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(400, text="bad")

    result = await _client(handler).send_group(group_name="g", content="x")
    assert result.ok is False
    assert calls["n"] == 1
    assert "400" in result.error


@pytest.mark.asyncio
async def test_non_json_success_body_fails():
    """HTTP 200 但响应体不是文档约定的 JSON：按失败处理，不重试。"""
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(200, text="ok")

    result = await _client(handler).send_group(group_name="g", content="x")
    assert result.ok is False
    assert calls["n"] == 1
    assert "not JSON" in result.error


@pytest.mark.asyncio
async def test_network_error_is_retried_and_returned_not_raised():
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        raise httpx.ConnectError("refused")

    result = await _client(handler).send_group(group_name="g", content="x")
    assert result.ok is False
    assert calls["n"] == 2
    assert "failed" in result.error


@pytest.mark.asyncio
async def test_oversized_content_is_split_into_limited_chunks():
    """sendContent 终端限制 20000 字符：超长内容切多条按序发送。"""
    payloads: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        payloads.append(json.loads(request.content))
        return _ok(result=f"mid-{len(payloads)}")

    content = "x" * (MAX_SEND_CONTENT * 2 + 5000)  # 45000 字符 → 至少 3 片
    result = await _client(handler).send_group(group_name="g", content=content)
    assert result.ok is True
    assert len(payloads) >= 3
    assert all(len(p["sendContent"]) <= MAX_SEND_CONTENT for p in payloads)
    assert all(p["groupName"] == "g" for p in payloads)
    assert result.message_id == "mid-3"


@pytest.mark.asyncio
async def test_multi_chunk_partial_failure_reports_chunk_position():
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return _ok()
        return httpx.Response(200, json={"code": "3", "message": "参数缺失", "result": None})

    content = "x" * (MAX_SEND_CONTENT * 2 + 5000)
    result = await _client(handler).send_group(group_name="g", content=content)
    assert result.ok is False
    assert "chunk 2/" in result.error
    assert calls["n"] == 2  # 第 2 片 code=3 不重试


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
