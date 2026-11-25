"""企微「数字员工」平台 —— 主动发消息（hermes → 企微）。

与入站响应（``message_list`` 回给平台）不同，这里是**我们自己**去调平台的
虚拟消息发送接口（``virtualSendMsg``），用于同步请求已经超时、最终答复迟到的场景。

协议按官方接口文档（2026-11-25 版）：

- ``POST {pushUrl}``，JSON body；Header 带 ``Authorization: <keyId>``（裸 keyId、
  无 Bearer 前缀——平台侧确认的口径）；
- 群聊 body：``{"keyId", "sendContent", "groupName", "atUserList"}``；
  私聊：``{"keyId", "sendContent", "sendToUser"}``；
- 响应 ``{"code": "...", "message": "...", "result": "..."}``：**code=="0" 才算成功**
  （成功时 result 是平台消息 id）；非零 code 一律失败，error 带 code/message；
- 重试：HTTP 5xx / token 过期（code=="5"）/ 网络异常 → 重试 1 次；
  参数缺失（code=="3"）等业务错误不重试；
- ``sendContent`` 上限 20000 字符：超长内容复用出站侧的 markdown 感知切分
  （``outbound.split_text``）按序发多条，任一条失败整体算失败。

``pushUrl`` 与 ``keyId`` 都可配；未配齐即关闭，超时降级退化为只回提示语。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Optional

try:
    import httpx

    HTTPX_AVAILABLE = True
except ImportError:  # pragma: no cover - httpx 是网关运行期依赖
    httpx = None  # type: ignore[assignment]
    HTTPX_AVAILABLE = False

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 30.0
# 接口文档：sendContent 终端限制 20000 字符长度。
MAX_SEND_CONTENT = 20000
# 文档重试口径：5xx / token 过期(code="5") 可重试；首试 + 1 次重试。
_RETRYABLE_CODES = {"5"}
_MAX_ATTEMPTS = 2


@dataclass(frozen=True)
class PushResult:
    ok: bool
    error: str = ""
    # 成功时平台返回的消息 id（响应 result 字段）；平台未给则为空串。
    message_id: str = ""

    def describe(self) -> str:
        return "sent" if self.ok else (self.error or "failed")


def _at_list(at_users: str) -> list[str]:
    """``"a,b"`` → ``["a", "b"]``；空串 → ``[]``（参考脚本同样传空列表）。"""
    return [u.strip() for u in str(at_users or "").split(",") if u.strip()]


def _split_content(content: str) -> list[str]:
    """按文档 20000 上限切分；切不开（网关缺位）退化为硬切，保住不超限的契约。"""
    from plugins.platforms.digital_employee.outbound import split_text

    chunks = split_text(content, MAX_SEND_CONTENT)
    if chunks:
        return chunks
    if len(content) <= MAX_SEND_CONTENT:
        return [content]
    return [content[i : i + MAX_SEND_CONTENT] for i in range(0, len(content), MAX_SEND_CONTENT)]


def _parse_response(resp: "httpx.Response") -> Optional[tuple[str, str, Any]]:
    """解析业务响应 ``{"code","message","result"}``；非 JSON / 缺 code 返回 None。"""
    try:
        payload = resp.json()
    except ValueError:
        return None
    if not isinstance(payload, dict) or payload.get("code") is None:
        return None
    return str(payload.get("code")), str(payload.get("message") or ""), payload.get("result")


class WeChatPushClient:
    """企微发消息客户端（群 / 私聊）。"""

    def __init__(
        self,
        *,
        url: str,
        key_id: str,
        client: Optional[httpx.AsyncClient] = None,
        timeout: float = DEFAULT_TIMEOUT,
    ) -> None:
        self._url = (url or "").strip()
        self._key_id = (key_id or "").strip()
        self._client = client
        self._timeout = timeout

    @property
    def enabled(self) -> bool:
        return HTTPX_AVAILABLE and bool(self._url and self._key_id)

    def _payload(self, content: str, **extra) -> dict:
        payload = {"keyId": self._key_id, "sendContent": content}
        payload.update(extra)
        return payload

    def _headers(self) -> dict:
        # 文档接口规范：Header 需带 Authorization；平台侧确认放裸 keyId（无 Bearer 前缀）。
        return {"Authorization": self._key_id}

    async def _post_once(self, payload: dict) -> tuple[PushResult, bool]:
        """发一次请求；返回 ``(结果, 是否可重试)``。失败不向上抛（迟到答复的推送
        不能把异常带进 turn 收尾路径）。"""
        if not self.enabled:
            missing = [n for n, v in (("pushUrl", self._url), ("pushKeyId", self._key_id)) if not v]
            return PushResult(ok=False, error="push disabled" + (f" (missing {', '.join(missing)})" if missing else "")), False
        owns = self._client is None
        client = self._client or httpx.AsyncClient(timeout=self._timeout, follow_redirects=True)
        try:
            resp = await client.post(self._url, json=payload, headers=self._headers())
        except (httpx.HTTPError, OSError) as exc:
            return PushResult(ok=False, error=f"push request failed: {exc}"), True
        finally:
            if owns:
                await client.aclose()
        if resp.status_code >= 500:
            return PushResult(ok=False, error=f"push HTTP {resp.status_code}: {resp.text[:200]}"), True
        if resp.status_code != 200:
            return PushResult(ok=False, error=f"push HTTP {resp.status_code}: {resp.text[:200]}"), False
        parsed = _parse_response(resp)
        if parsed is None:
            return PushResult(ok=False, error=f"push response not JSON: {resp.text[:200]}"), False
        code, message, result_value = parsed
        if code == "0":
            return PushResult(ok=True, message_id=str(result_value or "")), False
        error = f"push code={code}: {message or '执行失败'}"
        return PushResult(ok=False, error=error), code in _RETRYABLE_CODES

    async def _post_with_retry(self, payload: dict) -> PushResult:
        result = PushResult(ok=False, error="push not attempted")
        for attempt in range(_MAX_ATTEMPTS):
            result, retryable = await self._post_once(payload)
            if result.ok or not retryable or attempt == _MAX_ATTEMPTS - 1:
                return result
        return result  # pragma: no cover - 循环必经 return

    async def _send_all(self, extra: dict, content: str) -> PushResult:
        """切分后按序发送；任一分片最终失败即整体失败（error 记位置）。"""
        chunks = _split_content(content)
        last = PushResult(ok=False, error="empty content")
        for index, chunk in enumerate(chunks):
            payload = {"keyId": self._key_id, "sendContent": chunk}
            payload.update(extra)
            last = await self._post_with_retry(payload)
            if not last.ok:
                if len(chunks) > 1:
                    return PushResult(ok=False, error=f"{last.error} (chunk {index + 1}/{len(chunks)})")
                return last
        return last

    async def send_group(self, *, group_name: str, content: str, at_users: str = "") -> PushResult:
        if not group_name:
            return PushResult(ok=False, error="groupName is required")
        return await self._send_all(
            {"groupName": group_name, "atUserList": _at_list(at_users)}, content
        )

    async def send_private(self, *, user_name: str, content: str) -> PushResult:
        if not user_name:
            return PushResult(ok=False, error="sendToUser is required")
        return await self._send_all({"sendToUser": user_name}, content)
