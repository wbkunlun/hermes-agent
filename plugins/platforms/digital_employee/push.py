"""企微「数字员工」平台 —— 主动发消息（hermes → 企微）。

与入站响应（``message_list`` 回给平台）不同，这里是**我们自己**去调企微发消息
API，用于同步请求已经超时、最终答复迟到的场景。

协议照 ``wechat_group.py`` 的 ``_send_wechat_msg`` / ``send_message`` /
``send_private_message``：``POST {apiUrl}``，body 带 ``keyId``，只看 HTTP 200，
不解析业务错误码（参考脚本也不解析——200 即视为已受理）。

群聊::

    {"keyId": "...", "sendContent": "...", "groupName": "<群名>", "atUserList": [...]}

私聊::

    {"keyId": "...", "sendContent": "...", "sendToUser": "english(chinese)"}

``apiUrl`` 与 ``keyId`` 都可配；未配齐即关闭，超时降级退化为只回提示语。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

try:
    import httpx

    HTTPX_AVAILABLE = True
except ImportError:  # pragma: no cover - httpx 是网关运行期依赖
    httpx = None  # type: ignore[assignment]
    HTTPX_AVAILABLE = False

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 30.0


@dataclass(frozen=True)
class PushResult:
    ok: bool
    error: str = ""

    def describe(self) -> str:
        return "sent" if self.ok else (self.error or "failed")


def _at_list(at_users: str) -> list[str]:
    """``"a,b"`` → ``["a", "b"]``；空串 → ``[]``（参考脚本同样传空列表）。"""
    return [u.strip() for u in str(at_users or "").split(",") if u.strip()]


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

    async def _post(self, payload: dict) -> PushResult:
        if not self.enabled:
            missing = [n for n, v in (("pushUrl", self._url), ("pushKeyId", self._key_id)) if not v]
            return PushResult(ok=False, error="push disabled" + (f" (missing {', '.join(missing)})" if missing else ""))
        owns = self._client is None
        client = self._client or httpx.AsyncClient(timeout=self._timeout, follow_redirects=True)
        try:
            resp = await client.post(self._url, json=payload)
        except (httpx.HTTPError, OSError) as exc:
            # 网络异常（连接/超时/DNS/TLS）→ 投递失败，不向上抛：迟到答复的推送
            # 不能把异常带进 turn 收尾路径。
            return PushResult(ok=False, error=f"push request failed: {exc}")
        finally:
            if owns:
                await client.aclose()
        if resp.status_code != 200:
            return PushResult(ok=False, error=f"push HTTP {resp.status_code}: {resp.text[:200]}")
        return PushResult(ok=True)

    async def send_group(self, *, group_name: str, content: str, at_users: str = "") -> PushResult:
        if not group_name:
            return PushResult(ok=False, error="groupName is required")
        return await self._post(
            self._payload(content, groupName=group_name, atUserList=_at_list(at_users))
        )

    async def send_private(self, *, user_name: str, content: str) -> PushResult:
        if not user_name:
            return PushResult(ok=False, error="sendToUser is required")
        return await self._post(self._payload(content, sendToUser=user_name))
