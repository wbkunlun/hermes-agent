"""企微「数字员工」平台 —— 入站请求解析（platform → hermes）。

平台侧（Dify HTTP 节点）把变量模板展开成 JSON POST 过来，实际字段取决于
Dify 里配的模板，例如::

    {
      "user_name": "{{#sys.user_id#}}",
      "group_name": "{{#…#group_chat_name#}}",
      "question": "{{#…#query#}}",
      "service_name": "vivian",
      "reference_info": "{{#…#reference_info#}}",
      "user_type": "{{#…#user_type#}}",
      "conversation_type": "{{#…#conversation_type#}}",
      "conversation_id": "{{#…#conversation_id#}}",
      "conversation_name": "{{#…#conversation_name#}}",
      "session_id": "{{#…#session_id#}}",
      "query_info": {{#…#query_info#}},
      "history_list": {{#…#history_list#}}
    }

三个要点（都与旧版不同，改动时别改回去）：

* 正文在 **``question``**，不是 ``query``。
* ``query_info`` / ``history_list`` 模板里**没有加引号**，所以到达时通常是真正的
  **对象数组**；但旧模板（带引号）会给出 JSON **字符串**。两种都要接受。
* ``user_name`` 里装的其实是 **用户 id**（模板取的是 ``sys.user_id``），真正的
  显示名在 ``conversation_name`` / ``group_name`` 里；``user_name`` 为空时退回它。

这里把原始 dict 变成冻结的 dataclass，解析只发生在一处；坏字段各自降级，
不让整条消息消失。
"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass, field
from typing import Any, Optional

logger = logging.getLogger(__name__)

# 网关的持久化 admission 契约 (hermes_state_runtime._text) 拒绝空串、非 str
# 或 >1024 字符的标识符；超长会让整条入站 admission 失败而消息静默丢失。
MAX_ID_LEN = 256

CONTENT_TEXT = "text"
CONTENT_IMAGE = "image"
CONTENT_FILE = "file"


def json_list(raw: Any) -> list:
    """接受三种形态并统一成 list：真数组、JSON 字符串（旧模板）、空值。

    解析失败降级为空列表——一个附件字段坏掉不该让文本部分也跟着丢。
    """
    if raw is None:
        return []
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        return [raw]
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
        except (ValueError, TypeError):
            logger.warning("digital_employee: unparseable JSON field (len=%d)", len(text))
            return []
        if isinstance(parsed, list):
            return parsed
        if isinstance(parsed, dict):
            return [parsed]
    return []


def bound_id(value: Any) -> str:
    """约束 wire 标识符：超长时保留前缀 + 全串哈希，避免不同长 id 塌缩成同一个。"""
    raw = str(value or "").strip()
    if len(raw) <= MAX_ID_LEN:
        return raw
    digest = hashlib.sha256(raw.encode("utf-8", "replace")).hexdigest()[:12]
    return raw[: MAX_ID_LEN - 13] + "#" + digest


@dataclass(frozen=True)
class QueryPart:
    """``query_info`` 的一个子元素。"""

    message_id: str
    type: str
    content: str
    user_role: str = ""
    message_time: str = ""


@dataclass(frozen=True)
class InboundMessage:
    """一条来自数字员工平台的请求，已结构化。"""

    question: str
    parts: tuple[QueryPart, ...]
    history: tuple[dict[str, Any], ...]
    reference_info: str
    user_id: str
    user_type: str
    group_name: str
    service_name: str
    conversation_id: str
    conversation_type: str
    conversation_name: str
    session_id: str
    trace_id: str
    raw: dict[str, Any] = field(default_factory=dict, repr=False, compare=False)

    # ── 派生属性 ──────────────────────────────────────────────
    @property
    def is_group(self) -> bool:
        """群聊与否：``group_name`` 非空是最可靠的信号（单聊不会带群名）。"""
        return bool(self.group_name.strip())

    @property
    def chat_name(self) -> str:
        return (self.group_name or self.conversation_name or self.conversation_id).strip()

    @property
    def chat_type(self) -> str:
        return "group" if self.is_group else "dm"

    @property
    def message_id(self) -> str:
        """本轮消息 id：取第一个子元素的 ``message_id``（同一用户消息的子元素共享）。"""
        for part in self.parts:
            if part.message_id:
                return part.message_id
        return ""

    @property
    def media_parts(self) -> list[QueryPart]:
        return [p for p in self.parts if p.type in (CONTENT_IMAGE, CONTENT_FILE) and p.content]

    @property
    def has_media(self) -> bool:
        return bool(self.media_parts)

    def dedup_key(self) -> str:
        """幂等键：优先本轮 ``message_id``，其次内容哈希（同 body 5 分钟内只处理一次）。"""
        msg_id = self.message_id
        if msg_id:
            return f"msg:{msg_id}"
        try:
            payload = json.dumps(self.raw, sort_keys=True, ensure_ascii=False, default=str)
        except (TypeError, ValueError):
            payload = repr(self.raw)
        return "body:" + hashlib.sha256(payload.encode("utf-8", "replace")).hexdigest()

    def log_fields(self) -> dict[str, str]:
        """进 INFO 日志的字段。正文只记长度，不记内容（HX017）。"""
        return {
            "trace_id": self.trace_id,
            "service_name": self.service_name,
            "conversation_id": self.conversation_id,
            "user_id": self.user_id,
            "group": "1" if self.is_group else "0",
            "question_len": str(len(self.question)),
            "parts": str(len(self.parts)),
        }


def _part_from(raw: Any) -> Optional[QueryPart]:
    if not isinstance(raw, dict):
        return None
    type_name = str(raw.get("type") or CONTENT_TEXT).strip().lower() or CONTENT_TEXT
    # 平台侧 query_info 的正文类型是 "text"（回复类型才是 "txt"），两者都归一成 text。
    if type_name == "txt":
        type_name = CONTENT_TEXT
    return QueryPart(
        message_id=bound_id(raw.get("message_id")),
        type=type_name,
        content=str(raw.get("content") or ""),
        user_role=str(raw.get("user_role") or ""),
        message_time=str(raw.get("message_time") or ""),
    )


def _history_from(raw: Any) -> tuple[dict[str, Any], ...]:
    entries: list[dict[str, Any]] = []
    for item in json_list(raw):
        if not isinstance(item, dict):
            continue
        role = str(item.get("role") or "").strip()
        content = item.get("content")
        if not role or content is None:
            continue
        entries.append({"role": role, "content": str(content)})
    return tuple(entries)


def parse_inbound(body: dict[str, Any], *, trace_id: str = "") -> Optional[InboundMessage]:
    """解析平台请求体；结构不合法时返回 ``None``。

    唯一硬性要求是 ``conversation_id``：它是会话键、出站目标与群里回复的定位依据。
    """
    if not isinstance(body, dict):
        return None
    conversation_id = bound_id(body.get("conversation_id"))
    if not conversation_id:
        logger.warning("[digital_employee] dropped inbound without conversation_id")
        return None

    parts: list[QueryPart] = []
    for raw_part in json_list(body.get("query_info")):
        part = _part_from(raw_part)
        if part is not None:
            parts.append(part)

    question = str(body.get("question") or body.get("query") or "")
    if not question:
        question = "\n".join(p.content for p in parts if p.type == CONTENT_TEXT and p.content)

    return InboundMessage(
        question=question,
        parts=tuple(parts),
        history=_history_from(body.get("history_list")),
        reference_info=str(body.get("reference_info") or ""),
        # 模板把 sys.user_id 填进了 user_name：这里它就是用户 id。
        user_id=bound_id(body.get("user_name") or body.get("user_id")),
        user_type=str(body.get("user_type") or ""),
        group_name=str(body.get("group_name") or ""),
        service_name=str(body.get("service_name") or ""),
        conversation_id=conversation_id,
        conversation_type=str(body.get("conversation_type_v2") or body.get("conversation_type") or ""),
        conversation_name=str(body.get("conversation_name") or ""),
        session_id=bound_id(body.get("session_id")),
        trace_id=str(trace_id or ""),
        raw=body,
    )


def build_prompt_text(msg: InboundMessage, *, media_note: str = "") -> str:
    """拼出 agent 看到的文本：引用前缀 + 正文 + 附件占位 + 系统提示。"""
    pieces: list[str] = []
    if msg.reference_info:
        pieces.append(f"【引用内容】{msg.reference_info}【引用结束】")
    if msg.question:
        pieces.append(msg.question)
    for part in msg.media_parts:
        pieces.append("[图片]" if part.type == CONTENT_IMAGE else "[文件]")
    text = "\n".join(pieces).strip()
    if media_note:
        text = f"{text}\n\n{media_note}".strip()
    return text or "[空消息]"
