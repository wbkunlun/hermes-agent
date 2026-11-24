"""企微「数字员工」平台 —— 出站消息组装（hermes → platform）。

平台的响应形状固定为 ``{"message_list": [...]}``，元素：

.. code-block:: json

    {"type": "txt",   "content": "..."}

**类型名是 ``txt`` 不是 ``text``**——写成 ``text`` 平台会静默忽略这一条（文档
§2.4.3 的显式警告）。

分片沿用网关统一的 Markdown 感知切分（``chunk_markdown_message``）：围栏会闭合
重开、表格重复表头，不会把一段代码切成两块无头无尾的碎片。多片 = ``message_list``
多个 ``txt`` 元素，按序推送。
"""

from __future__ import annotations

import logging
import mimetypes
import os
from dataclasses import dataclass, field
from typing import Any, Iterable, Optional

logger = logging.getLogger(__name__)

TYPE_TEXT = "txt"
TYPE_IMAGE = "image"
TYPE_FILE = "file"
TYPE_VIDEO = "video"
TYPE_CARD = "card"

_IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp"}
_VIDEO_EXTS = {".mp4", ".mov", ".m4v"}
_AUDIO_EXTS = {".mp3", ".wav", ".amr", ".m4a", ".ogg"}


@dataclass
class OutboundMessage:
    """一条出站消息（已翻译成平台类型）。"""

    type: str
    content: str = ""
    title: str = ""
    sub_title: str = ""
    url: str = ""
    logo_url: str = ""

    def to_wire(self) -> dict[str, Any]:
        if self.type == TYPE_CARD:
            item: dict[str, Any] = {"type": TYPE_CARD, "title": self.title, "url": self.url}
            if self.sub_title:
                item["subTitle"] = self.sub_title
            if self.logo_url:
                item["logoUrl"] = self.logo_url
            return item
        return {"type": self.type, "content": self.content}


@dataclass
class ResponsePayload:
    """``/chat`` 的完整响应体。"""

    messages: list[OutboundMessage] = field(default_factory=list)

    def to_wire(self) -> dict[str, Any]:
        return {"message_list": [m.to_wire() for m in self.messages]}

    def is_empty(self) -> bool:
        return not self.messages


def media_kind_for_path(path: str) -> str:
    """按扩展名把本地文件映射到平台的出站类型。"""
    ext = os.path.splitext(str(path or ""))[1].lower()
    if ext in _IMAGE_EXTS:
        return TYPE_IMAGE
    if ext in _VIDEO_EXTS:
        return TYPE_VIDEO
    return TYPE_FILE


def guess_content_type(path: str) -> str:
    return mimetypes.guess_type(str(path or ""))[0] or "application/octet-stream"


def split_text(text: str, limit: int) -> list[str]:
    """把回复文本切成不超过 ``limit`` 的若干片（Markdown 感知）。"""
    body = str(text or "")
    if not body.strip():
        return []
    if limit <= 0 or len(body) <= limit:
        return [body]
    from gateway.platforms.base_chunking import chunk_markdown_message

    chunks = chunk_markdown_message(body, limit)
    return [c for c in chunks if c.strip()] or [body]


def text_messages(text: str, limit: int) -> list[OutboundMessage]:
    """回复正文 → 一串 ``txt`` 元素。空回复返回空列表（调用方负责兜底文案）。"""
    return [OutboundMessage(type=TYPE_TEXT, content=chunk) for chunk in split_text(text, limit)]


def card_message(*, title: str, url: str, sub_title: str = "", logo_url: str = "") -> OutboundMessage:
    return OutboundMessage(type=TYPE_CARD, title=title, url=url, sub_title=sub_title, logo_url=logo_url)


def empty_reply_notice() -> OutboundMessage:
    """agent 返回空内容时的兜底——平台对空 ``message_list`` 的处理不可控，
    与其让用户看到空白，不如明确说一句。"""
    return OutboundMessage(type=TYPE_TEXT, content="（这一轮没有可展示的内容）")


def build_response(
    text: str,
    *,
    chunk_limit: int,
    cards: Optional[Iterable[OutboundMessage]] = None,
) -> ResponsePayload:
    """组装最终响应：正文分片在前，媒体与卡片依序追加。

    """
    messages: list[OutboundMessage] = text_messages(text, chunk_limit)
    for card in cards or ():
        messages.extend((card,))
    if not messages:
        messages.append(empty_reply_notice())
    return ResponsePayload(messages=messages)
