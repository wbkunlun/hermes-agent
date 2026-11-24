"""数字员工平台 —— 出站组装。

平台的响应类型名是 **``txt`` 而不是 ``text``**（文档明确：写成 ``text`` 下游推送层
会忽略整条），文件的 ``content`` 是 **COS 存储 path** 而不是签名 URL。这两点写错
都不会报错，只会让用户收不到东西——所以必须有测试钉住。
"""

from __future__ import annotations

from plugins.platforms.digital_employee.outbound import (
    TYPE_CARD,
    TYPE_TEXT,
    build_response,
    card_message,
    split_text,
    text_messages,
)


def test_text_type_is_txt_not_text():
    payload = build_response("你好", chunk_limit=4000)
    assert payload.to_wire() == {"message_list": [{"type": "txt", "content": "你好"}]}


def test_long_text_splits_into_multiple_txt_messages():
    body = "段落一。" * 400  # 2000 字符
    msgs = text_messages(body, limit=500)
    assert len(msgs) > 1
    assert all(m.type == TYPE_TEXT for m in msgs)
    assert "".join(m.content for m in msgs).count("段落一") == 400


def test_split_respects_markdown_fences():
    body = "前言\n```python\n" + ("x = 1\n" * 200) + "```\n结尾"
    chunks = split_text(body, limit=300)
    assert len(chunks) > 1
    # 每一片里的围栏都应闭合，不能出现只有开头没有结尾的碎片
    for chunk in chunks:
        assert chunk.count("```") % 2 == 0


def test_empty_text_yields_no_message():
    assert text_messages("", 4000) == []
    assert text_messages("   ", 4000) == []


def test_build_response_is_text_then_cards():
    payload = build_response(
        "看这个",
        chunk_limit=4000,
        cards=[card_message(title="标题", url="https://example.com")],
    )
    wire = payload.to_wire()["message_list"]
    assert [m["type"] for m in wire] == ["txt", "card"]
    assert wire[1]["title"] == "标题"


def test_card_message_omits_empty_optional_fields():
    msg = card_message(title="T", url="u")
    assert msg.to_wire() == {"type": TYPE_CARD, "title": "T", "url": "u"}


def test_build_response_never_returns_an_empty_list():
    payload = build_response("", chunk_limit=4000)
    wire = payload.to_wire()["message_list"]
    assert len(wire) == 1
    assert wire[0]["type"] == "txt"
    assert wire[0]["content"]


def test_upload_notes_are_appended_as_text():
    payload = build_response("正文", chunk_limit=4000)
    payload.messages.extend(text_messages("文件上传失败", 4000))
    assert [m.type for m in payload.messages] == ["txt", "txt"]
