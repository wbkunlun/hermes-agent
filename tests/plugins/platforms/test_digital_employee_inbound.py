"""数字员工平台 —— 入站解析（新契约）。

平台侧 Dify 模板的实际形态（见设计文档 §2.1）：

* 正文在 ``question``（不是 ``query``）；
* ``user_name`` 里装的是 **用户 id**（模板取 ``sys.user_id``）；
* ``group_name`` 非空即群聊；
* ``service_name`` 是数字员工实例名（会话隔离维度）；
* ``query_info`` / ``history_list`` 模板里**没加引号**，到达时是对象数组；
  旧模板（带引号）给出的是 JSON 字符串——两种都要吃。
"""

from __future__ import annotations

import json

from plugins.platforms.digital_employee.inbound import (
    MAX_ID_LEN,
    bound_id,
    build_prompt_text,
    json_list,
    parse_inbound,
)

# 与用户给的真实模板一一对应（展开后的形态）
BODY = {
    "user_name": "zhangsan",
    "group_name": "运维群",
    "question": "这张图里是什么？",
    "service_name": "vivian",
    "reference_info": "",
    "user_type": "0",
    "conversation_type": "GROUP",
    "conversation_id": "conv-1",
    "conversation_name": "运维群(3284275877)",
    "session_id": "sess-1",
    "query_info": [
        {"message_id": "m1", "type": "image", "content": "dep-x/a.jpg", "user_role": "user"},
        {"message_id": "m1", "type": "text", "content": "这张图里是什么？", "user_role": "user"},
    ],
    "history_list": [
        {"role": "user", "content": "上一轮问题"},
        {"role": "assistant", "content": "上一轮回答"},
    ],
}


def test_question_field_is_the_prompt_body():
    msg = parse_inbound(BODY)
    assert msg is not None
    assert msg.question == "这张图里是什么？"


def test_legacy_query_field_still_accepted():
    body = dict(BODY, question="")
    body["query"] = "旧字段"
    assert parse_inbound(body).question == "旧字段"


def test_user_name_carries_the_user_id():
    msg = parse_inbound(BODY)
    assert msg.user_id == "zhangsan"


def test_group_name_marks_a_group_chat():
    assert parse_inbound(BODY).is_group is True
    single = dict(BODY, group_name="", conversation_type="SINGLE")
    assert parse_inbound(single).is_group is False


def test_query_info_as_array_is_parsed():
    msg = parse_inbound(BODY)
    assert [p.type for p in msg.parts] == ["image", "text"]
    assert msg.message_id == "m1"


def test_query_info_as_json_string_is_parsed():
    """旧模板（带引号的变量）到达时是字符串。"""
    body = dict(BODY, query_info=json.dumps(BODY["query_info"]), history_list=json.dumps(BODY["history_list"]))
    msg = parse_inbound(body)
    assert [p.type for p in msg.parts] == ["image", "text"]
    assert msg.history[0] == {"role": "user", "content": "上一轮问题"}


def test_broken_query_info_does_not_drop_the_question():
    body = dict(BODY, query_info="{not json")
    msg = parse_inbound(body)
    assert msg.parts == ()
    assert msg.question == "这张图里是什么？"


def test_missing_conversation_id_is_rejected():
    body = dict(BODY)
    body.pop("conversation_id")
    assert parse_inbound(body) is None


def test_service_name_is_kept_for_session_isolation():
    assert parse_inbound(BODY).service_name == "vivian"


def test_media_parts_expose_cos_ids():
    msg = parse_inbound(BODY)
    assert msg.has_media is True
    assert msg.media_parts[0].content == "dep-x/a.jpg"


def test_bounded_id_keeps_distinct_long_values_distinct():
    a = "x" * (MAX_ID_LEN + 50) + "A"
    b = "x" * (MAX_ID_LEN + 50) + "B"
    assert bound_id(a) != bound_id(b)
    assert len(bound_id(a)) == MAX_ID_LEN


def test_dedup_key_prefers_message_id_then_body_hash():
    assert parse_inbound(BODY).dedup_key() == "msg:m1"
    no_parts = dict(BODY, query_info=[])
    assert parse_inbound(no_parts).dedup_key().startswith("body:")


def test_reference_info_is_prefixed():
    msg = parse_inbound(dict(BODY, reference_info="被引用的那段话"))
    text = build_prompt_text(msg)
    assert text.startswith("【引用内容】被引用的那段话【引用结束】")


def test_media_parts_become_placeholders():
    assert "[图片]" in build_prompt_text(parse_inbound(BODY))


def test_json_list_accepts_every_shape():
    assert json_list([{"a": 1}]) == [{"a": 1}]
    assert json_list('[{"a": 1}]') == [{"a": 1}]
    assert json_list({"a": 1}) == [{"a": 1}]
    assert json_list("") == []
    assert json_list("{bad") == []
