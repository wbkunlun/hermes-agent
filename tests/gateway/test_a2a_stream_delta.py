"""A2A SSE stream delta bridge/sink (fork DPMS 563935).

Unit coverage for the intermediate-stream protocol: statusUpdate frames carrying
``metadata["ai-sandbox/stream"]`` (channel=text|reasoning|tool), seq monotonicity,
closed-bridge semantics, and the sink's no-op behavior without a live reader.
Terminal frames / the plain status_update shape are asserted unchanged.
"""
import json
from types import SimpleNamespace

import pytest

from plugins.platforms.a2a import adapter as a2a_adapter
from plugins.platforms.a2a import protocol


def _payload(raw: str) -> dict:
    """Parse one SSE data chunk into its JSON-RPC envelope result (§9.4)."""
    assert raw.startswith("data: "), raw
    envelope = json.loads(raw[len("data: "):])
    return envelope["result"]


def _meta(raw: str) -> dict:
    su = _payload(raw)["statusUpdate"]
    assert su["status"]["state"] == protocol.STATE_WORKING
    # a2a-go v2.7.0 keeps the EVENT-level metadata only (TaskStatus has no such field).
    return su["metadata"]["ai-sandbox/stream"]


def test_status_update_metadata_is_additive():
    frame = protocol.status_update("task-1", "ctx-1", protocol.STATE_WORKING,
                                   metadata={"ai-sandbox/stream": {"channel": "text", "text": "hi", "seq": 1}})
    su = frame["statusUpdate"]
    assert su["metadata"]["ai-sandbox/stream"] == {"channel": "text", "text": "hi", "seq": 1}
    assert "metadata" not in su["status"]
    # Ordinary frames keep the exact pre-fork shape (no metadata member at all).
    plain = protocol.status_update("task-1", "ctx-1", protocol.STATE_WORKING, "hello")
    assert "metadata" not in plain["statusUpdate"]["status"]
    assert plain["statusUpdate"]["status"]["message"]["parts"][0]["text"] == "hello"


def test_bridge_emits_seq_numbered_enveloped_frames():
    chunks: list[str] = []
    bridge = a2a_adapter.A2AStreamBridge(chunks.append, req_id=7, task_id="task-1", context_id="ctx-1")
    assert bridge.emit("text", {"text": "你"}) is True
    assert bridge.emit("text", {"text": "好"}) is True
    assert [chunk for chunk in chunks] == [
        "data: " + json.dumps({"jsonrpc": "2.0", "id": 7, "result": payload}, ensure_ascii=False) + "\n\n"
        for payload in (_payload(chunks[0]), _payload(chunks[1]))
    ]
    metas = [_meta(chunk) for chunk in chunks]
    assert [meta["seq"] for meta in metas] == [1, 2]
    assert [meta["channel"] for meta in metas] == ["text", "text"]
    assert metas[1]["text"] == "好"


def test_bridge_close_drops_producer_frames_and_write_raises():
    chunks: list[str] = []
    bridge = a2a_adapter.A2AStreamBridge(chunks.append, req_id=None, task_id="t", context_id="c")
    bridge.close()
    assert bridge.emit("text", {"text": "late"}) is False
    assert chunks == []
    with pytest.raises(ConnectionResetError):
        bridge.write("data: x\n\n")


def test_bridge_emit_swallows_broken_pipe_and_marks_closed():
    def boom(_chunk: str) -> None:
        raise BrokenPipeError()

    bridge = a2a_adapter.A2AStreamBridge(boom, req_id=None, task_id="t", context_id="c")
    assert bridge.emit("text", {"text": "x"}) is False
    assert bridge.closed is True


def test_sink_forwards_channels_without_bridge_is_noop():
    chunks: list[str] = []
    holder: dict = {}
    sink = a2a_adapter.A2AStreamSink(SimpleNamespace(_stream_bridges=holder), "ctx-1")
    sink.on_delta("no bridge")          # no live reader: silent no-op
    sink.on_reasoning_delta("…")
    sink.on_tool_event("tool.started", "wfaas")
    assert chunks == []
    holder["ctx-1"] = a2a_adapter.A2AStreamBridge(chunks.append, req_id=9, task_id="task-1", context_id="ctx-1")
    sink.on_delta("答案")
    sink.on_reasoning_delta("思考中")
    sink.on_tool_event("tool.started", "wfaas", "func list")
    sink.on_tool_event("tool.completed", "wfaas")
    sink.on_tool_event("_thinking")     # not a lifecycle event: filtered by the sink contract
    sink.on_delta(None)                 # segment boundary: no frame
    sink.on_delta("")                   # empty: no frame
    metas = [_meta(chunk) for chunk in chunks]
    assert metas[0] == {"channel": "text", "seq": 1, "text": "答案"}
    assert metas[1] == {"channel": "reasoning", "seq": 2, "text": "思考中"}
    assert metas[2] == {"channel": "tool", "seq": 3, "event": "started", "tool": "wfaas", "preview": "func list"}
    assert metas[3] == {"channel": "tool", "seq": 4, "event": "completed", "tool": "wfaas"}
    assert len(metas) == 4
