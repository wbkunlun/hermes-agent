"""Fork (2026-09-03): slash-path skill loads must emit a post_tool_call event.

``/skill-name`` and bundle paths call ``skill_view()`` directly (no tool
dispatcher), so the audit-callback plugin would never see the invocation.
``_load_skill_payload`` now emits the same hook event the dispatcher would.
"""

import json

import pytest


@pytest.fixture
def emitted(monkeypatch):
    calls = []

    def fake_emit(**kwargs):
        calls.append(kwargs)

    import model_tools

    monkeypatch.setattr(model_tools, "_emit_post_tool_call_hook", fake_emit)
    return calls


def _patch_skill_view(monkeypatch, payload: dict):
    from tools import skills_tool

    monkeypatch.setattr(
        skills_tool, "skill_view",
        lambda name, task_id=None, preprocess=True: json.dumps(payload),
    )


class TestSkillLoadAuditEmit:
    def test_successful_load_emits_skill_view_event(self, monkeypatch, emitted):
        _patch_skill_view(monkeypatch, {
            "success": True, "name": "deploy-helper", "path": "deploy/main.md",
            "skill_dir": "/opt/data/skills/deploy",
        })
        from agent.skill_commands import _load_skill_payload

        result = _load_skill_payload("deploy-helper", task_id="t1")

        assert result is not None  # load succeeded
        assert len(emitted) == 1
        event = emitted[0]
        assert event["function_name"] == "skill_view"
        assert event["function_args"] == {"name": "deploy-helper"}
        assert event["task_id"] == "t1"
        body = json.loads(event["result"])
        assert body["success"] is True
        assert body["name"] == "deploy-helper"

    def test_failed_load_emits_nothing(self, monkeypatch, emitted):
        _patch_skill_view(monkeypatch, {"success": False, "error": "not found"})
        from agent.skill_commands import _load_skill_payload

        assert _load_skill_payload("missing") is None
        assert emitted == []

    def test_emit_failure_never_breaks_loading(self, monkeypatch, emitted):
        _patch_skill_view(monkeypatch, {"success": True, "name": "x"})
        import model_tools

        def boom(**kwargs):
            raise RuntimeError("hook bus down")

        monkeypatch.setattr(model_tools, "_emit_post_tool_call_hook", boom)
        from agent.skill_commands import _load_skill_payload

        result = _load_skill_payload("x")
        assert result is not None  # load still succeeded despite hook error
