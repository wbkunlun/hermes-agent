"""Tests for the audit-callback plugin (Agent Execution Audit API client).

The plugin reports completed tool calls as ``IngestSingleRequest`` records to
``POST /api/v1/agent/audit``. It lives in a hyphenated directory
(``plugins/audit-callback/``) which is not a valid package name, so we load it
via ``importlib`` (like the plugin loader does), fresh per test to isolate the
module-level queue / worker / drop-counter state.
"""

import importlib.util
import json
from pathlib import Path

import pytest

_PLUGIN_PATH = (
    Path(__file__).resolve().parents[1]
    / "plugins" / "audit-callback" / "__init__.py"
)


@pytest.fixture
def plugin(monkeypatch):
    """Load the plugin fresh and keep the real worker thread from starting."""
    monkeypatch.setenv("HERMES_AUDIT_CALLBACK_URL", "http://audit/api/v1/agent/audit")
    spec = importlib.util.spec_from_file_location("audit_callback_under_test", _PLUGIN_PATH)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    monkeypatch.setattr(m, "_ensure_worker", lambda: None)
    return m


class _FakeResp:
    def __init__(self, status_code):
        self.status_code = status_code


class _FakeClient:
    """Records POSTs; can be programmed to fail N times before succeeding."""

    def __init__(self, fail_then=None):
        self.calls = []
        self._fail_then = fail_then or []  # list of exceptions to raise in order

    def post(self, url, content=None, headers=None):
        self.calls.append((url, content, headers))
        if self._fail_then:
            raise self._fail_then.pop(0)
        return _FakeResp(201)


# ---------------------------------------------------------------------------
# Activation gate
# ---------------------------------------------------------------------------

class TestActivation:
    def test_noop_when_url_unset(self, plugin, monkeypatch):
        monkeypatch.delenv("HERMES_AUDIT_CALLBACK_URL", raising=False)
        monkeypatch.setattr(plugin, "_classify_terminal", lambda c: ("dangerous", "x", ["r"]))
        assert plugin._on_post_tool_call(
            tool_name="terminal", args={"command": "rm -rf /"}, result="{}", status="ok"
        ) is None
        assert plugin._WORKER_STARTED is False
        assert plugin._QUEUE.empty()


# ---------------------------------------------------------------------------
# Event selection (what gets reported)
# ---------------------------------------------------------------------------

class TestEventSelection:
    def test_dangerous_command_reported_as_command(self, plugin, monkeypatch):
        monkeypatch.setattr(plugin, "_classify_terminal",
                            lambda c: ("dangerous", "recursive delete", ["destructive:rm_rf"]))
        plugin._on_post_tool_call(
            tool_name="terminal",
            args={"command": "rm -rf build/", "workdir": "/home/app"},
            result=json.dumps({"exit_code": 0, "output": "done", "cwd": "/home/app"}),
            status="ok", duration_ms=1200,
            tool_call_id="tc1", turn_id="t1",
        )
        body = plugin._QUEUE.get_nowait()
        assert body["event_type"] == "command"
        assert body["action"] == "command.exec"
        assert body["risk_level"] == "high"
        assert body["risk_reason"] == "recursive delete"
        assert body["execution_id"] == "tc1"
        assert body["decision"] == "allowed"
        assert body["result"] == "success"
        assert body["exit_code"] == 0
        assert body["duration_ms"] == 1200
        assert body["payload"]["command"] == "rm -rf build/"
        assert body["payload"]["cwd"] == "/home/app"
        assert body["payload"]["matched_rules"] == ["destructive:rm_rf"]
        assert body["payload"]["exit_code"] == 0
        # stdout sha + preview populated from the result
        assert body["payload"]["stdout_sha256"]
        assert body["payload"]["stdout_preview"] == "done"
        assert body["event_id"]  # client-generated, stable for retries
        assert body["event_time"]

    def test_hardline_is_critical(self, plugin, monkeypatch):
        monkeypatch.setattr(plugin, "_classify_terminal",
                            lambda c: ("hardline", "format filesystem (mkfs)", ["hardline:mkfs"]))
        # Hardline commands are blocked by the guard → blocked decision.
        plugin._on_post_tool_call(
            tool_name="terminal", args={"command": "mkfs.ext4 /dev/sda1"},
            result=json.dumps({"error": "BLOCKED (hardline): mkfs"}),
            status="blocked", duration_ms=5,
        )
        body = plugin._QUEUE.get_nowait()
        assert body["risk_level"] == "critical"
        assert body["decision"] == "blocked"
        assert body["result"] == "error"

    def test_skill_manage_always_reported(self, plugin):
        plugin._on_post_tool_call(
            tool_name="skill_manage",
            args={"action": "install", "name": "deploy-openclaw", "version": "1.2.0", "force": True},
            result=json.dumps({"exit_code": 0}),
            status="ok", duration_ms=54000,
            tool_call_id="tc9",
        )
        body = plugin._QUEUE.get_nowait()
        assert body["event_type"] == "skill"
        assert body["action"] == "skill.invoke"
        assert body["risk_level"] == "medium"
        assert body["skill_name"] == "deploy-openclaw"
        assert body["skill_version"] == "1.2.0"
        assert body["payload"]["destructive"] is True
        assert body["payload"]["summary"] == "install deploy-openclaw"
        assert body["payload"]["caller_type"] == "agent"

    def test_benign_command_not_reported_by_default(self, plugin, monkeypatch):
        monkeypatch.setattr(plugin, "_classify_terminal", lambda c: ("info", "", []))
        plugin._on_post_tool_call(
            tool_name="terminal", args={"command": "ls"}, result=json.dumps({"exit_code": 0}),
            status="ok", duration_ms=10,
        )
        assert plugin._QUEUE.empty()

    def test_report_all_commands_opt_in(self, plugin, monkeypatch):
        monkeypatch.setenv("HERMES_AUDIT_REPORT_ALL_COMMANDS", "1")
        monkeypatch.setattr(plugin, "_classify_terminal", lambda c: ("info", "", []))
        plugin._on_post_tool_call(
            tool_name="terminal", args={"command": "ls"}, result=json.dumps({"exit_code": 0}),
            status="ok", duration_ms=10,
        )
        body = plugin._QUEUE.get_nowait()
        assert body["risk_level"] == "info"


# ---------------------------------------------------------------------------
# Decision / result inference
# ---------------------------------------------------------------------------

class TestDecisionInference:
    def test_nonzero_exit_is_allowed_error(self, plugin, monkeypatch):
        monkeypatch.setattr(plugin, "_classify_terminal", lambda c: ("dangerous", "x", ["r"]))
        plugin._on_post_tool_call(
            tool_name="terminal", args={"command": "rm -rf x"},
            result=json.dumps({"exit_code": 2, "error": "no such file"}),
            status="ok", duration_ms=10,
        )
        body = plugin._QUEUE.get_nowait()
        assert body["decision"] == "allowed"
        assert body["result"] == "error"

    def test_blocked_status_is_blocked(self, plugin, monkeypatch):
        monkeypatch.setattr(plugin, "_classify_terminal", lambda c: ("dangerous", "x", ["r"]))
        plugin._on_post_tool_call(
            tool_name="terminal", args={"command": "rm -rf x"},
            result="BLOCKED: not in allowlist", status="blocked", duration_ms=1,
        )
        body = plugin._QUEUE.get_nowait()
        assert body["decision"] == "blocked"


# ---------------------------------------------------------------------------
# Auth header
# ---------------------------------------------------------------------------

class TestAuth:
    def test_control_plane_auth_passed_raw(self, plugin, monkeypatch):
        monkeypatch.setenv("CONTROL_PLANE_AUTH", "Bearer <jwt>")
        assert plugin._auth_header() == "Bearer <jwt>"

    def test_legacy_token_wrapped_as_bearer(self, plugin, monkeypatch):
        monkeypatch.delenv("CONTROL_PLANE_AUTH", raising=False)
        monkeypatch.setenv("HERMES_AUDIT_TOKEN", "tok123")
        assert plugin._auth_header() == "Bearer tok123"

    def test_no_credential_empty(self, plugin, monkeypatch):
        monkeypatch.delenv("CONTROL_PLANE_AUTH", raising=False)
        monkeypatch.delenv("HERMES_AUDIT_TOKEN", raising=False)
        assert plugin._auth_header() == ""


# ---------------------------------------------------------------------------
# Robustness + wire format
# ---------------------------------------------------------------------------

class TestRobustness:
    def test_hook_swallows_errors(self, plugin, monkeypatch):
        monkeypatch.setattr(plugin, "_classify_terminal", lambda c: (_ for _ in ()).throw(RuntimeError("boom")))
        # Must not propagate.
        assert plugin._on_post_tool_call(
            tool_name="terminal", args={"command": "ls"}, result="{}", status="ok"
        ) is None

    def test_enqueue_drops_on_full_queue(self, plugin):
        while not plugin._QUEUE.full():
            plugin._QUEUE.put({"event_id": "fill"}, block=False)
        before = plugin._dropped_count()
        plugin._enqueue({"event_id": "overflow"})
        assert plugin._dropped_count() == before + 1

    def test_post_one_serializes_body_with_headers(self, plugin):
        client = _FakeClient()
        body = {"event_id": "e1", "event_type": "command", "n": 1}
        plugin._post_one(client, "http://audit/api/v1/agent/audit",
                         {"Content-Type": "application/json", "Authorization": "Bearer t"}, body)
        url, content, headers = client.calls[0]
        assert url.endswith("/api/v1/agent/audit")
        assert json.loads(content) == body
        assert headers["Authorization"] == "Bearer t"

    def test_post_with_retry_retries_transient_then_stops(self, plugin, monkeypatch):
        import httpx
        # time is a module-level import in the plugin; patch sleep to be instant.
        monkeypatch.setattr(plugin.time, "sleep", lambda *_: None)
        client = _FakeClient(fail_then=[httpx.ConnectError("down"), httpx.ConnectError("down")])
        plugin._post_with_retry(client, "http://audit/x", {}, {"event_id": "e1"})
        # 2 transient failures → 3 attempts (2 retries), last returns 201
        assert len(client.calls) == 3

    def test_post_with_retry_no_retry_on_4xx(self, plugin):
        client = _FakeClient()
        # _FakeClient returns 201 by default; simulate 4xx via a tailored client
        client4 = type("C", (), {
            "calls": [],
            "post": lambda self, url, content=None, headers=None:
                (self.calls.append((url, content, headers)), _FakeResp(409))[1],
        })()
        plugin._post_with_retry(client4, "http://audit/x", {}, {"event_id": "e1"})
        assert len(client4.calls) == 1  # 409 duplicate → no retry
