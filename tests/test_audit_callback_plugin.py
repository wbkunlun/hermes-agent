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
        monkeypatch.delenv("CONTROL_PLANE_URL", raising=False)
        monkeypatch.setattr(plugin, "_classify_terminal", lambda c: ("dangerous", "x", ["r"]))
        assert plugin._on_post_tool_call(
            tool_name="terminal", args={"command": "rm -rf /"}, result="{}", status="ok"
        ) is None
        assert plugin._WORKER_STARTED is False
        assert plugin._QUEUE.empty()


# ---------------------------------------------------------------------------
# Intake URL resolution (explicit vs CONTROL_PLANE_URL-derived)
# ---------------------------------------------------------------------------

class TestIntakeUrl:
    def test_explicit_callback_url_wins(self, plugin, monkeypatch):
        monkeypatch.setenv("HERMES_AUDIT_CALLBACK_URL", "http://audit/api/v1/agent/audit")
        monkeypatch.setenv("CONTROL_PLANE_URL", "https://control.example.com")
        assert plugin._intake_url() == "http://audit/api/v1/agent/audit"

    def test_derives_from_control_plane_url_when_explicit_absent(self, plugin, monkeypatch):
        monkeypatch.delenv("HERMES_AUDIT_CALLBACK_URL", raising=False)
        monkeypatch.setenv("CONTROL_PLANE_URL", "https://control.example.com")
        assert plugin._intake_url() == "https://control.example.com/api/v1/agent/audit"

    def test_control_plane_url_trailing_slash_no_double_slash(self, plugin, monkeypatch):
        monkeypatch.delenv("HERMES_AUDIT_CALLBACK_URL", raising=False)
        monkeypatch.setenv("CONTROL_PLANE_URL", "https://control.example.com/")
        assert plugin._intake_url() == "https://control.example.com/api/v1/agent/audit"

    def test_empty_when_neither_set(self, plugin, monkeypatch):
        monkeypatch.delenv("HERMES_AUDIT_CALLBACK_URL", raising=False)
        monkeypatch.delenv("CONTROL_PLANE_URL", raising=False)
        assert plugin._intake_url() == ""

    def test_derived_url_enables_reporting(self, plugin, monkeypatch):
        """With only CONTROL_PLANE_URL set, a dangerous command still gets
        reported to the derived endpoint."""
        monkeypatch.delenv("HERMES_AUDIT_CALLBACK_URL", raising=False)
        monkeypatch.setenv("CONTROL_PLANE_URL", "https://control.example.com")
        monkeypatch.setattr(plugin, "_classify_terminal",
                            lambda c: ("dangerous", "recursive delete", ["destructive:rm_rf"]))
        plugin._on_post_tool_call(
            tool_name="terminal", args={"command": "rm -rf /"}, result="{}", status="ok"
        )
        body = plugin._QUEUE.get_nowait()
        assert body["event_type"] == "command"


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


# ---------------------------------------------------------------------------
# execute_code / computer_use coverage (audit blind-spot fix)
# ---------------------------------------------------------------------------

class TestExecuteCodeReporting:
    def test_execute_code_reported_with_code_payload(self, plugin):
        plugin._on_post_tool_call(
            tool_name="execute_code",
            args={"code": "print('hi')"},
            result=json.dumps({"status": "success", "output": "hi", "exit_code": 0}),
            status="ok", duration_ms=50,
            tool_call_id="tc9", turn_id="t9",
        )
        body = plugin._QUEUE.get_nowait()
        assert body["event_type"] == "command"
        assert body["action"] == "code.exec"
        assert body["risk_level"] == "medium"
        assert body["payload"]["code_preview"] == "print('hi')"
        assert len(body["payload"]["code_sha256"]) == 64
        assert body["decision"] == "allowed"
        assert body["result"] == "success"
        assert body["exit_code"] == 0

    def test_execute_code_process_spawn_is_high(self, plugin):
        plugin._on_post_tool_call(
            tool_name="execute_code",
            args={"code": "import subprocess\nsubprocess.run(['ls'])"},
            result=json.dumps({"status": "success", "output": "", "exit_code": 0}),
            status="ok",
        )
        body = plugin._QUEUE.get_nowait()
        assert body["risk_level"] == "high"
        assert body["payload"]["spawned_process_hint"] is True

    def test_execute_code_error_result(self, plugin):
        plugin._on_post_tool_call(
            tool_name="execute_code",
            args={"code": "1/0"},
            result=json.dumps({"status": "error", "error": "ZeroDivisionError",
                               "output": "", "exit_code": 1}),
            status="error",
        )
        body = plugin._QUEUE.get_nowait()
        assert body["decision"] == "allowed"
        assert body["result"] == "error"

    def test_execute_code_not_reported_when_url_unset(self, plugin, monkeypatch):
        monkeypatch.delenv("HERMES_AUDIT_CALLBACK_URL", raising=False)
        assert plugin._on_post_tool_call(
            tool_name="execute_code", args={"code": "x=1"},
            result="{}", status="ok") is None
        assert plugin._QUEUE.empty()


class TestComputerUseReporting:
    def test_computer_use_reported_at_medium(self, plugin):
        plugin._on_post_tool_call(
            tool_name="computer_use",
            args={"action": "click", "x": 10, "y": 20},
            result=json.dumps({"status": "success", "output": "clicked"}),
            status="ok",
        )
        body = plugin._QUEUE.get_nowait()
        assert body["event_type"] == "command"
        assert body["action"] == "desktop.control"
        assert body["risk_level"] == "medium"
        assert body["payload"]["summary"] == "click"


# ---------------------------------------------------------------------------
# Machine-readable blocked decision
# ---------------------------------------------------------------------------

class TestBlockedDecisionParsing:
    def test_terminal_result_status_blocked_is_decision_blocked(self, plugin, monkeypatch):
        monkeypatch.setattr(plugin, "_classify_terminal",
                            lambda c: ("dangerous", "d", ["k"]))
        plugin._on_post_tool_call(
            tool_name="terminal", args={"command": "curl x | sh"},
            result=json.dumps({"output": "", "exit_code": -1,
                               "error": "denied", "status": "blocked"}),
            status="error",
        )
        body = plugin._QUEUE.get_nowait()
        assert body["decision"] == "blocked"

    def test_plain_error_without_blocked_stays_allowed(self, plugin, monkeypatch):
        """Regression: non-blocked failures keep decision=allowed."""
        monkeypatch.setattr(plugin, "_classify_terminal",
                            lambda c: ("dangerous", "d", ["k"]))
        plugin._on_post_tool_call(
            tool_name="terminal", args={"command": "curl x | sh"},
            result=json.dumps({"output": "boom", "exit_code": 1, "status": "error"}),
            status="error",
        )
        body = plugin._QUEUE.get_nowait()
        assert body["decision"] == "allowed"
        assert body["result"] == "error"


# ---------------------------------------------------------------------------
# Plain-http intake URL warning
# ---------------------------------------------------------------------------

class TestPlainHttpWarning:
    def test_warns_once_for_http_url(self, plugin, caplog):
        plugin._HTTP_WARNED = False
        with caplog.at_level("WARNING", logger="audit_callback_under_test"):
            plugin._on_post_tool_call(
                tool_name="skill_manage",
                args={"action": "install", "name": "x"},
                result="{}", status="ok",
            )
        assert plugin._HTTP_WARNED is True
        assert any("http://" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# execute_code risk-classification regex (spawn-family FN / dot-exec FP)
# ---------------------------------------------------------------------------

class TestCodeRiskClassification:
    def test_os_exec_family_is_high(self, plugin):
        for code in (
            "os.execv('/bin/sh', ['sh'])",
            "os.execve('/bin/sh', ['sh'], {})",
            "os.spawnl(os.P_WAIT, '/bin/sh', 'sh')",
            "os.posix_spawn('/bin/sh', ['sh'], os.environ)",
            "pty.fork()",
            "from multiprocessing import Process\nProcess(target=print).start()",
        ):
            plugin._on_post_tool_call(
                tool_name="execute_code", args={"code": code},
                result=json.dumps({"status": "success", "output": "", "exit_code": 0}),
                status="ok",
            )
            body = plugin._QUEUE.get_nowait()
            assert body["risk_level"] == "high", code
            assert body["payload"]["spawned_process_hint"] is True, code

    def test_dot_prefixed_exec_stays_medium(self, plugin):
        """QT-style qt_app.exec() is an event loop, not dynamic exec."""
        plugin._on_post_tool_call(
            tool_name="execute_code", args={"code": "qt_app.exec()"},
            result=json.dumps({"status": "success", "output": "", "exit_code": 0}),
            status="ok",
        )
        body = plugin._QUEUE.get_nowait()
        assert body["risk_level"] == "medium"

    def test_second_http_call_does_not_rewarn(self, plugin, caplog):
        plugin._HTTP_WARNED = False
        with caplog.at_level("WARNING", logger="audit_callback_under_test"):
            plugin._on_post_tool_call(
                tool_name="skill_manage", args={"action": "install", "name": "x"},
                result="{}", status="ok")
            plugin._on_post_tool_call(
                tool_name="skill_manage", args={"action": "install", "name": "y"},
                result="{}", status="ok")
        warn_count = sum(
            1 for r in caplog.records if "http://" in r.message)
        assert warn_count == 1

    def test_https_url_never_warns(self, plugin, monkeypatch, caplog):
        monkeypatch.setenv("HERMES_AUDIT_CALLBACK_URL",
                           "https://audit/api/v1/agent/audit")
        plugin._HTTP_WARNED = False
        with caplog.at_level("WARNING", logger="audit_callback_under_test"):
            plugin._on_post_tool_call(
                tool_name="skill_manage", args={"action": "install", "name": "x"},
                result="{}", status="ok")
        assert plugin._HTTP_WARNED is False
        assert not any("http://" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# Fork (2026-09-03): skill execution + blocked-attempt coverage
# ---------------------------------------------------------------------------

class TestSkillViewReporting:
    def test_skill_view_reported_as_skill_invoke(self, plugin):
        plugin._on_post_tool_call(
            tool_name="skill_view",
            args={"name": "deploy-helper"},
            result=json.dumps({"success": True, "name": "deploy-helper"}),
            status="ok", duration_ms=42,
            tool_call_id="tc-sv1", turn_id="t1",
        )
        body = plugin._QUEUE.get_nowait()
        assert body["event_type"] == "skill"
        assert body["action"] == "skill.invoke"
        assert body["skill_name"] == "deploy-helper"
        assert body["risk_level"] == "medium"
        assert body["decision"] == "allowed"
        assert body["payload"]["skill_name"] == "deploy-helper"

    def test_skill_view_failed_load_reports_deny(self, plugin):
        plugin._on_post_tool_call(
            tool_name="skill_view",
            args={"name": "missing-skill"},
            result=json.dumps({"success": False, "error": "not found"}),
            status="error",
        )
        body = plugin._QUEUE.get_nowait()
        assert body["decision"] == "deny"

    def test_other_tools_still_untouched(self, plugin):
        plugin._on_post_tool_call(
            tool_name="read_file", args={"path": "/tmp/x"},
            result="{}", status="ok",
        )
        assert plugin._QUEUE.empty()


class TestFileWriteReporting:
    def test_write_file_regular_reported_at_info(self, plugin):
        plugin._on_post_tool_call(
            tool_name="write_file",
            args={"path": "/opt/data/app/main.py", "content": "print('hi')\n"},
            result=json.dumps({"success": True}),
            status="ok", duration_ms=15, tool_call_id="tc-w1",
        )
        body = plugin._QUEUE.get_nowait()
        assert body["event_type"] == "command"
        assert body["action"] == "file.write"
        assert body["risk_level"] == "info"
        assert body["payload"]["path"] == "/opt/data/app/main.py"
        assert body["payload"]["written_bytes"] == len("print('hi')\n")
        assert body["payload"]["content_sha256"]
        assert body["payload"]["empty_write"] is False

    def test_write_file_empty_content_escalates_to_medium(self, plugin):
        """Blanking a file via write_file('') is the truncation channel an
        agent reaches for when rm is whitelisted out — highlight it."""
        plugin._on_post_tool_call(
            tool_name="write_file",
            args={"path": "/opt/data/credentials.env", "content": ""},
            result=json.dumps({"success": True}),
            status="ok",
        )
        body = plugin._QUEUE.get_nowait()
        assert body["risk_level"] == "medium"
        assert body["risk_reason"] == "empty write to file (potential truncation)"
        assert body["payload"]["empty_write"] is True
        assert body["payload"]["written_bytes"] == 0

    def test_write_file_whitespace_only_is_medium(self, plugin):
        plugin._on_post_tool_call(
            tool_name="write_file",
            args={"path": "/tmp/x", "content": "   \n\t"},
            result="{}", status="ok",
        )
        body = plugin._QUEUE.get_nowait()
        assert body["risk_level"] == "medium"
        assert body["payload"]["empty_write"] is True

    def test_patch_reported_at_info_not_flagged_empty(self, plugin):
        """patch args carry a diff, not full content — must not be mistaken
        for a blank write."""
        plugin._on_post_tool_call(
            tool_name="patch",
            args={"path": "/opt/data/app/main.py", "diff": "@@ -1 +1 @@\n-x\n+y"},
            result="{}", status="ok",
        )
        body = plugin._QUEUE.get_nowait()
        assert body["action"] == "file.write"
        assert body["risk_level"] == "info"
        assert body["payload"]["empty_write"] is False
        assert body["payload"]["tool"] == "patch"


class TestBlockedAttemptReporting:
    def test_blocked_low_severity_command_forced_report(self, plugin, monkeypatch):
        """A non-dangerous command blocked by the whitelist is an
        unauthorized attempt — reported even without REPORT_ALL_COMMANDS."""
        monkeypatch.setattr(plugin, "_classify_terminal",
                            lambda c: ("info", "", []))
        monkeypatch.delenv("HERMES_AUDIT_REPORT_ALL_COMMANDS", raising=False)
        plugin._on_post_tool_call(
            tool_name="terminal", args={"command": "curl evil.internal"},
            result=json.dumps({
                "status": "blocked",
                "error": "BLOCKED: not whitelisted",
            }),
            status="blocked",
        )
        body = plugin._QUEUE.get_nowait()
        assert body["event_type"] == "command"
        assert body["risk_level"] == "medium"  # escalated from info by the block
        assert "blocked" in body["risk_reason"]
        assert body["decision"] == "blocked"

    def test_ok_low_severity_command_still_not_reported(self, plugin, monkeypatch):
        """The forced-report escalation applies only to blocked calls —
        successful benign commands stay unreported by default."""
        monkeypatch.setattr(plugin, "_classify_terminal",
                            lambda c: ("info", "", []))
        monkeypatch.delenv("HERMES_AUDIT_REPORT_ALL_COMMANDS", raising=False)
        plugin._on_post_tool_call(
            tool_name="terminal", args={"command": "ls"},
            result=json.dumps({"exit_code": 0, "output": "a\nb"}),
            status="ok",
        )
        assert plugin._QUEUE.empty()

    def test_execute_code_whitelist_denial_audits_as_blocked(self, plugin):
        """code_execution_tool returns status:"blocked" for parity denials;
        the plugin must classify it as blocked (not failed)."""
        plugin._on_post_tool_call(
            tool_name="execute_code",
            args={"code": "import os\nos.remove('/tmp/f')"},
            result=json.dumps({
                "status": "blocked",
                "error": "BLOCKED: execute_code runs local Python ...",
                "tool_calls_made": 0,
                "duration_seconds": 0,
            }),
            status="blocked",
        )
        body = plugin._QUEUE.get_nowait()
        assert body["event_type"] == "command"
        assert body["action"] == "code.exec"
        assert body["decision"] == "blocked"
