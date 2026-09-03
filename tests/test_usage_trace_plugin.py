"""Tests for the usage-trace fork plugin (local JSONL agent trace flow).

The plugin lives in a hyphenated directory (``plugins/usage-trace/``) which
is not a valid package name, so we load it via ``importlib`` (like the plugin
loader and test_audit_callback_plugin.py do), fresh per test to isolate the
module-level queue / worker / dedup-set state.
"""

import importlib.util
import json
from pathlib import Path

import pytest

_PLUGIN_PATH = (
    Path(__file__).resolve().parents[1]
    / "plugins" / "usage-trace" / "__init__.py"
)


@pytest.fixture
def plugin(monkeypatch, tmp_path):
    """Load the plugin fresh; point output at tmp; keep the real worker off."""
    monkeypatch.setenv("HERMES_USAGE_TRACE", "1")
    monkeypatch.setenv("HERMES_USAGE_TRACE_DIR", str(tmp_path / "traces"))
    monkeypatch.delenv("HERMES_USAGE_TRACE_CAPTURE", raising=False)
    spec = importlib.util.spec_from_file_location("usage_trace_under_test", _PLUGIN_PATH)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    # Tests drive writes synchronously via _drain_now(); never start the thread.
    monkeypatch.setattr(m, "_ensure_worker", lambda: None)
    return m


def _lines(plugin, tmp_path, name):
    """Parse the JSONL file written for session `name` under the test dir."""
    p = tmp_path / "traces" / f"{name}.jsonl"
    assert p.exists(), f"expected {p}"
    return [json.loads(line) for line in p.read_text(encoding="utf-8").splitlines()]


# ---------------------------------------------------------------------------
# Config / capture-mode / path helpers (Task 1)
# ---------------------------------------------------------------------------

class TestCaptureModes:
    def test_metadata_mode_has_no_content(self, plugin, monkeypatch):
        monkeypatch.setenv("HERMES_USAGE_TRACE_CAPTURE", "metadata")
        out = plugin._capture_text("hello world")
        assert out == {"chars": 11}

    def test_full_mode_returns_truncated_text(self, plugin, monkeypatch):
        monkeypatch.setenv("HERMES_USAGE_TRACE_CAPTURE", "full")
        monkeypatch.setenv("HERMES_USAGE_TRACE_MAX_CHARS", "5")
        out = plugin._capture_text("abcdefg")
        assert out["chars"] == 7
        assert out["text"] == "abcde"

    def test_sanitized_mode_redacts(self, plugin, monkeypatch):
        monkeypatch.setenv("HERMES_USAGE_TRACE_CAPTURE", "sanitized")

        def fake_redact(text, force=False):
            return text.replace("SECRET", "[REDACTED]")

        monkeypatch.setattr("agent.redact.redact_sensitive_text", fake_redact)
        out = plugin._capture_text("pw=SECRET;")
        assert out["text"] == "pw=[REDACTED];"
        assert out["chars"] == 10

    def test_sanitized_degrades_to_metadata_when_redactor_fails(self, plugin, monkeypatch):
        monkeypatch.setenv("HERMES_USAGE_TRACE_CAPTURE", "sanitized")

        def boom(text, force=False):
            raise RuntimeError("redactor broken")

        monkeypatch.setattr("agent.redact.redact_sensitive_text", boom)
        out = plugin._capture_text("pw=SECRET;")
        assert "text" not in out
        assert out == {"chars": 10}

    def test_capture_obj_json_serializes_dict(self, plugin, monkeypatch):
        monkeypatch.setenv("HERMES_USAGE_TRACE_CAPTURE", "full")
        out = plugin._capture_obj({"command": "ls"})
        assert out["bytes"] == len('{"command": "ls"}')
        assert json.loads(out["data"]) == {"command": "ls"}

    def test_default_mode_is_sanitized(self, plugin):
        assert plugin._capture_mode() == "sanitized"

    def test_invalid_mode_falls_back_to_sanitized(self, plugin, monkeypatch):
        monkeypatch.setenv("HERMES_USAGE_TRACE_CAPTURE", "bogus")
        assert plugin._capture_mode() == "sanitized"


class TestSessionFile:
    def test_filename_sanitizes_unsafe_chars(self, plugin, tmp_path):
        p = plugin._session_file("abc/../../etc x!")
        assert p.parent == tmp_path / "traces"
        assert "/" not in p.name and p.name == "abc_____etc_x_.jsonl"

    def test_empty_session_lands_in_no_session_bucket(self, plugin):
        assert plugin._session_file("").name == "no-session.jsonl"


class TestMasterSwitch:
    def test_disabled_by_env(self, plugin, monkeypatch):
        monkeypatch.setenv("HERMES_USAGE_TRACE", "0")
        assert plugin._enabled() is False

    def test_enabled_by_default(self, plugin, monkeypatch):
        monkeypatch.delenv("HERMES_USAGE_TRACE", raising=False)
        assert plugin._enabled() is True
