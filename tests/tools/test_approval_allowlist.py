"""Tests for the operator command allowlist (HERMES_COMMAND_ALLOWLIST env /
approvals.allow config).

A configured allowlist is a STRICT gate: non-matching commands are blocked
BEFORE the --yolo / mode=off bypass, mirroring approvals.deny. Unset/empty =
feature off (three-state None → the rest of the pipeline is unchanged).
Hardline still wins over the allowlist, so even an allowlisted catastrophic
command is blocked.
"""

import pytest

from tools import approval as mod
from tools.approval import check_all_command_guards


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def allow_config(monkeypatch):
    """Install an allow list via config (approvals.allow) and return a setter.

    The HERMES_COMMAND_ALLOWLIST env var takes priority over config, so we
    delete it to expose the config-fallback path.
    """
    state = {"config": {"mode": "manual", "allow": []}}
    monkeypatch.delenv("HERMES_COMMAND_ALLOWLIST", raising=False)

    def set_allow(patterns, **extra):
        state["config"] = {"mode": "manual", "allow": list(patterns), **extra}
        monkeypatch.setattr(mod, "_get_approval_config", lambda: state["config"])

    monkeypatch.setattr(mod, "_get_approval_config", lambda: state["config"])
    return set_allow


@pytest.fixture
def clean_env(monkeypatch):
    """Non-interactive, non-gateway, non-cron, non-yolo baseline; no allowlist env.

    ``is_current_session_yolo_enabled`` is pinned off so the guard logic can be
    exercised without pulling the gateway/agent import chain (which needs the
    full runtime deps installed; CI has them, a bare local venv may not).
    """
    for var in ("HERMES_YOLO_MODE", "HERMES_GATEWAY_SESSION",
                "HERMES_CRON_SESSION", "HERMES_INTERACTIVE",
                "HERMES_EXEC_ASK", "HERMES_COMMAND_ALLOWLIST"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setattr(mod, "_YOLO_MODE_FROZEN", False)
    monkeypatch.setattr(mod, "is_current_session_yolo_enabled", lambda: False)


# ---------------------------------------------------------------------------
# _match_user_allow_rule — three-state matcher
# ---------------------------------------------------------------------------

class TestMatchUserAllowRule:
    def test_no_config_no_env_is_none(self, allow_config):
        allow_config([])
        assert mod._match_user_allow_rule("git status") is None

    def test_missing_allow_key_is_none(self, monkeypatch):
        monkeypatch.delenv("HERMES_COMMAND_ALLOWLIST", raising=False)
        monkeypatch.setattr(mod, "_get_approval_config", lambda: {"mode": "manual"})
        assert mod._match_user_allow_rule("git status") is None

    def test_env_matches(self, monkeypatch):
        monkeypatch.setenv("HERMES_COMMAND_ALLOWLIST", "git *,ls")
        assert mod._match_user_allow_rule("git status") is True
        assert mod._match_user_allow_rule("ls -la") is True

    def test_env_configured_but_no_match_is_false(self, monkeypatch):
        monkeypatch.setenv("HERMES_COMMAND_ALLOWLIST", "git *,ls")
        assert mod._match_user_allow_rule("npm install") is False

    def test_config_fallback_matches(self, allow_config):
        allow_config(["git *", "ls"])
        assert mod._match_user_allow_rule("git status") is True
        assert mod._match_user_allow_rule("npm install") is False

    def test_star_glob_is_allow_all(self, monkeypatch):
        monkeypatch.setenv("HERMES_COMMAND_ALLOWLIST", "*")
        assert mod._match_user_allow_rule("rm -rf build") is True

    def test_case_insensitive(self, monkeypatch):
        monkeypatch.setenv("HERMES_COMMAND_ALLOWLIST", "GIT *")
        assert mod._match_user_allow_rule("git status") is True

    def test_non_string_and_empty_entries_ignored(self, monkeypatch):
        monkeypatch.setenv("HERMES_COMMAND_ALLOWLIST", ",  ,git *")
        assert mod._match_user_allow_rule("git status") is True
        assert mod._match_user_allow_rule("ls") is False

    def test_quote_obfuscation_still_matches(self, monkeypatch):
        """Deobfuscation variants feed allow matching (same as deny)."""
        monkeypatch.setenv("HERMES_COMMAND_ALLOWLIST", "git push --force*")
        assert mod._match_user_allow_rule('git pu""sh --force origin main') is True

    def test_bare_name_allows_any_args(self, monkeypatch):
        """A bare program name allows that program with any arguments."""
        monkeypatch.setenv("HERMES_COMMAND_ALLOWLIST", "git")
        assert mod._match_user_allow_rule("git") is True
        assert mod._match_user_allow_rule("git status") is True
        assert mod._match_user_allow_rule("git push --force origin main") is True
        # First-token match only — a longer name must NOT sneak in.
        assert mod._match_user_allow_rule("gitx") is False
        assert mod._match_user_allow_rule("got") is False

    def test_precise_pattern_scopes_to_whole_command(self, monkeypatch):
        """An entry with spaces / '*' matches the whole command via fnmatch."""
        monkeypatch.setenv("HERMES_COMMAND_ALLOWLIST", "git push *")
        assert mod._match_user_allow_rule("git push origin main") is True
        # `git push *` does NOT match bare `git` or `git status`.
        assert mod._match_user_allow_rule("git status") is False


# ---------------------------------------------------------------------------
# check_all_command_guards — integration of the allowlist into the pipeline
# ---------------------------------------------------------------------------

class TestCheckAllCommandGuardsAllowlist:
    def test_unset_allowlist_leaves_pipeline_unchanged(self, clean_env, allow_config):
        allow_config([])
        # A benign command proceeds as before the feature existed.
        result = check_all_command_guards("ls", "local")
        assert result["approved"] is True

    def test_whitelisted_command_is_approved(self, clean_env, monkeypatch):
        monkeypatch.setenv("HERMES_COMMAND_ALLOWLIST", "git *,ls")
        assert check_all_command_guards("git status", "local")["approved"] is True
        assert check_all_command_guards("ls", "local")["approved"] is True

    def test_non_whitelisted_command_is_blocked(self, clean_env, monkeypatch):
        monkeypatch.setenv("HERMES_COMMAND_ALLOWLIST", "git *,ls")
        result = check_all_command_guards("npm install", "local")
        assert result["approved"] is False
        assert result.get("user_allow") is True
        assert "allowlist" in result["message"]

    def test_yolo_cannot_bypass_allowlist(self, clean_env, monkeypatch):
        # Yolo is on, but the allowlist gate sits BEFORE the yolo bypass.
        monkeypatch.setattr(mod, "_YOLO_MODE_FROZEN", True)
        monkeypatch.setenv("HERMES_COMMAND_ALLOWLIST", "git *,ls")
        result = check_all_command_guards("npm install", "local")
        assert result["approved"] is False
        assert result.get("user_allow") is True

    def test_hardline_still_wins_over_allow_all(self, clean_env, monkeypatch):
        # Even an explicit allow-all ('*') cannot unblock a hardline command.
        monkeypatch.setenv("HERMES_COMMAND_ALLOWLIST", "*")
        result = check_all_command_guards("mkfs.ext4 /dev/sda1", "local")
        assert result["approved"] is False
        assert result.get("hardline") is True

    def test_allow_all_lets_normal_command_through(self, clean_env, monkeypatch):
        monkeypatch.setenv("HERMES_COMMAND_ALLOWLIST", "*")
        assert check_all_command_guards("ls", "local")["approved"] is True
