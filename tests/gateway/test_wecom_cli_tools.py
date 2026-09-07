"""Tests for the wecom-cli subprocess bridge and business tools (① gap)."""

import json
import subprocess

import pytest

from plugins.platforms.wecom import cli as wecom_cli


class FakeCompleted:
    def __init__(self, stdout="", stderr="", returncode=0):
        self.stdout = stdout
        self.stderr = stderr
        self.returncode = returncode


class TestRunCliArgv:
    def test_argv_no_shell_json_payload(self, monkeypatch):
        """argv 是列表、--json 单参数、绝无 shell=True。"""
        captured = {}

        def fake_run(argv, **kwargs):
            captured["argv"] = argv
            captured["kwargs"] = kwargs
            return FakeCompleted(stdout='{"ok": true}')

        monkeypatch.setattr(subprocess, "run", fake_run)
        out = wecom_cli.run_cli(("doc",), "create", {"title": "周报"})

        assert out == '{"ok": true}'
        assert captured["argv"] == [
            "wecom-cli", "doc", "create", "--json", json.dumps({"title": "周报"}, ensure_ascii=False),
        ]
        assert not captured["kwargs"].get("shell")

    def test_env_minimal_and_config_dir_injected(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HERMES_HOME", str(tmp_path))
        monkeypatch.delenv("WECOM_CLI_CONFIG_DIR", raising=False)
        monkeypatch.setenv("SECRET_THAT_MUST_NOT_LEAK", "s3cr3t")
        captured = {}

        def fake_run(argv, **kwargs):
            captured["env"] = kwargs["env"]
            return FakeCompleted(stdout="{}")

        monkeypatch.setattr(subprocess, "run", fake_run)
        wecom_cli.run_cli(("auth",), "show")

        env = captured["env"]
        assert env["WECOM_CLI_CONFIG_DIR"] == str(tmp_path / "wecom-cli")
        assert "SECRET_THAT_MUST_NOT_LEAK" not in env
        assert set(env) <= {"PATH", "HOME", "WECOM_CLI_CONFIG_DIR"}

    def test_config_dir_env_override_wins(self, monkeypatch, tmp_path):
        monkeypatch.setenv("WECOM_CLI_CONFIG_DIR", str(tmp_path / "custom"))
        assert wecom_cli.config_dir() == tmp_path / "custom"

    def test_segment_whitelist_rejects_shell_shaped_input(self):
        for bad in ("doc;rm", "Doc", "../etc", "a b", "", "-x"):
            with pytest.raises(ValueError):
                wecom_cli.validate_segments((bad,))

    def test_surrounding_whitespace_is_stripped_not_rejected(self):
        # 换行/空格被归一化剥离，不会进入 argv（比拒绝更安全也更宽容）
        assert wecom_cli.validate_segments(("doc\n",)) == ["doc"]

    def test_valid_segments_pass(self):
        assert wecom_cli.validate_segments(("message", "aibot", "sessions")) == [
            "message", "aibot", "sessions",
        ]


class TestRunCliErrors:
    def test_exit1_preserves_structured_error(self, monkeypatch):
        err = '{"error": {"type": "AuthError", "code": 893201, "message": "unauthorized"}}'

        def fake_run(argv, **kwargs):
            return FakeCompleted(stdout=err, returncode=1)

        monkeypatch.setattr(subprocess, "run", fake_run)
        with pytest.raises(wecom_cli.WecomCliError) as exc_info:
            wecom_cli.run_cli(("doc",), "list")
        assert "893201" in str(exc_info.value)

    def test_timeout_raises(self, monkeypatch):
        def fake_run(argv, **kwargs):
            raise subprocess.TimeoutExpired(cmd=argv, timeout=0.01)

        monkeypatch.setattr(subprocess, "run", fake_run)
        with pytest.raises(wecom_cli.WecomCliError, match="timed out"):
            wecom_cli.run_cli(("doc",), "list", timeout=0.01)

    def test_binary_missing_raises(self, monkeypatch):
        def fake_run(argv, **kwargs):
            raise FileNotFoundError("nope")

        monkeypatch.setattr(subprocess, "run", fake_run)
        with pytest.raises(wecom_cli.WecomCliError, match="not found"):
            wecom_cli.run_cli(("doc",), "list")

    def test_bin_override(self, monkeypatch):
        monkeypatch.setenv("WECOM_CLI_BIN", "/opt/wecom-cli/bin/wecom-cli")
        captured = {}

        def fake_run(argv, **kwargs):
            captured["argv"] = argv
            return FakeCompleted(stdout="{}")

        monkeypatch.setattr(subprocess, "run", fake_run)
        wecom_cli.run_cli(("auth",), "show")
        assert captured["argv"][0] == "/opt/wecom-cli/bin/wecom-cli"
