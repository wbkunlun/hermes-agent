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

    def test_bare_allows_version_flag(self, monkeypatch):
        captured = {}

        def fake_run(argv, **kwargs):
            captured["argv"] = argv
            return FakeCompleted(stdout='"wecom-cli 1.2.0"')

        monkeypatch.setattr(subprocess, "run", fake_run)
        out = wecom_cli.run_cli((), extra_argv=["--version"], bare=True)
        assert captured["argv"] == ["wecom-cli", "--version"]
        assert out == '"wecom-cli 1.2.0"'


class TestCliProbe:
    def test_probe_false_when_binary_missing(self, monkeypatch):
        import shutil as _shutil
        from plugins.platforms.wecom import tools as wecom_tools

        wecom_tools.reset_cli_probe_cache()
        monkeypatch.setattr(_shutil, "which", lambda name: None)
        assert wecom_tools.cli_tools_available() is False

    def test_probe_true_when_authorized(self, monkeypatch):
        import shutil as _shutil
        from plugins.platforms.wecom import tools as wecom_tools

        wecom_tools.reset_cli_probe_cache()
        monkeypatch.setattr(_shutil, "which", lambda name: "/usr/bin/wecom-cli")
        monkeypatch.setattr(
            subprocess, "run", lambda argv, **kw: FakeCompleted(stdout="authorized")
        )
        assert wecom_tools.cli_tools_available() is True

    def test_probe_false_when_unauthorized(self, monkeypatch):
        import shutil as _shutil
        from plugins.platforms.wecom import tools as wecom_tools

        wecom_tools.reset_cli_probe_cache()
        monkeypatch.setattr(_shutil, "which", lambda name: "/usr/bin/wecom-cli")
        monkeypatch.setattr(
            subprocess, "run", lambda argv, **kw: FakeCompleted(stdout="unauthorized")
        )
        assert wecom_tools.cli_tools_available() is False

    def test_probe_cached_within_ttl(self, monkeypatch):
        import shutil as _shutil
        from plugins.platforms.wecom import tools as wecom_tools

        wecom_tools.reset_cli_probe_cache()
        calls = []

        def fake_run(argv, **kw):
            calls.append(argv)
            return FakeCompleted(stdout="authorized")

        monkeypatch.setattr(_shutil, "which", lambda name: "/usr/bin/wecom-cli")
        monkeypatch.setattr(subprocess, "run", fake_run)
        assert wecom_tools.cli_tools_available() is True
        assert wecom_tools.cli_tools_available() is True
        assert len(calls) == 1  # 第二次命中模块级缓存


class TestBaseToolHandlers:
    def test_status_handler_merges_version_and_auth(self, monkeypatch):
        import asyncio

        from plugins.platforms.wecom import tools as wecom_tools

        def fake_run_cli(service_path, method=None, args=None, **kw):
            if list(service_path) == ["auth"]:
                return "authorized"
            if kw.get("extra_argv") == ["--version"]:
                return '"wecom-cli 1.2.0 (npm 2026-05-01 abc123)"'
            raise AssertionError(f"unexpected {service_path} {kw}")

        monkeypatch.setattr(wecom_tools, "run_cli", fake_run_cli)
        result = asyncio.run(wecom_tools._handler_status({}))
        assert "authorized" in result and "1.2.0" in result

    def test_schema_handler_list_vs_get(self, monkeypatch):
        import asyncio

        from plugins.platforms.wecom import tools as wecom_tools

        calls = []

        def fake_run_cli(service_path, method=None, args=None, **kw):
            calls.append((tuple(service_path), method, tuple(kw.get("extra_argv") or ())))
            return '["message", "doc"]'

        monkeypatch.setattr(wecom_tools, "run_cli", fake_run_cli)
        asyncio.run(wecom_tools._handler_schema({}))
        asyncio.run(wecom_tools._handler_schema({"target": "doc.create"}))
        assert calls == [
            (("schema",), "list", ()),
            (("schema",), "get", ("doc.create",)),
        ]

    def test_generic_handler_forwards_args_json(self, monkeypatch):
        import asyncio

        from plugins.platforms.wecom import tools as wecom_tools

        captured = {}

        def fake_run_cli(service_path, method=None, args=None, **kw):
            captured["path"] = tuple(service_path)
            captured["method"] = method
            captured["args"] = args
            return '{"created": "docid-1"}'

        monkeypatch.setattr(wecom_tools, "run_cli", fake_run_cli)
        out = asyncio.run(wecom_tools._handler_generic({
            "service_path": ["doc"], "method": "create", "args": {"title": "x"},
        }))
        assert captured == {"path": ("doc",), "method": "create", "args": {"title": "x"}}
        assert "docid-1" in out

    def test_generic_handler_rejects_bad_segment(self):
        import asyncio

        from plugins.platforms.wecom import tools as wecom_tools

        out = asyncio.run(wecom_tools._handler_generic({
            "service_path": ["doc;rm"], "method": "create", "args": {},
        }))
        assert "error" in out

    def test_generic_handler_error_json_passes_through(self, monkeypatch):
        import asyncio

        from plugins.platforms.wecom import tools as wecom_tools
        from plugins.platforms.wecom.cli import WecomCliError

        def fake_run_cli(service_path, method=None, args=None, **kw):
            raise WecomCliError(
                'wecom-cli exited 1: {"error": {"code": 893201, "message": "unauthorized"}}'
            )

        monkeypatch.setattr(wecom_tools, "run_cli", fake_run_cli)
        out = asyncio.run(wecom_tools._handler_generic({
            "service_path": ["doc"], "method": "list", "args": None,
        }))
        assert "893201" in out


class TestCuratedTools:
    def test_curated_table_maps_to_expected_cli_calls(self, monkeypatch):
        import asyncio

        from plugins.platforms.wecom import tools as wecom_tools

        captured = []

        def fake_run_cli(service_path, method=None, args=None, **kw):
            captured.append((tuple(service_path), method))
            return '{"ok": true}'

        monkeypatch.setattr(wecom_tools, "run_cli", fake_run_cli)
        for spec in wecom_tools.CURATED_TOOLS:
            handler = wecom_tools._curated_handler(spec["cli"], spec["method"])
            asyncio.run(handler({"args": {"any": "payload"}}))

        assert captured == [
            (("doc",), "create"), (("doc",), "get"), (("doc",), "append"),
            (("sheet",), "get"), (("sheet",), "append"),
            (("calendar",), "create"), (("calendar",), "list"),
            (("todo",), "create"), (("todo",), "complete"),
            (("mail",), "send"), (("mail",), "search"),
            (("contact",), "search"),
            (("message",), "send"),
        ]
        assert len(wecom_tools.CURATED_TOOLS) == 13

    def test_curated_handler_rejects_non_object_args(self):
        import asyncio

        from plugins.platforms.wecom import tools as wecom_tools

        handler = wecom_tools._curated_handler(("doc",), "create")
        out = asyncio.run(handler({"args": "not-an-object"}))
        assert "error" in out

    def test_register_tools_registers_all_sixteen(self):
        from plugins.platforms.wecom import tools as wecom_tools

        registered = []

        class FakeCtx:
            def register_tool(self, **kwargs):
                registered.append(kwargs)

        wecom_tools.register_tools(FakeCtx())
        names = sorted(r["name"] for r in registered)
        assert len(names) == 16
        assert set(names) == {
            "wecom_cli_status", "wecom_cli_schema", "wecom_cli",
            *(spec["name"] for spec in wecom_tools.CURATED_TOOLS),
        }
        for r in registered:
            assert r["toolset"] == "wecom"
            assert r["check_fn"] is wecom_tools.cli_tools_available
            assert r["is_async"] is True
            assert r["handler"] is not None
