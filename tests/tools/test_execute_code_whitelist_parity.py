"""Fork (2026-09-02): execute_code ↔ command-whitelist parity tests.

execute_code runs arbitrary local Python whose file/process APIs bypass the
terminal command whitelist — an agent blocked on ``rm`` could call
``os.remove()`` instead. The parity gate maps those operations to their shell
equivalents and runs them through the same whitelist as terminal(), before
the yolo bypass.
"""

import pytest

from tools.approval import (
    _execute_code_mapped_commands,
    _execute_code_whitelist_parity,
    check_execute_code_guard,
)


@pytest.fixture
def cpwl_off(monkeypatch):
    """Isolate from a live control-plane whitelist (falls back to static)."""
    monkeypatch.delenv("CONTROL_PLANE_URL", raising=False)
    monkeypatch.delenv("CONTROL_PLANE_AUTH", raising=False)
    from tools import control_plane_whitelist as cpwl

    cpwl._reset_for_tests()
    yield
    cpwl._reset_for_tests()


# ---------------------------------------------------------------------------
# Static mapping
# ---------------------------------------------------------------------------

class TestMappedCommands:
    @pytest.mark.parametrize("code,expected", [
        ('os.remove("/tmp/f")', ["rm /tmp/f"]),
        ("import os\nos.remove('/tmp/f')", ["rm /tmp/f"]),
        ("import os\nos.unlink('x')", ["rm x"]),
        ("import os as o\no.unlink('x')", ["rm x"]),
        ("from os import remove\nremove('x')", ["rm x"]),
        ("import shutil\nshutil.rmtree('dir')", ["rm -r dir"]),
        ("import os\nos.rmdir('d')", ["rmdir d"]),
        ("from pathlib import Path\nPath('x').unlink()", ["rm"]),
        ("import os\nos.system('rm -rf /data')", ["rm -rf /data"]),
        ("import os\nos.popen('ls')", ["ls"]),
        ("import subprocess\nsubprocess.run(['rm', 'f'])", ["rm f"]),
        ("import subprocess\nsubprocess.call('ls -la', shell=True)", ["ls -la"]),
        ("import subprocess\nsp = subprocess.Popen\nsp(['git', 'status'])",
         []),  # indirect alias — static scan does not chase assignments
        ("print(1+1)", []),
        ("x = [1, 2]\nsum(x)", []),
        ("def f(p):\n    return os.remove(p)", ["rm"]),  # dynamic operand → bare command
        ("this is not python", []),
    ])
    def test_mapping(self, code, expected):
        commands, unresolved = _execute_code_mapped_commands(code)
        assert commands == expected
        assert unresolved == []

    def test_dynamic_subprocess_fails_closed(self):
        commands, unresolved = _execute_code_mapped_commands(
            "import subprocess\nsubprocess.run(cmd)"
        )
        assert commands == []
        assert len(unresolved) == 1
        assert "subprocess.run" in unresolved[0]

    def test_dynamic_os_system_fails_closed(self):
        commands, unresolved = _execute_code_mapped_commands(
            "import os\nos.system(user_input)"
        )
        assert commands == []
        assert len(unresolved) == 1
        assert "os.system" in unresolved[0]


# ---------------------------------------------------------------------------
# Parity gate
# ---------------------------------------------------------------------------

class TestParityGate:
    def test_os_remove_blocked_when_rm_not_whitelisted(self, cpwl_off, monkeypatch):
        monkeypatch.setenv("HERMES_COMMAND_ALLOWLIST", "ls,git status")
        verdict = _execute_code_whitelist_parity("import os\nos.remove('/tmp/f')")
        assert verdict is not None
        assert verdict["approved"] is False
        assert "BLOCKED" in verdict["message"]
        assert "rm" in verdict["message"]

    def test_os_remove_allowed_when_rm_whitelisted(self, cpwl_off, monkeypatch):
        monkeypatch.setenv("HERMES_COMMAND_ALLOWLIST", "ls,rm")
        assert _execute_code_whitelist_parity("import os\nos.remove('/tmp/f')") is None

    def test_no_gate_without_whitelist(self, cpwl_off, monkeypatch):
        monkeypatch.delenv("HERMES_COMMAND_ALLOWLIST", raising=False)
        assert _execute_code_whitelist_parity("import os\nos.remove('/tmp/f')") is None

    def test_harmless_code_untouched(self, cpwl_off, monkeypatch):
        monkeypatch.setenv("HERMES_COMMAND_ALLOWLIST", "ls")
        assert _execute_code_whitelist_parity("print('hello')") is None

    def test_dynamic_subprocess_blocked_under_whitelist(self, cpwl_off, monkeypatch):
        monkeypatch.setenv("HERMES_COMMAND_ALLOWLIST", "ls")
        verdict = _execute_code_whitelist_parity(
            "import subprocess\nsubprocess.run(cmd)"
        )
        assert verdict is not None
        assert verdict["approved"] is False
        assert "dynamic" in verdict["message"]

    def test_subprocess_list_checked_against_globs(self, cpwl_off, monkeypatch):
        monkeypatch.setenv("HERMES_COMMAND_ALLOWLIST", "ls,git *")
        # git status via subprocess is whitelisted → passes the parity gate
        assert _execute_code_whitelist_parity(
            "import subprocess\nsubprocess.run(['git', 'status'])"
        ) is None
        # rm via subprocess is not
        verdict = _execute_code_whitelist_parity(
            "import subprocess\nsubprocess.run(['rm', 'f'])"
        )
        assert verdict is not None
        assert verdict["approved"] is False


class TestGuardIntegration:
    def test_guard_blocks_os_remove_under_whitelist(self, cpwl_off, monkeypatch):
        monkeypatch.setenv("HERMES_COMMAND_ALLOWLIST", "ls")
        monkeypatch.delenv("HERMES_EXEC_ASK", raising=False)
        verdict = check_execute_code_guard(
            "import os\nos.remove('/tmp/f')", env_type="local"
        )
        assert verdict["approved"] is False
        assert "BLOCKED" in verdict["message"]
        assert verdict.get("pattern_key") == "execute_code_whitelist_parity"

    def test_guard_parity_outranks_yolo(self, cpwl_off, monkeypatch):
        """The terminal allowlist contract: a pinned allowlist cannot be
        bypassed by yolo — execute_code parity matches it."""
        monkeypatch.setenv("HERMES_COMMAND_ALLOWLIST", "ls")
        monkeypatch.setenv("HERMES_YOLO", "1")
        verdict = check_execute_code_guard(
            "import os\nos.remove('/tmp/f')", env_type="local"
        )
        assert verdict["approved"] is False

    def test_guard_allows_whitelisted_equivalent(self, cpwl_off, monkeypatch):
        monkeypatch.setenv("HERMES_COMMAND_ALLOWLIST", "ls,rm")
        monkeypatch.setenv("HERMES_YOLO", "1")  # skip the approval prompt path
        verdict = check_execute_code_guard(
            "import os\nos.remove('/tmp/f')", env_type="local"
        )
        assert verdict["approved"] is True

    def test_guard_vercel_sandbox_skips_parity(self, cpwl_off, monkeypatch):
        """Isolated sandbox backends keep the container fast-path."""
        monkeypatch.setenv("HERMES_COMMAND_ALLOWLIST", "ls")
        verdict = check_execute_code_guard(
            "import os\nos.remove('/tmp/f')", env_type="vercel_sandbox"
        )
        assert verdict["approved"] is True
