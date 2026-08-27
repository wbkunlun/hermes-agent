"""Tests for the control-plane dynamic whitelist client (fork).

Platform semantics: enabled → platform lists REPLACE env lists; fetch
failure falls back to cached lists; no data at all = deny everything
(fail-closed); empty list = that class unrestricted.
"""

import json
from pathlib import Path

import pytest

from tools import control_plane_whitelist as cpwl_mod
from tools.control_plane_whitelist import WhitelistSnapshot


@pytest.fixture
def cp_enabled(monkeypatch, tmp_path):
    """Enable the control plane with a tmp cache path; yields the client."""
    monkeypatch.setenv("CONTROL_PLANE_URL", "https://control.example.com")
    monkeypatch.setenv("CONTROL_PLANE_AUTH", "Bearer test-jwt")
    cpwl_mod._reset_for_tests()
    client = cpwl_mod.get_platform_whitelist()
    assert client is not None
    client._cache_path = tmp_path / "whitelist-cache.json"
    client._snapshot = None  # construction may have read the real /opt/data cache
    yield client
    cpwl_mod._reset_for_tests()


def _install(client, *, commands=(), users=()):
    """Install a snapshot directly (tests must not need HTTP)."""
    client._snapshot = WhitelistSnapshot(
        commands=tuple(commands),
        users=tuple(users),
        updated_at="2026-08-27T08:00:00Z",
        fetched_at=1_800_000_000.0,
    )


class TestSingleton:
    def test_disabled_when_either_env_missing(self, monkeypatch):
        """Either env unset → get_platform_whitelist() is None (env paths win)."""
        cpwl_mod._reset_for_tests()
        monkeypatch.delenv("CONTROL_PLANE_URL", raising=False)
        monkeypatch.delenv("CONTROL_PLANE_AUTH", raising=False)
        assert cpwl_mod.get_platform_whitelist() is None

    def test_enabled_builds_client_once(self, cp_enabled):
        """Both envs set → singleton client; second call returns the same one."""
        assert cpwl_mod.get_platform_whitelist() is cp_enabled
        assert cp_enabled._url == "https://control.example.com/api/v1/agent/whitelist"

    def test_url_trailing_slash_stripped(self, monkeypatch):
        """Trailing slash on the base URL must not double up the path."""
        cpwl_mod._reset_for_tests()
        monkeypatch.setenv("CONTROL_PLANE_URL", "https://control.example.com/")
        monkeypatch.setenv("CONTROL_PLANE_AUTH", "Bearer test-jwt")
        client = cpwl_mod.get_platform_whitelist()
        assert client._url == "https://control.example.com/api/v1/agent/whitelist"
        cpwl_mod._reset_for_tests()


class TestUserAllowed:
    def test_no_snapshot_denies(self, cp_enabled):
        """Fail-closed: never-fetched + no disk cache → deny everyone."""
        assert cp_enabled.user_allowed("zhangsan", "Zhang San") is False

    def test_empty_users_allows_all(self, cp_enabled):
        """Empty platform users list = unrestricted."""
        _install(cp_enabled, users=[])
        assert cp_enabled.user_allowed("anyone", "Anyone") is True

    def test_exact_match_on_id_or_name(self, cp_enabled):
        """Exact match on either sender_id or sender_name allows."""
        _install(cp_enabled, users=["zhangsan", "Zhang San"])
        assert cp_enabled.user_allowed("zhangsan", "") is True
        assert cp_enabled.user_allowed("", "Zhang San") is True

    def test_match_is_case_sensitive(self, cp_enabled):
        """Contract: exact username comparison, case-sensitive."""
        _install(cp_enabled, users=["zhangsan"])
        assert cp_enabled.user_allowed("ZhangSan", "") is False


class TestGroupAllowed:
    """Group decisions share the users list; match full chat id or name."""

    def test_no_snapshot_denies(self, cp_enabled):
        """Fail-closed: never-fetched + no disk cache → deny all groups."""
        assert cp_enabled.group_allowed(chat_id="S:1;R:2", chat_name="g") is False

    def test_empty_users_allows_all(self, cp_enabled):
        """Empty platform users list = all groups unrestricted."""
        _install(cp_enabled, users=[])
        assert cp_enabled.group_allowed(chat_id="S:1;R:2", chat_name="g") is True

    def test_match_on_group_name_or_full_chat_id(self, cp_enabled):
        """Same users list as DM; matches group name or the FULL wechatId
        (never a substring / R:-segment extraction)."""
        chat_id = "S:1688858099504500_8444250708322274;R:3284275877"
        _install(cp_enabled, users=["运维群", chat_id])
        assert cp_enabled.group_allowed(chat_id=chat_id, chat_name="") is True
        assert cp_enabled.group_allowed(chat_id="", chat_name="运维群") is True
        assert cp_enabled.group_allowed(chat_id="R:3284275877", chat_name="其他群") is False


class TestDiskCache:
    """Atomic JSON persist + boot-time load fallback (/opt/data)."""

    def test_persist_writes_atomic_json(self, cp_enabled, tmp_path):
        """A successful snapshot lands on disk with both lists."""
        _install(cp_enabled, commands=["ls*"], users=["zhangsan"])
        cp_enabled._persist()
        raw = json.loads(cp_enabled._cache_path.read_text(encoding="utf-8"))
        assert raw["commands"] == ["ls*"]
        assert raw["users"] == ["zhangsan"]
        assert raw["updated_at"] == "2026-08-27T08:00:00Z"
        assert raw["fetched_at"] == 1_800_000_000.0

    def test_persist_failure_is_non_fatal(self, cp_enabled, monkeypatch):
        """Unwritable cache dir must not raise (memory state still serves)."""
        _install(cp_enabled)

        def _raise(*args, **kwargs):
            raise OSError("read-only")

        monkeypatch.setattr(Path, "write_text", _raise)
        cp_enabled._persist()  # must not raise

    def test_startup_loads_disk_cache(self, monkeypatch, tmp_path):
        """Fresh process + platform unreachable at boot → disk cache decides."""
        cpwl_mod._reset_for_tests()
        cache = tmp_path / "whitelist-cache.json"
        cache.write_text(json.dumps({
            "commands": ["ls*"], "users": ["zhangsan"],
            "updated_at": None, "fetched_at": 1.0,
        }), encoding="utf-8")
        monkeypatch.setenv("CONTROL_PLANE_URL", "https://control.example.com")
        monkeypatch.setenv("CONTROL_PLANE_AUTH", "Bearer test-jwt")
        client = cpwl_mod.WhitelistClient(
            url="https://control.example.com/api/v1/agent/whitelist",
            auth="Bearer test-jwt", cache_path=cache,
        )
        assert client.user_allowed("zhangsan") is True
        assert client.user_allowed("lisi") is False
        assert client.command_gate("ls -l") == "bypass"
        cpwl_mod._reset_for_tests()

    def test_corrupt_cache_is_ignored_fail_closed(self, monkeypatch, tmp_path):
        """Corrupt/invalid cache files are silently ignored → no snapshot."""
        cpwl_mod._reset_for_tests()
        for bad in ("{not json", '{"commands": "x", "users": []}', '{"commands": []}', "[]"):
            cache = tmp_path / "whitelist-cache.json"
            cache.write_text(bad, encoding="utf-8")
            client = cpwl_mod.WhitelistClient(
                url="https://x/api", auth="Bearer t", cache_path=cache,
            )
            assert client.snapshot is None, f"corrupt cache {bad!r} must not load"
        cpwl_mod._reset_for_tests()
