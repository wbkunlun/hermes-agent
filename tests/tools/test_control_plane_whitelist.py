"""Tests for the control-plane dynamic whitelist client (fork).

Platform semantics: enabled → platform lists REPLACE env lists; fetch
failure falls back to cached lists; no data at all = deny everything
(fail-closed); empty list = that class unrestricted.
"""

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
