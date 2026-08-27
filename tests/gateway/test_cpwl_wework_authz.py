"""Control-plane whitelist integration in gateway authorization (fork).

When enabled, the platform users list REPLACES the env allowlists for
WeWork at the gateway authz layer too (DM and group share the list; a
group message is authorized when the chat OR the sender is on it).
Pairing stays a UNION; no cached data fails closed.
"""

from unittest.mock import MagicMock

import pytest

from gateway.config import GatewayConfig, Platform
from gateway.session import SessionSource


def _dm_source(user_id="zhangsan"):
    return SessionSource(
        platform=Platform("wework"),
        user_id=user_id,
        chat_id="dm-chat",
        user_name=user_id,
        chat_type="dm",
        profile=None,
    )


def _group_source(user_id="zhangsan", chat_id="S:1_2;R:room1"):
    return SessionSource(
        platform=Platform("wework"),
        user_id=user_id,
        chat_id=chat_id,
        user_name=user_id,
        chat_type="group",
        profile=None,
    )


@pytest.fixture
def runner(monkeypatch):
    from gateway.run import GatewayRunner

    for key in (
        "WEWORK_ALLOW_FROM", "WEWORK_GROUP_ALLOW_FROM",
        "GATEWAY_ALLOWED_USERS", "GATEWAY_ALLOW_ALL_USERS",
        "WEWORK_ALLOW_ALL_USERS",
    ):
        monkeypatch.delenv(key, raising=False)
    r = object.__new__(GatewayRunner)
    r.config = GatewayConfig()
    r.adapters = {}
    r._profile_adapters = {}
    r.pairing_store = MagicMock()
    r.pairing_store.is_approved.return_value = False
    return r


@pytest.fixture
def cpwl(monkeypatch, tmp_path):
    from tools import control_plane_whitelist as cpwl_mod
    from tools.control_plane_whitelist import WhitelistSnapshot

    monkeypatch.setenv("CONTROL_PLANE_URL", "https://control.example.com")
    monkeypatch.setenv("CONTROL_PLANE_AUTH", "Bearer test-jwt")
    cpwl_mod._reset_for_tests()
    client = cpwl_mod.get_platform_whitelist()
    assert client is not None
    client._cache_path = tmp_path / "cpwl-nonexistent.json"
    client._snapshot = None  # neutralize any real /opt/data cache

    def install(users=None):
        if users is None:
            client._snapshot = None  # enabled, never fetched → fail-closed
        else:
            client._snapshot = WhitelistSnapshot(
                commands=(), users=tuple(users), updated_at=None, fetched_at=1.0,
            )

    yield install
    cpwl_mod._reset_for_tests()


def test_dm_user_on_platform_list_authorized(runner, cpwl):
    """A DM sender on the platform users list is authorized at the gateway."""
    cpwl(users=["zhangsan"])
    assert runner._is_user_authorized(_dm_source()) is True


def test_dm_user_not_listed_denied(runner, cpwl):
    """A DM sender missing from a non-empty platform list is denied."""
    cpwl(users=["zhangsan"])
    assert runner._is_user_authorized(_dm_source(user_id="lisi")) is False


def test_empty_platform_list_allows_all(runner, cpwl):
    """Empty platform users list = unrestricted (matches client semantics)."""
    cpwl(users=[])
    assert runner._is_user_authorized(_dm_source(user_id="anyone")) is True


def test_no_data_fails_closed(runner, cpwl):
    """Enabled but never fetched → deny even a previously-known sender."""
    cpwl()  # enabled, never fetched
    assert runner._is_user_authorized(_dm_source()) is False


def test_group_authorized_by_chat_id_or_sender(runner, cpwl):
    """Group auth = chat id OR sender on the platform users list."""
    chat_id = "S:1_2;R:room1"
    cpwl(users=[chat_id])
    assert runner._is_user_authorized(_group_source(user_id="member-9")) is True
    cpwl(users=["zhangsan"])
    assert runner._is_user_authorized(_group_source()) is True
    cpwl(users=["someone-else"])
    assert runner._is_user_authorized(_group_source()) is False


def test_pairing_union_survives_platform_deny(runner, cpwl):
    """A paired user stays authorized even when not on the platform list."""
    cpwl(users=["someone-else"])
    runner.pairing_store.is_approved.return_value = True
    assert runner._is_user_authorized(_dm_source()) is True


def test_disabled_falls_back_to_env_allowlist(runner, monkeypatch):
    """No CONTROL_PLANE envs → the classic env path still authorizes."""
    from hermes_cli.plugins import discover_plugins
    from tools import control_plane_whitelist as cpwl_mod

    # wework's WEWORK_ALLOW_FROM resolves through platform_registry, whose
    # entries are scoped per HERMES_HOME. The autouse test-isolation fixture
    # redirects HERMES_HOME to a tmpdir, so discovery must run HERE (after the
    # redirect); a module-level call would register under the real home and
    # be invisible during the test.
    discover_plugins()  # idempotent

    cpwl_mod._reset_for_tests()
    monkeypatch.delenv("CONTROL_PLANE_URL", raising=False)
    monkeypatch.delenv("CONTROL_PLANE_AUTH", raising=False)
    monkeypatch.setenv("WEWORK_ALLOW_FROM", "zhangsan")
    assert runner._is_user_authorized(_dm_source()) is True
    cpwl_mod._reset_for_tests()
