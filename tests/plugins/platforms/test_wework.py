"""WeWork platform plugin discovery.

WeWork is a plugin-style adapter (``register(ctx)`` + ``ctx.register_platform``),
so it MUST live under ``plugins/platforms/`` with a ``plugin.yaml`` to be
discovered. An earlier revision placed it under ``gateway/platforms/wework/``
where neither the plugin loader (``hermes_cli.plugins.discover_plugins``) nor
the ``Platform`` enum reaches it — leaving it orphaned and the platform
silently unavailable. These tests pin the discoverable location.
"""

import pytest


def test_wework_resolves_as_platform():
    """plugin.yaml under plugins/platforms/wework/ makes Platform('wework')
    resolve via Platform._missing_ (bundled-plugin scan)."""
    from gateway.config import Platform

    assert Platform("wework").value == "wework"


def test_wework_registers_via_plugin_loader():
    """discover_plugins() loads plugins/platforms/wework and runs register(),
    so the platform registry knows about 'wework'."""
    from gateway.platform_registry import platform_registry
    from hermes_cli.plugins import discover_plugins

    discover_plugins()  # idempotent

    assert platform_registry.is_registered("wework"), (
        "wework not registered — plugins/platforms/wework/ missing or unloadable"
    )


def test_looks_like_wechat_id_classifies_by_R_segment():
    """Group wechatId contains an ``R:<roomid>`` segment; private chat is
    ``S:<a>_<b>;S:<b>_<a>`` (two S: segments, no R:). Detection MUST key on
    R: — the old ``"S:" in v and ";" in v`` heuristic misclassifies every
    private chat as a group (private wechatIds are also multi-segment),
    sending replies to an invalid group target so the user gets nothing."""
    from plugins.platforms.wework.adapter import _looks_like_wechat_id

    # Real production formats (see wehermes wework-group-prefix memory).
    private = "S:1688857642682584_8444250708322274;S:1688851343262740_1688857642682584"
    group = "S:1688858099504500_8444250708322274;R:3284275877"

    assert _looks_like_wechat_id(group) is True
    assert _looks_like_wechat_id(private) is False, (
        "private chat (no R: segment) must NOT be classified as a group"
    )
    assert _looks_like_wechat_id("") is False
    assert _looks_like_wechat_id(None) is False


def test_parse_inbound_dm_targets_user_field_not_wechat_id():
    """A private-chat reply must go to the sender's ``user`` field (API
    ``userName``), never the wechatId. The wechatId is a conversation marker
    (``S:..;S:..``), not a valid userName — sending to it makes the WeWork API
    reject the reply ("fail"), so the user gets nothing. Group replies still
    use the wechatId (API ``to``)."""
    from gateway.config import PlatformConfig
    from plugins.platforms.wework.adapter import WeWorkAdapter

    adapter = WeWorkAdapter(PlatformConfig(enabled=True, extra={"keyid": "k"}))
    payload = {
        "cmd": "/new",
        "user": "brycehuang",
        "userFullName": "Bryce Huang",
        "wechatId": "S:1688857642682584_8444250708322274;S:1688851343262740_1688857642682584",
        "fromGroup": "",
        "sendTime": "t1",
        "storeKey": "s1",
    }

    inbound = adapter._parse_inbound(payload)

    assert inbound is not None
    assert inbound.is_group is False
    assert inbound.chat_id == "brycehuang", (
        "DM reply target must be the user field (userName=brycehuang), not the "
        f"wechatId (got {inbound.chat_id!r})"
    )


def test_parse_inbound_group_targets_wechat_id():
    """Group replies use the wechatId (API ``to``)."""
    from gateway.config import PlatformConfig
    from plugins.platforms.wework.adapter import WeWorkAdapter

    adapter = WeWorkAdapter(
        PlatformConfig(
            enabled=True, extra={"keyid": "k", "groupPolicy": "open", "requireMention": False}
        )
    )
    payload = {
        "cmd": "hi",
        "user": "brycehuang",
        "userFullName": "Bryce Huang",
        "wechatId": "S:1688858099504500_8444250708322274;R:3284275877",
        "fromGroup": "",
        "sendTime": "t2",
        "storeKey": "s2",
        "isAt": True,
    }

    inbound = adapter._parse_inbound(payload)

    assert inbound is not None
    assert inbound.is_group is True
    assert inbound.chat_id == "S:1688858099504500_8444250708322274;R:3284275877"


class TestControlPlaneWhitelistGate:
    """Platform (control-plane) whitelist REPLACES the env allow/policy
    config for BOTH DM senders and groups when CONTROL_PLANE_URL/AUTH are
    both set. No cached data = drop (fail-closed, silent like groups today)."""

    @pytest.fixture
    def cpwl(self, monkeypatch, tmp_path):
        """Enable the control-plane whitelist with an isolated client (disk
        cache pointed at a nonexistent tmp path); yields install(users)."""
        from tools import control_plane_whitelist as cpwl_mod
        from tools.control_plane_whitelist import WhitelistSnapshot

        monkeypatch.delenv("WEWORK_ALLOW_FROM", raising=False)
        monkeypatch.delenv("WEWORK_GROUP_ALLOW_FROM", raising=False)
        monkeypatch.setenv("CONTROL_PLANE_URL", "https://control.example.com")
        monkeypatch.setenv("CONTROL_PLANE_AUTH", "Bearer test-jwt")
        cpwl_mod._reset_for_tests()
        client = cpwl_mod.get_platform_whitelist()
        assert client is not None
        # construction read the REAL /opt/data cache — neutralize it
        client._cache_path = tmp_path / "cpwl-nonexistent.json"
        client._snapshot = None

        def install(users=None):
            # install() with no args = enabled but never fetched (no
            # snapshot) → fail-closed; users=[] is a REAL empty list →
            # allow all. These are different states in WhitelistClient.
            if users is None:
                client._snapshot = None
            else:
                client._snapshot = WhitelistSnapshot(
                    commands=(), users=tuple(users),
                    updated_at=None, fetched_at=1.0,
                )

        yield install
        cpwl_mod._reset_for_tests()

    @staticmethod
    def _adapter(extra=None):
        from gateway.config import PlatformConfig
        from plugins.platforms.wework.adapter import WeWorkAdapter

        return WeWorkAdapter(PlatformConfig(enabled=True, extra={"keyid": "k", **(extra or {})}))

    @staticmethod
    def _dm_payload(user="zhangsan", name="张三", t="cp1"):
        return {
            "cmd": "hi", "user": user, "userFullName": name,
            "wechatId": "S:1688857642682584_8444250708322274;S:1688851343262740_1688857642682584",
            "fromGroup": "", "sendTime": t, "storeKey": "sk-" + t,
        }

    @staticmethod
    def _group_payload(from_group="运维群", t="cp1", is_at=True):
        return {
            "cmd": "hi", "user": "zhangsan", "userFullName": "张三",
            "wechatId": "S:1688858099504500_8444250708322274;R:3284275877",
            "fromGroup": from_group, "sendTime": t, "storeKey": "sk-" + t,
            "isAt": is_at,
        }

    def test_dm_allowed_by_platform_users(self, cpwl):
        """A DM sender listed in the platform users list passes the gate."""
        cpwl(users=["zhangsan"])
        assert self._adapter()._parse_inbound(self._dm_payload()) is not None

    def test_dm_dropped_when_not_listed(self, cpwl):
        """A DM sender absent from the platform users list is silently dropped."""
        cpwl(users=["zhangsan"])
        assert self._adapter()._parse_inbound(self._dm_payload(user="lisi")) is None

    def test_group_allowed_by_group_name(self, cpwl):
        """requireMention defaults True; payload isAt=True satisfies it."""
        cpwl(users=["运维群"])
        assert self._adapter()._parse_inbound(self._group_payload()) is not None

    def test_group_dropped_when_group_not_listed(self, cpwl):
        """A group whose name (and chat id) is not in the users list is dropped."""
        cpwl(users=["运维群"])
        assert self._adapter()._parse_inbound(self._group_payload(from_group="其他群")) is None

    def test_group_allowed_by_full_chat_id(self, cpwl):
        """Groups can also be whitelisted by their FULL wechatId chat id."""
        chat_id = "S:1688858099504500_8444250708322274;R:3284275877"
        cpwl(users=[chat_id])
        assert self._adapter()._parse_inbound(self._group_payload()) is not None

    def test_empty_users_allows_all(self, cpwl):
        """An empty platform users list means unrestricted: DM and group pass."""
        cpwl(users=[])
        assert self._adapter()._parse_inbound(self._dm_payload()) is not None
        assert self._adapter()._parse_inbound(self._group_payload()) is not None

    def test_no_data_drops_everything(self, cpwl):
        """Enabled but never fetched and no cache: fail-closed drops all."""
        cpwl()  # enabled, never fetched, no cache
        assert self._adapter()._parse_inbound(self._dm_payload()) is None
        assert self._adapter()._parse_inbound(self._group_payload()) is None

    def test_require_mention_still_applies_on_platform_path(self, cpwl):
        """@-mention logic is orthogonal to the whitelist and keeps running."""
        cpwl(users=["运维群"])
        assert self._adapter()._parse_inbound(self._group_payload(is_at=False)) is None

    def test_env_path_unchanged_when_platform_disabled(self, monkeypatch):
        """No CONTROL_PLANE envs → existing env/config policy path intact:
        default groupPolicy=allowlist with empty list drops the group."""
        from gateway.config import PlatformConfig
        from plugins.platforms.wework.adapter import WeWorkAdapter
        from tools import control_plane_whitelist as cpwl_mod

        cpwl_mod._reset_for_tests()
        monkeypatch.delenv("CONTROL_PLANE_URL", raising=False)
        monkeypatch.delenv("CONTROL_PLANE_AUTH", raising=False)
        monkeypatch.delenv("WEWORK_ALLOW_FROM", raising=False)
        monkeypatch.delenv("WEWORK_GROUP_ALLOW_FROM", raising=False)
        adapter = WeWorkAdapter(PlatformConfig(enabled=True, extra={"keyid": "k"}))
        assert adapter._parse_inbound(self._group_payload(t="cp9")) is None
        cpwl_mod._reset_for_tests()

    def test_drop_logs_rate_limited(self, cpwl, monkeypatch, caplog):
        """No-data drops emit a rate-limited warning (operator visibility)."""
        import logging as _logging

        from plugins.platforms.wework import adapter as adapter_mod

        cpwl()  # enabled, never fetched, no cache → fail-closed drops
        monkeypatch.setattr(adapter_mod, "_cpwl_last_drop_log", 0.0)
        with caplog.at_level(_logging.WARNING, logger="plugins.platforms.wework.adapter"):
            adapter = self._adapter()
            adapter._parse_inbound(self._dm_payload(t="lg1"))
            adapter._parse_inbound(self._dm_payload(t="lg2"))
        warnings = [r for r in caplog.records if "control-plane whitelist dropped" in r.getMessage()]
        assert len(warnings) == 1  # second drop within 5 min is suppressed
