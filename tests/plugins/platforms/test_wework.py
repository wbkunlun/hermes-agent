"""WeWork platform plugin discovery.

WeWork is a plugin-style adapter (``register(ctx)`` + ``ctx.register_platform``),
so it MUST live under ``plugins/platforms/`` with a ``plugin.yaml`` to be
discovered. An earlier revision placed it under ``gateway/platforms/wework/``
where neither the plugin loader (``hermes_cli.plugins.discover_plugins``) nor
the ``Platform`` enum reaches it — leaving it orphaned and the platform
silently unavailable. These tests pin the discoverable location.
"""


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
