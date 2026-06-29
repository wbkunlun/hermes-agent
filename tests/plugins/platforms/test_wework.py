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
