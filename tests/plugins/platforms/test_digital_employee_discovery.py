"""数字员工平台 —— 插件发现。

平台必须能在 ``plugins/platforms/`` 下被插件加载器发现，``Platform("digital_employee")``
可解析，且注册进平台注册表——否则网关永远不会为它建适配器。

注意两个名字不同，都来自契约：插件 manifest ``name: digital-employee-platform``
派生出的**平台名**是 ``digital-employee``（插件加载器去掉 ``-platform`` 后缀），
而目录名 ``digital_employee`` 才是 `Platform(...)` 的解析键（bundled 扫描按目录名）。
适配器注册时必须用平台名，否则注册表里查不到。
"""

from __future__ import annotations


def test_directory_name_resolves_via_bundled_scan():
    from gateway.config import Platform

    assert Platform("digital_employee").value == "digital_employee"


def test_plugin_registers_platform():
    from gateway.platform_registry import platform_registry
    from hermes_cli.plugins import discover_plugins

    discover_plugins()  # 幂等

    assert platform_registry.is_registered("digital-employee")


def test_registered_entry_points_at_our_adapter():
    from gateway.platform_registry import platform_registry
    from hermes_cli.plugins import discover_plugins
    from plugins.platforms.digital_employee.adapter import DigitalEmployeeAdapter

    discover_plugins()
    entry = platform_registry.get("digital-employee")
    assert entry is not None
    assert entry.adapter_factory is DigitalEmployeeAdapter
    assert entry.allowed_users_env == "DIGITAL_EMPLOYEE_ALLOWED_USERS"
    # 这条通道收的是平台已过滤的在册流量：不能被误判为需要配对
    assert entry.allowlist_suppresses_pairing is True


def test_adapter_platform_matches_the_registry_entry():
    """适配器的 ``platform.value`` 必须能被注册表查到。

    两个名字来自不同约定：``Platform(...)`` 按**目录名**解析出 ``digital_employee``，
    而插件加载器按 manifest 派生注册表键 ``digital-employee``。若适配器用了查不到
    的那个名字，``authz_mixin`` 按 ``platform.value`` 找 ``PlatformEntry`` 就会落空，
    本平台的 allowed_users_env 等授权配置全部失效——静默、无日志。
    """
    from gateway.config import PlatformConfig
    from gateway.platform_registry import platform_registry
    from hermes_cli.plugins import discover_plugins
    from plugins.platforms.digital_employee.adapter import _platform_names

    discover_plugins()
    adapter = _adapter_factory()(PlatformConfig(enabled=True))
    names = _platform_names(adapter.platform)
    assert any(platform_registry.get(name) is not None for name in names), (
        f"no registry entry for {names}"
    )


def _adapter_factory():
    from gateway.platform_registry import platform_registry
    from hermes_cli.plugins import discover_plugins

    discover_plugins()
    entry = platform_registry.get("digital-employee") or platform_registry.get("digital_employee")
    return entry.adapter_factory
