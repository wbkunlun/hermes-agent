"""WeCom platform plugin package body.

FORK(wbkunlun): stays import-light per the upstream deferred-platform contract
("tools live in a ``tools`` submodule so ``__init__`` stays import-light") — the
v0.22 cold-activation flow executes this package body during deferred gating
(``_register_deferred_platform_tools`` → ``_load_directory_module``), so a
top-level adapter import here would pull the whole WeCom adapter into every
CLI/TUI/config-load process even when only other platforms are configured.
The adapter loads inside ``register`` / when the platform materialises."""

__all__ = ["register"]


def register(ctx) -> None:
    from .adapter import register as _adapter_register

    _adapter_register(ctx)
