"""WeWork platform plugin package body.

FORK(wbkunlun): stays import-light per the upstream deferred-platform contract
(same rationale as the wecom package body) — the adapter loads inside
``register`` / when the platform materialises."""

__all__ = ["register"]


def register(ctx) -> None:
    from .adapter import register as _adapter_register

    _adapter_register(ctx)
