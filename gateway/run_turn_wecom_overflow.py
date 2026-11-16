"""fork 2026-11-16 (DPMS 561213): the WeCom overflow delivery at the gateway level.

A WeCom reply too long for one stream frame cannot be edited into shape and used to be regular-
sent as a SECOND message beside the half-drawn bubble. It is now delivered by the reply consumer
as sealed follow-up bubbles (plugins/platforms/wecom/stream_delivery.py), and the gateway only
decides what still belongs to it: whether the regular send is redundant, and which tail (segment,
MEDIA: tag) the bubbles cannot carry.
"""
from __future__ import annotations

from typing import Any


def _wecom_delivery_consumer(consumer: Any) -> bool:
    """A WeCom reply consumer that frames its own overflow (WeComStreamDelivery)."""
    return consumer is not None and hasattr(consumer, "overflow_parts") and hasattr(consumer, "attach_overflow")


def _wecom_overflow_claimed(consumer: Any) -> bool:
    """The reply body is drawn and its full payload is going out as overflow bubbles — the
    gateway's regular send of that body would be a second copy."""
    return _wecom_delivery_consumer(consumer) and bool(getattr(consumer, "delivery_claimed", False))
