"""fork 2026-11-16 (DPMS 561213): the reply lane — one order for every send of ONE reply.

A reply reaches WeCom through several senders: the stream bubble's frames, the follow-up "overflow"
bubbles (a reply too long for the bubble), and the gateway's regular send. Without a lane the
stream's own frames and the sends queued in parallel could overtake each other.

The lane is a per-chat ``asyncio.Lock`` with a BOUNDED acquire (``_REPLY_LANE_WAIT_SECONDS``):

* whoever holds it, a send queues FIFO behind it — so a send made WHILE the reply is streamed
  cannot overtake the frame the stream is about to push.
* the tasks the holder itself starts (``asyncio.create_task`` copies the contextvar) enter at once:
  a send the reply's own stream context triggers never waits on its own reply.
* nobody waits longer than the bound: a task from OUTSIDE the holder's reply that is still waiting
  after the timeout enters anyway (unordered with the reply ahead, but never hung) and a WARNING
  is logged. The reply's own stream loop therefore does NOT hold the lane for its lifetime — it
  takes it per frame and per bubble — or its holder's send landing inside that window would wait
  the whole bound before every reply.

Deliberately not the task-attached ownership protocol it replaced (``hold_reply_lane`` /
``adopt_current_task``): the gateway never registered a holder for the stream task, so ownership
never applied, and the unbounded acquire deadlocked a holder waiting synchronously on a send it
had started — every reply, not only long ones (close-out notes on DPMS 561213).
"""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)

# Longest a send waits for the reply in front of it before entering unordered (never hangs).
_REPLY_LANE_WAIT_SECONDS = 15.0

# The reply's holder for the current context: a ReplyLaneScope while it is inside the lane. Each
# entry records the context it set, so releasing an entry whose context was already reset (a
# context this task left and came back to) cannot unset a NEWER holder of the same task.
_LANE_HOLDER: contextvars.ContextVar = contextvars.ContextVar("wecom_reply_lane_holder", default=None)

_LANES: dict[str, _Lane] = {}


class _Lane:
    __slots__ = ("holders", "lock")

    def __init__(self) -> None:
        self.lock = asyncio.Lock()
        self.holders = 0


class ReplyLaneScope:
    """One reply's lane: the holding task and every task started while it holds the lane."""

    __slots__ = ("chat_id", "task", "tasks")

    def __init__(self, chat_id: str, task: Any) -> None:
        self.chat_id = chat_id
        self.task = task
        self.tasks: set[int] = {id(task)} if task is not None else set()


def _lane(chat_id: str) -> _Lane:
    lane = _LANES.get(chat_id)
    if lane is None:
        lane = _LANES[chat_id] = _Lane()
    return lane


def _drop_idle(chat_id: str, lane: _Lane) -> None:
    if _LANES.get(chat_id) is lane and lane.holders == 0 and not lane.lock.locked():
        _LANES.pop(chat_id, None)


@contextlib.asynccontextmanager
async def reply_lane(chat_id: Optional[str], *, timeout: Optional[float] = None):
    """Serialize every send of ONE reply to *chat_id*, waiting at most ``timeout`` seconds.

    Re-entrant for the holder and the tasks it started; everyone else queues (FIFO) behind it."""
    chat = str(chat_id or "").strip()
    if not chat:
        yield
        return
    task = asyncio.current_task()
    outer = _LANE_HOLDER.get()
    if outer is not None:
        if outer.chat_id == chat:
            outer.tasks.add(id(task))
        # Already inside ANOTHER chat's reply (a reply reacting across chats): queueing on this
        # chat's lane could wait on that reply — go unordered, its own reply is still ordered.
        yield
        return
    lane = _lane(chat)
    wait = _REPLY_LANE_WAIT_SECONDS if timeout is None else max(0.0, timeout)
    try:
        await asyncio.wait_for(lane.lock.acquire(), timeout=wait)
    except TimeoutError:
        # A bound acquire has no late hand-over: the caller did not take the lane. It still sends —
        # unordered with respect to the reply ahead, but never hung on it.
        logger.warning("[%s] reply lane busy >%.0fs — sending unordered rather than waiting", chat, wait)
        scope = ReplyLaneScope(chat, task)
        token = _LANE_HOLDER.set(scope)
        try:
            yield scope
        finally:
            if _LANE_HOLDER.get() is scope:
                _LANE_HOLDER.reset(token)
        return
    scope = ReplyLaneScope(chat, task)
    token = _LANE_HOLDER.set(scope)
    lane.holders += 1
    try:
        yield scope
    finally:
        if _LANE_HOLDER.get() is scope:  # this task may already hold a newer scope
            _LANE_HOLDER.reset(token)
        lane.holders -= 1
        lane.lock.release()
        _drop_idle(chat, lane)
