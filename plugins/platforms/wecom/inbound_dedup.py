"""fork 2026-11-18 (DPMS 561213): the content-keyed dedup of WeCom inbound callbacks.

The platform re-delivers a callback it did not see answered in time. Production hm-5958d932-1f3
(2026-11-18): the agent's turn took 48s+, the same callback came back at +64s and +141s, and the
user got the same answer twice. Such a re-delivery carries NO msgid — the platform's own id is
absent — so the old dedup key (the msgid, else the random req_id) never matched and the agent ran
twice for ONE user message.

This module keys such a callback by its CONTENT: the same sender, chat, msgtype and text always
produce the same key, so a re-delivery of one message is recognisable as such. The key carries NO
time semantics — the age of a key comes from the cache, never from the key string.

Two tiers, both keyed on the age of the FIRST sighting:
  * younger than ``CONTENT_DEDUP_DROP_SECONDS`` (the shape of a platform replay): dropped
    silently — the user must see ONE reply.
  * older, but inside the TTL: the user may not have seen the first reply, so the reply that was
    already delivered is replayed instead of running the agent a second time.
"""

from __future__ import annotations

import hashlib
import logging
import time
from typing import Any, Optional

from gateway.platforms.helpers import MessageDeduplicator, bounded_put
from utils import env_float

logger = logging.getLogger("plugins.platforms.wecom.adapter")

# A re-delivery this soon after the first one is a platform replay, not a new question: the user
# must see ONE reply. The measured re-deliveries of production hm-5958d932-1f3 sat 0.5-1.8s apart.
CONTENT_DEDUP_DROP_SECONDS = 30.0
# Beyond this the same text is a NEW user message (the user may repeat a question on purpose).
CONTENT_DEDUP_TTL_SECONDS = 300.0
# The content-key caches are bounded like the msgid dedup beside them.
CONTENT_DEDUP_MAX_SIZE = 1000


class WeComInboundDedupMixin:
    """fork 2026-11-18 (DPMS 561213): the content-keyed dedup of the WeCom inbound callbacks.

    Expects the adapter to provide ``name``, ``send()`` and ``_remember_chat_req_id()``.
    """

    # ------------------------------------------------------------------
    # State
    # ------------------------------------------------------------------

    def _init_content_dedup(self) -> None:
        """Create the content-key caches. Called once from the adapter's ``__init__``."""
        self._content_dedup_drop_seconds = env_float(
            "HERMES_WECOM_CONTENT_DEDUP_DROP_SECONDS", CONTENT_DEDUP_DROP_SECONDS
        )
        self._content_dedup_ttl_seconds = max(
            env_float("HERMES_WECOM_CONTENT_DEDUP_TTL_SECONDS", CONTENT_DEDUP_TTL_SECONDS),
            self._content_dedup_drop_seconds + 1.0,
        )
        # The content-keyed dedup is its OWN cache: the msgid dedup keeps its own semantics, and
        # this one is carried across a reconnect by ``carry_inbound_dedup`` (a MessageDeduplicator).
        self._content_dedup = MessageDeduplicator(
            max_size=CONTENT_DEDUP_MAX_SIZE, ttl_seconds=self._content_dedup_ttl_seconds
        )
        # inbound msg_id -> its content key (the msg_id of a content-keyed inbound IS its req_id)
        self._content_key_by_msg_id: dict[str, str] = {}
        # content key -> (the reply that was delivered for it, when)
        self._delivered_reply_by_key: dict[str, tuple[str, float]] = {}
        # the content keys whose reply was already replayed (a replay goes out ONCE)
        self._replayed_content_keys: set[str] = set()

    # ------------------------------------------------------------------
    # Keys
    # ------------------------------------------------------------------

    def _content_key_for(self, body: dict[str, Any], sender_id: str, chat_id: str) -> Optional[str]:
        """fork 2026-11-18 (DPMS 561213): the stable content key of a callback without a msgid.

        ``content:<sender>|<chat>|<msgtype>|<sha1(text)[:16]>`` — the same user message always
        produces the same key, and two different messages never collide (a different sender, chat
        or text yields a different key). ``None`` when the message carries no text (a pure media
        message): there is no stable content to key on, and the caller keeps the old req_id key.
        """
        text, _reply_text = self._extract_text(body)
        if not text:
            return None
        digest = hashlib.sha1(text.encode("utf-8", "replace")).hexdigest()[:16]
        return f"content:{sender_id}|{chat_id}|{body.get('msgtype') or ''}|{digest}"

    def _remember_content_key(self, msg_id: str, content_key: str) -> None:
        """Bind the content key to the inbound that carried it."""
        if msg_id:
            bounded_put(self._content_key_by_msg_id, msg_id, content_key, CONTENT_DEDUP_MAX_SIZE)

    # ------------------------------------------------------------------
    # Classification
    # ------------------------------------------------------------------

    async def _handle_content_keyed_inbound(
        self, *, content_key: str, msg_id: str, chat_id: str, sender_id: str, req_id: str
    ) -> bool:
        """Classify a content-keyed inbound and consume it when it is a re-delivery.

        Returns True when the callback was consumed (a re-delivery: dropped or replayed) and
        False when it is a NEW user message the agent must answer.
        """
        first_seen = self._content_dedup.seen_at(content_key)
        if first_seen is None:
            # First sighting — or the TTL expired, so the user repeated the question on purpose.
            # Register the key (the age of a key comes from this cache, never from the key) and
            # let the message through.
            self._content_dedup.is_duplicate(content_key)
            self._remember_content_key(msg_id, content_key)
            return False
        age = max(0.0, time.time() - first_seen)
        if age < self._content_dedup_drop_seconds:
            # A platform re-delivery this soon after the first one: the agent must NOT run again
            # and the user must NOT get a second reply — the first one is on its way.
            logger.info(
                "[%s] Duplicate inbound dropped (content key, <%.0fs) sender=%s text_sha=%s age=%.1fs dropped=1 replayed=0 chat=%s",
                self.name, self._content_dedup_drop_seconds, sender_id,
                content_key.rsplit("|", 1)[-1], age, chat_id,
            )
            return True
        # Older than that: the user may not have seen the first reply. Do NOT run the agent a
        # second time — replay the reply that was already delivered.
        await self._replay_content_reply(
            content_key=content_key, chat_id=chat_id, sender_id=sender_id, age=age, req_id=req_id
        )
        return True

    async def _replay_content_reply(
        self, *, content_key: str, chat_id: str, sender_id: str, age: float, req_id: str
    ) -> bool:
        """Replay the reply already delivered for this message; never run the agent again.

        The replay goes out ONCE per message: a platform that re-delivers three times must not
        put three copies of the same answer in front of the user.
        """
        text_sha = content_key.rsplit("|", 1)[-1]
        if content_key in self._replayed_content_keys:
            logger.info(
                "[%s] Duplicate inbound dropped (content key, reply already replayed) sender=%s text_sha=%s age=%.1fs dropped=1 replayed=0 chat=%s",
                self.name, sender_id, text_sha, age, chat_id,
            )
            return False
        delivered = self._delivered_reply_by_key.get(content_key)
        if not delivered or not delivered[0]:
            logger.info(
                "[%s] Duplicate inbound dropped (content key, no delivered reply to replay) sender=%s text_sha=%s age=%.1fs dropped=1 replayed=0 chat=%s",
                self.name, sender_id, text_sha, age, chat_id,
            )
            return False
        # The re-delivered callback carries a FRESH req_id: cache it, so the replay goes out as a
        # passive reply on it (and falls back to a proactive send when the platform refuses it).
        # The replay itself carries ``is_redelivery``: a failed replay is never re-stashed for a
        # later group redelivery (a replay loop), and its send records no second delivered reply.
        if req_id:
            self._remember_chat_req_id(chat_id, req_id)
        result = await self.send(chat_id=chat_id, content=delivered[0], metadata={"is_redelivery": True})
        replayed = bool(result.success)
        if replayed:
            self._replayed_content_keys.add(content_key)
            while len(self._replayed_content_keys) > CONTENT_DEDUP_MAX_SIZE:
                self._replayed_content_keys.pop()
        logger.info(
            "[%s] Duplicate inbound replayed (content key, %.0f-%.0fs) sender=%s text_sha=%s age=%.1fs dropped=0 replayed=%d chat=%s",
            self.name, self._content_dedup_drop_seconds, self._content_dedup_ttl_seconds,
            sender_id, text_sha, age, 1 if replayed else 0, chat_id,
        )
        return replayed

    # ------------------------------------------------------------------
    # The delivered reply (what a re-delivery replays)
    # ------------------------------------------------------------------

    def _record_delivered_reply(self, chat_id: str, text: str, *, req_id: Optional[str] = None) -> None:
        """fork 2026-11-18 (DPMS 561213): remember the reply delivered for an inbound.

        The content key is resolved from the request id the reply was sent on (the inbound's own
        req_id), so a reply is never filed under another message's key. With no req_id the chat's
        stream turn is consulted (the turn the reply was drawn in) and, failing that, the chat's
        latest content key.
        """
        key = self._content_key_for_reply(chat_id, req_id)
        if not key or not text:
            return
        delivered = getattr(self, "_delivered_reply_by_key", None)
        if delivered is None:
            return
        delivered[key] = (text, time.time())
        self._bound_delivered_replies()

    def _bound_delivered_replies(self) -> None:
        """Keep the delivered-reply cache bounded: the oldest entry yields its slot."""
        delivered = getattr(self, "_delivered_reply_by_key", None)
        if not isinstance(delivered, dict):
            return
        while len(delivered) > CONTENT_DEDUP_MAX_SIZE:
            oldest = min(delivered.items(), key=lambda item: item[1][1])[0]
            delivered.pop(oldest, None)

    def _content_key_for_reply(self, chat_id: str, req_id: Optional[str]) -> Optional[str]:
        """The content key a reply belongs to: the request id it was sent on.

        No chat-level fallback ON PURPOSE: a reply whose request id does not map to a content
        key (a msgid-keyed inbound, or a push that never answered an inbound) belongs to NO
        content key, and filing it under the chat's latest one would make a later re-delivery
        replay the wrong text.
        """
        by_msg_id = getattr(self, "_content_key_by_msg_id", None) or {}
        candidate = str(req_id or "").strip()
        if candidate and candidate in by_msg_id:
            return by_msg_id[candidate]
        turn_req_id = str(getattr(self._reply_turn(chat_id), "req_id", "") or "").strip()
        return by_msg_id.get(turn_req_id)

    def _reply_turn(self, chat_id: str) -> Any:
        """The stream turn a reply was drawn in: the chat's live turn, else its last sealed one."""
        turns = getattr(self, "_stream_turns", None)
        if isinstance(turns, dict):
            live = next(
                (t for t in turns.values()
                 if getattr(t, "chat_id", None) == chat_id and not getattr(t, "finalized", False)),
                None,
            )
            if live is not None:
                return live
        sealed = getattr(self, "_last_stream_turn_by_chat", None)
        return sealed.get(chat_id) if isinstance(sealed, dict) else None

    # ------------------------------------------------------------------
    # Reconnect
    # ------------------------------------------------------------------

    def carry_content_dedup_state(self, predecessor: Any) -> None:
        """fork 2026-11-18 (DPMS 561213): adopt the replaced adapter's content-key state.

        ``carry_inbound_dedup`` absorbs ``_content_dedup`` by itself (a MessageDeduplicator); the
        msg_id -> key map, the delivered-reply cache and the replayed set are plain containers it
        cannot know. Without them a re-delivery arriving just after a reconnect — exactly when the
        platform re-pushes what went unanswered — would be admitted as a first sighting and the
        agent would run again.
        """
        previous_keys = getattr(predecessor, "_content_key_by_msg_id", None)
        if isinstance(previous_keys, dict):
            for msg_id, key in previous_keys.items():
                bounded_put(self._content_key_by_msg_id, msg_id, key, CONTENT_DEDUP_MAX_SIZE)
        previous_replies = getattr(predecessor, "_delivered_reply_by_key", None)
        if isinstance(previous_replies, dict):
            self._delivered_reply_by_key.update(previous_replies)
            self._bound_delivered_replies()
        previous_replayed = getattr(predecessor, "_replayed_content_keys", None)
        if isinstance(previous_replayed, set):
            self._replayed_content_keys |= previous_replayed
