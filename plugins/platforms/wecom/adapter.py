"""WeCom (Enterprise WeChat) AI Bot adapter over the ``openws`` WebSocket gateway.
Streaming lives in ``streaming.py``, media in ``media.py``, per-chat send queue in ``send_queue.py``.
Config (``platforms.wecom.extra``): ``bot_id``/``secret`` (or WECOM_BOT_ID / WECOM_SECRET), ``websocket_url``,
``dm_policy``/``group_policy`` (open|allowlist|disabled|pairing), ``allow_from``, ``group_allow_from``,
``groups: {<group_id>: {allow_from: [...]}}``."""

from __future__ import annotations

from pm import install_hint
import asyncio
import contextlib
import json
import logging
import os
import re
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse  # FORK(wbkunlun): explicit — the deleted PLUGIN-COMPAT
# shim (#126164) used to re-export this name; _open_connection needs it for proxy resolution.

try:
    import aiohttp
except ImportError:
    aiohttp = None  # type: ignore[assignment]
try:
    import httpx
except ImportError:
    httpx = None  # type: ignore[assignment]
AIOHTTP_AVAILABLE = aiohttp is not None
HTTPX_AVAILABLE = httpx is not None

from gateway.config import Platform, PlatformConfig
from gateway.platforms.helpers import MessageDeduplicator, bounded_put, send_chunks
from gateway.platforms.access_policy_mixin import OwnAccessPolicyMixin
from plugins.platforms.wecom.stream_delivery import WeComStreamDelivery
from gateway.platforms.base import gateway_trust_env, BasePlatformAdapter, SendResult
from gateway.platforms.event import MessageEvent, MessageType
from utils import env_float, env_int

from gateway.platforms._shared import get_scoped_secret as _get_scoped_secret, send_error
from plugins.platforms.wecom.send_queue import ChatSendQueueMixin
from plugins.platforms.wecom.learned_chats import LearnedChatsMixin
from plugins.platforms.wecom.media import WeComMediaMixin, APP_CMD_SEND
from plugins.platforms.wecom.callback_adapter import _split_markdown_bytes
from plugins.platforms.wecom.streaming import (
    WeComStreamMixin, ReplyQueue, StreamTurn, APP_CMD_RESPONSE,
    STREAM_NOT_SUBSCRIBED_ERRCODE, MAX_STREAM_CONTENT_LENGTH,
    STREAM_SAFE_DURATION_SECONDS, STREAM_KEEPALIVE_INTERVAL_SECONDS, STREAM_KEEPALIVE_ENABLED_DEFAULT,
)


logger = logging.getLogger(__name__)

DEFAULT_WS_URL = "wss://openws.work.weixin.qq.com"

APP_CMD_SUBSCRIBE = "aibot_subscribe"
APP_CMD_CALLBACK = "aibot_msg_callback"
APP_CMD_LEGACY_CALLBACK = "aibot_callback"
APP_CMD_EVENT_CALLBACK = "aibot_event_callback"
APP_CMD_SEND = "aibot_send_msg"
APP_CMD_RESPONSE = "aibot_respond_msg"
APP_CMD_RESPONSE_WELCOME = "aibot_respond_welcome_msg"
APP_CMD_PING = "ping"
APP_CMD_PONG = "pong"
APP_CMD_UPLOAD_MEDIA_INIT = "aibot_upload_media_init"
APP_CMD_UPLOAD_MEDIA_CHUNK = "aibot_upload_media_chunk"
APP_CMD_UPLOAD_MEDIA_FINISH = "aibot_upload_media_finish"

CALLBACK_COMMANDS = {APP_CMD_CALLBACK, APP_CMD_LEGACY_CALLBACK}
NON_RESPONSE_COMMANDS = CALLBACK_COMMANDS | {APP_CMD_EVENT_CALLBACK}

MAX_MESSAGE_LENGTH = 4000
# fork 2026-09-30 (audit 2026-09-29 module-1 M1.8): aibot markdown frames are capped in UTF-8
# BYTES (server-side), like stream frames — the old [:4000] CHAR slice shipped
# 12KB CJK frames whole and the server rejected them. 4000 chars of ASCII stays
# one segment; CJK splits at ≤4096 bytes.
# 与 callback 渠道 MARKDOWN_MAX_BYTES=4096 同值——两渠道上限独立演进，改一处勿忘另一处。
AIBOT_MARKDOWN_MAX_BYTES = 4096
CONNECT_TIMEOUT_SECONDS = 20.0
REQUEST_TIMEOUT_SECONDS = 15.0
HEARTBEAT_INTERVAL_SECONDS = 30.0
RECONNECT_BACKOFF = [2, 5, 10, 30, 60]

DEDUP_MAX_SIZE = 1000


def _parse_group_retry_delays(raw: str) -> Tuple[float, ...]:
    """fork 2026-10-08: timed-retry schedule for group sends parked in the 846609 dead
    window (socket dead → subscribe-ack pending; a send landing there gets a REAL 846609
    response because _open_connection sets self._ws before the SUBSCRIBE ack). Reconnect
    backoff is 2..60s, so 30s/120s retries land after the resubscribe; a quiet group's
    next inbound may be hours away. Comma-separated seconds; "0" disables."""
    try:
        values = [float(part) for part in str(raw or "").replace(";", ",").split(",") if str(part).strip()]
    except ValueError:
        return (30.0, 120.0)
    values = [v for v in values if v > 0]
    return tuple(values) or (30.0, 120.0)


def check_wecom_requirements() -> bool:
    return AIOHTTP_AVAILABLE and HTTPX_AVAILABLE


def _coerce_list(value: Any) -> List[str]:
    """Coerce config values (None | "a, b" | iterable | scalar) into a trimmed, non-empty string list."""
    if isinstance(value, str):
        value = value.split(",")
    elif not isinstance(value, (list, tuple, set)):
        value = [] if value is None else [value]
    return [item for item in (str(item).strip() for item in value) if item]


def _normalize_entry(raw: str) -> str:
    """Normalize allowlist entries such as ``wecom:user:foo``."""
    value = re.sub(r"^wecom:", "", str(raw).strip(), flags=re.IGNORECASE)
    return re.sub(r"^(user|group):", "", value, flags=re.IGNORECASE).strip()


def _entry_matches(entries: List[str], target: str) -> bool:
    """Case-insensitive allowlist match with ``*`` support."""
    normalized_target = str(target).strip().lower()
    return any(_normalize_entry(e).lower() in ("*", normalized_target) for e in entries)


def _dict_or_empty(container: Dict[str, Any], key: str) -> Dict[str, Any]:
    return container.get(key) if isinstance(container.get(key), dict) else {}


def _content_of(container: Dict[str, Any], key: str) -> str:
    return str(_dict_or_empty(container, key).get("content") or "").strip()


class WeComAdapter(WeComStreamMixin, WeComMediaMixin, ChatSendQueueMixin, LearnedChatsMixin, OwnAccessPolicyMixin, BasePlatformAdapter):
    """WeCom AI Bot adapter backed by a persistent WebSocket connection."""

    ALLOW_ALL_ENV_PREFIX = "WECOM"
    MAX_MESSAGE_LENGTH = MAX_MESSAGE_LENGTH
    SUPPORTS_MESSAGE_EDITING = False
    # WeCom AI Bot supports msgtype: "stream" via aibot_respond_msg, which
    # the gateway streaming consumer treats as a transport that bypasses the
    # edit-based path. See ``send_stream_frame`` and ``supports_native_streaming``.
    SUPPORTS_NATIVE_STREAMING = True
    # Fork: clawrelay-style single-bubble delivery (see stream_delivery.py).
    # gateway/run.py branches on this class attribute to route WeCom
    # streaming through WeComStreamDelivery (think-block UX, 300ms throttle,
    # running indicator) instead of the generic GatewayStreamConsumer.
    WECOM_STREAM_DELIVERY = WeComStreamDelivery
    MAX_STREAM_CONTENT_LENGTH = MAX_STREAM_CONTENT_LENGTH
    splits_long_messages = True  # send() chunks via truncate_message(MAX_MESSAGE_LENGTH)
    _SPLIT_THRESHOLD = 3900  # chunks near the 4000-char client split are almost certainly continued

    def __init__(self, config: PlatformConfig):
        super().__init__(config, Platform.WECOM)
        extra = config.extra or {}

        def _extra_float(key: str, default: float) -> float:
            try:
                return float(extra.get(key, default))
            except (TypeError, ValueError):
                return default

        def _setting(*keys: str, env: str = "", default: str = "") -> str:
            return str(next((extra[k] for k in keys if extra.get(k)), None) or (_get_scoped_secret(env, default) if env else "")).strip()

        self._bot_id = _setting("bot_id", env="WECOM_BOT_ID")
        self._secret = _setting("secret", env="WECOM_SECRET")
        self._ws_url = _setting("websocket_url", "websocketUrl", env="WECOM_WEBSOCKET_URL", default=DEFAULT_WS_URL) or DEFAULT_WS_URL
        self._dm_policy = _setting("dm_policy", env="WECOM_DM_POLICY", default="pairing").lower()
        # WECOM_ALLOWED_USERS fallback: env-only allowlist setups otherwise drop every DM at intake.
        self._allow_from = _coerce_list(extra.get("allow_from") or extra.get("allowFrom") or _get_scoped_secret("WECOM_ALLOWED_USERS", ""))
        self._group_policy = _setting("group_policy", env="WECOM_GROUP_POLICY", default="pairing").lower()
        # fork: mirror the DM allowlist — group_policy honors WECOM_GROUP_POLICY, so the
        # group allowlist honors WECOM_GROUP_ALLOWED_USERS too. Without the env fallback an
        # env-only setup (group_policy=allowlist via env, no config extra) runs with an
        # empty group allowlist.
        self._group_allow_from = _coerce_list(
            extra.get("group_allow_from")
            or extra.get("groupAllowFrom")
            or _get_scoped_secret("WECOM_GROUP_ALLOWED_USERS", "")
        )
        self._groups = extra.get("groups") if isinstance(extra.get("groups"), dict) else {}
        self._session = self._ws = self._http_client = self._listen_task = self._heartbeat_task = None
        self._pending_responses: Dict[str, asyncio.Future] = {}
        self._reply_queues: Dict[str, ReplyQueue] = {}
        self._dedup, self._reply_req_ids = MessageDeduplicator(max_size=DEDUP_MAX_SIZE), {}
        # Text batching (clients split long messages ~4000 chars); attachment-only frames are held
        # for the merge window so the trailing text callback joins the same event (official: 800ms).
        self._text_batch_delay_seconds = env_float("HERMES_WECOM_TEXT_BATCH_DELAY_SECONDS", 0.6)
        self._text_batch_split_delay_seconds = env_float("HERMES_WECOM_TEXT_BATCH_SPLIT_DELAY_SECONDS", 2.0)
        self._attachment_text_merge_delay_seconds = _extra_float("attachment_text_merge_delay_seconds", 0.8)
        # fork: group pending-redelivery queue (a group send with no live req_id AND a dead
        # proactive aibot_send_msg is stashed and flushed pre-turn on the group's next inbound
        # message — fork 2026-10-06 added the proactive fallback). TTL 0 disables.
        self._group_pending_ttl_seconds = env_float("HERMES_WECOM_GROUP_PENDING_TTL_SECONDS", 21600.0)
        self._group_pending_max = env_int("HERMES_WECOM_GROUP_PENDING_MAX", 5)
        # fork 2026-10-08: dead-window timed retry schedule for parked group sends.
        self._group_retry_delays = _parse_group_retry_delays(os.getenv("HERMES_WECOM_GROUP_RETRY_DELAYS", "30,120"))
        self._group_retry_timers: Dict[str, asyncio.TimerHandle] = {}
        self._group_retry_attempts: Dict[str, int] = {}
        # Stream keep-alive config (see streaming.py STREAM_* constants).
        self._stream_safe_duration_seconds = _extra_float("stream_safe_duration_seconds", STREAM_SAFE_DURATION_SECONDS)
        self._stream_keepalive_enabled = bool(extra.get("stream_keepalive_enabled", STREAM_KEEPALIVE_ENABLED_DEFAULT))
        self._stream_keepalive_interval_seconds = _extra_float("stream_keepalive_interval_seconds", STREAM_KEEPALIVE_INTERVAL_SECONDS)
        # fork: stable device id across restarts (WECOM_DEVICE_ID) keeps the subscription
        # identity — a fresh random hex per boot can trigger server-side session churn.
        self._device_id = os.getenv("WECOM_DEVICE_ID") or uuid.uuid4().hex
        self._last_chat_req_ids: Dict[str, str] = {}
        # Turns keyed f"{chat_id}:{req_id|turn_id}"; expired chats clear on the next inbound req_id.
        self._stream_turns: Dict[str, StreamTurn] = {}
        self._stream_expired_chats, self._group_chat_ids = set(), set()  # groups: passive reply first, proactive aibot_send_msg fallback (fork 2026-10-06)
        # fork: (enqueue_time, content) lists per group chat awaiting the next inbound req_id; and the
        # DM chatid→userid map (wohR-style chat ids learned from inbound senders) so agent-fallback
        # can resolve a self-built-app touser. The learned classification persists to
        # wecom_learned_chats.json (fork 2026-10-08, see learned_chats.py); the pending queue stays
        # in-memory by design (TTL/时效语义).
        self._pending_group_sends: Dict[str, List[Tuple[float, str]]] = {}
        self._dm_userid_by_chat: Dict[str, str] = {}
        # fork 2026-10-08: learned classification survives restarts (quiet groups otherwise lose
        # their group semantics and scheduled pushes take the DM branch — 2026-10-08 incident).
        self._learned_save_handle = None
        self._load_learned_chats()
        # Per-chat FIFO send queues (normal + control lanes) + token buckets — see send_queue.py.
        self._chat_queues, self._chat_workers, self._control_queues, self._control_workers, self._chat_token_usage = {}, {}, {}, {}, {}

    def _startup_failure(self, code: str, message: str, log_msg: str, *args: Any) -> bool:
        self._set_fatal_error(code, message, retryable=True)
        logger.warning(log_msg, self.name, message, *args)
        return False

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        for available, dep in ((AIOHTTP_AVAILABLE, "aiohttp"), (HTTPX_AVAILABLE, "httpx")):
            if not available:
                return self._startup_failure("wecom_missing_dependency", f"WeCom startup failed: {dep} not installed", "[%s] %s. Run: pip install %s", dep)
        if not self._bot_id or not self._secret:
            return self._startup_failure("wecom_missing_credentials", "WeCom startup failed: WECOM_BOT_ID and WECOM_SECRET are required", "[%s] %s")
        try:
            # Tighter keepalive so idle CLOSE_WAIT drains promptly.
            # See #18451.
            from gateway.platforms._http_client_limits import platform_httpx_limits
            from gateway.platforms.base import _ssrf_redirect_guard
            from tools.url_safety import create_ssrf_safe_async_client
            self._http_client = create_ssrf_safe_async_client(timeout=30.0, follow_redirects=True, event_hooks={"response": [_ssrf_redirect_guard]}, limits=platform_httpx_limits())
            await self._open_connection()
            self._mark_connected()
            self._listen_task, self._heartbeat_task = asyncio.create_task(self._listen_loop()), asyncio.create_task(self._heartbeat_loop())
            logger.info("[%s] Connected to %s", self.name, self._ws_url)
            self._wire_plugin_handlers(None)  # ctx.register_platform_handler hooks
            _warn_if_agent_fallback_unconfigured()
            return True
        except Exception as exc:
            self._set_fatal_error("wecom_connect_error", f"WeCom startup failed: {exc}", retryable=True)
            logger.error("[%s] Failed to connect: %s", self.name, exc, exc_info=True)
            await self._teardown()
            return False

    async def disconnect(self) -> None:
        self._running = False
        self._mark_disconnected()
        self._cancel_learned_save()
        for handle in list(self._group_retry_timers.values()):
            handle.cancel()
        self._group_retry_timers.clear()
        self._group_retry_attempts.clear()
        for task in list(self._chat_workers.values()) + list(self._control_workers.values()):
            task.cancel()
        for registry in (self._chat_workers, self._control_workers, self._chat_queues, self._control_queues):
            registry.clear()
        for attr in ("_listen_task", "_heartbeat_task"):
            task = getattr(self, attr)
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            setattr(self, attr, None)
        self._fail_all(RuntimeError("WeCom adapter disconnected"))
        await self._teardown()
        self._dedup.clear()
        logger.info("[%s] Disconnected", self.name)

    def _fail_all(self, exc: Exception) -> None:
        self._fail_pending_responses(exc)
        self._fail_reply_queues(exc)

    async def _cleanup_ws(self) -> None:
        """Close the live websocket, then its session, if any."""
        for attr in ("_ws", "_session"):
            live = getattr(self, attr)
            if live and not live.closed:
                await live.close()
            setattr(self, attr, None)

    async def _teardown(self) -> None:
        """_cleanup_ws, then close the httpx client."""
        await self._cleanup_ws()
        if self._http_client:
            await self._http_client.aclose()
            self._http_client = None

    async def _open_connection(self) -> None:
        await self._cleanup_ws()
        # aiohttp's trust_env does an EXACT scheme match (wss:// needs
        # WSS_PROXY, not HTTP_PROXY), so a deployment that only sets
        # HTTP_PROXY gets NO proxy for the WebSocket and times out behind
        # an HTTP egress proxy (e.g. Tencent Cloud). Resolve the proxy
        # explicitly and pass it to ws_connect; with no proxy configured,
        # fall back to trust_env=True (the previous behavior).
        from gateway.platforms.base import gateway_trust_env, proxy_kwargs_for_aiohttp, resolve_proxy_url
        ws_host = urlparse(self._ws_url).hostname
        proxy_url = resolve_proxy_url(target_hosts=[ws_host] if ws_host else None)
        sess_kw, req_kw = proxy_kwargs_for_aiohttp(proxy_url)
        if not sess_kw:
            # No explicit proxy resolved: honor the gateway.trust_env config gate (upstream).
            sess_kw = {"trust_env": gateway_trust_env() and not bool(proxy_url)}
        # Use certifi's CA bundle so aiohttp trusts the same roots as
        # urllib/requests — avoids SSL_CERTIFICATE_VERIFY_FAILED on macOS
        # where the OpenSSL default path may be empty or stale.
        import ssl as _ssl
        try:
            import certifi
            cafile = certifi.where()
        except ImportError:
            cafile = None
        _ssl_ctx = _ssl.create_default_context(cafile=cafile)
        _connector = aiohttp.TCPConnector(ssl=_ssl_ctx)
        sess_kw.setdefault("connector", _connector)
        self._session = aiohttp.ClientSession(**sess_kw)
        # Defense-in-depth: if anything between ws_connect and the SUBSCRIBE
        # ack raises (proxy failure, server-side close mid-handshake, errcode
        # on the ack), reset _ws and _session so the next read goes through
        # the "not connected" branch and the listen loop reconnects from a
        # clean state. Without this, a failed handshake leaves _ws pointing
        # at a closed socket, which used to trigger a CPU-spin in the
        # listen loop.
        try:
            self._ws = await self._session.ws_connect(
                self._ws_url,
                heartbeat=HEARTBEAT_INTERVAL_SECONDS * 2,
                timeout=CONNECT_TIMEOUT_SECONDS,
                **req_kw,
            )

            req_id = self._new_req_id("subscribe")
            await self._send_json(
                {
                    "cmd": APP_CMD_SUBSCRIBE,
                    "headers": {"req_id": req_id},
                    "body": {
                        "bot_id": self._bot_id,
                        "secret": self._secret,
                        "device_id": self._device_id,
                    },
                }
            )

            auth_payload = await self._wait_for_handshake(req_id)
            errcode = auth_payload.get("errcode", 0)
            if errcode not in {0, None}:
                errmsg = auth_payload.get("errmsg", "authentication failed")
                raise RuntimeError(f"{errmsg} (errcode={errcode})")
        except BaseException:
            # Close the session/ws we just opened so a failed handshake
            # doesn't leak an aiohttp ClientSession (seen as "Unclosed
            # client session" warnings during network outages with
            # repeated reconnect attempts). try/except guards against
            # CancelledError during the close itself; aiohttp close() is
            # idempotent so calling on an already-closed object is safe.
            if self._ws:
                try:
                    await self._ws.close()
                except Exception:
                    pass
            self._ws = None
            if self._session:
                try:
                    await self._session.close()
                except Exception:
                    pass
            self._session = None
            raise

    async def _wait_for_handshake(self, req_id: str) -> Dict[str, Any]:
        if not self._ws:
            raise RuntimeError("WebSocket not initialized")
        loop = asyncio.get_running_loop()
        deadline = loop.time() + CONNECT_TIMEOUT_SECONDS
        while (remaining := deadline - loop.time()) > 0:
            msg = await asyncio.wait_for(self._ws.receive(), timeout=remaining)
            if msg.type == aiohttp.WSMsgType.TEXT:
                payload = self._parse_json(msg.data)
                if not payload or payload.get("cmd") == APP_CMD_PING:
                    continue
                if self._payload_req_id(payload) == req_id:
                    return payload
                logger.debug("[%s] Ignoring pre-auth payload: %s", self.name, payload.get("cmd"))
            elif msg.type in {aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.ERROR}:
                raise RuntimeError("WeCom websocket closed during authentication")
        raise TimeoutError("Timed out waiting for WeCom subscribe acknowledgement")

    async def _listen_loop(self) -> None:
        backoff_idx = 0
        while self._running:
            try:
                await self._read_events()
                backoff_idx = 0
            except asyncio.CancelledError:
                return
            except Exception as exc:
                if not self._running:
                    return
                logger.warning("[%s] WebSocket error: %s", self.name, exc)
                self._fail_all(RuntimeError("WeCom connection interrupted"))
                await asyncio.sleep(RECONNECT_BACKOFF[min(backoff_idx, len(RECONNECT_BACKOFF) - 1)])
                backoff_idx += 1
                try:
                    await self._open_connection()
                    backoff_idx = 0
                    self._mark_connected()
                    logger.info("[%s] Reconnected", self.name)
                except Exception as reconnect_exc:
                    logger.warning("[%s] Reconnect failed: %s", self.name, reconnect_exc)

    async def _read_events(self) -> None:
        if not self._ws:
            raise RuntimeError("WebSocket not connected")
        # Guard against the post-failed-handshake zombie state: ``_ws`` is set
        # but the server closed the socket during ``_wait_for_handshake``,
        # so the while-loop body below would skip and the function would
        # silently return — making ``_listen_loop`` CPU-spin with no reconnect.
        # Raising here routes the failure back through the listen loop's
        # reconnect path with proper backoff and logging.
        if self._ws.closed:
            raise RuntimeError("WeCom websocket already closed before read")

        while self._running and self._ws and not self._ws.closed:
            msg = await self._ws.receive()
            if msg.type in (aiohttp.WSMsgType.TEXT, aiohttp.WSMsgType.BINARY):
                await self._handle_frame(msg.data, msg.type == aiohttp.WSMsgType.BINARY)
            elif msg.type in {aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSING}:
                raise RuntimeError("WeCom websocket closed")
            else:
                logger.info("[%s] Inbound frame ignored: WSMsgType=%s", self.name, msg.type)

    async def _handle_frame(self, data: Any, is_binary: bool) -> None:
        """Parse one TEXT/BINARY frame and dispatch it; every drop is logged at INFO."""
        data_len = len(data) if isinstance(data, (str, bytes, bytearray)) else -1
        if is_binary:  # WeCom should send TEXT; log a preview so an unhandled transport isn't silently dropped
            decoded = data.decode("utf-8", errors="replace") if isinstance(data, (bytes, bytearray)) else "<undecodable>"
            logger.info("[%s] Inbound BINARY frame received (len=%d) head=%r — attempting JSON parse", self.name, data_len, decoded[:200])
        payload = self._parse_json(data)
        if payload:
            await self._dispatch_payload(payload)
        elif is_binary:
            logger.info("[%s] BINARY frame not parseable as JSON — dropped", self.name)
        else:  # _parse_json logged the detail; make the DROP itself visible at INFO
            logger.info("[%s] Inbound TEXT frame dropped (unparseable/non-dict) len=%d", self.name, data_len)

    async def _heartbeat_loop(self) -> None:
        try:
            while self._running:
                await asyncio.sleep(HEARTBEAT_INTERVAL_SECONDS)
                try:
                    if self._ws and not self._ws.closed:
                        await self._send_json({"cmd": APP_CMD_PING, "headers": {"req_id": self._new_req_id("ping")}, "body": {}})
                except Exception as exc:
                    logger.debug("[%s] Heartbeat send failed: %s", self.name, exc)
        except asyncio.CancelledError:
            pass

    async def _dispatch_payload(self, payload: Dict[str, Any]) -> None:
        req_id = self._payload_req_id(payload)
        cmd = str(payload.get("cmd") or "")
        body_dict = payload.get("body") if isinstance(payload.get("body"), dict) else None
        if self._reply_queues and cmd != APP_CMD_PING:
            logger.debug("[%s] _dispatch_payload[ALL]: req_id=%s cmd=%r active_queues=%s", self.name, req_id or "(none)", cmd or "(empty)", list(self._reply_queues.keys()))
        if req_id and self._reply_queues.get(req_id):
            logger.debug(
                "[%s] _dispatch_payload: req_id=%s cmd=%r has_pending_ack=%s errcode=%s in_NON_RESPONSE=%s payload_keys=%s", self.name, req_id, cmd,
                self._reply_queues[req_id].pending_ack is not None, body_dict.get("errcode", "N/A") if body_dict is not None else "N/A", cmd in NON_RESPONSE_COMMANDS, list(payload.keys()),
            )
        # Reply-queue acks (inbound req_id, no/other cmd) MUST win over _pending_responses.
        if req_id and cmd not in NON_RESPONSE_COMMANDS:
            if self._resolve_reply_ack(req_id, payload):
                return
            if req_id in self._pending_responses:
                future = self._pending_responses[req_id]
                if future and not future.done():
                    future.set_result(payload)
                return
        if cmd in CALLBACK_COMMANDS:
            await self._on_message(payload)
            return
        if cmd == APP_CMD_PING:
            # Answer server pings or the subscription eventually dies with
            # errcode 846609: the server pings the app over the long
            # connection and expects a pong keyed to the same req_id.
            if req_id:
                await self._send_json({
                    "cmd": APP_CMD_PONG,
                    "headers": {"req_id": req_id},
                    "body": {},
                })
            return
        if cmd == APP_CMD_EVENT_CALLBACK:
            # Check for "kicked by server" event — WeCom sends this when a new
            # connection is established elsewhere (another instance). Mirror the
            # official OpenClaw SDK: suppress reconnect to avoid mutual kicking.
            body = payload.get("body") or {}
            event_type = str(body.get("event_type") or "")
            if event_type == "disconnected_event":
                logger.warning(
                    "[%s] Kicked by server (another WS connection established). "
                    "Suppressing reconnect to avoid mutual kicking. "
                    "Check for duplicate gateway instances.",
                    self.name,
                )
                self._running = False  # stop _listen_loop from reconnecting
            # Route aibot events (e.g. enter_chat welcome) to the handler.
            await self._on_event(payload)
            return

        # Unrouted payload — did not match reply-queue, pending-response,
        # callback, ping, or event. If WeCom delivers group messages under a
        # cmd not in CALLBACK_COMMANDS, they land here and are dropped. Log at
        # INFO with cmd + body keys so we can spot an unhandled callback cmd.
        body_keys = list(payload.get("body", {}).keys()) if isinstance(payload.get("body"), dict) else None
        logger.info(
            "[%s] Unrouted websocket payload dropped: cmd=%r req_id=%s body_keys=%s",
            self.name, cmd or "(empty)", req_id or "(none)", body_keys,
        )

    def _fail_pending_responses(self, exc: Exception) -> None:
        for req_id, future in list(self._pending_responses.items()):
            if not future.done():
                future.set_exception(exc)
            self._pending_responses.pop(req_id, None)

    def _require_ws(self) -> None:
        if not self._ws or self._ws.closed:
            raise RuntimeError("WeCom websocket is not connected")

    async def _send_json(self, payload: Dict[str, Any]) -> None:
        self._require_ws()
        await self._ws.send_json(payload)

    async def _request(self, cmd: str, req_id: str, body: Dict[str, Any], timeout: float) -> Dict[str, Any]:
        future = self._pending_responses[req_id] = asyncio.get_running_loop().create_future()
        try:
            await self._send_json({"cmd": cmd, "headers": {"req_id": req_id}, "body": body})
            return await asyncio.wait_for(future, timeout=timeout)
        finally:
            self._pending_responses.pop(req_id, None)

    async def send_welcome(self, reply_req_id: str, content: str) -> None:
        """Send a welcome message via ``aibot_respond_welcome_msg``."""
        try:
            await self._send_reply_request(
                reply_req_id,
                {"msgtype": "text", "text": {"content": content[: self.MAX_MESSAGE_LENGTH]}},
                cmd=APP_CMD_RESPONSE_WELCOME,
            )
        except Exception as exc:
            logger.debug("[%s] welcome send failed: %s", self.name, exc)

    async def _on_event(self, payload: Dict[str, Any]) -> None:
        """Handle ``aibot_event_callback`` events (e.g. enter_chat welcome)."""
        body = payload.get("body") or {}
        event = body.get("event") or {}
        eventtype = str(event.get("eventtype") or "")
        req_id = self._payload_req_id(payload)

        if eventtype == "enter_chat" and req_id:
            user_id = str((body.get("from") or {}).get("userid") or "")
            name = user_id or "朋友"
            await self.send_welcome(req_id, f"你好 {name}！我是 AI 助手，有什么可以帮您的吗？")
            return

        logger.debug("[%s] Ignoring WeCom event: %s", self.name, eventtype)

    async def _send_request(self, cmd: str, body: Dict[str, Any], timeout: float = REQUEST_TIMEOUT_SECONDS) -> Dict[str, Any]:
        self._require_ws()
        return await self._request(cmd, self._new_req_id(cmd), body, timeout)

    async def _send_reply_request(self, reply_req_id: str, body: Dict[str, Any], cmd: str = APP_CMD_RESPONSE, timeout: float = REQUEST_TIMEOUT_SECONDS) -> Dict[str, Any]:
        """Send a reply frame correlated to an inbound callback req_id."""
        self._require_ws()
        normalized = self._require_reply_req_id(reply_req_id)
        if cmd != APP_CMD_RESPONSE:
            # welcome frames use their own cmd and their own correlation slot
            return await self._request(cmd, normalized, body, timeout)
        # fork 2026-09-30 (audit 2026-09-29 module-1 H2): respond_msg frames share the req_id
        # namespace with stream frames — route through the single reply registry.
        return await self._send_reply_correlated(normalized, body, timeout)

    @staticmethod
    def _require_reply_req_id(reply_req_id: str) -> str:
        normalized = str(reply_req_id or "").strip()
        if not normalized:
            raise ValueError("reply_req_id is required")
        return normalized

    @staticmethod
    def _new_req_id(prefix: str) -> str:
        return f"{prefix}-{uuid.uuid4().hex}"

    @staticmethod
    def _payload_req_id(payload: Dict[str, Any]) -> str:
        headers = payload.get("headers")
        return str(headers.get("req_id") or "") if isinstance(headers, dict) else ""

    @staticmethod
    def _parse_json(raw: Any) -> Optional[Dict[str, Any]]:
        raw_len = len(raw) if isinstance(raw, (str, bytes)) else -1
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            # WeCom sometimes sends raw control chars inside JSON strings; strict=False accepts them.
            try:
                text = raw if isinstance(raw, str) else raw.decode("utf-8", errors="replace")
                payload = json.JSONDecoder(strict=False).decode(text)
                logger.info("WeCom payload required strict=False fallback (len=%d)", raw_len)
            except Exception as exc2:
                tail = raw[-100:] if isinstance(raw, (str, bytes)) and len(raw) > 100 else raw
                logger.warning("Failed to parse WeCom payload (strict=False also failed): error=%s len=%d tail=%r", exc2, raw_len, tail)
                return None
        except Exception as exc:
            logger.warning("Failed to parse WeCom payload: error=%s len=%d", exc, raw_len)
            return None
        return payload if isinstance(payload, dict) else None

    async def _on_message(self, payload: Dict[str, Any]) -> None:
        body = payload.get("body")
        if not isinstance(body, dict):
            return
        req_id = self._payload_req_id(payload)
        msg_id = str(body.get("msgid") or req_id or uuid.uuid4().hex)
        sender = _dict_or_empty(body, "from")
        sender_id = str(sender.get("userid") or "").strip()
        if self._dedup.is_duplicate(msg_id):
            # INFO: a msgid redelivered after a processing exception is dropped for the TTL.
            logger.info("[%s] Duplicate message %s ignored (dedup drop) req_id=%s sender=%r chattype=%r", self.name, msg_id, req_id, sender.get("userid") if sender else None, body.get("chattype"))
            return
        if req_id:
            bounded_put(self._reply_req_ids, msg_id, req_id, DEDUP_MAX_SIZE)
        chat_id = str(body.get("chatid") or sender_id).strip()
        logger.info("[%s] Inbound callback: chattype=%r chatid=%r sender=%r msgtype=%r has_chatid=%s", self.name, body.get("chattype"), body.get("chatid"), sender_id, body.get("msgtype"), bool(body.get("chatid")))
        if not chat_id:
            logger.info("[%s] Missing chat id, skipping message; body_keys=%s", self.name, list(body.keys()))
            return
        is_group = str(body.get("chattype") or "").lower() == "group"
        if not self._admit_inbound(is_group, chat_id, sender_id):
            return
        # Post-policy: cache req_id so sends can fall back to passive reply (required in groups).
        self._remember_chat_req_id(chat_id, req_id)
        # fork: learn the DM touser (wohR-style chat ids carry the userid only on the sender), then
        # flush any pending group redeliveries — pre-turn reply frames on a fresh req_id are legal
        # (welcome + approval-prompt precedents), and this runs before the text-batch/turn so the
        # redelivery is ordered ahead of the reply on the same per-chat normal lane.
        if not is_group and sender_id and sender_id != chat_id:
            self._remember_dm_userid(chat_id, sender_id)  # fork 2026-10-08: learn AND persist the fallback touser
        if self._pending_group_sends.get(chat_id):
            self._flush_group_pending(chat_id)
        text, reply_text = self._extract_text(body)
        if is_group and text:
            text = re.sub(r"^@\S+\s*", "", text).strip()  # "@Bot /approve" -> "/approve"
        media_urls, media_types = await self._extract_media(body)
        message_type = self._derive_message_type(body, text, media_types)
        has_reply_context = bool(reply_text and (text or media_urls))
        if reply_text and not has_reply_context:  # quote-only message: the quote becomes the text
            text = reply_text
        if not text and not media_urls:
            logger.info("[%s] Empty WeCom message skipped: is_group=%s chat=%s msgtype=%r", self.name, is_group, chat_id, body.get("msgtype"))
            return
        source = self.build_source(chat_id=chat_id, chat_type="group" if is_group else "dm", user_id=sender_id or None, user_name=sender_id or None,
                                   message_id=msg_id)
        event = MessageEvent(
            text=text, message_type=message_type, source=source, raw_message=payload, message_id=msg_id, media_urls=media_urls, media_types=media_types,
            reply_to_message_id=f"quote:{msg_id}" if has_reply_context else None, reply_to_text=reply_text if has_reply_context else None, timestamp=datetime.now(tz=timezone.utc),
        )
        # Only plain text is batched, EXCEPT attachment-only messages, which are held so the
        # trailing text callback merges instead of "interrupting" a run the attachment spawned.
        has_pending_batch = self._text_batch_key(event) in self._pending_text_batches
        is_attachment_only = bool(media_urls) and not (text or "").strip()
        if (message_type == MessageType.TEXT and (self._text_batch_delay_seconds > 0 or has_pending_batch)) or (is_attachment_only and self._attachment_text_merge_delay_seconds > 0):
            self._enqueue_text_event(event)
        else:
            await self.handle_message(event)

    # ------------------------------------------------------------------
    # Text message aggregation (handles WeCom client-side splits)
    # ------------------------------------------------------------------

    def _text_batch_key(self, event: MessageEvent) -> str:
        """Session-scoped key for text message batching."""
        from gateway.session import build_session_key
        return build_session_key(
            event.source,
            group_sessions_per_user=str(
                self.config.extra.get("group_sessions_per_user")
                or os.getenv("WECOM_GROUP_SESSIONS_PER_USER", "false")
            ).strip().lower() in {"true", "1", "yes", "on"},
            thread_sessions_per_user=self.config.extra.get("thread_sessions_per_user", False),
            profile=self._session_key_profile(event.source),
        )

    def _admit_inbound(self, is_group: bool, chat_id: str, sender_id: str) -> bool:
        """Apply group_policy / dm_policy at intake; logs and returns False when dropped."""
        if not is_group:
            allowed = self._is_dm_intake_allowed(sender_id)
            if not allowed:
                logger.info("[%s] DM sender %s blocked by policy", self.name, sender_id)
            return allowed
        # fork 2026-10-08: classify AND persist — pre-policy by design, so a policy-dropped
        # group still gets group semantics on the send path (passive-first, ⏰ parking).
        self._note_learned_group(chat_id)
        allowed = self._is_group_allowed(chat_id, sender_id)
        if not allowed:
            logger.info(
                "[%s] Group message DROPPED by policy: chat=%s sender=%s group_policy=%r (set group_policy to 'open' or add to group_allow_from to receive)",
                self.name, chat_id, sender_id, self._group_policy,
            )
        return allowed

    def _enqueue_text_event(self, event: MessageEvent) -> None:
        """Buffer + reset the flush timer; real text joining a buffered attachment promotes it to TEXT and inherits the quote context."""
        existing = self._pending_text_batches.get(self._text_batch_key(event))
        super()._enqueue_text_event(event)  # merge text/media + restart the flush timer
        if existing is not None and event.text and event.text.strip():
            existing.message_type = MessageType.TEXT
            if event.reply_to_text and not existing.reply_to_text:
                existing.reply_to_text = event.reply_to_text
                existing.reply_to_message_id = event.reply_to_message_id

    def _text_batch_delay_for(self, pending: Optional[MessageEvent]) -> float:
        if pending is not None and pending.media_urls and not (pending.text or "").strip():
            return self._attachment_text_merge_delay_seconds  # attachment-only: wait for the text frame
        return super()._text_batch_delay_for(pending)

    @staticmethod
    def _extract_text(body: Dict[str, Any]) -> Tuple[str, Optional[str]]:
        msgtype = str(body.get("msgtype") or "").lower()
        if msgtype == "mixed":
            items = _dict_or_empty(body, "mixed").get("msg_item")
            text_parts = [_content_of(item, "text") for item in (items if isinstance(items, list) else []) if isinstance(item, dict) and str(item.get("msgtype") or "").lower() == "text"]
        else:  # voice transcript / appmsg attachment title (filename) follow the text; empties drop below
            text_parts = [
                _content_of(body, "text"), _content_of(body, "voice") if msgtype == "voice" else "",
                str(_dict_or_empty(body, "appmsg").get("title") or "").strip() if msgtype == "appmsg" else "",
            ]
        quote = _dict_or_empty(body, "quote")
        quote_type = str(quote.get("msgtype") or "").lower()
        reply_text = _content_of(quote, quote_type) or None if quote_type in ("text", "voice") else None
        return "\n".join(part for part in text_parts if part).strip(), reply_text

    @staticmethod
    def _derive_message_type(body: Dict[str, Any], text: str, media_types: List[str]) -> MessageType:
        if any(mtype.startswith(("application/", "text/")) for mtype in media_types):
            return MessageType.DOCUMENT
        if any(mtype.startswith("image/") for mtype in media_types):
            return MessageType.TEXT if text else MessageType.PHOTO
        if str(body.get("msgtype") or "").lower() == "voice":
            return MessageType.VOICE
        return MessageType.TEXT

    def _entry_matches(self, entries: List[str], target: str) -> bool:
        return _entry_matches(entries, target)

    def _is_group_allowed(self, chat_id: str, sender_id: str) -> bool:
        """Per-group ``groups.<id>.allow_from`` restricts senders on top of the chat-level policy."""
        if not super()._is_group_allowed(chat_id):
            return False
        group_cfg = self._resolve_group_cfg(chat_id)
        sender_allow = _coerce_list(group_cfg.get("allow_from") or group_cfg.get("allowFrom"))
        return _entry_matches(sender_allow, sender_id) if sender_allow else True

    def _resolve_group_cfg(self, chat_id: str) -> Dict[str, Any]:
        """Exact key, then case-insensitive key, then ``"*"``; only dict values count."""
        if not isinstance(self._groups, dict):
            return {}
        lowered = chat_id.lower()
        candidates = (self._groups.get(chat_id), next((v for k, v in self._groups.items() if isinstance(k, str) and k.lower() == lowered and isinstance(v, dict)), None), self._groups.get("*"))
        return next((c for c in candidates if isinstance(c, dict)), {})

    def _remember_chat_req_id(self, chat_id: str, req_id: str) -> None:
        """Cache the chat's latest inbound req_id; a fresh one also resurrects its stream channel."""
        chat_id, req_id = str(chat_id or "").strip(), str(req_id or "").strip()
        if chat_id and req_id:
            bounded_put(self._last_chat_req_ids, chat_id, req_id, DEDUP_MAX_SIZE)
            self._stream_expired_chats.discard(chat_id)

    def _reply_req_id_for_message(self, reply_to: Optional[str]) -> Optional[str]:
        normalized = str(reply_to or "").strip()
        return None if not normalized or normalized.startswith("quote:") else self._reply_req_ids.get(normalized)

    def _cached_reply_req_id(self, chat_id: str, reply_to: Optional[str]) -> Optional[str]:
        """Explicit reply_to mapping, else the chat's last inbound req_id."""
        return self._reply_req_id_for_message(reply_to) or self._last_chat_req_ids.get(chat_id)

    def _stash_group_pending(self, chat_id: str, content: str) -> bool:
        """fork: park an undeliverable group send for redelivery on the chat's next inbound req_id.

        Both group channels must be dead to land here (passive req_id gone AND proactive
        aibot_send_msg failed — e.g. a chat that never messaged the bot, the official
        prerequisite); the entry is flushed pre-turn by ``_flush_group_pending``. Returns True when
        the content is parked (or already parked — the live and standalone cron lanes both land
        here with identical content, so exact-content dedup collapses the duplicate attempt
        without dropping the redelivery).
        """
        pending = getattr(self, "_pending_group_sends", None)  # bare __new__ test stubs skip queueing
        if pending is None:
            return False
        ttl = getattr(self, "_group_pending_ttl_seconds", 21600.0)
        if ttl <= 0:
            return False
        now = time.time()
        entries = [(ts, text) for ts, text in pending.get(chat_id, []) if now - ts < ttl]
        if not entries:
            self._group_retry_attempts.pop(chat_id, None)  # fork 2026-10-08: fresh parking episode — retry budget restored
        if any(text == content for _, text in entries):
            logger.info("[%s] Group send already queued for redelivery (chat=%s, queued=%d) — duplicate live/standalone attempt not re-queued", self.name, chat_id, len(entries))
            pending[chat_id] = entries
            return True
        max_pending = getattr(self, "_group_pending_max", 5)
        while len(entries) >= max_pending:  # newest scheduled reports matter more; drop oldest
            dropped_ts, _ = entries.pop(0)
            logger.warning("[%s] Group pending queue full (chat=%s, max=%d) — dropping entry scheduled %s", self.name, chat_id, max_pending, time.strftime("%H:%M", time.localtime(dropped_ts)))
        entries.append((now, content))
        pending[chat_id] = entries
        logger.warning("[%s] Group send queued for redelivery on next group message (chat=%s, queued=%d, ttl=%.0fs)", self.name, chat_id, len(entries), ttl)
        return True

    def _flush_group_pending(self, chat_id: str) -> None:
        """Redeliver parked group sends on the fresh inbound req_id (fire-and-forget).

        Each entry goes through the normal ``send()`` lane so ordering with the upcoming turn reply
        holds; a failed redelivery is dropped (``is_redelivery`` blocks re-stashing — a fresh
        req_id failing means a later one will not help either).
        """
        pending = getattr(self, "_pending_group_sends", None)
        if not pending:
            return
        ttl = getattr(self, "_group_pending_ttl_seconds", 21600.0)
        now = time.time()
        stashed = pending.pop(chat_id, [])
        entries = [(ts, text) for ts, text in stashed if now - ts < ttl]
        if len(entries) != len(stashed):
            logger.info("[%s] Group redelivery: %d expired of %d stashed entries dropped (chat=%s, ttl=%.0fs)", self.name, len(stashed) - len(entries), len(stashed), chat_id, ttl)
        for ts, content in entries:
            header = f"⏰ 定时补发（原定 {time.strftime('%H:%M', time.localtime(ts))}）"
            # No pre-slice: _send_inner byte-segments downstream; the header rides segment 1.
            asyncio.ensure_future(self._redeliver_group_pending(chat_id, f"{header}\n---\n{content}"))

    async def _redeliver_group_pending(self, chat_id: str, composed: str) -> None:
        """One flushed entry; never raises into the event loop (fire-and-forget owner)."""
        try:
            result = await self.send(chat_id, composed, metadata={"is_redelivery": True})
        except Exception as exc:  # noqa: BLE001
            logger.warning("[%s] Group redelivery raised and was dropped (chat=%s): %s", self.name, chat_id, exc)
            return
        if not getattr(result, "success", False):
            logger.warning("[%s] Group redelivery failed and was dropped (chat=%s, error=%s)", self.name, chat_id, getattr(result, "error", None))

    def _schedule_group_retry(self, chat_id: str) -> None:
        """fork 2026-10-08: 846609 dead-window — retry parked group sends after the
        resubscribe instead of only waiting for the group's next inbound. One timer per
        chat (the live+standalone cron lanes stash identical content ~35ms apart and
        coalesce here); attempts are capped at len(_group_retry_delays)."""
        if chat_id in self._group_retry_timers:
            return
        delays = getattr(self, "_group_retry_delays", ()) or ()
        attempt = self._group_retry_attempts.get(chat_id, 0)
        if attempt >= len(delays):
            return
        delay = float(delays[attempt])
        loop = asyncio.get_running_loop()
        self._group_retry_timers[chat_id] = loop.call_later(
            delay, lambda cid=chat_id: self._group_retry_fire(cid)
        )
        logger.info("[%s] Group send parked in the 846609 dead window — timed retry in %.0fs (chat=%s)", self.name, delay, chat_id)

    def _group_retry_fire(self, chat_id: str) -> None:
        self._group_retry_timers.pop(chat_id, None)
        asyncio.ensure_future(self._group_retry_flush(chat_id))

    async def _group_retry_flush(self, chat_id: str) -> None:
        """Timed flush of parked entries: deliver VERBATIM (on time — no ⏰ header), keep
        entries parked on failure (unlike the inbound flush's drop-on-failure: at +30s the
        req_id cache is still empty and the resubscribe may genuinely not be done)."""
        pending = getattr(self, "_pending_group_sends", None)
        entries = list(pending.get(chat_id, [])) if pending else []
        if not entries:
            self._group_retry_attempts.pop(chat_id, None)
            return
        ttl = getattr(self, "_group_pending_ttl_seconds", 21600.0)
        now = time.time()
        dead_session, delivered = False, 0
        for ts, content in entries:
            if now - ts >= ttl:
                continue
            result = await self.send(chat_id, content, metadata={"is_redelivery": True})
            if getattr(result, "success", False):
                self._unstash_group_pending(chat_id, ts, content)
                delivered += 1
            elif str(STREAM_NOT_SUBSCRIBED_ERRCODE) in str(getattr(result, "error", "") or ""):
                dead_session = True
        remaining = [e for e in (pending.get(chat_id) or []) if now - e[0] < ttl]
        if not remaining:
            self._group_retry_attempts.pop(chat_id, None)
            return
        if delivered or not dead_session:
            return  # non-dead failures stay parked for the inbound ⏰ flush
        self._group_retry_attempts[chat_id] = self._group_retry_attempts.get(chat_id, 0) + 1
        self._schedule_group_retry(chat_id)

    def _unstash_group_pending(self, chat_id: str, ts: float, content: str) -> None:
        """Remove one delivered entry from the pending queue (timed-retry success path)."""
        pending = getattr(self, "_pending_group_sends", None)
        entries = pending.get(chat_id) if pending else None
        if not entries:
            return
        with contextlib.suppress(ValueError):
            entries.remove((ts, content))
        if not entries:
            pending.pop(chat_id, None)

    async def _force_reconnect_on_stale_subscription(self, errcode: int) -> None:
        """On 846609 (subscription lost) drop req_ids bound to the dead session. Do NOT close the
        WS: a second connection gets kicked and invalidates the first (infinite kick loop)."""
        if errcode != STREAM_NOT_SUBSCRIBED_ERRCODE:
            return
        logger.warning("[%s] Got errcode %d (subscription lost) — clearing stale state", self.name, errcode)
        self._last_chat_req_ids.clear()
        self._reply_req_ids.clear()

    @staticmethod
    def _response_error(response: Dict[str, Any]) -> Optional[str]:
        errcode = response.get("errcode", 0)
        return None if errcode in {0, None} else f"WeCom errcode {errcode}: {response.get('errmsg') or 'unknown error'}"

    @classmethod
    def _raise_for_wecom_error(cls, response: Dict[str, Any], operation: str) -> None:
        error = cls._response_error(response)
        if error:
            raise RuntimeError(f"{operation} failed: {error}")

    async def _send_reply_markdown(self, reply_req_id: str, content: str) -> Dict[str, Any]:
        # fork 2026-09-30: sequential segments on one req_id — same multi-frame
        # precedent the stream path set (APP_CMD_RESPONSE frames are versioned per
        # req_id). NOTE production watchpoint: if WeCom ever replaces (errcode 6000)
        # instead of stacking markdown reply frames, segment tails will vanish —
        # watch agent.log for 6000 on multi-segment sends after rollout.
        segments = _markdown_segments(content)
        if not segments:
            # 注意：不要用 "send reply markdown failed:" 前缀——那是生产旧镜像的判别指纹
            raise RuntimeError("send reply markdown: empty content")
        response: Dict[str, Any] = {}
        for segment in segments:
            response = await self._send_reply_request(reply_req_id, {"msgtype": "markdown", "markdown": {"content": segment}})
            # "segment" 措辞同样是反指纹：错误串前缀绝不能是 "send reply markdown failed:"
            self._raise_for_wecom_error(response, "send reply markdown segment")
        return response

    async def _send_proactive_markdown(self, chat_id: str, content: str, chat_type: Optional[int] = None) -> Dict[str, Any]:
        segments = _markdown_segments(content)
        if not segments:
            raise RuntimeError("send proactive markdown: empty content")
        response: Dict[str, Any] = {}
        for segment in segments:
            # fork 2026-10-06: chat_type only sent when explicit — DMs keep the historical unset
            # body (0/absent = compat mode); groups pass 2 per official aibot_send_msg docs.
            body: Dict[str, Any] = {"chatid": chat_id, "msgtype": "markdown", "markdown": {"content": segment}}
            if chat_type is not None:
                body["chat_type"] = chat_type
            response = await self._send_request(APP_CMD_SEND, body)
            # 质量评审 C-1：非末段 errcode 曾被后续成功段覆盖 → 静默部分丢失 + 假成功
            self._raise_for_wecom_error(response, "send proactive markdown segment")
        return response

    async def _group_proactive_send(self, chat_id: str, content: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        """fork 2026-10-06: official aibot_send_msg accepts group chatids (chat_type=2; docs:
        群聊填回调事件中获取的 chatid, prerequisite = the chat has messaged the bot before).
        Passive reply stays first choice; this is the fallback when no live req_id exists
        (scheduled pushes, post-846609 purge). Returns (response, None) or (None, error)."""
        try:
            response = await self._send_proactive_markdown(chat_id, content, chat_type=2)
        except (asyncio.TimeoutError, RuntimeError) as exc:
            return None, str(exc)
        error = self._response_error(response)
        return (response, None) if error is None else (None, error)

    async def send(self, chat_id: str, content: str, reply_to: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None) -> SendResult:
        """Send standalone markdown (never touches active streams); serialized per chat for the 30 msgs/min
        limit (846607). ``metadata["is_approval_prompt"]`` uses the control lane."""
        if not chat_id:
            return SendResult(success=False, error="chat_id is required")
        metadata = metadata or {}  # pops mutate the caller's dict on purpose (consumed flags)
        is_control = metadata.pop("is_approval_prompt", False)
        # Approval *confirmations* must not consume the req_id the stream consumer still needs.
        force_proactive = bool(metadata.pop("force_proactive_send", False))
        # fork: a flushed pending entry must never re-stash on failure (redelivery loop guard).
        is_redelivery = bool(metadata.pop("is_redelivery", False))
        # One queued send per chunk so each one draws a token from the 30 msgs/min bucket.
        return await send_chunks(self.truncate_message(content, self.MAX_MESSAGE_LENGTH), lambda chunk: self._enqueue_chat_send(
            chat_id, lambda: self._send_inner(chat_id, chunk, reply_to, force_proactive=force_proactive, is_control=is_control, is_redelivery=is_redelivery), is_control=is_control))

    def _is_group_chat(self, chat_id: str) -> bool:
        """fork: single source of truth for group-ness — inbound-learned set UNION
        operator-configured groups. The learned set persists to
        ``<HERMES_HOME>/wecom_learned_chats.json`` (fork 2026-10-08) so a restart no
        longer wipes it; a configured group keeps its group semantics (passive-first
        reply, redelivery parking, no agent fallback) across restarts either way.
        (getattr: bare __new__ test stubs drive send paths without __init__.)
        (audit 2026-09-29 module-1 M1.7)"""
        return chat_id in self._group_chat_ids or chat_id in getattr(self, "_groups", {})

    async def _send_inner(self, chat_id: str, content: str, reply_to: Optional[str] = None, *, force_proactive: bool = False, is_control: bool = False, is_redelivery: bool = False) -> SendResult:
        """Send under the per-chat queue; force_proactive skips passive reply except in groups."""
        is_group_chat = self._is_group_chat(chat_id)
        try:
            reply_req_id = None if force_proactive and not is_group_chat else self._cached_reply_req_id(chat_id, reply_to)
            if reply_req_id:
                try:
                    response = await self._send_reply_markdown(reply_req_id, content)
                except (asyncio.TimeoutError, RuntimeError) as passive_err:
                    if is_group_chat:
                        # fork 2026-10-06: passive reply died (stale req_id after a resubscribe) — the
                        # official aibot_send_msg delivers to groups too (chat_type=2); try it, then park.
                        logger.warning("[%s] Passive reply failed for group chat %s (%s) — trying proactive aibot_send_msg", self.name, chat_id, passive_err)
                        response, proactive_error = await self._group_proactive_send(chat_id, content)
                        if proactive_error is None:
                            logger.info("[%s] Group send delivered via proactive aibot_send_msg after passive failure (chat=%s)", self.name, chat_id)
                            return SendResult(success=True, message_id=self._payload_req_id(response) or uuid.uuid4().hex[:12], raw_response=response)
                        stashed = not is_control and not is_redelivery and self._stash_group_pending(chat_id, content)
                        combined = f"Group send failed (passive: {passive_err}; proactive: {proactive_error})" + (" (queued for redelivery on next group message)" if stashed else "")
                        if stashed and str(STREAM_NOT_SUBSCRIBED_ERRCODE) in combined:
                            self._schedule_group_retry(chat_id)
                        return self._send_failure(combined, str(STREAM_NOT_SUBSCRIBED_ERRCODE) in combined)
                    # req_id may be stale after a reconnect — proactive send needs none.
                    logger.warning("[%s] Passive reply failed (%s), falling back to proactive send", self.name, passive_err)
                    response = await self._send_proactive_markdown(chat_id, content)
            elif is_group_chat:
                # fork 2026-10-06: quiet group at fire time (no live req_id) — official aibot_send_msg
                # supports group chatids; deliver proactively instead of parking for the next inbound.
                logger.info("[%s] No cached req_id for group chat %s — sending via proactive aibot_send_msg", self.name, chat_id)
                response, proactive_error = await self._group_proactive_send(chat_id, content)
                if proactive_error is None:
                    return SendResult(success=True, message_id=self._payload_req_id(response) or uuid.uuid4().hex[:12], raw_response=response)
                logger.warning("[%s] Group proactive send failed (chat=%s): %s", self.name, chat_id, proactive_error)
                stashed = not is_control and not is_redelivery and self._stash_group_pending(chat_id, content)
                message = f"Group proactive send failed: {proactive_error}" + (" (queued for redelivery on next group message)" if stashed else "")
                if stashed and str(STREAM_NOT_SUBSCRIBED_ERRCODE) in proactive_error:
                    self._schedule_group_retry(chat_id)
                # 846609 schedules the stale-req_id purge even when stashed — the cache holds dead req_ids.
                return self._send_failure(message, str(STREAM_NOT_SUBSCRIBED_ERRCODE) in proactive_error)
            else:
                response = await self._send_proactive_markdown(chat_id, content)
        except asyncio.TimeoutError:
            fb = await self._try_agent_fallback(chat_id, content, "bot send timeout")
            if fb is not None:
                return fb
            return SendResult(success=False, error="Timeout sending message to WeCom")
        except Exception as exc:
            logger.error("[%s] Send failed: %s", self.name, exc)
            return await self._fail_or_fallback(chat_id, content, str(exc), str(STREAM_NOT_SUBSCRIBED_ERRCODE) in str(exc))
        if error := self._response_error(response):
            return await self._fail_or_fallback(chat_id, content, error, response.get("errcode", 0) == STREAM_NOT_SUBSCRIBED_ERRCODE)
        return SendResult(success=True, message_id=self._payload_req_id(response) or uuid.uuid4().hex[:12], raw_response=response)


    async def _fail_or_fallback(self, chat_id: str, content: str, error: str, subscription_lost: bool) -> SendResult:
        """fork: on a bot-delivery failure try the self-built-app agent channel (DM only)
        before failing; otherwise the upstream failure path (846609 schedules the
        stale-req_id purge so later sends recover)."""
        fb = await self._try_agent_fallback(chat_id, content, f"bot send failed: {error}")
        if fb is not None:
            return fb
        return self._send_failure(error, subscription_lost)

    def _send_failure(self, error: str, subscription_lost: bool) -> SendResult:
        """Failed SendResult; on 846609 schedule the stale-req_id purge so later sends recover."""
        if subscription_lost:
            asyncio.ensure_future(self._force_reconnect_on_stale_subscription(STREAM_NOT_SUBSCRIBED_ERRCODE))
        return SendResult(success=False, error=error)

    async def _try_agent_fallback(
        self, chat_id: str, content: str, reason: str,
    ) -> Optional[SendResult]:
        """Bot delivery failed → try the self-built-app channel (DM only).

        Enabled iff WECOM_CALLBACK_{CORP_ID,CORP_SECRET,AGENT_ID} are set and
        WECOM_AGENT_FALLBACK is not an explicit off value (see
        ``_agent_fallback_client``).  Group chats never fall back — the aibot
        group chat_id is not a valid self-built-app touser (documented
        limitation; groups fall back to proactive aibot_send_msg instead,
        fork 2026-10-06).  DM chat ids that ARE
        the corp userid go through unchanged; wohR-style DM room ids resolve
        via ``_dm_userid_by_chat`` (in-memory, learned from inbound senders —
        an unknown id after a restart falls back to the raw chat_id).
        """
        if self._is_group_chat(chat_id):
            return None
        client = _agent_fallback_client()
        if client is None:
            return None
        touser = (getattr(self, "_dm_userid_by_chat", None) or {}).get(chat_id, chat_id)
        try:
            ok, err = await client.send_markdown(touser, content)
        except Exception as exc:  # noqa: BLE001
            logger.warning("[%s] agent fallback raised after bot failure (%s): %s",
                           self.name, reason, exc)
            return None
        if not ok:
            logger.warning("[%s] agent fallback failed after bot failure (%s): %s",
                           self.name, reason, err)
            return None
        logger.info("[%s] delivered via agent fallback (bot failure: %s)", self.name, reason)
        return SendResult(
            success=True,
            message_id=f"agent-fallback:{uuid.uuid4().hex[:8]}",
            raw_response={"agent_fallback": True, "reason": reason},
        )


    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        return {"name": chat_id, "type": "group" if chat_id and chat_id.lower().startswith("group") else "dm"}


_QR_GENERATE_URL = "https://work.weixin.qq.com/ai/qc/generate"
_QR_QUERY_URL = "https://work.weixin.qq.com/ai/qc/query_result"
_QR_CODE_PAGE = "https://work.weixin.qq.com/ai/qc/gen?source=hermes&scode="
_QR_POLL_INTERVAL, _QR_POLL_TIMEOUT = 3, 300  # seconds (poll every 3s, give up after 5 minutes)


def qr_scan_for_bot_info(*, timeout_seconds: int = _QR_POLL_TIMEOUT) -> Optional[Dict[str, str]]:
    """Fetch a WeCom QR code, render it, poll until scanned or timeout; ``{"bot_id", "secret"}`` or None.
    The ``ai/qc/*`` endpoints back the admin console, not the public API, and may change."""
    import urllib.request
    import urllib.parse

    def _get_json(url: str, timeout: int) -> Dict[str, Any]:
        req = urllib.request.Request(url, headers={"User-Agent": "HermesAgent/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def _fail(log_msg: str, detail: Any, shown: Any) -> None:
        logger.error(log_msg, detail)
        print(f" failed: {shown}")

    print("  Connecting to WeCom...", end="", flush=True)
    try:
        raw = _get_json(f"{_QR_GENERATE_URL}?source=hermes", 15)
    except Exception as exc:
        return _fail("WeCom QR: failed to fetch QR code: %s", exc, exc)
    scode, auth_url = (str((raw.get("data") or {}).get(k) or "").strip() for k in ("scode", "auth_url"))
    if not scode or not auth_url:
        return _fail("WeCom QR: unexpected response format: %s", raw, "unexpected response format")
    print(" done.\n")
    page_url = f"{_QR_CODE_PAGE}{urllib.parse.quote(scode)}"
    try:
        import qrcode as _qrcode
        qr = _qrcode.QRCode()
        qr.add_data(auth_url)
        qr.make(fit=True)
        qr.print_ascii(invert=True)
        print(f"\n  Scan the QR code above, or open this URL directly:\n  {page_url}")
    except Exception:
        print(f"  Open this URL in WeCom on your phone:\n\n  {page_url}\n")
        print("  Tip: from the Hermes environment, run: "
              f"{install_hint('messaging')} "
              "to display a scannable QR code here next time")
    print("\n  Fetching configuration results...", end="", flush=True)
    deadline = time.monotonic() + timeout_seconds
    query_url = f"{_QR_QUERY_URL}?scode={urllib.parse.quote(scode)}"
    while time.monotonic() < deadline:
        try:
            result = _get_json(query_url, 10)
            print(".", end="", flush=True)  # progress dot on every poll
        except Exception as exc:
            logger.debug("WeCom QR poll error: %s", exc)
            result = {}
        result_data = result.get("data") or {}
        if str(result_data.get("status") or "").lower() != "success":
            time.sleep(_QR_POLL_INTERVAL)
            continue
        bot_info = result_data.get("bot_info") or {}
        bot_id, secret = str(bot_info.get("botid") or bot_info.get("bot_id") or "").strip(), str(bot_info.get("secret") or "").strip()
        if bot_id and secret:
            print()
            return {"bot_id": bot_id, "secret": secret}
        logger.warning("WeCom QR: scan reported success but bot_info missing or incomplete: %s", result_data)
        print("\n  QR scan reported success but no bot credentials were returned.\n  This usually means the bot was not actually created on the WeCom side.\n  Falling back to manual credential entry.")
        return None
    print(f"\n  QR scan timed out ({timeout_seconds // 60} minutes). Please try again.")
    return None


async def _send_via(adapter, chat_id, message, *, live: bool):
    try:
        result = await adapter.send(chat_id, message)
    except Exception as e:
        return send_error(f"WeCom live adapter send failed: {e}" if live else f"WeCom send failed: {e}")
    if result.success:
        return {"success": True, "platform": "wecom", "chat_id": chat_id, "message_id": result.message_id}
    return send_error(f"WeCom send failed: {result.error}")


_AGENT_FALLBACK_OFF_VALUES = {"0", "false", "off", "no"}
_agent_fallback_client_cache: Dict[str, Any] = {"env": None, "client": None}


def _warn_if_agent_fallback_unconfigured() -> None:
    """fork (audit 2026-09-29 module-1 M1.5): a WARNING on each successful connect
    (startup, reconnects, ephemeral standalone connects) when the DM fallback channel
    is missing its env — the 846609 DM-loss incident had this as its silent half."""
    import os as _os

    if _os.getenv("WECOM_AGENT_FALLBACK", "").strip().lower() in _AGENT_FALLBACK_OFF_VALUES:
        return
    missing = [
        name
        for name, value in (
            ("WECOM_CALLBACK_CORP_ID", _os.getenv("WECOM_CALLBACK_CORP_ID", "").strip()),
            ("WECOM_CALLBACK_CORP_SECRET", _os.getenv("WECOM_CALLBACK_CORP_SECRET", "").strip()),
            ("WECOM_CALLBACK_AGENT_ID", _os.getenv("WECOM_CALLBACK_AGENT_ID", "").strip()),
        )
        if not value
    ]
    if missing:
        logger.warning(
            "[wecom] agent-fallback disabled: missing env %s — if the bot channel fails "
            "(846609) DMs have no self-built-app fallback and will be lost. Set "
            "WECOM_AGENT_FALLBACK=off to silence this when intentional.",
            ", ".join(missing),
        )


def _markdown_segments(content: str) -> list:
    """Byte-accurate markdown segments for aibot reply/proactive frames."""
    return _split_markdown_bytes(str(content or ""), max_bytes=AIBOT_MARKDOWN_MAX_BYTES)


def _agent_fallback_client() -> Optional[Any]:
    """Build (and memoise per env tuple) the Bot→Agent fallback client.

    Enabled iff WECOM_CALLBACK_{CORP_ID,CORP_SECRET,AGENT_ID} are all set and
    WECOM_AGENT_FALLBACK is not an explicit off value.  Mirrors the official
    wecom-openclaw-plugin's Bot-first / Agent-fallback delivery: when the
    Smart-Robot channel cannot deliver, route markdown through the
    self-built-app message/send API instead.
    """
    import os as _os

    if _os.getenv("WECOM_AGENT_FALLBACK", "").strip().lower() in _AGENT_FALLBACK_OFF_VALUES:
        return None
    env = (
        _os.getenv("WECOM_CALLBACK_CORP_ID", "").strip(),
        _os.getenv("WECOM_CALLBACK_CORP_SECRET", "").strip(),
        _os.getenv("WECOM_CALLBACK_AGENT_ID", "").strip(),
    )
    if not all(env):
        return None
    if _agent_fallback_client_cache["env"] != env:
        from plugins.platforms.wecom.callback_adapter import WecomAgentFallbackClient
        _agent_fallback_client_cache["env"] = env
        _agent_fallback_client_cache["client"] = WecomAgentFallbackClient(*env)
    return _agent_fallback_client_cache["client"]


def _consume_cross_loop_result(future) -> None:
    """Absorb the outcome of a cross-loop send abandoned at timeout (shielded, still running on
    the gateway loop) so its result/exception is observed — the consume_detached_task_result
    pattern; without this the executor logs "exception was never retrieved"."""
    try:
        future.result()
    except Exception:  # noqa: BLE001 — observation only, the owner already gave up
        pass


async def _standalone_send(

    pconfig,
    chat_id,
    message,
    *,
    thread_id=None,
    media_files=None,
    force_document=False,
):
    """Out-of-process WeCom delivery via the adapter's WebSocket send pipeline.

    Implements the standalone_sender_fn contract so deliver=wecom cron jobs
    succeed when cron runs separately from the gateway. When a live in-process
    adapter is reachable (``_live_adapter``), it is reused directly — no
    competing WebSocket. Otherwise opens an ephemeral WeComAdapter, connects,
    sends, and disconnects. Replaces the legacy _send_wecom helper.

    .. note::
       WeCom's server only allows one active WebSocket session per
       ``bot_id``.  An ephemeral connection (the no-runner fallback) will
       **displace** any existing gateway session on the same bot.  The
       gateway detects the resulting ``errcode 846609`` and clears stale
       session state (see ``_force_reconnect_on_stale_subscription``), but
       operators should be aware that a cron job firing mid-conversation
       may cause a brief interruption.
    """
    # Reuse the gateway's live in-process adapter when available. Opening an
    # ephemeral WS here would displace the gateway's sole subscription
    # (errcode 846609); this mirrors _send_via_adapter's live-first path so a
    # direct in-process caller never opens a competing connection. Only true
    # out-of-process callers (no runner) fall through to the ephemeral connect.
    # _live_adapter resolves the ACTIVE profile's adapter under multiplex — a
    # bare ``runner.adapters`` hit would leave with the default profile's
    # identity — and returns (runner, None) on lookup failure, falling through
    # to the agent-fallback/ephemeral paths below.
    try:
        from tools.send_message_senders import _live_adapter
        _runner, _live = _live_adapter(Platform.WECOM)
    except Exception:
        _runner = None
    if _runner is not None and _live is not None:
        # fork: awaiting the live adapter off the gateway loop parks the per-chat queue's future
        # on THIS loop while the gateway worker completes it cross-thread via plain call_soon —
        # the wakeup never reaches our selector, so a failure known in ~100ms only surfaces at the
        # caller's 60s wait_for (observed in prod: the "delivery error" logged exactly +60s).
        # Detect the foreign loop up front and go straight to the threadsafe dispatch.
        _gateway_loop = getattr(_runner, "_gateway_loop", None)
        _off_loop = _gateway_loop is not None and _gateway_loop.is_running() and asyncio.get_running_loop() is not _gateway_loop
        _live_error: Optional[str] = None
        _need_cross_loop = _off_loop
        if not _off_loop:
            try:
                _result = await _live.send(chat_id, message)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                _live_error = f"WeCom send failed: {e}"
            else:
                if getattr(_result, "success", False):
                    return {
                        "success": True,
                        "platform": "wecom",
                        "chat_id": chat_id,
                        "message_id": getattr(_result, "message_id", None),
                    }
                _live_error = f"WeCom send failed: {getattr(_result, 'error', None)}"
            # Genuine send errors (expired req_id, group policy, etc.) return
            # immediately — an ephemeral path won't help.
            if _live_error and "different loop" not in _live_error.lower():
                return {"error": _live_error}
            # Cross-loop: the live adapter failed because we're on a different
            # event loop (cron's asyncio.run fallback creates a new loop, but
            # the adapter's websocket futures are bound to the gateway loop).
            # Schedule the send back onto the gateway loop via
            # safe_schedule_threadsafe instead of opening an ephemeral WS —
            # an ephemeral connection would displace the gateway's sole
            # subscription (errcode 846609), disrupting all live traffic.
            _need_cross_loop = True
        if _need_cross_loop:
            if _gateway_loop is not None and _gateway_loop.is_running():
                try:
                    from agent.async_utils import safe_schedule_threadsafe
                    _future = safe_schedule_threadsafe(
                        _live.send(chat_id, message),
                        _gateway_loop,
                    )
                except Exception as _sched_err:
                    logger.debug(
                        "[%s] standalone_send: cross-loop schedule failed (%s)",
                        "wecom", _sched_err,
                    )
                    return {"error": _live_error}
                if _future is not None:
                    try:
                        # wrap_future bridges the concurrent future with call_soon_threadsafe (a
                        # correct cross-thread wakeup); shield keeps the gateway-loop send
                        # un-cancelled on timeout (#115469; mirrors _dispatch_on_gateway_loop).
                        _cross_result = await asyncio.wait_for(
                            asyncio.shield(asyncio.wrap_future(_future)), timeout=30)
                    except asyncio.TimeoutError:
                        _future.add_done_callback(_consume_cross_loop_result)
                        return {"error": "WeCom cross-loop send failed: timeout"}
                    except Exception as _cross_err:
                        logger.debug(
                            "[%s] standalone_send: cross-loop send failed (%s)",
                            "wecom", _cross_err,
                        )
                        return {"error": f"WeCom cross-loop send failed: {_cross_err}"}
                    if getattr(_cross_result, "success", False):
                        return {
                            "success": True,
                            "platform": "wecom",
                            "chat_id": chat_id,
                            "message_id": getattr(_cross_result, "message_id", None),
                        }
                    return {
                        "error": f"WeCom send failed: {getattr(_cross_result, 'error', None)}",
                    }
            # No reachable gateway loop: do NOT open an ephemeral WS here —
            # it would displace the live subscription (846609). Return the
            # original error; the caller may retry.
            return {"error": _live_error or "WeCom send failed: no reachable gateway loop"}

    # Agent-channel fallback BEFORE any ephemeral WebSocket: an ephemeral
    # subscribe displaces the gateway's sole WS session (errcode 846609),
    # while the self-built-app message/send API has no such constraint.
    # DM-only in practice — group chat ids are not valid touser values and
    # fail server-side, falling through to the ephemeral path below.
    _fb = _agent_fallback_client()
    if _fb is not None:
        try:
            _fb_ok, _fb_err = await _fb.send_markdown(chat_id, message)
        except Exception as _fb_exc:  # noqa: BLE001
            _fb_ok, _fb_err = False, str(_fb_exc)
        if _fb_ok:
            return {
                "success": True,
                "platform": "wecom",
                "chat_id": chat_id,
                "message_id": None,
                "via": "agent_fallback",
            }
        logger.debug(
            "[wecom] standalone_send: agent fallback failed (%s), trying ephemeral WS",
            _fb_err,
        )

    if not check_wecom_requirements():
        return send_error("WeCom requirements not met. Need aiohttp + WECOM_BOT_ID/SECRET.")
    try:
        adapter = WeComAdapter(pconfig)
        if not await adapter.connect():
            return send_error(f"WeCom: failed to connect - {getattr(adapter, 'fatal_error_message', None) or 'unknown error'}")
        try:
            return await _send_via(adapter, chat_id, message, live=False)
        finally:
            await adapter.disconnect()
    except Exception as e:
        return send_error(f"WeCom send failed: {e}")


_MANUAL_SETUP_STEPS = (
    "1. Go to WeCom Application → Workspace → Smart Robot -> Create smart robots",
    "2. Select API Mode",
    "3. Copy the Bot ID and Secret from the bot's credentials info",
    "4. The bot connects via WebSocket — no public endpoint needed",
)
# (menu label, env saves, (print level, message)...) per unauthorized-user choice; index 3 = skip
_ACCESS_CHOICES = (
    ("Enable open access (anyone can message the bot)", (("WECOM_DM_POLICY", "open"), ("GATEWAY_ALLOW_ALL_USERS", "true")),
     (("warning", "Open access enabled — anyone can use your bot!"),)),
    ("Use DM pairing (unknown users request access, you approve with 'hermes pairing approve')", (("WECOM_DM_POLICY", "pairing"),),
     (("success", "DM pairing mode — users will receive a code to request access."), ("info", "Approve with: hermes pairing approve <platform> <code>"))),
    ("Disable direct messages", (("WECOM_DM_POLICY", "disabled"),), (("warning", "Direct messages disabled."),)),
    ("Skip for now (bot will deny all users until configured)", (), (("info", "Skipped — configure later with 'hermes gateway setup'"),)),
)


def interactive_setup() -> None:
    from hermes_cli.config import remove_env_value, save_env_value
    from hermes_cli.setup import prompt_choice
    from hermes_cli.cli_output import prompt, print_header, print_info, print_success, print_warning
    from hermes_cli.setup_platforms import declines_reconfigure
    print_header("WeCom (Enterprise WeChat)")
    if declines_reconfigure("WeCom", "Reconfigure WeCom?", "WECOM_BOT_ID"):
        return
    method_idx = prompt_choice("How would you like to set up WeCom?", ["Scan QR code to obtain Bot ID and Secret automatically (recommended)", "Enter existing Bot ID and Secret manually"], 0)
    bot_id = secret = None
    if method_idx == 0:
        try:
            credentials = qr_scan_for_bot_info() or {}
        except KeyboardInterrupt:
            print_warning("WeCom setup cancelled.")
            return
        except Exception as exc:
            print_warning(f"QR scan failed: {exc}")
            credentials = {}
        if credentials:
            bot_id, secret = credentials.get("bot_id", ""), credentials.get("secret", "")
            print_success("✔ QR scan successful! Bot ID and Secret obtained.")
        if not bot_id or not secret:
            print_info("QR scan did not complete. Continuing with manual input.")
            bot_id = secret = None
    if not bot_id or not secret:
        for line in _MANUAL_SETUP_STEPS:
            print_info(line)
        creds = []
        for label, password in (("Bot ID", False), ("Secret", True)):
            creds.append(prompt(label, password=password))
            if not creds[-1]:
                print_warning(f"Skipped — WeCom won't work without a {label}.")
                return
        bot_id, secret = creds
    save_env_value("WECOM_BOT_ID", bot_id)
    save_env_value("WECOM_SECRET", secret)
    print_info("The gateway DENIES all users by default for security.")
    print_info("Enter user IDs to create an allowlist, or leave empty.")
    allowed = prompt("Allowed user IDs (comma-separated, or empty)", password=False)
    if allowed:
        save_env_value("WECOM_ALLOWED_USERS", allowed.replace(" ", ""))
        print_success("Saved — only these users can interact with the bot.")
    else:
        access_idx = prompt_choice("How should unauthorized users be handled?", [label for label, _, _ in _ACCESS_CHOICES], 1)
        _, saves, messages = _ACCESS_CHOICES[access_idx if access_idx in (0, 1, 2) else 3]
        for key, value in saves:
            save_env_value(key, value)
        for level, message in messages:
            {"warning": print_warning, "success": print_success, "info": print_info}[level](message)
    if home := prompt("Home chat ID (optional, for cron/notifications)", password=False).strip():
        save_env_value("WECOM_HOME_CHANNEL", home)
        print_success(f"Home channel set to {home}")
    elif remove_env_value("WECOM_HOME_CHANNEL"):
        print_info("Home channel cleared.")
    print_success("💬 WeCom configured!")


def _is_connected(config) -> bool:
    return bool((getattr(config, "extra", {}) or {}).get("bot_id"))


def _callback_is_connected(config) -> bool:
    """Callback mode: corp_id or a multi-app `apps` block."""
    extra = getattr(config, "extra", {}) or {}
    return bool(extra.get("corp_id") or extra.get("apps"))



def _build_callback_adapter(config):
    from plugins.platforms.wecom.callback_adapter import WecomCallbackAdapter
    return WecomCallbackAdapter(config)


def register(ctx) -> None:
    """Plugin entry point — registers both WeCom platforms."""
    # wecom-cli business tools (docs/sheets/calendar/todo/mail/contact/…).
    # Registered first so they exist even when the WS platform never
    # materialises; discovery also pre-registers them via provides_tools.
    try:
        from plugins.platforms.wecom.tools import register_tools
        register_tools(ctx)
    except Exception:
        logger.warning("[wecom] failed to register wecom-cli business tools", exc_info=True)

    common = dict(install_hint="Run `hermes setup` to install WeCom support.", emoji="💼", allow_update_command=True)
    ctx.register_platform(
        name="wecom", label="WeCom (Enterprise WeChat)", adapter_factory=WeComAdapter, check_fn=check_wecom_requirements,
        is_connected=_is_connected, validate_config=_is_connected, required_env=["WECOM_BOT_ID", "WECOM_SECRET"],
        setup_fn=interactive_setup, allowed_users_env="WECOM_ALLOWED_USERS", allow_all_env="WECOM_ALLOW_ALL_USERS",
        cron_deliver_env_var="WECOM_HOME_CHANNEL", standalone_sender_fn=_standalone_send, max_message_length=4000, **common,
    )
    from plugins.platforms.wecom.callback_adapter import check_wecom_callback_requirements, ensure_wecom_callback_requirements
    ctx.register_platform(
        name="wecom_callback", label="WeCom Callback (self-built apps)", adapter_factory=_build_callback_adapter,
        check_fn=check_wecom_callback_requirements, ensure_deps_fn=ensure_wecom_callback_requirements,
        is_connected=_callback_is_connected, validate_config=_callback_is_connected,
        required_env=["WECOM_CALLBACK_CORP_ID", "WECOM_CALLBACK_CORP_SECRET"],
        allowed_users_env="WECOM_CALLBACK_ALLOWED_USERS", allow_all_env="WECOM_CALLBACK_ALLOW_ALL_USERS", **common,
    )
