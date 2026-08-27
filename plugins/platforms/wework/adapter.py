"""
WeWork (企业微信) Platform Adapter for Hermes Agent.

Implements a webhook inbound server + outbound send API, aligned with
`weclaw/extensions/wework` (OpenClaw channel plugin).

Inbound (webhook):
  - Default: POST http://0.0.0.0:13776/
  - Payload: JSON fields like cmd/user/userFullName/wechatId/fromGroup/sendTime/storeKey/isAt/...
  - Non-blocking: immediately 200 OK; message processing happens in background.

Outbound (send API):
  - POST to apiUrl with JSON: { keyid, serviceName, to|userName, type, content, ... }
  - Supports chunking long text with textChunkLimit (default 3500).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

try:
    from aiohttp import web

    AIOHTTP_AVAILABLE = True
except ImportError:
    web = None  # type: ignore[assignment]
    AIOHTTP_AVAILABLE = False

try:
    import httpx

    HTTPX_AVAILABLE = True
except ImportError:
    httpx = None  # type: ignore[assignment]
    HTTPX_AVAILABLE = False

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
)

logger = logging.getLogger(__name__)

DEFAULT_WEBHOOK_HOST = "0.0.0.0"
DEFAULT_WEBHOOK_PORT = 13776
DEFAULT_WEBHOOK_PATH = "/"

DEFAULT_SERVICE_NAME = "oa-dev"
DEFAULT_API_URL = "http://tctpdevops.weoa.com/pros-chatbot/yuanfang/sendEMsg"

DEFAULT_TEXT_CHUNK_LIMIT = 3500
DEFAULT_DM_POLICY = "pairing"  # open | pairing | allowlist
DEFAULT_GROUP_POLICY = "allowlist"  # open | allowlist | disabled
DEFAULT_REQUIRE_MENTION = True

DEDUP_TTL_SECONDS = 300
DEDUP_MAX_SIZE = 20000

# Rate limit for control-plane whitelist drop warnings: per-message logging
# would be noise under a flood; one line per interval is enough for an
# operator to grep "why is the bot silent" (spec §6.2).
_CPWL_DROP_LOG_INTERVAL_S = 300.0
_cpwl_last_drop_log = 0.0


def check_wework_requirements() -> bool:
    return AIOHTTP_AVAILABLE and HTTPX_AVAILABLE


def _truthy(val: Any, default: bool = False) -> bool:
    if val is None:
        return default
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        return bool(val)
    if isinstance(val, str):
        v = val.strip().lower()
        if v in {"true", "1", "yes", "y", "on"}:
            return True
        if v in {"false", "0", "no", "n", "off"}:
            return False
    return default


def _env_str(name: str) -> Optional[str]:
    """Read env var; empty string is treated as unset (weclaw fill_if_incoming semantics)."""
    val = os.getenv(name)
    if val is None:
        return None
    s = str(val).strip()
    return s if s else None


def _wework_env_enabled() -> bool:
    """Match weclaw: only WEWORK_ENABLED=true turns on the channel."""
    return _truthy(os.getenv("WEWORK_ENABLED"), False)


def _coerce_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [v.strip() for v in value.split(",") if v.strip()]
    if isinstance(value, (list, tuple, set)):
        return [str(v).strip() for v in value if str(v).strip()]
    return [str(value).strip()] if str(value).strip() else []


def _chunk_text(text: str, limit: int) -> List[str]:
    if not text:
        return [""]
    if limit <= 0 or len(text) <= limit:
        return [text]
    chunks: List[str] = []
    remaining = text
    while remaining:
        if len(remaining) <= limit:
            chunks.append(remaining)
            break
        chunk_end = limit
        last_newline = remaining.rfind("\n", 0, limit + 1)
        if last_newline > int(limit * 0.8):
            chunk_end = last_newline + 1
        chunks.append(remaining[:chunk_end])
        remaining = remaining[chunk_end:]
    return chunks


def _looks_like_wechat_id(value: str) -> bool:
    """True when *value* is a group (room) wechatId.

    WeCom encodes the conversation type into wechatId:
      - group:   ``S:<bot>_<x>;R:<roomid>``  — contains an ``R:`` segment
      - private: ``S:<a>_<b>;S:<b>_<a>``     — only ``S:`` segments, no ``R:``

    Detection MUST key on ``R:``. The old ``"S:" in v and ";" in v`` heuristic
    misclassifies every private chat as a group (private wechatIds are also
    multi-segment ``S:..;S:..``), routing replies to an invalid group target
    so the user receives nothing.
    """
    v = str(value or "").strip()
    if not v:
        return False
    return "R:" in v


@dataclass
class _Inbound:
    is_group: bool
    sender_user: str
    sender_full_name: str
    chat_id: str  # gateway chat id used for replies
    chat_name: str  # for allowlist/routing display
    text: str
    message_id: str
    mentioned_bot: bool


class _DedupCache:
    def __init__(self) -> None:
        self._seen: Dict[str, float] = {}

    def add_if_new(self, key: str) -> bool:
        now = time.time()
        cutoff = now - DEDUP_TTL_SECONDS
        # prune opportunistically
        if len(self._seen) > DEDUP_MAX_SIZE:
            self._seen = {k: v for k, v in self._seen.items() if v > cutoff}
        ts = self._seen.get(key)
        if ts and ts > cutoff:
            return False
        self._seen[key] = now
        return True


class WeWorkAdapter(BasePlatformAdapter):
    """Webhook-based WeWork adapter."""

    # WeWork API 不支持编辑已发送消息 — 跳过 GatewayStreamConsumer
    # 流式路径，改为完整回答 → send() → _chunk_text 分片。
    SUPPORTS_MESSAGE_EDITING = False

    def __init__(self, config: PlatformConfig):
        platform = Platform("wework")
        super().__init__(config=config, platform=platform)

        extra = getattr(config, "extra", {}) or {}

        self._keyid = str(_env_str("WEWORK_KEYID") or extra.get("keyid") or "").strip()
        self._service_name = str(
            _env_str("WEWORK_SERVICE_NAME") or extra.get("serviceName") or extra.get("service_name") or DEFAULT_SERVICE_NAME
        ).strip()
        self._api_url = str(_env_str("WEWORK_API_URL") or extra.get("apiUrl") or extra.get("api_url") or DEFAULT_API_URL).strip()

        self._host = str(
            _env_str("WEWORK_WEBHOOK_HOST") or extra.get("webhookHost") or extra.get("host") or DEFAULT_WEBHOOK_HOST
        ).strip() or DEFAULT_WEBHOOK_HOST
        self._port = int(_env_str("WEWORK_WEBHOOK_PORT") or extra.get("webhookPort") or extra.get("port") or DEFAULT_WEBHOOK_PORT)
        self._path = str(
            _env_str("WEWORK_WEBHOOK_PATH") or extra.get("webhookPath") or extra.get("path") or DEFAULT_WEBHOOK_PATH
        ).strip() or DEFAULT_WEBHOOK_PATH

        self._text_chunk_limit = int(
            _env_str("WEWORK_TEXT_CHUNK_LIMIT") or extra.get("textChunkLimit") or extra.get("text_chunk_limit") or DEFAULT_TEXT_CHUNK_LIMIT
        )

        self._dm_policy = str(
            _env_str("WEWORK_DM_POLICY") or extra.get("dmPolicy") or extra.get("dm_policy") or DEFAULT_DM_POLICY
        ).strip().lower()
        self._allow_from = _coerce_list(_env_str("WEWORK_ALLOW_FROM") or extra.get("allowFrom") or extra.get("allow_from"))

        self._group_policy = str(
            _env_str("WEWORK_GROUP_POLICY") or extra.get("groupPolicy") or extra.get("group_policy") or DEFAULT_GROUP_POLICY
        ).strip().lower()
        self._group_allow_from = _coerce_list(
            _env_str("WEWORK_GROUP_ALLOW_FROM") or extra.get("groupAllowFrom") or extra.get("group_allow_from")
        )
        self._require_mention = _truthy(
            _env_str("WEWORK_REQUIRE_MENTION") or extra.get("requireMention") or extra.get("require_mention"),
            DEFAULT_REQUIRE_MENTION,
        )

        self._app: Optional["web.Application"] = None
        self._runner: Optional["web.AppRunner"] = None
        self._site: Optional["web.TCPSite"] = None
        self._http_client: Optional["httpx.AsyncClient"] = None
        self._queue: "asyncio.Queue[_Inbound]" = asyncio.Queue()
        self._poll_task: Optional[asyncio.Task] = None
        self._dedup = _DedupCache()

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        if not check_wework_requirements():
            self._set_fatal_error(
                "wework_missing_dependency",
                "WeWork startup failed: aiohttp/httpx not installed",
                retryable=True,
            )
            logger.warning("[%s] aiohttp/httpx not installed", self.name)
            return False
        if not self._keyid:
            self._set_fatal_error(
                "wework_missing_credentials",
                "WeWork startup failed: WEWORK_KEYID (or platforms.wework.extra.keyid) is required",
                retryable=True,
            )
            logger.warning("[%s] missing WEWORK_KEYID", self.name)
            return False

        try:
            self._http_client = httpx.AsyncClient(timeout=30.0, follow_redirects=True)

            self._app = web.Application()
            self._app.router.add_get("/health", self._handle_health)
            self._app.router.add_post(self._path, self._handle_webhook)
            # Compatibility: accept root POST when users forward without path.
            if self._path != "/":
                self._app.router.add_post("/", self._handle_webhook)

            self._runner = web.AppRunner(self._app)
            await self._runner.setup()
            self._site = web.TCPSite(self._runner, self._host, self._port)
            await self._site.start()

            self._poll_task = asyncio.create_task(self._poll_loop())

            self._mark_connected()
            logger.info(
                "[%s] Webhook listening on %s:%s%s",
                self.name,
                self._host,
                self._port,
                self._path,
            )
            return True
        except Exception as exc:
            message = f"WeWork startup failed: {exc}"
            self._set_fatal_error("wework_connect_error", message, retryable=True)
            logger.error("[%s] Failed to start: %s", self.name, exc, exc_info=True)
            await self._cleanup()
            return False

    async def disconnect(self) -> None:
        self._running = False
        if self._poll_task:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
            self._poll_task = None
        await self._cleanup()
        self._mark_disconnected()
        logger.info("[%s] Disconnected", self.name)

    async def _cleanup(self) -> None:
        self._site = None
        if self._runner:
            try:
                await self._runner.cleanup()
            except Exception:
                pass
            self._runner = None
        self._app = None
        if self._http_client:
            try:
                await self._http_client.aclose()
            except Exception:
                pass
            self._http_client = None

    async def _handle_health(self, request: "web.Request") -> "web.Response":
        return web.json_response({"status": "ok", "platform": "wework"})

    async def _handle_webhook(self, request: "web.Request") -> "web.Response":
        try:
            body = await request.json()
        except Exception:
            return web.json_response({"ok": False, "error": "invalid json"}, status=400)

        if not isinstance(body, dict):
            return web.json_response({"ok": False, "error": "invalid payload"}, status=400)

        # Always ACK fast; do processing in background.
        inbound = self._parse_inbound(body)
        if inbound is not None:
            try:
                self._queue.put_nowait(inbound)
            except asyncio.QueueFull:
                logger.warning("[%s] inbound queue full; dropping message", self.name)

        return web.json_response({"ok": True})

    def _parse_inbound(self, payload: Dict[str, Any]) -> Optional[_Inbound]:
        cmd = str(payload.get("cmd") or "").strip()
        sender_user = str(payload.get("user") or "").strip()
        sender_full_name = str(payload.get("userFullName") or "").strip()
        wechat_id = str(payload.get("wechatId") or "").strip()
        from_group = str(payload.get("fromGroup") or "").strip()
        send_time = str(payload.get("sendTime") or "").strip()
        store_key = str(payload.get("storeKey") or "").strip()

        # Unnamed groups may have an empty fromGroup, but their wechatId still looks
        # like a group id (e.g. S:...;S:...). Treat those as group chats too.
        is_group = bool(from_group) or _looks_like_wechat_id(wechat_id)
        mentioned_bot = _truthy(payload.get("isAt"), False)

        # Dedup key aligns with weclaw: wechatId + sendTime + storeKey
        message_key = f"{wechat_id}_{send_time}_{store_key}"
        if wechat_id and send_time and store_key:
            if not self._dedup.add_if_new(message_key):
                logger.debug("[%s] duplicate message ignored: %s", self.name, message_key)
                return None

        if is_group:
            chat_id = wechat_id  # send target (API `to`)
            chat_name = from_group or wechat_id  # allowlist/routing display
        else:
            # Private chat: the reply target is the sender's user id (API
            # ``userName``), NOT the wechatId. The wechatId is a conversation
            # marker (``S:..;S:..``), not a valid userName — sending to it
            # makes the WeWork API reject the reply, so the user gets nothing.
            chat_id = sender_user  # send target (API `userName`)
            chat_name = sender_full_name or sender_user

        logger.info(
            "[%s] inbound: is_group=%s chat_id=%s chat_name=%s sender=%s text=%s",
            self.name, is_group, chat_id, chat_name, sender_user, cmd[:80] if cmd else "",
        )

        # Basic allowlist policies.
        #
        # Control-plane dynamic whitelist (fork): when enabled it REPLACES
        # the env/config policies below for BOTH DM senders and groups (one
        # shared platform users list). No cached data = drop (fail-closed;
        # drops emit a rate-limited warning via _log_cpwl_drop). The
        # env/config path below is untouched when the control plane is
        # disabled.
        try:
            from tools.control_plane_whitelist import get_platform_whitelist

            _cpwl = get_platform_whitelist()
        except Exception:
            logger.warning(
                "control-plane whitelist unavailable (module error); "
                "falling back to env policies",
                exc_info=True,
            )
            _cpwl = None
        if _cpwl is not None:
            if is_group:
                if not _cpwl.group_allowed(chat_id=chat_id, chat_name=chat_name):
                    self._log_cpwl_drop(is_group=is_group, sender=sender_user,
                                        chat_id=chat_id, chat_name=chat_name)
                    return None
                if self._require_mention and not mentioned_bot:
                    return None
            elif not _cpwl.user_allowed(sender_user, sender_full_name):
                self._log_cpwl_drop(is_group=is_group, sender=sender_user,
                                    chat_id=chat_id, chat_name=chat_name)
                return None
        elif is_group:
            if self._group_policy == "disabled":
                return None
            if self._group_policy == "allowlist":
                if not self._is_group_allowed(chat_id=chat_id, chat_name=chat_name):
                    return None
            if self._require_mention and not mentioned_bot:
                return None
        else:
            if self._dm_policy == "allowlist":
                if not self._is_sender_allowed(sender_user, sender_full_name):
                    return None

        # Media payloads may have empty cmd; keep a placeholder so the agent
        # has something to respond to.
        if not cmd:
            content_type = int(payload.get("contentType") or 0)
            if content_type == 7:
                cmd = "<media:image>"
            elif content_type == 9:
                cmd = "<media:audio>"
            elif content_type == 5:
                cmd = "<media:video>"
            elif content_type == 8:
                cmd = "<media:attachment>"
            else:
                cmd = "<message>"

        message_id = f"{send_time}_{store_key}" if send_time or store_key else str(int(time.time() * 1000))
        return _Inbound(
            is_group=is_group,
            sender_user=sender_user,
            sender_full_name=sender_full_name,
            chat_id=chat_id,
            chat_name=chat_name,
            text=cmd,
            message_id=message_id,
            mentioned_bot=mentioned_bot,
        )

    def _log_cpwl_drop(self, *, is_group: bool, sender: str, chat_id: str, chat_name: str) -> None:
        """Rate-limited visibility for control-plane whitelist drops (fork).

        Per-message logging would be noise under a flood; one line per 5 min
        is enough for an operator to grep 'why is the bot silent'."""
        global _cpwl_last_drop_log
        now = time.time()
        if now - _cpwl_last_drop_log < _CPWL_DROP_LOG_INTERVAL_S:
            return
        _cpwl_last_drop_log = time.time()
        target = (chat_name or chat_id) if is_group else (sender or "unknown")
        logger.warning(
            "[%s] control-plane whitelist dropped inbound (%s): %s — "
            "platform list miss or no cached data",
            self.name, "group" if is_group else "dm", str(target)[:80],
        )

    def _is_group_allowed(self, *, chat_id: str, chat_name: str) -> bool:
        allow = [v.strip() for v in self._group_allow_from if str(v).strip()]
        if not allow:
            return True if self._group_policy != "allowlist" else False
        lowered = {v.lower() for v in allow}
        if "*" in allow:
            return True
        if chat_name and chat_name.lower() in lowered:
            return True
        if chat_id and chat_id.lower() in lowered:
            return True
        return False

    def _is_sender_allowed(self, sender_id: str, sender_name: str) -> bool:
        allow = [v.strip() for v in self._allow_from if str(v).strip()]
        if not allow:
            return True
        lowered = {v.lower() for v in allow}
        if "*" in allow:
            return True
        if sender_id and sender_id.lower() in lowered:
            return True
        if sender_name and sender_name.lower() in lowered:
            return True
        return False

    async def _poll_loop(self) -> None:
        while True:
            inbound = await self._queue.get()
            try:
                await self._dispatch_inbound(inbound)
            except Exception:
                logger.exception("[%s] failed to dispatch inbound message", self.name)

    async def _dispatch_inbound(self, inbound: _Inbound) -> None:
        source = self.build_source(
            chat_id=inbound.chat_id,
            chat_name=inbound.chat_name or inbound.chat_id,
            chat_type="group" if inbound.is_group else "dm",
            user_id=inbound.sender_user or None,
            user_name=inbound.sender_full_name or inbound.sender_user or None,
        )

        event = MessageEvent(
            text=inbound.text,
            message_type=MessageType.TEXT,
            source=source,
            raw_message={"provider": "wework"},
            message_id=inbound.message_id,
        )

        await self.handle_message(event)

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        del reply_to, metadata

        if not chat_id:
            return SendResult(success=False, error="chat_id is required")
        if not self._http_client:
            return SendResult(success=False, error="http client not initialized")

        text = str(content or "")
        if not text.strip():
            return SendResult(success=True, message_id="empty")

        is_group = _looks_like_wechat_id(chat_id)
        chunks = _chunk_text(text, self._text_chunk_limit)

        for idx, chunk in enumerate(chunks):
            payload: Dict[str, Any] = {
                "keyid": self._keyid,
                "serviceName": self._service_name,
                "type": "text",
                "content": chunk,
            }
            if is_group:
                payload["to"] = chat_id
            else:
                payload["userName"] = chat_id

            try:
                resp = await self._http_client.post(
                    self._api_url,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                )
                resp.raise_for_status()
                data = resp.json()
            except Exception as exc:
                host_hint = self._api_url.replace("https://", "").replace("http://", "").split("/")[0]
                return SendResult(success=False, error=f"WeWork API request failed (host={host_hint}): {exc}")

            # Weclaw expects { Code: 0 }.
            try:
                code = data.get("Code") if isinstance(data, dict) else None
            except Exception:
                code = None
            if code not in (0, "0", None):
                message = ""
                if isinstance(data, dict):
                    message = str(data.get("Message") or data.get("message") or "")
                return SendResult(success=False, error=message or f"WeWork API error: {data}")

            if idx < len(chunks) - 1:
                await asyncio.sleep(0.5)

        return SendResult(success=True, message_id=str(int(time.time() * 1000)))

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        return {"name": chat_id, "type": "group" if _looks_like_wechat_id(chat_id) else "dm"}


def _validate_config(cfg: PlatformConfig) -> bool:
    extra = getattr(cfg, "extra", {}) or {}
    keyid = str(_env_str("WEWORK_KEYID") or extra.get("keyid") or "").strip()
    return bool(keyid)


def _env_enablement() -> Optional[dict]:
    if not _wework_env_enabled():
        return None
    seeded: dict[str, Any] = {}
    for env, key in (
        ("WEWORK_KEYID", "keyid"),
        ("WEWORK_API_URL", "apiUrl"),
        ("WEWORK_SERVICE_NAME", "serviceName"),
        ("WEWORK_WEBHOOK_HOST", "webhookHost"),
        ("WEWORK_WEBHOOK_PORT", "webhookPort"),
        ("WEWORK_WEBHOOK_PATH", "webhookPath"),
        ("WEWORK_TEXT_CHUNK_LIMIT", "textChunkLimit"),
        ("WEWORK_DM_POLICY", "dmPolicy"),
        ("WEWORK_ALLOW_FROM", "allowFrom"),
        ("WEWORK_GROUP_POLICY", "groupPolicy"),
        ("WEWORK_GROUP_ALLOW_FROM", "groupAllowFrom"),
        ("WEWORK_REQUIRE_MENTION", "requireMention"),
    ):
        val = _env_str(env)
        if val is None:
            continue
        seeded[key] = val
    return seeded or None


def _wework_apply_yaml_config(yaml_cfg: dict, platform_cfg: dict) -> Optional[dict]:
    """Translate wework ``config.yaml`` keys into ``extra`` / env vars.

    Called by the gateway during config loading so that YAML-based config
    works without env vars.  Returns extra fields to merge into
    ``PlatformConfig.extra``.
    """
    import os as _os

    extra: dict = {}
    section = yaml_cfg.get("extra") if isinstance(yaml_cfg, dict) else None
    if not isinstance(section, dict):
        return extra

    _env_map = {
        "keyid": "WEWORK_KEYID",
        "apiUrl": "WEWORK_API_URL",
        "serviceName": "WEWORK_SERVICE_NAME",
        "webhookHost": "WEWORK_WEBHOOK_HOST",
        "webhookPort": "WEWORK_WEBHOOK_PORT",
        "webhookPath": "WEWORK_WEBHOOK_PATH",
        "textChunkLimit": "WEWORK_TEXT_CHUNK_LIMIT",
        "dmPolicy": "WEWORK_DM_POLICY",
        "allowFrom": "WEWORK_ALLOW_FROM",
        "groupPolicy": "WEWORK_GROUP_POLICY",
        "groupAllowFrom": "WEWORK_GROUP_ALLOW_FROM",
        "requireMention": "WEWORK_REQUIRE_MENTION",
    }
    for yaml_key, env_key in _env_map.items():
        val = section.get(yaml_key)
        if val is not None and not _os.getenv(env_key):
            _os.environ[env_key] = str(val)
        if val is not None:
            extra[yaml_key] = val

    return extra


async def _wework_standalone_send(
    pconfig, chat_id: str, message: str,
    *, thread_id=None, media_files=None, force_document=False,
) -> dict:
    """Deliver a message without a live gateway adapter.

    Used by ``cron`` when it runs in a separate process from the gateway.
    Opens an ephemeral HTTP client, sends the message, and closes.
    """
    import httpx as _httpx

    if not chat_id or not message:
        return {"error": "chat_id and message are required"}

    extra = getattr(pconfig, "extra", {}) or {}
    keyid = str(extra.get("keyid") or os.getenv("WEWORK_KEYID") or "").strip()
    if not keyid:
        return {"error": "WEWORK_KEYID not configured"}

    service_name = str(
        extra.get("serviceName") or os.getenv("WEWORK_SERVICE_NAME") or "oa-dev"
    ).strip()
    api_url = str(
        extra.get("apiUrl") or os.getenv("WEWORK_API_URL")
        or "http://tctpdevops.weoa.com/pros-chatbot/yuanfang/sendEMsg"
    ).strip()

    is_group = _looks_like_wechat_id(chat_id)
    payload: dict = {
        "keyid": keyid,
        "serviceName": service_name,
        "type": "text",
        "content": message,
    }
    if is_group:
        payload["to"] = chat_id
    else:
        payload["userName"] = chat_id

    try:
        async with _httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(api_url, json=payload)
            resp.raise_for_status()
            data = resp.json()
            code = data.get("Code") if isinstance(data, dict) else None
            if code not in (0, "0", None):
                msg = str(data.get("Message", "")) if isinstance(data, dict) else ""
                return {"error": msg or f"API error: {data}"}
            return {"success": True, "message_id": str(int(time.time() * 1000))}
    except Exception as exc:
        return {"error": f"WeWork standalone send failed: {exc}"}


def register(ctx) -> None:
    """Plugin entry point: called by the Hermes plugin system."""
    ctx.register_platform(
        name="wework",
        label="WeWork",
        adapter_factory=lambda cfg: WeWorkAdapter(cfg),
        check_fn=check_wework_requirements,
        validate_config=_validate_config,
        is_connected=_validate_config,
        required_env=["WEWORK_KEYID"],
        install_hint="pip install 'hermes-agent[all]'",
        env_enablement_fn=_env_enablement,
        apply_yaml_config_fn=_wework_apply_yaml_config,
        standalone_sender_fn=_wework_standalone_send,
        allowed_users_env="WEWORK_ALLOW_FROM",
        cron_deliver_env_var="WEWORK_HOME_CHANNEL",
        emoji="💬",
        platform_hint="你正在使用企业微信（WeWork）与用户对话。",
    )

