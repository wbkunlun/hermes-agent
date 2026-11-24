"""企微「数字员工」平台适配器。

与 wework / wecom 的**根本区别**：这条通道是**同步请求-响应**的。
平台侧（Dify HTTP 节点）``POST`` 过来后在同一连接上等我们返回 ``message_list``，
它自己负责把内容推给企微用户。所以不是"先 ACK 再异步回"，而是
"收 → 交给 agent 跑 → 等到结果 → 在同一响应里返回"。

三条路径：

1. **正常**：收到请求 → 派发 turn → 等 :class:`_PendingReply` future →
   ``message_list`` 同步返回。
2. **超时**：等满 ``responseTimeout``（默认 110s，给平台 120s 留余量）→
   立刻回一句"正在处理"，并把该会话标记为"交给主动推送"。
3. **超时后的迟到结果**：``send()`` 发现该会话已超时/无等待方 → 用企微发消息
   API（``push.py``，地址与 keyId 可配）主动推送：群聊按群名，私聊按用户名。

会话隔离：``service_name`` 进会话键——同一个 ``conversation_id`` 下不同数字员工
实例是不同的对话，否则多个数字人会串台。

**文件暂不支持**：入站附件只给 agent 一句"请用文字描述"的说明，出站附件
回一条文字提示（见 ``_media_unsupported``）。等确定 COS 的凭据来源再接。
"""

from __future__ import annotations

import asyncio
import hmac
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Any, Optional

try:
    from aiohttp import web

    AIOHTTP_AVAILABLE = True
except ImportError:  # pragma: no cover
    web = None  # type: ignore[assignment]
    AIOHTTP_AVAILABLE = False

try:
    import httpx

    HTTPX_AVAILABLE = True
except ImportError:  # pragma: no cover
    httpx = None  # type: ignore[assignment]
    HTTPX_AVAILABLE = False

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import BasePlatformAdapter, MessageEvent, MessageType, SendResult

from plugins.platforms.digital_employee.config import DigitalEmployeeConfig, load_config
from plugins.platforms.digital_employee.inbound import InboundMessage, build_prompt_text, parse_inbound
from plugins.platforms.digital_employee.outbound import (
    ResponsePayload,
    build_response,
    text_messages,
)
from plugins.platforms.digital_employee.push import WeChatPushClient

logger = logging.getLogger(__name__)

DEDUP_TTL_SECONDS = 300
DEDUP_MAX_SIZE = 20000

# 纯附件轮（没有正文 send()）的兜底延迟：等网关把附件的 send_document 送完。

def _resolve_platform() -> Platform:
    """适配器的 ``Platform`` 成员。

    两个名字来源不同：插件加载器从 manifest ``name: digital-employee-platform``
    派生出注册表键 ``digital-employee``，而 ``Platform(...)`` 对 bundled 插件按
    **目录名**解析出 ``digital_employee``。适配器用后者（``_missing_`` 能直接解析的
    那个），注册表查询时两种都认——``authz_mixin`` 按 ``platform.value`` 查表。
    """
    return Platform("digital_employee")


def _platform_names(platform: Platform) -> tuple[str, ...]:
    values = [platform.value]
    if "digital-employee" not in values:
        values.append("digital-employee")
    return tuple(values)


def session_key(inbound: InboundMessage) -> str:
    """会话隔离键：``service_name`` + ``conversation_id``。

    同一会话里可能并存多个数字员工（Dify 侧每个员工一个 ``service_name``），
    只用 ``conversation_id`` 会让它们共用 hermes 的上下文而串台。
    """
    service = (inbound.service_name or "").strip() or "default"
    return f"{service}:{inbound.conversation_id}"


@dataclass
class _PendingReply:
    """一次同步请求的等待槽位。"""

    key: str
    conversation_id: str
    created_at: float
    future: asyncio.Future[Any] = field(init=False)
    timed_out: bool = False
    inbound: Optional[InboundMessage] = None


class _DedupCache:
    def __init__(self, ttl: float = DEDUP_TTL_SECONDS) -> None:
        self._seen: dict[str, float] = {}
        self._ttl = ttl

    def add_if_new(self, key: str) -> bool:
        now = time.time()
        cutoff = now - self._ttl
        if len(self._seen) > DEDUP_MAX_SIZE:
            self._seen = {k: v for k, v in self._seen.items() if v > cutoff}
        ts = self._seen.get(key)
        if ts and ts > cutoff:
            return False
        self._seen[key] = now
        return True


class DigitalEmployeeAdapter(BasePlatformAdapter):
    """数字员工平台适配器（同步 HTTP 请求-响应）。"""

    # 平台不支持编辑，也不支持流式（HTTP 模式是同步调用）。
    SUPPORTS_MESSAGE_EDITING = False
    splits_long_messages = True

    def __init__(self, config: PlatformConfig):
        super().__init__(config=config, platform=_resolve_platform())
        self.settings: DigitalEmployeeConfig = load_config(getattr(config, "extra", {}) or {})

        self._app: Optional[web.Application] = None
        self._runner: Optional[web.AppRunner] = None
        self._site: Optional[web.TCPSite] = None
        self._client: Optional[httpx.AsyncClient] = None
        self._dedup = _DedupCache()

        # session_key → 正在等待的槽位；chat_id 复用它来找回等待方。
        self._pending: dict[str, _PendingReply] = {}
        # chat_id → 最近一次 inbound（send() 需要 service_name / group_name / user_token）
        self._inbounds: dict[str, InboundMessage] = {}

    # ── 生命周期 ──────────────────────────────────────────────
    async def connect(self, *, is_reconnect: bool = False) -> bool:
        if not AIOHTTP_AVAILABLE or not HTTPX_AVAILABLE:
            self._set_fatal_error(
                "digital_employee_missing_dependency",
                "digital_employee startup failed: aiohttp/httpx not installed",
                retryable=True,
            )
            return False
        try:
            self._client = httpx.AsyncClient(timeout=30.0, follow_redirects=True)
            self._app = web.Application(client_max_size=8 * 1024 * 1024)
            self._app.router.add_get("/health", self._handle_health)
            self._app.router.add_post(self.settings.webhook_path, self._handle_chat)
            if self.settings.webhook_path != "/":
                self._app.router.add_post("/", self._handle_chat)

            self._runner = web.AppRunner(self._app)
            await self._runner.setup()
            self._site = web.TCPSite(self._runner, self.settings.webhook_host, self.settings.webhook_port)
            await self._site.start()
            self._mark_connected()
            logger.info(
                "[%s] listening on %s:%s%s (timeout=%ss auth=%s push=%s)",
                self.name,
                self.settings.webhook_host,
                self.settings.webhook_port,
                self.settings.webhook_path,
                self.settings.response_timeout,
                "on" if self.settings.auth_enabled else "off",
                "on" if self.settings.push_enabled else "off",
            )
            missing = self.settings.missing_push()
            if missing and self.settings.push_url:
                logger.warning(
                    "[%s] push partially configured, delayed replies will be dropped (missing: %s)",
                    self.name, ", ".join(missing),
                )
            return True
        except Exception as exc:
            self._set_fatal_error("digital_employee_connect_error", f"startup failed: {exc}", retryable=True)
            logger.error("[%s] failed to start: %s", self.name, exc, exc_info=True)
            await self._cleanup()
            return False

    async def disconnect(self) -> None:
        self._running = False
        await self._cleanup()
        self._mark_disconnected()
        logger.info("[%s] disconnected", self.name)

    async def _cleanup(self) -> None:
        # 关停路径清理失败不能把进程卡在 half-disconnected：对象照样丢弃，
        # 但保留 traceback，否则"连不上且没日志"无从排查。
        self._site = None
        if self._runner is not None:
            try:
                await self._runner.cleanup()
            except Exception:
                logger.warning("[%s] runner cleanup failed", self.name, exc_info=True)
            self._runner = None
        self._app = None
        if self._client is not None:
            try:
                await self._client.aclose()
            except Exception:
                logger.warning("[%s] http client close failed", self.name, exc_info=True)
            self._client = None

    # ── HTTP 入口 ─────────────────────────────────────────────
    async def _handle_health(self, request: web.Request) -> web.Response:
        return web.json_response({"status": "ok", "platform": "digital_employee"})

    def _supplied_token(self, request: web.Request) -> str:
        header = self.settings.auth_header or "Authorization"
        raw = request.headers.get(header, "")
        if header.lower() == "authorization":
            scheme, _, value = raw.partition(" ")
            return value.strip() if scheme.lower() == "bearer" else ""
        return raw.strip()

    def _authorized(self, request: web.Request) -> bool:
        """配置了 ``apiKey`` 才校验；未配置（内网直连）不产生额外开销。"""
        expected = self.settings.api_key
        if not expected:
            return True
        supplied = self._supplied_token(request)
        if not supplied:
            return False
        return hmac.compare_digest(
            supplied.encode("utf-8", "replace"), expected.encode("utf-8", "replace")
        )

    async def _handle_chat(self, request: web.Request) -> web.Response:
        # 先鉴权再解析 body：未授权请求不消耗 JSON 解析。
        if not self._authorized(request):
            logger.warning("[%s] rejected unauthorized request from %s", self.name, request.remote)
            return web.json_response({"error": "unauthorized"}, status=401)
        try:
            body = await request.json()
        except (ValueError, UnicodeDecodeError):
            # aiohttp 对非法 JSON 抛 JSONDecodeError（ValueError 子类），
            # 对非 UTF-8 正文抛 UnicodeDecodeError；两者都是 400。
            return web.json_response({"error": "invalid json"}, status=400)

        inbound = parse_inbound(body, trace_id=request.headers.get("X-Trace-Id", ""))
        if inbound is None:
            return web.json_response({"error": "conversation_id is required"}, status=400)

        logger.info("[%s] inbound %s", self.name, inbound.log_fields())
        key = session_key(inbound)

        # 同一会话已有在途请求：汇入它，不重跑 agent（并发/重投是平台侧的常态）。
        existing = self._pending.get(key)
        if existing is not None and not existing.timed_out:
            logger.info("[%s] joining in-flight turn for %s", self.name, key)
            return await self._await_and_respond(existing)

        if not self._dedup.add_if_new(inbound.dedup_key()):
            logger.info("[%s] duplicate delivery (%s) — processing it again", self.name, key)

        return await self._dispatch_and_wait(inbound, key)

    async def _dispatch_and_wait(self, inbound: InboundMessage, key: str) -> web.Response:
        pending = _PendingReply(key=key, conversation_id=inbound.conversation_id, created_at=time.time())
        pending.inbound = inbound
        pending.future = asyncio.get_running_loop().create_future()
        self._pending[key] = pending
        self._inbounds[inbound.conversation_id] = inbound

        try:
            await self._dispatch_inbound(inbound)
        except Exception:
            logger.exception("[%s] failed to dispatch inbound", self.name)
            self._pending.pop(key, None)
            return web.json_response({"error": "internal error dispatching message"}, status=500)

        return await self._await_and_respond(pending)

    async def _await_and_respond(self, pending: _PendingReply) -> web.Response:
        """等 agent 结果；超时回提示语并把会话标记为"交给主动推送"。"""
        try:
            payload = await asyncio.wait_for(
                asyncio.shield(pending.future), timeout=self.settings.response_timeout
            )
        except TimeoutError:
            pending.timed_out = True
            logger.warning(
                "[%s] agent exceeded %.0fs for %s (push=%s) — replying with notice",
                self.name,
                self.settings.response_timeout,
                pending.key,
                "on" if self.settings.push_enabled else "off",
            )
            notice = ResponsePayload(
                messages=text_messages(self.settings.timeout_notice, self.settings.text_chunk_limit)
            )
            return web.json_response(notice.to_wire())
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("[%s] waiting for agent result failed", self.name)
            return web.json_response({"error": "internal error"}, status=500)
        finally:
            # 槽位生命周期到此为止：此后的迟到结果一律走主动推送。
            if self._pending.get(pending.key) is pending:
                self._pending.pop(pending.key, None)
        return web.json_response(payload.to_wire())

    async def _dispatch_inbound(self, inbound: InboundMessage) -> None:
        media_note = await self._media_note(inbound)

        source = self.build_source(
            chat_id=inbound.conversation_id,
            chat_name=inbound.chat_name or inbound.conversation_id,
            chat_type=inbound.chat_type,
            user_id=inbound.user_id or None,
            user_name=inbound.user_id or None,
            message_id=inbound.message_id or None,
        )
        event = MessageEvent(
            text=build_prompt_text(inbound, media_note=media_note),
            message_type=MessageType.TEXT,
            source=source,
            raw_message={
                "provider": "digital_employee",
                "service_name": inbound.service_name,
                "group_name": inbound.group_name,
                "session_id": inbound.session_id,
                "trace_id": inbound.trace_id,
                "conversation_type": inbound.conversation_type,
                "history": list(inbound.history),
            },
            message_id=inbound.message_id or inbound.conversation_id,
        )
        await self.handle_message(event)

    # ── 入站附件（暂不支持） ──────────────────────────────────
    async def _media_note(self, inbound: InboundMessage) -> str:
        """附件当前不下载：给 agent 一句说明，让它如实告诉用户用文字描述。

        COS 读取暂未接入（平台模板里没有 ``user_token``）；文本通道不受影响。
        """
        if not inbound.has_media:
            return ""
        logger.info("[%s] inbound has %d attachment(s); media is text-only for now",
                    self.name, len(inbound.media_parts))
        return "[系统提示：当前无法读取附件内容，请告知用户用文字描述。]"

    # ── 出站 ──────────────────────────────────────────────────
    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> SendResult:
        del reply_to
        metadata = metadata or {}
        chat_id = str(chat_id or "").strip()
        if not chat_id:
            return SendResult(success=False, error="chat_id is required")

        payload = build_response(str(content or ""), chunk_limit=self.settings.text_chunk_limit)
        return await self._deliver(chat_id, payload, metadata)

    def _pending_for(self, chat_id: str) -> Optional[_PendingReply]:
        inbound = self._inbounds.get(chat_id)
        if inbound is None:
            return None
        pending = self._pending.get(session_key(inbound))
        return pending if pending is not None and not pending.timed_out else None

    async def _deliver(self, chat_id: str, payload: ResponsePayload, metadata: dict[str, Any]) -> SendResult:
        """交给同步等待方；没有等待方（已超时）则主动推送。"""
        pending = self._pending_for(chat_id)
        if pending is not None and not pending.future.done():
            pending.future.set_result(payload)
            return SendResult(success=True, message_id=f"sync-{int(time.time() * 1000)}")
        return await self._push(chat_id, payload, metadata)

    async def _push(self, chat_id: str, payload: ResponsePayload, metadata: dict[str, Any]) -> SendResult:
        """超时后的迟到答复：走企微发消息 API（群按群名，私聊按用户名）。"""
        text = "\n\n".join(m.content for m in payload.messages if m.type == "txt" and m.content).strip()
        if not text:
            return SendResult(success=True, message_id="empty")
        if not self.settings.push_enabled:
            logger.warning(
                "[%s] result ready after timeout but push is not configured — reply dropped "
                "(conversation=%s). Set digital_employee.pushUrl/pushKeyId to enable.",
                self.name, chat_id,
            )
            return SendResult(success=False, error="push not configured; delayed reply dropped")

        inbound = self._inbounds.get(chat_id)
        client = WeChatPushClient(
            url=self.settings.push_url, key_id=self.settings.push_key_id, client=self._client
        )
        if inbound is not None and inbound.group_name:
            at_users = str(metadata.get("at_users") or "")
            result = await client.send_group(group_name=inbound.group_name, content=text, at_users=at_users)
        else:
            user_name = (inbound.user_id if inbound is not None else "") or chat_id
            result = await client.send_private(user_name=user_name, content=text)

        if result.ok:
            logger.info("[%s] delivered delayed reply via push (conversation=%s)", self.name, chat_id)
            return SendResult(success=True, message_id=f"push-{int(time.time() * 1000)}")
        logger.error("[%s] push delivery failed for %s: %s", self.name, chat_id, result.describe())
        return SendResult(success=False, error=result.describe())

    # ── 媒体（暂不支持） ──────────────────────────────────────
    async def send_document(self, chat_id: str, file_path: str, caption: Optional[str] = None,
                            file_name: Optional[str] = None, reply_to: Optional[str] = None,
                            metadata: Optional[dict[str, Any]] = None, **kwargs) -> SendResult:
        del file_name, reply_to, metadata, kwargs
        return await self._media_unsupported(chat_id, file_path, caption)

    async def send_image_file(self, chat_id: str, image_path: str, caption: Optional[str] = None,
                              reply_to: Optional[str] = None,
                              metadata: Optional[dict[str, Any]] = None, **kwargs) -> SendResult:
        del reply_to, metadata, kwargs
        return await self._media_unsupported(chat_id, image_path, caption)

    async def _media_unsupported(self, chat_id: str, path: str, caption: Optional[str]) -> SendResult:
        """出站附件暂不支持：告诉用户文件路径/说明文本，而不是静默丢弃。

        平台出站文件的 ``content`` 要的是 COS 存储 path，而我们没有上传通道
        （COS 接入暂缓）。让用户拿到可读的信息优于"发了但什么都没有"。
        """
        name = os.path.basename(str(path or "")) or "附件"
        text = f"（附件 {name} 暂不支持通过本渠道发送）"
        if caption:
            text = f"{caption}\n\n{text}"
        logger.info("[%s] outbound attachment not supported, sent notice instead (%s)", self.name, name)
        await self.send(chat_id=chat_id, content=text)
        return SendResult(success=True, message_id=f"media-unsupported-{int(time.time() * 1000)}")

    async def get_chat_info(self, chat_id: str) -> dict[str, Any]:
        inbound = self._inbounds.get(chat_id)
        if inbound is not None:
            return {"name": inbound.chat_name, "type": inbound.chat_type}
        return {"name": chat_id, "type": "dm"}


def _validate_config(cfg: PlatformConfig) -> bool:
    return True


def register(ctx) -> None:
    from plugins.platforms.digital_employee.config import YAML_ENV_SPEC
    from plugins.platforms.digital_employee.platform_hint import PLATFORM_HINT

    def _apply_yaml(yaml_cfg: dict, platform_cfg: dict) -> Optional[dict]:
        from gateway.platforms._shared import apply_yaml_bridge

        section = yaml_cfg.get("extra") if isinstance(yaml_cfg, dict) else None
        if not isinstance(section, dict):
            return {}
        return apply_yaml_bridge(section, YAML_ENV_SPEC) or {}

    ctx.register_platform(
        name="digital-employee",
        label="企微数字员工 (Digital Employee)",
        adapter_factory=DigitalEmployeeAdapter,
        check_fn=lambda: AIOHTTP_AVAILABLE and HTTPX_AVAILABLE,
        validate_config=_validate_config,
        is_connected=_validate_config,
        required_env=[],
        install_hint="数字员工平台经 HTTP 直连本网关，无需额外 SDK。",
        emoji="🧑‍💼",
        platform_hint=PLATFORM_HINT,
        allowed_users_env="DIGITAL_EMPLOYEE_ALLOWED_USERS",
        allowlist_suppresses_pairing=True,
        apply_yaml_config_fn=_apply_yaml,
        cron_deliver_env_var="DIGITAL_EMPLOYEE_HOME_CHANNEL",
    )
