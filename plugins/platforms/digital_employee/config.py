"""企微「数字员工」平台 —— 配置读取。

所有配置走同一条链：env → 本 profile ``config.yaml`` 的
``platforms.digital_employee.extra.<key>`` → 默认值（``_shared.extra_or_secret``，
multiplex 下不会借读其它 profile 的 env）。

**主动发消息的地址与 keyId 一律可配**（UAT / OA / SF 各环境只换配置）；
未配齐即关闭——超时降级退化为"只回提示语"。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from gateway.platforms._shared import extra_or_secret

ENV_PREFIX = "DIGITAL_EMPLOYEE_"

DEFAULT_WEBHOOK_HOST = "0.0.0.0"
DEFAULT_WEBHOOK_PORT = 13777  # 与 wework 的 13776 错开，可同时启用
DEFAULT_WEBHOOK_PATH = "/chat"
# 平台侧 Dify 节点通常配 120s 超时；我们提前 10s 给出提示，避免平台先判超时。
DEFAULT_RESPONSE_TIMEOUT = 110.0
DEFAULT_TIMEOUT_NOTICE = "正在处理，稍后把结果发给你。"
DEFAULT_TEXT_CHUNK_LIMIT = 4000
DEFAULT_REQUIRE_MENTION = True


def _env(name: str) -> str:
    return ENV_PREFIX + name


def _as_float(value: Any, default: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return default
    return parsed if parsed > 0 else default


def _as_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return default


def _as_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y", "on"}:
        return True
    if text in {"false", "0", "no", "n", "off"}:
        return False
    return default


@dataclass(frozen=True)
class DigitalEmployeeConfig:
    """插件运行时配置（构造后不可变）。"""

    api_key: str
    auth_header: str
    webhook_host: str
    webhook_port: int
    webhook_path: str
    response_timeout: float
    timeout_notice: str
    push_url: str
    push_key_id: str
    text_chunk_limit: int
    require_mention: bool
    allowed_users: str

    @property
    def auth_enabled(self) -> bool:
        return bool(self.api_key)

    @property
    def push_enabled(self) -> bool:
        return bool(self.push_url and self.push_key_id)

    def missing_push(self) -> list[str]:
        return [n for n, v in (("pushUrl", self.push_url), ("pushKeyId", self.push_key_id)) if not v]


def load_config(extra: Optional[dict] = None) -> DigitalEmployeeConfig:
    """从 env + ``extra``（``PlatformConfig.extra``）读出配置。"""
    extra = extra or {}

    def read(key: str, default: Any = "") -> Any:
        return extra_or_secret(extra, key, _env(key.upper()), default)

    return DigitalEmployeeConfig(
        api_key=str(read("apiKey", "") or "").strip(),
        # 鉴权头名：默认标准 Authorization: Bearer；某些网关只认自定义头时改成 X-Api-Key。
        auth_header=str(read("authHeader", "Authorization") or "Authorization").strip(),
        webhook_host=str(read("webhookHost", DEFAULT_WEBHOOK_HOST) or DEFAULT_WEBHOOK_HOST).strip(),
        webhook_port=_as_int(read("webhookPort", DEFAULT_WEBHOOK_PORT), DEFAULT_WEBHOOK_PORT),
        webhook_path=_normalize_path(str(read("webhookPath", DEFAULT_WEBHOOK_PATH) or DEFAULT_WEBHOOK_PATH)),
        response_timeout=_as_float(read("responseTimeout", DEFAULT_RESPONSE_TIMEOUT), DEFAULT_RESPONSE_TIMEOUT),
        timeout_notice=str(read("timeoutNotice", DEFAULT_TIMEOUT_NOTICE) or DEFAULT_TIMEOUT_NOTICE),
        push_url=str(read("pushUrl", "") or "").strip(),
        push_key_id=str(read("pushKeyId", "") or "").strip(),
        text_chunk_limit=_as_int(read("textChunkLimit", DEFAULT_TEXT_CHUNK_LIMIT), DEFAULT_TEXT_CHUNK_LIMIT),
        require_mention=_as_bool(read("requireMention", None), DEFAULT_REQUIRE_MENTION),
        allowed_users=str(read("allowedUsers", "") or "").strip(),
    )


def _normalize_path(path: str) -> str:
    path = (path or "").strip() or DEFAULT_WEBHOOK_PATH
    return path if path.startswith("/") else "/" + path


# ``config.yaml`` 键 → env 名（``apply_yaml_bridge``；env 优先于 YAML）。
YAML_ENV_SPEC = tuple(
    (key, _env(key.upper()), "str")
    for key in (
        "apiKey",
        "authHeader",
        "webhookHost",
        "webhookPort",
        "webhookPath",
        "responseTimeout",
        "timeoutNotice",
        "pushUrl",
        "pushKeyId",
        "textChunkLimit",
        "requireMention",
        "allowedUsers",
    )
)
