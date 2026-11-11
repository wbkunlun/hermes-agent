"""reply-footer — fork plugin (wehermes): model usage footer appended to replies.

Adds a Claude Code style usage footer to the end of each final reply text
(same message, no separate stats message):

    <正文>

    ---
    📊 Model: {model} · Input {1,234} · Output {567} · Duration {3.2}s · Turns {4}

How it works:

* ``post_api_request`` fires once per API call within a turn (shared
  ``turn_id``); the callback folds each call into a per-turn bucket keyed by
  ``turn_id``: ``model_first`` / ``model_last``, ``input_tokens`` /
  ``output_tokens`` (summed — ``usage`` is the canonical per-call bucket),
  ``api_duration`` (per-call seconds, summed), ``api_call_count``
  (turn-cumulative, last value wins).
* ``transform_llm_output`` fires once per turn before the final row is
  persisted; it takes the turn's bucket and returns
  ``response_text + "\n\n---\n📊 ..."``. First non-empty string wins in the
  framework, so returning the rewritten text here is what the user sees
  (wecom is non-streaming by fork default — no hold_stream needed, and none
  is registered: hold_stream would fail-closed the whole turn).

Guards:

* Platform gate: only runs when the payload ``platform`` is listed in
  ``config.json`` ``platforms`` (default ``["wecom"]``). The framework has no
  declarative platform gating, so each callback checks it itself.
* Idempotence: a ``response_text`` that already contains ``"📊 Model:"`` is
  left alone (the framework memoizes per turn; this backs the held/replay
  paths up).
* WeCom single-message limits: the footer is only appended when the whole
  message stays within ``single_message_max_chars`` (4000 chars) and
  ``single_message_max_bytes`` (4096 UTF-8 bytes); otherwise the text goes
  out untouched. Texts over ``max_text_bytes`` (20000) are skipped up front.
* Bounded memory: at most ``_BUCKET_MAX`` (512) turn buckets, oldest evicted.
* Fail-open everywhere: every callback swallows its own exceptions (the
  framework already isolates one callback's error; this is the second belt).

Config lives in ``config.json`` next to this file. The framework does not
read plugin config.json, so both hooks re-read it on every fire (mtime-cached)
— flipping ``enabled`` takes effect immediately, no restart.
"""

from __future__ import annotations

import json
import logging
import os
import threading
from collections import OrderedDict
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

_BUCKET_MAX = 512
_FOOTER_MARKER = "📊 Model:"

# Loaded config.json cache: (mtime, size, config dict). Re-checked every hook
# fire so config edits apply without a restart.
_cfg_lock = threading.Lock()
_cfg_cache: Optional[tuple[float, int, dict[str, Any]]] = None

_CONFIG_PATH = Path(__file__).resolve().parent / "config.json"

_CONFIG_DEFAULTS: dict[str, Any] = {
    "enabled": True,
    "platforms": ["wecom"],
    "auto_disable_reply_badge": True,
    "max_text_bytes": 20000,
    "single_message_max_bytes": 4096,
    "single_message_max_chars": 4000,
}

# turn_id -> bucket. post_api_request writes, transform_llm_output consumes.
_buckets: OrderedDict[str, dict[str, Any]] = OrderedDict()
_buckets_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Configuration (self-read: the framework never reads plugin config.json)
# ---------------------------------------------------------------------------

def _load_config() -> dict[str, Any]:
    """config.json next to this plugin, mtime-cached; parse failure keeps the
    last good copy (or defaults) — never raises, never disables the plugin."""
    global _cfg_cache
    try:
        st = _CONFIG_PATH.stat()
        key = (st.st_mtime, st.st_size)
        with _cfg_lock:
            if _cfg_cache is not None and _cfg_cache[:2] == key:
                return _cfg_cache[2]
        cfg = dict(_CONFIG_DEFAULTS)
        try:
            raw = json.loads(_CONFIG_PATH.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                cfg.update(raw)
        except (OSError, ValueError) as exc:
            logger.warning("reply-footer: config.json unreadable (%s), using defaults", exc)
        with _cfg_lock:
            _cfg_cache = (key[0], key[1], cfg)
        return cfg
    except OSError:  # stat failed (missing dir etc.) — defaults, no cache churn
        return dict(_CONFIG_DEFAULTS)


def _platform_allowed(cfg: dict[str, Any], platform: Any) -> bool:
    allowed = cfg.get("platforms")
    if not isinstance(allowed, list) or not allowed:
        return False
    return platform in allowed


# ---------------------------------------------------------------------------
# Per-turn buckets
# ---------------------------------------------------------------------------

def _fold_api_call(bucket: dict[str, Any], kw: dict[str, Any]) -> None:
    """Fold one post_api_request payload into the turn bucket.

    ``usage`` is the canonical per-call bucket (input_tokens/output_tokens/...,
    agent/usage_pricing.py CanonicalUsage — there is no completion_tokens);
    ``usage=None`` (streaming / error paths) counts as zero. ``api_duration``
    is the single call's seconds (api_start_time resets before every call,
    agent/conversation_loop.py) so it is summed; ``api_call_count`` is the
    turn-cumulative counter so the last fired value wins. ``model`` is the
    requested model, ``response_model`` the one that actually answered.
    """
    usage = kw.get("usage")
    if isinstance(usage, dict):
        bucket["input_tokens"] += int(usage.get("input_tokens") or 0)
        bucket["output_tokens"] += int(usage.get("output_tokens") or 0)
    try:
        duration = kw.get("api_duration")
        if duration is not None:
            bucket["api_duration"] += float(duration)
    except (TypeError, ValueError):
        pass
    count = kw.get("api_call_count")
    if count is not None:
        bucket["api_call_count"] = int(count)
    model = kw.get("response_model") or kw.get("model")
    if model:
        if not bucket.get("model_first"):  # "" placeholder must not win over the first real model
            bucket["model_first"] = str(model)
        bucket["model_last"] = str(model)


def _take_bucket(turn_id: Any) -> Optional[dict[str, Any]]:
    if not turn_id:
        return None
    with _buckets_lock:
        bucket = _buckets.pop(str(turn_id), None)
    return bucket


def _record_api_call(kw: dict[str, Any]) -> None:
    turn_id = kw.get("turn_id")
    if not turn_id:
        return
    with _buckets_lock:
        bucket = _buckets.get(str(turn_id))
        if bucket is None:
            bucket = {
                "model_first": "", "model_last": "",
                "input_tokens": 0, "output_tokens": 0,
                "api_duration": 0.0, "api_call_count": 0,
            }
            _buckets[str(turn_id)] = bucket
        _buckets.move_to_end(str(turn_id))
        while len(_buckets) > _BUCKET_MAX:  # bounded: drop the oldest turns
            _buckets.popitem(last=False)
    _fold_api_call(bucket, kw)


# ---------------------------------------------------------------------------
# Footer rendering
# ---------------------------------------------------------------------------

def _model_label(bucket: dict[str, Any]) -> str:
    first, last = bucket.get("model_first") or "", bucket.get("model_last") or ""
    if first and last and first != last:
        return f"{first} → {last}"
    return last or first or "unknown"


def _format_footer(bucket: dict[str, Any]) -> Optional[str]:
    """``📊 Model: ... · Input ... · Output ... · Duration ...s · Turns ...``

    Model always renders; zero-valued fields are dropped; when everything
    else is zero too the whole footer is skipped (nothing to report).
    """
    parts = [f"Model: {_model_label(bucket)}"]
    input_tokens = int(bucket.get("input_tokens") or 0)
    output_tokens = int(bucket.get("output_tokens") or 0)
    duration = float(bucket.get("api_duration") or 0.0)
    call_count = int(bucket.get("api_call_count") or 0)
    if input_tokens:
        parts.append(f"Input {input_tokens:,}")
    if output_tokens:
        parts.append(f"Output {output_tokens:,}")
    if duration:
        parts.append(f"Duration {duration:.1f}s")
    if call_count:
        parts.append(f"Turns {call_count:,}")
    if len(parts) == 1:  # only Model — nothing to report
        return None
    return "📊 " + " · ".join(parts)


def _build_footer_reply(response_text: str, bucket: dict[str, Any],
                        cfg: dict[str, Any]) -> Optional[str]:
    """Final text with the footer, or None when limits say hands off."""
    max_text_bytes = int(cfg.get("max_text_bytes") or _CONFIG_DEFAULTS["max_text_bytes"])
    max_chars = int(cfg.get("single_message_max_chars") or _CONFIG_DEFAULTS["single_message_max_chars"])
    max_bytes = int(cfg.get("single_message_max_bytes") or _CONFIG_DEFAULTS["single_message_max_bytes"])
    try:
        if len(response_text.encode("utf-8")) > max_text_bytes:
            return None
    except UnicodeDecodeError:
        return None
    footer = _format_footer(bucket)
    if footer is None:
        return None
    candidate = response_text + "\n\n---\n" + footer
    # WeCom single-message caps: chars and UTF-8 bytes both — over either, the
    # footer is dropped and the body goes out as-is.
    if len(candidate) > max_chars or len(candidate.encode("utf-8")) > max_bytes:
        return None
    return candidate


# ---------------------------------------------------------------------------
# Hook callbacks (each self-guarded: plugin errors never reach the agent loop)
# ---------------------------------------------------------------------------

def _on_post_api_request_inner(**kw: Any) -> None:
    cfg = _load_config()
    if not cfg.get("enabled"):
        return
    if not _platform_allowed(cfg, kw.get("platform")):
        return
    _record_api_call(kw)


def _on_transform_llm_output_inner(**kw: Any) -> Optional[str]:
    cfg = _load_config()
    if not cfg.get("enabled"):
        return None
    if not _platform_allowed(cfg, kw.get("platform")):
        return None
    response_text = kw.get("response_text")
    if not isinstance(response_text, str) or not response_text:
        return None
    if _FOOTER_MARKER in response_text:  # already footed — replay/held path
        return None
    bucket = _take_bucket(kw.get("turn_id"))
    if bucket is None:
        return None
    return _build_footer_reply(response_text, bucket, cfg)


def _guard(fn):
    """Fail-open wrapper: log and return None instead of raising."""
    def wrapped(**kw: Any):
        try:
            return fn(**kw)
        except Exception:
            logger.warning("reply-footer: %s failed", getattr(fn, "__name__", fn), exc_info=True)
            return None
    wrapped.__name__ = getattr(fn, "__name__", "reply_footer_hook")
    return wrapped


_on_post_api_request = _guard(_on_post_api_request_inner)
_on_transform_llm_output = _guard(_on_transform_llm_output_inner)


# ---------------------------------------------------------------------------
# reply-badge auto-disable (opt-out companion plugin)
# ---------------------------------------------------------------------------

def _maybe_disable_reply_badge(cfg: dict[str, Any]) -> None:
    """Fold ``reply-badge`` into ``plugins.disabled`` when we take over its job.

    Detection: the badge plugin present under ``$HERMES_HOME/plugins/`` or
    listed in ``config.yaml`` ``plugins.enabled``. Write is
    ``atomic_roundtrip_yaml_update`` (comment-preserving, atomic replace);
    any failure is logged and ignored (fail-open). NOTE: ``plugins.disabled``
    is consulted at plugin-load time only — the badge stays active in the
    current process until the next gateway restart
    (``supervisorctl restart hermes-gateway``).
    """
    if not cfg.get("auto_disable_reply_badge"):
        return
    try:
        from hermes_constants import get_hermes_home
        home = get_hermes_home()
        badge_dir = home / "plugins" / "reply-badge"
        config_yaml = home / "config.yaml"
        enabled_has_badge = False
        disabled = []
        if config_yaml.is_file():
            from ruamel.yaml import YAML
            with open(config_yaml, encoding="utf-8") as f:
                data = YAML(typ="safe").load(f) or {}
            plugins = data.get("plugins", {}) if isinstance(data, dict) else {}
            enabled = plugins.get("enabled")
            if isinstance(enabled, str):
                enabled = [enabled]
            enabled_has_badge = isinstance(enabled, list) and "reply-badge" in enabled
            disabled = plugins.get("disabled") or []
            if not isinstance(disabled, list):
                disabled = [disabled]
        if "reply-badge" in disabled:
            return
        if not badge_dir.is_dir() and not enabled_has_badge:
            return
        # atomic_roundtrip_yaml_update takes a value, not a mutator — read the
        # current disabled list above, then append.
        from utils import atomic_roundtrip_yaml_update

        atomic_roundtrip_yaml_update(config_yaml, "plugins.disabled", disabled + ["reply-badge"])
        logger.info("reply-footer: added reply-badge to plugins.disabled (active next restart)")
    except Exception:
        logger.warning("reply-footer: auto-disable reply-badge failed", exc_info=True)


def register(ctx) -> None:
    cfg = _load_config()
    _maybe_disable_reply_badge(cfg)
    # No hold_stream: wecom is non-streaming by fork default, and a held
    # transform fails closed with a fixed notice instead of the reply.
    ctx.register_hook("post_api_request", _on_post_api_request)
    ctx.register_hook("transform_llm_output", _on_transform_llm_output)
