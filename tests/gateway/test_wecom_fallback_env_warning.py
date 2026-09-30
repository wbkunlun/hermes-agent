"""Regression (audit 2026-09-29 module-1 M1.5): a missing WECOM_CALLBACK_* env made
the agent fallback silently nonexistent — the other half of the 846609 DM-loss
incident. Operators had no startup signal at all."""
import logging

from plugins.platforms.wecom import adapter as adapter_mod


def test_missing_env_warns_once(caplog, monkeypatch):
    for name in ("WECOM_CALLBACK_CORP_ID", "WECOM_CALLBACK_CORP_SECRET", "WECOM_CALLBACK_AGENT_ID", "WECOM_AGENT_FALLBACK"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("WECOM_CALLBACK_CORP_ID", "corp")
    # secret + agent_id missing
    with caplog.at_level(logging.WARNING, logger=adapter_mod.__name__):
        adapter_mod._warn_if_agent_fallback_unconfigured()
    # caplog.text（非 r.message——LogRecord.message 仅在 formatter 跑过后存在，% 惰性参数下不可靠）
    assert "WECOM_CALLBACK_CORP_SECRET" in caplog.text and "WECOM_CALLBACK_AGENT_ID" in caplog.text


def test_all_env_set_no_warning(caplog, monkeypatch):
    monkeypatch.setenv("WECOM_CALLBACK_CORP_ID", "corp")
    monkeypatch.setenv("WECOM_CALLBACK_CORP_SECRET", "s")
    monkeypatch.setenv("WECOM_CALLBACK_AGENT_ID", "1")
    monkeypatch.delenv("WECOM_AGENT_FALLBACK", raising=False)
    with caplog.at_level(logging.WARNING, logger=adapter_mod.__name__):
        adapter_mod._warn_if_agent_fallback_unconfigured()
    assert "agent-fallback disabled" not in caplog.text


def test_explicit_off_silences(caplog, monkeypatch):
    monkeypatch.delenv("WECOM_CALLBACK_CORP_ID", raising=False)
    monkeypatch.setenv("WECOM_AGENT_FALLBACK", "off")
    with caplog.at_level(logging.WARNING, logger=adapter_mod.__name__):
        adapter_mod._warn_if_agent_fallback_unconfigured()
    assert "agent-fallback disabled" not in caplog.text


def test_warning_matches_client_availability_matrix(caplog, monkeypatch):
    """fork (audit module-1 M1.5): 防漂移矩阵——告警触发必须 ⇔ (client 为 None 且未显式 off)。
    两处 env/off 判定任何一边单独演化都会让操作员信号失真。"""
    from plugins.platforms.wecom import callback_adapter as ca_mod

    monkeypatch.setattr(ca_mod, "WecomAgentFallbackClient", lambda *a: object())

    def _warned() -> bool:
        caplog.clear()
        with caplog.at_level(logging.WARNING, logger=adapter_mod.__name__):
            adapter_mod._warn_if_agent_fallback_unconfigured()
        return "agent-fallback disabled" in caplog.text

    for corp in ("corp", "", "  "):
        for secret in ("s", "", "  "):
            for agent in ("1", "", "  "):
                for off in ("", "0", "off", "yes"):
                    for name in ("WECOM_CALLBACK_CORP_ID", "WECOM_CALLBACK_CORP_SECRET", "WECOM_CALLBACK_AGENT_ID", "WECOM_AGENT_FALLBACK"):
                        monkeypatch.delenv(name, raising=False)
                    monkeypatch.setenv("WECOM_CALLBACK_CORP_ID", corp)
                    monkeypatch.setenv("WECOM_CALLBACK_CORP_SECRET", secret)
                    monkeypatch.setenv("WECOM_CALLBACK_AGENT_ID", agent)
                    if off:
                        monkeypatch.setenv("WECOM_AGENT_FALLBACK", off)
                    adapter_mod._agent_fallback_client_cache.update(env=None, client=None)  # 重置 memo，env 元组每轮变化
                    explicit_off = off.strip().lower() in adapter_mod._AGENT_FALLBACK_OFF_VALUES
                    expected = (adapter_mod._agent_fallback_client() is None) and not explicit_off
                    assert _warned() == expected, (
                        f"warn/client 判定漂移: corp={corp!r} secret={secret!r} agent={agent!r} off={off!r}"
                    )
