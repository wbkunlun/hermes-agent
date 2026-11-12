"""fork 2026-11-11: the new ``model.max_output_tokens`` config key feeds the runtime
``max_tokens`` slot on every gateway/api_server runtime resolution path. The legacy
keys stay ignored (see test_output_caps_removed.py)."""

import json

_FIXTURE_PROVIDERS = {
    "local-fixture": {"api": "http://127.0.0.1:1/v1", "api_key": "fixture"}}


def _write_config(tmp_path, model_section):
    (tmp_path / "config.yaml").write_text(json.dumps({
        "model": {"default": "fixture", "provider": "local-fixture", **model_section},
        "providers": _FIXTURE_PROVIDERS}))


def test_configured_cap_flows_to_runtime(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    _write_config(tmp_path, {"max_output_tokens": 1234})
    from gateway.run import _resolve_runtime_agent_kwargs, _resolve_runtime_agent_kwargs_for_provider
    from gateway.platforms.api_server import _resolve_request_runtime_agent_kwargs

    runtime = _resolve_runtime_agent_kwargs()
    assert runtime["max_tokens"] == 1234
    assert runtime["base_url"].startswith("http://127.0.0.1:1")
    request = _resolve_request_runtime_agent_kwargs(provider="local-fixture", target_model="fixture")
    assert request["max_tokens"] == 1234
    assert request["base_url"].startswith("http://127.0.0.1:1")
    per_provider = _resolve_runtime_agent_kwargs_for_provider("local-fixture", "fixture")
    assert per_provider["max_tokens"] == 1234
    assert per_provider["base_url"].startswith("http://127.0.0.1:1")


def test_missing_or_nonpositive_cap_not_sent(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    from gateway.run import _resolve_runtime_agent_kwargs
    from gateway.platforms.api_server import _resolve_request_runtime_agent_kwargs

    for model_section in ({}, {"max_output_tokens": 0}, {"max_output_tokens": -5},
                          {"max_output_tokens": "1234"}, {"max_output_tokens": 1234.0},
                          {"max_output_tokens": True}):
        _write_config(tmp_path, model_section)
        runtime = _resolve_runtime_agent_kwargs()
        assert runtime.get("max_tokens") is None, model_section
        assert runtime["base_url"].startswith("http://127.0.0.1:1")
        request = _resolve_request_runtime_agent_kwargs(provider="local-fixture", target_model="fixture")
        assert request.get("max_tokens") is None, model_section


def test_legacy_max_tokens_still_ignored(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    _write_config(tmp_path, {"max_tokens": 17})
    from gateway.run import _resolve_runtime_agent_kwargs
    from gateway.platforms.api_server import _resolve_request_runtime_agent_kwargs

    runtime = _resolve_runtime_agent_kwargs()
    assert runtime.get("max_tokens") is None
    assert runtime["base_url"].startswith("http://127.0.0.1:1")
    request = _resolve_request_runtime_agent_kwargs(provider="local-fixture", target_model="fixture")
    assert request.get("max_tokens") is None
