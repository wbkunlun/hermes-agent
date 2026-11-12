"""POST /api/model/fallback/auto-fill: one-click top-N fallback fill for the web config page.

The route must reuse the CLI write semantics exactly (``fallback_cmd._write_chain``:
write ``fallback_providers`` + pop the legacy ``fallback_model`` key), keep every
existing/manual chain entry, skip the primary deployment (a provider cannot be a
fallback for itself) and skip duplicates — while never storing a plaintext
``api_key`` (custom endpoints carry only ``base_url``/``key_env`` pointers).

FastAPI TestClient pattern (per tests/hermes_cli/test_web_server_fs.py):
a minimal ``FastAPI()`` app + ``include_router(models.router)``; config I/O is
monkeypatched on ``hermes_cli.config`` (the router resolves ``load_config`` /
``save_config`` / ``require_readable_config_before_write`` late at call time) and
the inventory payload builder is patched on ``hermes_cli.inventory``.
"""

from __future__ import annotations

import copy
from contextlib import contextmanager
from pathlib import Path

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("starlette.testclient")
from fastapi import FastAPI
from starlette.testclient import TestClient

AUTO_FILL_PATH = "/api/model/fallback/auto-fill"


@pytest.fixture()
def config_home(tmp_path, monkeypatch):
    """Keep any config/env resolution inside the test's own home."""
    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    return home


@pytest.fixture()
def config_state():
    """The dict ``load_config`` / ``require_readable_config_before_write`` serve; tests mutate it."""
    return {"model": {"provider": "other", "default": "hand-added"}}


@pytest.fixture()
def saved_configs():
    """What ``save_config`` received (the on-disk equivalent, captured per call)."""
    return []


@pytest.fixture()
def patch_config(monkeypatch, config_state, saved_configs):
    from hermes_cli import config as config_mod

    monkeypatch.setattr(config_mod, "load_config", lambda: copy.deepcopy(config_state))
    monkeypatch.setattr(
        config_mod, "require_readable_config_before_write", lambda *a, **k: copy.deepcopy(config_state)
    )

    def _fake_save(cfg, **_kwargs):
        saved_configs.append(copy.deepcopy(cfg))
        # Writes land "on disk": subsequent reads (and the next request's RMW) see them.
        config_state.clear()
        config_state.update(copy.deepcopy(cfg))

    monkeypatch.setattr(config_mod, "save_config", _fake_save)


@pytest.fixture()
def inventory_rows():
    return [
        {"slug": "prov-a", "name": "Provider A", "models": ["a1", "a2", "a3", "a4", "a5", "a6", "a7"]},
        {"slug": "prov-b", "name": "Provider B", "models": ["b1", "b2"]},
        {
            "slug": "custom:foo", "name": "Foo endpoint", "models": ["f1", "f2", "f3"],
            "api_url": "http://127.0.0.1:9/v1", "is_user_defined": True,
        },
    ]


@pytest.fixture()
def patch_inventory(monkeypatch, inventory_rows):
    from hermes_cli import inventory

    monkeypatch.setattr(
        inventory, "load_picker_context", lambda: object(), raising=True,
    )

    def _fake_build(ctx):
        return {"providers": copy.deepcopy(inventory_rows), "model": "", "provider": ""}

    monkeypatch.setattr(inventory, "build_models_payload", _fake_build, raising=True)


@pytest.fixture()
def no_profile_scope(monkeypatch):
    """Passthrough for both profile scopes: the route enters them (contextvar-only,
    threadpool-safe) but the test serves no real profile or secret scope."""
    import hermes_cli.web_server_profiles as profiles

    @contextmanager
    def _passthrough(_profile=None):
        yield None

    monkeypatch.setattr(profiles, "_config_profile_scope", _passthrough)
    monkeypatch.setattr(profiles, "_profile_scope", _passthrough)


@pytest.fixture()
def client(patch_config, patch_inventory, no_profile_scope):
    from hermes_cli.web_routers.models import router

    app = FastAPI()
    app.include_router(router)
    return TestClient(app)


def test_top_five_taken_and_short_lists_all_taken(client, config_state):
    """① prov-a fills exactly 5 of its 7 models; prov-b (2 models, fewer than 5) fills all of them."""
    config_state["fallback_providers"] = []

    resp = client.post(AUTO_FILL_PATH, json={"provider": "prov-a", "top_n": 5})
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["ok"] is True
    assert data["requested"] == 5
    assert data["excluded"] == []
    assert data["chain"] == [
        {"provider": "prov-a", "model": "a1"},
        {"provider": "prov-a", "model": "a2"},
        {"provider": "prov-a", "model": "a3"},
        {"provider": "prov-a", "model": "a4"},
        {"provider": "prov-a", "model": "a5"},
    ]

    resp_b = client.post(AUTO_FILL_PATH, json={"provider": "prov-b", "top_n": 5})
    assert resp_b.status_code == 200, resp.text
    data_b = resp_b.json()
    assert data_b["requested"] == 2  # short list: everything is taken
    assert [e["model"] for e in data_b["chain"]][-2:] == ["b1", "b2"]
    assert len(data_b["chain"]) == 7  # the earlier prov-a entries are preserved


def test_manual_entries_kept_and_duplicates_skipped(client, config_state):
    """② hand-added cross-provider entries stay first; an entry already in the
    chain is kept once and reported as excluded/duplicate, not re-added."""
    config_state["fallback_providers"] = [
        {"provider": "other", "model": "hand-added"},
        {"provider": "prov-a", "model": "a1"},
    ]

    resp = client.post(AUTO_FILL_PATH, json={"provider": "prov-a", "top_n": 5})
    assert resp.status_code == 200, resp.text
    data = resp.json()

    chain = data["chain"]
    assert chain[0] == {"provider": "other", "model": "hand-added"}
    assert chain[1] == {"provider": "prov-a", "model": "a1"}
    models = [e["model"] for e in chain]
    assert len(models) == len(set(models)) == 6  # hand-added + a1 + a2..a5, no duplicates
    assert {"provider": "other", "model": "hand-added"} in data["added"] or data["added"] == [
        {"provider": "prov-a", "model": m} for m in ("a2", "a3", "a4", "a5")
    ]
    assert [(e["entry"], e["reason"]) for e in data["excluded"]] == [
        ({"provider": "prov-a", "model": "a1"}, "duplicate"),
    ]


def test_same_deployment_as_primary_excluded(client, config_state):
    """③ the primary itself (config.model) must never become its own fallback."""
    config_state["model"] = {"provider": "prov-a", "default": "a1"}
    config_state["fallback_providers"] = []

    resp = client.post(AUTO_FILL_PATH, json={"provider": "prov-a", "top_n": 5})
    assert resp.status_code == 200, resp.text
    data = resp.json()

    models = [e["model"] for e in data["chain"]]
    assert "a1" not in models
    assert models == ["a2", "a3", "a4", "a5"]
    assert [(e["entry"], e["reason"]) for e in data["excluded"]] == [
        ({"provider": "prov-a", "model": "a1"}, "same_deployment"),
    ]


def test_legacy_fallback_model_key_popped_on_write(client, config_state, saved_configs):
    """④ the write goes through fallback_cmd._write_chain semantics: the legacy
    ``fallback_model`` key disappears and its entry is folded into the chain."""
    config_state["model"] = {"provider": "other", "default": "legacy-primary"}
    config_state["fallback_providers"] = [{"provider": "deepseek", "model": "deepseek-v4-flash"}]
    config_state["fallback_model"] = {"provider": "nous", "model": "Hermes-4"}

    resp = client.post(AUTO_FILL_PATH, json={"provider": "prov-a", "top_n": 2})
    assert resp.status_code == 200, resp.text

    assert len(saved_configs) == 1
    saved = saved_configs[0]
    assert "fallback_model" not in saved, "legacy key must be popped, not left behind"
    assert saved["fallback_providers"] == [
        {"provider": "deepseek", "model": "deepseek-v4-flash"},
        {"provider": "nous", "model": "Hermes-4"},  # legacy entry merged, kept
        {"provider": "prov-a", "model": "a1"},
        {"provider": "prov-a", "model": "a2"},
    ]
    assert resp.json()["chain"] == saved["fallback_providers"]


def test_custom_endpoint_entry_carries_pointers_not_plaintext_key(client, config_state):
    """⑤ a custom:endpoint row yields {provider, model, base_url, key_env} entries — never api_key."""
    config_state["providers"] = {
        "foo": {
            "name": "Foo endpoint",
            "base_url": "http://127.0.0.1:9/v1",
            "key_env": "HERMES_CUSTOM_FOO_API_KEY",
        },
    }
    config_state["fallback_providers"] = []

    resp = client.post(AUTO_FILL_PATH, json={"provider": "custom:foo", "top_n": 5})
    assert resp.status_code == 200, resp.text
    data = resp.json()

    entries = [e for e in data["chain"] if e["provider"] == "custom:foo"]
    assert [e["model"] for e in entries] == ["f1", "f2", "f3"]
    for entry in entries:
        assert entry["base_url"] == "http://127.0.0.1:9/v1"
        assert entry["key_env"] == "HERMES_CUSTOM_FOO_API_KEY"
        assert "api_key" not in entry


def test_configpage_excludes_fallback_providers_from_generic_form():
    """⑥ the chain guard: ConfigPage must drop ``fallback_providers`` from the
    schema-driven AutoField render (a list-of-dict rendered/edited as a comma
    string would destroy the whole chain on the next save) — the same exclusion
    mechanism memory.provider already uses. The schema entry itself stays for
    other consumers (Desktop settings)."""
    repo_root = Path(__file__).resolve().parents[2]
    text = (repo_root / "web" / "src" / "pages" / "ConfigPage.tsx").read_text(encoding="utf-8")
    assert 'delete fields["fallback_providers"];' in text
    assert 'delete fields["memory.provider"];' in text  # same exclusion block


def test_unknown_provider_returns_404(client):
    resp = client.post(AUTO_FILL_PATH, json={"provider": "ghost-lab", "top_n": 5})
    assert resp.status_code == 404
    assert "provider not found" in resp.json()["detail"]


def test_provider_without_models_returns_422(client, config_state, inventory_rows):
    inventory_rows.append({"slug": "prov-empty", "name": "Empty", "models": []})
    resp = client.post(AUTO_FILL_PATH, json={"provider": "prov-empty", "top_n": 5})
    assert resp.status_code == 422
