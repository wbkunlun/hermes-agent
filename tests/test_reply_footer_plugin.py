"""Tests for the reply-footer fork plugin (usage footer appended to replies).

The plugin lives in a hyphenated directory (``plugins/reply-footer/``) which
is not a valid package name, so we load it via ``importlib`` (like the plugin
loader and test_usage_trace_plugin.py do), fresh per test to isolate the
module-level config cache and per-turn bucket state.
"""

import importlib.util
import json
from pathlib import Path

import pytest

_PLUGIN_PATH = (
    Path(__file__).resolve().parents[1]
    / "plugins" / "reply-footer" / "__init__.py"
)


@pytest.fixture
def plugin(monkeypatch, tmp_path):
    """Load the plugin fresh; point config.json at a per-test copy."""
    spec = importlib.util.spec_from_file_location("reply_footer_under_test", _PLUGIN_PATH)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    cfg = json.loads(Path(_PLUGIN_PATH).parent.joinpath("config.json").read_text(encoding="utf-8"))
    _write_config(m, tmp_path, cfg)
    return m


def _write_config(plugin, tmp_path, cfg):
    """Swap the plugin's config.json for a per-test copy (cache reset included)."""
    p = tmp_path / "config.json"
    p.write_text(json.dumps(cfg), encoding="utf-8")
    plugin._CONFIG_PATH = p
    plugin._cfg_cache = None
    return p


def _post(plugin, **kw):
    kw.setdefault("platform", "wecom")
    kw.setdefault("turn_id", "t1")
    plugin._on_post_api_request(**kw)


def _transform(plugin, text, **kw):
    kw.setdefault("platform", "wecom")
    kw.setdefault("turn_id", "t1")
    return plugin._on_transform_llm_output(response_text=text, **kw)


FOOT_ARGS = dict(model="claude-x", usage={"input_tokens": 1200, "output_tokens": 300},
                 api_duration=2.25, api_call_count=3)


class TestHappyPath:
    def test_footer_appended_to_body(self, plugin):
        _post(plugin, **FOOT_ARGS)
        out = _transform(plugin, "你好")
        assert out == "你好\n\n---\n📊 Model: claude-x · Input 1,200 · Output 300 · Duration 2.2s · Turns 3"

    def test_thousands_separators(self, plugin):
        _post(plugin, model="m", usage={"input_tokens": 12345, "output_tokens": 6789},
              api_duration=12.0, api_call_count=1234)
        out = _transform(plugin, "x")
        assert "Input 12,345" in out
        assert "Output 6,789" in out
        assert "Turns 1,234" in out

    def test_duration_one_decimal_with_s(self, plugin):
        _post(plugin, model="m", usage=None, api_duration=3.14159, api_call_count=1)
        out = _transform(plugin, "x")
        assert "Duration 3.1s" in out

    def test_model_fallback_first_ne_last(self, plugin):
        # Model switch mid-turn: first vs last model both shown, arrow-joined.
        _post(plugin, model="claude-a", usage={"input_tokens": 10, "output_tokens": 5},
              api_duration=1.0, api_call_count=1)
        _post(plugin, model="claude-b", usage={"input_tokens": 10, "output_tokens": 5},
              api_duration=1.0, api_call_count=2)
        out = _transform(plugin, "x")
        assert "Model: claude-a → claude-b" in out

    def test_usage_none_counts_zero_but_keeps_nonzero_fields(self, plugin):
        # usage=None (streaming/error path) counts 0 tokens; Duration/Turns still render.
        _post(plugin, model="m", usage=None, api_duration=4.0, api_call_count=2)
        out = _transform(plugin, "x")
        assert out is not None
        assert "Input" not in out
        assert "Output" not in out
        assert "Duration 4.0s" in out
        assert "Turns 2" in out

    def test_all_zero_fields_no_footer(self, plugin):
        # Nothing to report (only Model) — the whole footer line is skipped.
        _post(plugin, model="m", usage=None, api_duration=0, api_call_count=0)
        assert _transform(plugin, "x") is None

    def test_no_bucket_no_footer(self, plugin):
        # transform without any prior post_api_request for this turn: untouched.
        assert _transform(plugin, "x") is None


class TestIdempotence:
    def test_already_footed_text_skipped(self, plugin):
        # Replay/held path: a body that already carries the footer is left alone.
        footed = "body\n\n---\n📊 Model: m · Input 1 · Turns 1"
        _post(plugin, **FOOT_ARGS)
        assert _transform(plugin, footed) is None


class TestLimits:
    def test_over_chars_limit_skipped(self, plugin):
        # WeCom single-message cap in chars (4000): footer would push over.
        _post(plugin, **FOOT_ARGS)
        body = "a" * 3990  # + "\n\n---\n📊 ..." lands past 4000 chars
        assert _transform(plugin, body) is None

    def test_over_bytes_limit_skipped(self, plugin):
        # UTF-8 bytes cap (4096): CJK body is 3 bytes/char, chars stay under 4000.
        _post(plugin, **FOOT_ARGS)
        body = "你" * 1400  # 4200 bytes, 1400 chars
        assert _transform(plugin, body) is None

    def test_within_limits_appends(self, plugin):
        # Sanity: a normal-sized body does get the footer.
        _post(plugin, **FOOT_ARGS)
        assert _transform(plugin, "hi") is not None

    def test_max_text_bytes_skip(self, plugin, tmp_path):
        # Bodies already over max_text_bytes are skipped up front.
        cfg = json.loads(Path(_PLUGIN_PATH).parent.joinpath("config.json").read_text(encoding="utf-8"))
        cfg["max_text_bytes"] = 100
        _write_config(plugin, tmp_path, cfg)
        _post(plugin, **FOOT_ARGS)
        assert _transform(plugin, "b" * 200) is None


class TestPlatformGate:
    def test_non_wecom_platform_no_footer(self, plugin):
        _post(plugin, platform="slack", **FOOT_ARGS)
        assert _transform(plugin, "x", platform="slack") is None

    def test_platform_bucket_does_not_leak_across_platforms(self, plugin):
        # wecom bucket recorded, then a non-wecom transform must not consume it
        # as if it were its own (platform checked before bucket lookup).
        _post(plugin, **FOOT_ARGS)
        assert _transform(plugin, "x", platform="slack") is None
        assert _transform(plugin, "x") is not None


class TestConfigEnabled:
    def test_enabled_false_noops_both_hooks(self, plugin, tmp_path):
        cfg = json.loads(Path(_PLUGIN_PATH).parent.joinpath("config.json").read_text(encoding="utf-8"))
        cfg["enabled"] = False
        _write_config(plugin, tmp_path, cfg)
        _post(plugin, **FOOT_ARGS)
        assert plugin._buckets == {}  # post hook recorded nothing
        assert _transform(plugin, "x") is None

    def test_config_edit_takes_effect_live(self, plugin, tmp_path):
        # The framework never reads plugin config.json; the plugin self-reads
        # with an mtime cache — a mid-session flip applies on the next fire.
        cfg = json.loads(Path(_PLUGIN_PATH).parent.joinpath("config.json").read_text(encoding="utf-8"))
        cfg["enabled"] = False
        p = _write_config(plugin, tmp_path, cfg)
        _post(plugin, **FOOT_ARGS)
        assert _transform(plugin, "x") is None
        cfg["enabled"] = True
        p.write_text(json.dumps(cfg), encoding="utf-8")
        import os
        os.utime(p, (1_000_000_000, 1_000_000_000))  # force a distinct mtime
        _post(plugin, **FOOT_ARGS)
        assert _transform(plugin, "x") is not None


class TestBuckets:
    def test_bucket_eviction_bounded_at_512(self, plugin):
        for i in range(513):
            _post(plugin, turn_id=f"t{i}", model="m", usage=None, api_duration=1.0, api_call_count=1)
        assert len(plugin._buckets) == 512
        assert "t0" not in plugin._buckets  # oldest evicted
        assert "t512" in plugin._buckets

    def test_invalid_usage_types_fail_open(self, plugin):
        # Garbage payload shapes must not raise out of the hook.
        _post(plugin, model=None, usage="not-a-dict", api_duration="x", api_call_count="y")
        assert _transform(plugin, "x") is None  # no usable model, tokens stay 0


class TestAutoDisableReplyBadge:
    def _fake_ctx(self):
        class Ctx:
            hooks = []

            def register_hook(self, name, cb, **kw):
                self.hooks.append((name, cb))
        return Ctx()

    def test_register_both_hooks_no_hold_stream(self, plugin, monkeypatch, tmp_path):
        # No badge anywhere: register() must just register the two hooks,
        # without hold_stream (held transforms fail closed).
        monkeypatch.setenv("HERMES_HOME", str(tmp_path / "home"))
        ctx = self._fake_ctx()
        plugin.register(ctx)
        assert [h for h, _ in ctx.hooks] == ["post_api_request", "transform_llm_output"]

    def test_badge_enabled_in_config_gets_disabled(self, plugin, monkeypatch, tmp_path):
        import os
        home = tmp_path / "home"
        (home / "plugins").mkdir(parents=True)
        (home / "config.yaml").write_text("plugins:\n  enabled:\n    - reply-badge\n    - health\n", encoding="utf-8")
        monkeypatch.setenv("HERMES_HOME", str(home))
        ctx = self._fake_ctx()
        plugin.register(ctx)
        from ruamel.yaml import YAML
        with open(home / "config.yaml", encoding="utf-8") as f:
            data = YAML(typ="safe").load(f)
        assert data["plugins"]["disabled"] == ["reply-badge"]
        assert data["plugins"]["enabled"] == ["reply-badge", "health"]  # enabled untouched

    def test_badge_dir_presence_triggers_disable(self, plugin, monkeypatch, tmp_path):
        home = tmp_path / "home"
        (home / "plugins" / "reply-badge").mkdir(parents=True)
        monkeypatch.setenv("HERMES_HOME", str(home))
        ctx = self._fake_ctx()
        plugin.register(ctx)
        assert "reply-badge" in (home / "config.yaml").read_text(encoding="utf-8")

    def test_no_badge_no_write(self, plugin, monkeypatch, tmp_path):
        home = tmp_path / "home"
        home.mkdir()
        monkeypatch.setenv("HERMES_HOME", str(home))
        ctx = self._fake_ctx()
        plugin.register(ctx)
        assert not (home / "config.yaml").exists()
