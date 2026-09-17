"""Tests for the (fork) health plugin (passive in-process health endpoint).

health_lib/server are imported by path (plugins/AGENTS.md convention — sibling
lib modules stay relative-import-free); __init__ is exercised through the real
loader. HERMES_HOME is isolated per test.
"""

import importlib.util
import json
import socket
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[2]
_LIB_PATH = _REPO / "plugins" / "health" / "health_lib.py"
_SRV_PATH = _REPO / "plugins" / "health" / "server.py"
_PLUGIN_DIR = _REPO / "plugins" / "health"


def _load(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def lib():
    return _load(_LIB_PATH, "health_lib_under_test")


@pytest.fixture
def home(tmp_path, monkeypatch):
    """Isolated HERMES_HOME; also returned for direct file fixtures."""
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    return tmp_path


def _iso_ago(seconds: float) -> str:
    return (datetime.now(timezone.utc) - timedelta(seconds=seconds)).isoformat()


def _write_heartbeat(home: Path, *, age_s: float = 0.0, start_age_s: float | None = None) -> None:
    d = home / "state"
    d.mkdir(parents=True, exist_ok=True)
    payload = {"pid": 123, "updated_at": _iso_ago(age_s),
               "monotonic": 0.0}
    if start_age_s is not None:
        payload["start_time"] = time.time() - start_age_s
    (d / "gateway.heartbeat").write_text(json.dumps(payload), encoding="utf-8")


def _write_gateway_state(home: Path, payload: dict) -> None:
    (home / "gateway_state.json").write_text(json.dumps(payload), encoding="utf-8")


# ---------------------------------------------------------------------------
# HealthCounters (Task 1)
# ---------------------------------------------------------------------------

class TestHealthCounters:
    def test_snapshot_starts_empty(self, lib):
        snap = lib.HealthCounters(window_s=900).snapshot(now=1000.0)
        assert snap["calls"] == 0 and snap["errors"] == 0
        assert snap["consecutive_errors"] == 0
        assert snap["last_success_ts"] is None and snap["last_error_at"] is None
        assert snap["in_flight"] is False and snap["in_flight_age_s"] is None

    def test_success_resets_consecutive_errors(self, lib):
        c = lib.HealthCounters(window_s=900)
        c.on_error(ts=100.0)
        c.on_error(ts=101.0)
        c.on_success(ts=102.0)
        c.on_error(ts=103.0)
        snap = c.snapshot(now=104.0)
        assert snap["consecutive_errors"] == 1
        assert snap["errors"] == 3 and snap["calls"] == 1
        assert snap["last_error"]["reason"] == ""

    def test_error_records_classification(self, lib):
        c = lib.HealthCounters(window_s=900)
        c.on_error(reason="rate_limit", status_code=429, retryable=True,
                   error_type="RateLimitError", provider="anthropic", ts=100.0)
        info = c.snapshot(now=101.0)["last_error"]
        assert info["reason"] == "rate_limit" and info["status_code"] == 429
        assert info["retryable"] is True and info["provider"] == "anthropic"

    def test_in_flight_pairing(self, lib):
        c = lib.HealthCounters(window_s=900)
        c.on_pre(model="glm-4.7", ts=100.0)
        snap = c.snapshot(now=160.0)
        assert snap["in_flight"] is True and snap["in_flight_age_s"] == 60.0
        assert snap["in_flight_model"] == "glm-4.7"
        c.on_success(ts=170.0)
        snap = c.snapshot(now=171.0)
        assert snap["in_flight"] is False and snap["in_flight_age_s"] is None

    def test_error_also_clears_in_flight(self, lib):
        c = lib.HealthCounters(window_s=900)
        c.on_pre(ts=100.0)
        c.on_error(ts=130.0)
        assert c.snapshot(now=131.0)["in_flight"] is False

    def test_window_prunes_old_events(self, lib):
        c = lib.HealthCounters(window_s=100.0)
        c.on_success(ts=1000.0)
        c.on_error(ts=1050.0)
        snap = c.snapshot(now=2000.0)  # both events out of window
        assert snap["calls"] == 0 and snap["errors"] == 0
        # clocks persist beyond the window (used by stuck/model derivations)
        assert snap["last_success_ts"] == 1000.0

    def test_inbound_and_tool_clocks(self, lib):
        c = lib.HealthCounters(window_s=900)
        c.on_inbound(ts=500.0)
        c.on_tool(ts=510.0)
        snap = c.snapshot(now=520.0)
        assert snap["last_inbound_ts"] == 500.0 and snap["last_tool_ts"] == 510.0
        assert snap["last_inbound_at"].endswith("+00:00")

    def test_thread_safety_smoke(self, lib):
        c = lib.HealthCounters(window_s=900, max_events=5000)  # cap above event count

        def hammer():
            for i in range(500):
                c.on_success(ts=float(i))
                _ = c.snapshot(now=float(i))

        threads = [threading.Thread(target=hammer) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert c.snapshot(now=600.0)["calls"] == 2000


# ---------------------------------------------------------------------------
# model_check / stuck_check (Task 2)
# ---------------------------------------------------------------------------

def _snap(lib, *, calls=0, errors=0, consecutive=0, last_success=None, last_error=None,
          last_inbound=None, last_tool=None, in_flight=False, in_flight_age=None,
          last_error_info=None, window_s=900):
    return {
        "calls": calls, "errors": errors, "consecutive_errors": consecutive,
        "last_success_ts": last_success, "last_success_at": lib.utc_now_iso(last_success),
        "last_error_ts": last_error, "last_error_at": lib.utc_now_iso(last_error),
        "last_inbound_ts": last_inbound, "last_inbound_at": lib.utc_now_iso(last_inbound),
        "last_tool_ts": last_tool, "last_tool_at": lib.utc_now_iso(last_tool),
        "in_flight": in_flight, "in_flight_age_s": in_flight_age,
        "last_error": last_error_info, "window_s": window_s,
    }


class TestModelCheck:
    def test_no_data_when_idle(self, lib):
        out = lib.model_check(_snap(lib), now_epoch=1000.0)
        assert out["status"] == "no_data"

    def test_ok_after_success(self, lib):
        out = lib.model_check(_snap(lib, calls=2, last_success=990.0), now_epoch=1000.0)
        assert out["status"] == "ok"

    def test_down_on_consecutive_failures(self, lib):
        snap = _snap(lib, consecutive=3, errors=3, last_error=995.0,
                     last_error_info={"reason": "auth", "status_code": 401})
        out = lib.model_check(snap, now_epoch=1000.0, consecutive_down=3)
        assert out["status"] == "down"
        assert "API key" in out["remediation"]

    def test_down_rate_limit_hint(self, lib):
        snap = _snap(lib, consecutive=5, errors=5,
                     last_error_info={"reason": "rate_limit", "status_code": 429})
        out = lib.model_check(snap, now_epoch=1000.0)
        assert out["status"] == "down" and "限流" in out["remediation"]

    def test_degraded_on_error_success_mix(self, lib):
        snap = _snap(lib, calls=2, errors=1, consecutive=0, last_success=998.0,
                     last_error=995.0, last_error_info={"reason": "timeout"})
        out = lib.model_check(snap, now_epoch=1000.0)
        assert out["status"] == "degraded" and "fallback" in out["remediation"]

    def test_degraded_on_stale_in_flight(self, lib):
        snap = _snap(lib, in_flight=True, in_flight_age=700.0, calls=1, last_success=800.0)
        out = lib.model_check(snap, now_epoch=1000.0, inflight_stale_s=600.0)
        assert out["status"] == "degraded" and "/stop" in out["remediation"]

    def test_fresh_in_flight_is_ok(self, lib):
        snap = _snap(lib, in_flight=True, in_flight_age=30.0, calls=1, last_success=990.0)
        assert lib.model_check(snap, now_epoch=1000.0)["status"] == "ok"


class TestStuckCheck:
    def test_ok_when_no_inbound(self, lib):
        assert lib.stuck_check(_snap(lib), now_epoch=1000.0)["status"] == "ok"

    def test_ok_when_progress_after_inbound(self, lib):
        snap = _snap(lib, last_inbound=900.0, last_success=950.0)
        assert lib.stuck_check(snap, now_epoch=1000.0)["status"] == "ok"

    def test_ok_when_tool_progress_after_inbound(self, lib):
        snap = _snap(lib, last_inbound=900.0, last_tool=920.0)
        assert lib.stuck_check(snap, now_epoch=1000.0)["status"] == "ok"

    def test_ok_while_in_flight(self, lib):
        snap = _snap(lib, last_inbound=900.0, in_flight=True)
        assert lib.stuck_check(snap, now_epoch=1000.0, stuck_minutes=10.0)["status"] == "ok"

    def test_degraded_when_unanswered_beyond_threshold(self, lib):
        snap = _snap(lib, last_inbound=900.0)  # 100s unanswered, threshold 60s
        out = lib.stuck_check(snap, now_epoch=1000.0, stuck_minutes=1.0)
        assert out["status"] == "degraded"
        assert out["unanswered_min"] == 1.7  # 100s in minutes
        assert "/new" in out["remediation"]

    def test_ok_within_grace_window(self, lib):
        snap = _snap(lib, last_inbound=950.0)
        assert lib.stuck_check(snap, now_epoch=1000.0, stuck_minutes=10.0)["status"] == "ok"


# ---------------------------------------------------------------------------
# File-contract probes (Task 3)
# ---------------------------------------------------------------------------

class TestLoopProbe:
    def test_missing_file_is_unknown(self, lib, home):
        out = lib.probe_loop(home)
        assert out["status"] == "unknown"

    def test_fresh_heartbeat_ok_with_uptime(self, lib, home):
        _write_heartbeat(home, age_s=10.0, start_age_s=3600.0)
        out = lib.probe_loop(home)
        assert out["status"] == "ok"
        assert 0 <= out["heartbeat_age_s"] <= 30
        assert 3500 <= out["uptime_s"] <= 3700

    def test_stale_heartbeat_is_down_with_remediation(self, lib, home):
        _write_heartbeat(home, age_s=400.0)
        out = lib.probe_loop(home)
        assert out["status"] == "down"
        assert out["heartbeat_age_s"] >= 400
        assert "主循环" in out["remediation"]


class TestProcessProbe:
    def test_missing_file_is_ok_starting(self, lib, home):
        out = lib.probe_process(home)
        assert out["status"] == "ok" and out["file"] == "missing"

    def test_own_pid_live_ok(self, lib, home, monkeypatch):
        import os
        # start_time needs to match the live process for the reuse guard; patch the
        # validator instead of reproducing the fingerprint.
        import gateway.status as gw_status
        monkeypatch.setattr(gw_status, "runtime_status_pid_is_live", lambda record: True)
        _write_gateway_state(home, {"pid": os.getpid(), "gateway_state": "running"})
        out = lib.probe_process(home)
        assert out["status"] == "ok" and out["gateway_state"] == "running"

    def test_dead_file_pid_degraded(self, lib, home, monkeypatch):
        import gateway.status as gw_status
        monkeypatch.setattr(gw_status, "runtime_status_pid_is_live", lambda record: False)
        _write_gateway_state(home, {"pid": 999999})
        out = lib.probe_process(home)
        assert out["status"] == "degraded"

    def test_foreign_live_pid_degraded(self, lib, home, monkeypatch):
        import os
        import gateway.status as gw_status
        monkeypatch.setattr(gw_status, "runtime_status_pid_is_live", lambda record: True)
        _write_gateway_state(home, {"pid": os.getpid() + 1})
        out = lib.probe_process(home)
        assert out["status"] == "degraded" and "双实例" in out["remediation"]


class TestPlatformProbe:
    def _record(self, **wecom):
        return {"platforms": {"wecom": wecom}} if wecom else {"platforms": {}}

    def test_missing_platform_unknown(self, lib, home):
        _write_gateway_state(home, self._record())
        assert lib.probe_platform(home)["status"] == "unknown"

    def test_connected_ok(self, lib, home):
        _write_gateway_state(home, self._record(state="connected"))
        assert lib.probe_platform(home)["status"] == "ok"

    def test_recent_disconnect_degraded(self, lib, home):
        _write_gateway_state(home, self._record(state="disconnected", retrying_since=_iso_ago(60)))
        out = lib.probe_platform(home)
        assert out["status"] == "degraded"

    def test_sustained_disconnect_down(self, lib, home):
        _write_gateway_state(home, self._record(state="disconnected", retrying_since=_iso_ago(900)))
        out = lib.probe_platform(home, platform_down_minutes=10.0)
        assert out["status"] == "down" and "WeCom" in out["remediation"]

    def test_needs_attention_down(self, lib, home):
        _write_gateway_state(home, self._record(state="connected", needs_attention=True))
        assert lib.probe_platform(home)["status"] == "down"


class TestSystemProbe:
    def test_all_ok_with_model_config(self, lib, home):
        (home / "config.yaml").write_text("model: glm-4.7\n", encoding="utf-8")
        out = lib.probe_system(home, runtime_status={"gateway_state": "running"})
        assert out["status"] == "ok"
        assert out["checks"]["model"]["status"] == "ok"
        assert "memory" in out

    def test_missing_model_config_degraded(self, lib, home):
        out = lib.probe_system(home, runtime_status={})
        assert out["status"] == "degraded"
        assert out["checks"]["model"]["status"] == "degraded"

    def test_critical_memory_degraded(self, lib, home, monkeypatch):
        (home / "config.yaml").write_text("model: glm-4.7\n", encoding="utf-8")
        import gateway.memory_status as gw_mem
        monkeypatch.setattr(gw_mem, "collect_memory_status",
                            lambda home=None, **kw: {"pressure": "critical"})
        out = lib.probe_system(home, runtime_status={})
        assert out["status"] == "degraded" and "内存" in out["remediation"]


# ---------------------------------------------------------------------------
# Aggregate + payload build (Task 4)
# ---------------------------------------------------------------------------

class TestAggregate:
    def test_worst_wins(self, lib):
        assert lib.aggregate("ok", "degraded", "unknown") == "degraded"
        assert lib.aggregate("ok", "no_data", "unknown") == "ok"
        assert lib.aggregate("down", "ok") == "down"

    def test_empty_is_ok(self, lib):
        assert lib.aggregate() == "ok"


class TestBuildDetail:
    def _home_fresh(self, home):
        import os
        _write_heartbeat(home, age_s=5.0, start_age_s=100.0)
        # own PID so the process probe sees the state file as ours (not a foreign gateway)
        _write_gateway_state(home, {"pid": os.getpid(), "gateway_state": "running",
                                    "platforms": {"wecom": {"state": "connected"}}})

    def test_shape_and_ok(self, lib, home):
        self._home_fresh(home)
        (home / "config.yaml").write_text("model: glm-4.7\n", encoding="utf-8")
        detail = lib.build_health_detail(
            lib.HealthCounters(window_s=900).snapshot(), home=home)
        assert detail["status"] == "ok"
        assert set(detail) >= {"status", "checked_at", "process", "loop", "platform",
                               "model", "stuck", "system", "remediation", "uptime_s"}
        assert detail["model"]["status"] == "no_data"
        assert detail["remediation"] == []

    def test_down_when_loop_stale(self, lib, home):
        _write_heartbeat(home, age_s=400.0)
        detail = lib.build_health_detail(
            lib.HealthCounters().snapshot(), home=home)
        assert detail["status"] == "down"
        assert any("主循环" in r for r in detail["remediation"])

    def test_remediations_collected_across_checks(self, lib, home):
        import os
        _write_heartbeat(home, age_s=5.0)
        _write_gateway_state(home, {"pid": os.getpid(), "gateway_state": "running",
                                    "platforms": {"wecom": {"state": "disconnected",
                                                            "retrying_since": _iso_ago(1200)}}})
        detail = lib.build_health_detail(
            lib.HealthCounters().snapshot(), home=home)
        assert detail["platform"]["status"] == "down"
        assert any("WeCom" in r for r in detail["remediation"])

    def test_summarize_minimal(self, lib):
        detail = {"status": "degraded", "checked_at": "2026-09-15T00:00:00+00:00", "x": 1}
        assert lib.summarize(detail) == {"status": "degraded",
                                         "checked_at": "2026-09-15T00:00:00+00:00"}
