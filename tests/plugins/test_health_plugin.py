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
