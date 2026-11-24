"""Peer-call audit ledger (denials included, DPMS 563037).

The local ``a2a_audit.jsonl`` is the only call ledger (no platform ingest), so
the ledger must cover refusals too:

- 401 (no/invalid bearer) ⇒ ``policy`` record ``decision=deny reason=unauthenticated``
  with the remote address and **no request body**;
- 403 untrusted peer ⇒ ``policy`` record ``decision=deny reason=untrusted_peer``;
- peer-token hit ⇒ ``inbound`` record ``decision=allow identity=<peer name>``;
  master-token path records ``identity=owner``.

Runs a live adapter on a free port with mocked agent transport, per
``test_a2a_plugin.py``'s fixture style.
"""
import asyncio
import json
import socket
import urllib.error
import urllib.request

import pytest

from plugins.platforms.a2a import protocol, security

MASTER = "master-token-0000"
PEER_NAME = "alice-9f2a"
PEER_TOKEN = "peer-token-aaaa"


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


@pytest.fixture()
def audit_env(monkeypatch, tmp_path):
    """Token-protected adapter env + isolated audit home (loopback bind keeps the
    trusted-peer gate open except in the 403 test, which tightens it)."""
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setenv("A2A_BEARER_TOKEN", MASTER)
    monkeypatch.setenv("A2A_PEER_TOKENS", f"{PEER_NAME}:{PEER_TOKEN}")
    return tmp_path


@pytest.fixture()
def live_adapter(monkeypatch):
    """Run ``fn(base_url)`` against a real adapter server; returns its result."""
    from plugins.platforms.a2a.adapter import A2AAdapter
    from gateway.config import PlatformConfig

    def run(fn):
        port = _free_port()
        monkeypatch.setenv("A2A_PORT", str(port))
        adapter = A2AAdapter(PlatformConfig(enabled=True, extra={"port": port}))

        async def runner():
            assert await adapter.connect() is True
            try:
                return fn(f"http://127.0.0.1:{port}")
            finally:
                await adapter.disconnect()

        return asyncio.run(runner())

    return run


def _records(tmp_path):
    lines = (tmp_path / "a2a_audit.jsonl").read_text().strip().splitlines()
    return [json.loads(line) for line in lines]


def _post(base_url, body, token=None):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(base_url, data=json.dumps(body).encode(), headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode() or "{}")


def _send(text="hello"):
    return {"jsonrpc": "2.0", "id": "1", "method": "message/send",
            "params": {"message": protocol.text_message(protocol.ROLE_USER, text, context_id="ctx-audit")}}


def test_401_writes_deny_record(audit_env, live_adapter):
    live_adapter(lambda base: _post(base, _send(), token="wrong-token")[0] == 401 or None)
    rec = _records(audit_env)[-1]
    assert rec["direction"] == "policy"
    assert rec["decision"] == "deny"
    assert rec["reason"] == "unauthenticated"
    assert rec["peer"] == "unknown"
    assert rec["remote_addr"] == "127.0.0.1"
    assert rec["summary"] == ""  # the request body never enters the ledger


def test_401_no_token_writes_deny_record(audit_env, live_adapter):
    live_adapter(lambda base: _post(base, _send()))
    assert _records(audit_env)[-1]["reason"] == "unauthenticated"


def test_403_untrusted_peer_writes_deny_record(audit_env, monkeypatch, live_adapter):
    monkeypatch.setenv("A2A_TRUSTED_PEERS", "someone-else")  # alice-9f2a not on it
    status_holder = {}

    def scenario(base):
        status_holder["status"], _ = _post(base, _send(), token=PEER_TOKEN)

    live_adapter(scenario)
    assert status_holder["status"] == 403
    rec = _records(audit_env)[-1]
    assert rec["direction"] == "policy" and rec["decision"] == "deny"
    assert rec["reason"] == "untrusted_peer"
    assert rec["peer"] == PEER_NAME
    assert rec["summary"] == ""


def test_peer_hit_writes_allow_record_with_identity(audit_env, live_adapter):
    live_adapter(lambda base: _post(base, _send(), token=PEER_TOKEN))
    rec = _records(audit_env)[-1]
    assert rec["direction"] == "inbound"
    assert rec["decision"] == "allow"
    assert rec["identity"] == PEER_NAME  # the human-readable alias, not a digest
    assert rec["peer"] == PEER_NAME


def test_master_hit_records_owner_identity(audit_env, live_adapter):
    live_adapter(lambda base: _post(base, _send(), token=MASTER))
    rec = _records(audit_env)[-1]
    assert rec["decision"] == "allow"
    assert rec["identity"] == "owner"


def test_audit_extra_is_optional(audit_env, monkeypatch):
    """Base shape unchanged when ``extra`` is omitted (backward compatible)."""
    security.audit("outbound", "peer-y", "task-9", "hello")
    rec = _records(audit_env)[-1]
    assert rec == {"ts": rec["ts"], "direction": "outbound", "peer": "peer-y",
                   "task_id": "task-9", "summary": "hello"}
