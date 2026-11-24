"""A2A outbound admission hook (platform caller-identity, DPMS 563037).

With ``A2A_OUTBOUND_HOOK_URL`` set, every ``a2a_call``/``a2a_orchestrate`` send
must first ask the platform who is calling and which token to use:

- identity ladder: verified agent-auth UM (covers the IM sender) > session
  chat sender (chained A2A aliases pass through as-is); no identity denies
  before any request;
- denial / unreachable hook ⇒ the peer is never contacted, no retries;
- legacy (env unset) ⇒ the entry's static ``auth.token`` is kept unchanged.

Identity never comes from model arguments; all transport is mocked here.
"""
import json
import urllib.error
import urllib.request

import pytest

import plugins.platforms.a2a.tools as a2at
from plugins.platforms.a2a import protocol

HOOK_URL = "http://control-plane.example.com/api/v1/agent/a2a/outbound"
PEER_URL = "http://peer.example.com"


def _set_hook(monkeypatch, enabled=True):
    if enabled:
        monkeypatch.setenv("A2A_OUTBOUND_HOOK_URL", HOOK_URL)
        monkeypatch.setenv("CONTROL_PLANE_AUTH", "cp-auth-token")
    else:
        monkeypatch.delenv("A2A_OUTBOUND_HOOK_URL", raising=False)


def _peer_config(auth_token=None):
    entry = {"url": PEER_URL}
    if auth_token:
        entry["auth"] = {"type": "bearer", "token": auth_token}
    return {"a2a_agents": {"p": entry}}


def _reply(url, body, headers, timeout):
    return protocol.jsonrpc_result(
        body["id"], protocol.build_task("t1", "ctx-1", protocol.STATE_COMPLETED, "peer reply"))


def _fake_hook_urlopen(monkeypatch, hook_answer=None, hook_error=None):
    """Capture outbound hook requests; the peer transport (http_post/fetch_card)
    is patched separately, so any urlopen outside the hook fails the test."""
    calls = []

    def fake_urlopen(req, timeout=None):
        calls.append(req)
        if req.full_url != HOOK_URL:
            raise AssertionError(f"unexpected request to {req.full_url}")
        if hook_error is not None:
            raise hook_error
        return _FakeResp(hook_answer)

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    return calls


class _FakeResp:
    def __init__(self, payload):
        self._raw = json.dumps(payload).encode()

    def read(self):
        return self._raw

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _hook_body(req):
    return json.loads(req.data.decode("utf-8"))


# ---------------------------------------------------------------- legacy ---


def test_env_unset_keeps_static_token(monkeypatch):
    _set_hook(monkeypatch, enabled=False)
    monkeypatch.setattr(a2at, "_load_config", lambda: _peer_config("static-tok"))
    monkeypatch.setattr(a2at, "_fetch_card", lambda url, h, t: None)
    captured = {}
    monkeypatch.setattr(a2at, "_http_post_json", lambda url, body, h, t: captured.update(url=url, headers=h) or _reply(url, body, h, t))
    out = a2at.a2a_call({"agent": "p", "message": "hi"})
    assert "peer reply" in out
    assert captured["headers"]["Authorization"] == "Bearer static-tok"
    assert "X-A2A-Principal" not in captured["headers"]


def test_admission_legacy_short_circuits(monkeypatch):
    _set_hook(monkeypatch, enabled=False)
    calls = _fake_hook_urlopen(monkeypatch)
    admission, denial = a2at._outbound_admission("p", PEER_URL, "a2a_call")
    assert admission == {} and denial is None
    assert not calls


# ------------------------------------------------------------- no identity ---


def test_no_identity_denies_before_any_request(monkeypatch, tmp_path):
    _set_hook(monkeypatch)
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setattr(a2at, "_verified_um_identity", lambda: ("", ""))
    monkeypatch.setattr(a2at, "_session_identity", lambda: ("", ""))
    monkeypatch.setattr(a2at, "_load_config", lambda: _peer_config("static-tok"))
    calls = _fake_hook_urlopen(monkeypatch)
    out = a2at.a2a_call({"agent": "p", "message": "hi"})
    assert "no verified user identity" in out
    assert not calls  # fail fast: not even the hook is asked
    # Local policy record (audit) with no request body.
    rec = json.loads((tmp_path / "a2a_audit.jsonl").read_text().strip().splitlines()[-1])
    assert rec["direction"] == "policy" and rec["decision"] == "deny"
    assert rec["reason"] == "outbound_no_session_user"
    assert rec["summary"] == ""


# ------------------------------------------------------------ IM sender path ---


def test_im_sender_uses_hook_token_and_identity_headers(monkeypatch):
    _set_hook(monkeypatch)
    monkeypatch.setattr(a2at, "_verified_um_identity", lambda: ("", ""))
    monkeypatch.setattr(a2at, "_session_identity", lambda: ("wzhang", ""))
    monkeypatch.setattr(a2at, "_load_config", lambda: _peer_config("static-tok"))
    monkeypatch.setattr(a2at, "_fetch_card", lambda url, h, t: None)
    calls = _fake_hook_urlopen(monkeypatch, hook_answer={
        "allow": True, "token": "hook-tok", "url": PEER_URL,
        "principal": "wzhang-9f2a", "via": "hm-a"})
    captured = {}
    monkeypatch.setattr(a2at, "_http_post_json", lambda url, body, h, t: captured.update(url=url, headers=h) or _reply(url, body, h, t))
    out = a2at.a2a_call({"agent": "p", "message": "hi"})
    assert "peer reply" in out
    body = _hook_body(calls[0])
    assert body["peer"] == "p" and body["user_id"] == "wzhang" and body["tool"] == "a2a_call"
    assert "um_user" not in body
    assert calls[0].headers.get("Authorization") == "Bearer cp-auth-token"
    # Peer request carries the hook-issued token and identity headers; static token gone.
    assert captured["headers"]["Authorization"] == "Bearer hook-tok"
    assert captured["headers"]["X-A2A-Principal"] == "wzhang-9f2a"
    assert captured["headers"]["X-A2A-Via"] == "hm-a"
    assert captured["url"] == PEER_URL


# ---------------------------------------------------------------- UM path ---


def test_um_identity_overrides_im_sender(monkeypatch):
    _set_hook(monkeypatch)
    monkeypatch.setattr(a2at, "_verified_um_identity", lambda: ("zhangsan", "张三"))
    monkeypatch.setattr(a2at, "_session_identity", lambda: ("wzhang", ""))
    monkeypatch.setattr(a2at, "_load_config", lambda: _peer_config())
    monkeypatch.setattr(a2at, "_fetch_card", lambda url, h, t: None)
    calls = _fake_hook_urlopen(monkeypatch, hook_answer={
        "allow": True, "token": "t", "url": PEER_URL, "principal": "zhangsan-3c1e", "via": "hm-a"})
    monkeypatch.setattr(a2at, "_http_post_json", lambda url, body, h, t: _reply(url, body, h, t))
    a2at.a2a_call({"agent": "p", "message": "hi"})
    body = _hook_body(calls[0])
    assert body["um_user"] == "zhangsan" and body["um_name"] == "张三"
    assert "user_id" not in body  # UM covers the IM sender exclusively


def test_verified_um_identity_reads_plugin_manager(monkeypatch):
    """The fork reads the agent-auth plugin instance through the plugin manager,
    with getattr guards: instance method present → identity; missing/absent →
    ("", "") and never an exception."""
    class _FakeInstance:
        verified_identity_ok = True

        def verified_identity(self, allow_group=False):
            assert allow_group is False
            return {"principal": "wecom:zhangsan", "user": "zhangsan", "name": "张三"}

    class _FakeOldInstance:  # deployed before the platform added the method
        pass

    class _FakeLoaded:
        def __init__(self, module, enabled=True, error=None):
            self.module, self.enabled, self.error = module, enabled, error

    class _FakeModule:
        pass

    class _FakeManager:
        def __init__(self, plugins):
            self._plugins = plugins

    mod_new, mod_old = _FakeModule(), _FakeModule()
    mod_new._instance = _FakeInstance()
    mod_old._instance = _FakeOldInstance()
    monkeypatch.setattr("hermes_cli.plugins.get_plugin_manager",
                        lambda: _FakeManager({"agent-auth": _FakeLoaded(mod_new)}))
    assert a2at._verified_um_identity() == ("zhangsan", "张三")
    # Old deployment without the method → silently falls back.
    monkeypatch.setattr("hermes_cli.plugins.get_plugin_manager",
                        lambda: _FakeManager({"agent-auth": _FakeLoaded(mod_old)}))
    assert a2at._verified_um_identity() == ("", "")
    # Plugin missing entirely.
    monkeypatch.setattr("hermes_cli.plugins.get_plugin_manager",
                        lambda: _FakeManager({}))
    assert a2at._verified_um_identity() == ("", "")
    # Disabled / errored plugin entries are skipped.
    monkeypatch.setattr("hermes_cli.plugins.get_plugin_manager", lambda: _FakeManager({
        "a": _FakeLoaded(mod_new, enabled=False), "b": _FakeLoaded(mod_new, error="boom")}))
    assert a2at._verified_um_identity() == ("", "")
    # Manager unavailable at all.
    monkeypatch.setattr("hermes_cli.plugins.get_plugin_manager",
                        lambda: (_ for _ in ()).throw(RuntimeError("no manager")))
    assert a2at._verified_um_identity() == ("", "")


# ---------------------------------------------------------------- denials ---


def test_hook_deny_blocks_peer_and_reports_reason(monkeypatch, tmp_path):
    _set_hook(monkeypatch)
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setattr(a2at, "_verified_um_identity", lambda: ("", ""))
    monkeypatch.setattr(a2at, "_session_identity", lambda: ("wzhang", ""))
    monkeypatch.setattr(a2at, "_load_config", lambda: _peer_config("static-tok"))
    calls = _fake_hook_urlopen(monkeypatch, hook_answer={"allow": False, "reason": "user_not_allowed_on_peer"})
    peer_posts = []
    monkeypatch.setattr(a2at, "_http_post_json", lambda *a, **k: peer_posts.append(a) or (_ for _ in ()).throw(AssertionError("peer contacted after denial")))
    monkeypatch.setattr(a2at, "_fetch_card", lambda *a, **k: (_ for _ in ()).throw(AssertionError("peer card fetched after denial")))
    out = a2at.a2a_call({"agent": "p", "message": "hi"})
    assert "denied" in out and "caller list" in out
    assert len(calls) == 1 and not peer_posts  # one hook ask, zero peer contact, no retry
    rec = json.loads((tmp_path / "a2a_audit.jsonl").read_text().strip().splitlines()[-1])
    assert rec["reason"] == "outbound_user_not_allowed_on_peer" and rec["identity"] == "wzhang"


def test_hook_unreachable_fails_closed(monkeypatch):
    _set_hook(monkeypatch)
    monkeypatch.setattr(a2at, "_verified_um_identity", lambda: ("", ""))
    monkeypatch.setattr(a2at, "_session_identity", lambda: ("wzhang", ""))
    monkeypatch.setattr(a2at, "_load_config", lambda: _peer_config())
    calls = _fake_hook_urlopen(monkeypatch, hook_error=urllib.error.URLError("conn refused"))
    monkeypatch.setattr(a2at, "_fetch_card", lambda *a, **k: (_ for _ in ()).throw(AssertionError("peer reached on hook failure")))
    out = a2at.a2a_call({"agent": "p", "message": "hi"})
    assert "unreachable" in out and "fail-closed" in out
    assert len(calls) == 1


def test_hook_timeout_fails_closed(monkeypatch):
    _set_hook(monkeypatch)
    monkeypatch.setattr(a2at, "_verified_um_identity", lambda: ("", ""))
    monkeypatch.setattr(a2at, "_session_identity", lambda: ("wzhang", ""))
    monkeypatch.setattr(a2at, "_load_config", lambda: _peer_config())
    _fake_hook_urlopen(monkeypatch, hook_error=TimeoutError("timed out"))
    out = a2at.a2a_call({"agent": "p", "message": "hi"})
    assert "denied" in out


def test_peer_not_listed_reason_wording(monkeypatch):
    _set_hook(monkeypatch)
    monkeypatch.setattr(a2at, "_verified_um_identity", lambda: ("", ""))
    monkeypatch.setattr(a2at, "_session_identity", lambda: ("wzhang", ""))
    monkeypatch.setattr(a2at, "_load_config", lambda: _peer_config())
    _fake_hook_urlopen(monkeypatch, hook_answer={"allow": False, "reason": "peer_not_listed"})
    out = a2at.a2a_call({"agent": "p", "message": "hi"})
    assert "registered peer" in out


# --------------------------------------------------------------- direct URL ---


def test_direct_url_passes_url_field(monkeypatch):
    _set_hook(monkeypatch)
    monkeypatch.setattr(a2at, "_verified_um_identity", lambda: ("", ""))
    monkeypatch.setattr(a2at, "_session_identity", lambda: ("wzhang", ""))
    calls = _fake_hook_urlopen(monkeypatch, hook_answer={"allow": False, "reason": "peer_not_listed"})
    out = a2at.a2a_call({"agent": "http://direct.example.com", "message": "hi"})
    assert "denied" in out
    body = _hook_body(calls[0])
    assert body["url"] == "http://direct.example.com" and "peer" not in body
    assert len(calls) == 1  # denial is final: no retry with another address


# ------------------------------------------------------------ orchestrate ---


def test_orchestrate_routes_through_hook(monkeypatch):
    _set_hook(monkeypatch)
    monkeypatch.setattr(a2at, "_verified_um_identity", lambda: ("zhangsan", ""))
    monkeypatch.setattr(a2at, "_load_config", lambda: _peer_config())
    calls = _fake_hook_urlopen(monkeypatch, hook_answer={
        "allow": True, "token": "hook-tok", "url": PEER_URL, "principal": "zhangsan-3c1e", "via": "hm-a"})
    monkeypatch.setattr(a2at, "_fetch_card", lambda url, h, t: None)
    seen_headers = []

    def fake_post(url, body, headers, timeout):
        seen_headers.append(headers)
        return _reply(url, body, headers, timeout)

    monkeypatch.setattr(a2at, "_http_post_json", fake_post)
    out = a2at.a2a_orchestrate({"capability": "*", "message": "go"})
    assert "peer reply" in out
    assert all(body["tool"] == "a2a_orchestrate" for body in (_hook_body(c) for c in calls))
    assert all(h["Authorization"] == "Bearer hook-tok" for h in seen_headers)


# ------------------------------------------------- session identity source ---


def test_session_identity_reads_session_context(monkeypatch):
    from gateway import session_context as sc
    monkeypatch.setenv("HERMES_SESSION_USER_ID", "env-user")
    assert a2at._session_identity()[0] == "env-user"
    token = sc.set_session_vars(user_id="ctx-user", user_name="李四")
    try:
        assert a2at._session_identity() == ("ctx-user", "李四")
    finally:
        sc.clear_session_vars(token)
