"""The WeCom send relay: an out-of-process sender reaches the gateway's single live
WS subscription instead of opening a competing one (fork 2026-10-26, wehermes).

WeCom allows ONE live subscription per bot — a second connection displaces the first
(the 2026-10 cron-report incident). The bot-identity lock refuses the competing
connection, so the gateway publishes a secret-gated local endpoint that delivers
through the live adapter; the standalone lane must try that BEFORE the ephemeral
connect, and only fall through when no relay answered.
"""

import asyncio
import json
import threading
from types import SimpleNamespace

import pytest

from gateway.config import PlatformConfig
from plugins.platforms.wecom import relay_client, relay_server


@pytest.fixture
def gateway_loop():
    """A running event loop in a background thread — the relay dispatches onto it."""
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_forever, name="relay-test-loop", daemon=True)
    thread.start()
    try:
        yield loop
    finally:
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=5)
        loop.close()


@pytest.fixture
def relay(gateway_loop, tmp_path, monkeypatch):
    """A live relay on an ephemeral port with a published rendezvous file."""
    monkeypatch.setattr(relay_server, "rendezvous_path", lambda: tmp_path / "wecom-relay.json")
    monkeypatch.setattr(relay_client, "rendezvous_path", lambda: tmp_path / "wecom-relay.json")
    dispatched = []

    async def _dispatch(chat_id: str, message: str) -> dict:
        dispatched.append((chat_id, message))
        return {"success": True, "platform": "wecom", "chat_id": chat_id, "message_id": "mid-1"}

    server = relay_server.start_server(
        dispatch=_dispatch, loop=gateway_loop, port=0,
        secret="s3cret", logger_=relay_server.logger,
    )
    assert server is not None
    try:
        yield SimpleNamespace(port=server.server_address[1], secret="s3cret",
                              dispatched=dispatched, server=server)
    finally:
        server.shutdown()
        server.server_close()


def _post(port, secret, body):
    from urllib.request import Request, urlopen

    request = Request(
        f"http://127.0.0.1:{port}/notify",
        data=json.dumps(body).encode("utf-8"),
        method="POST",
        headers={"Content-Type": "application/json", "X-Relay-Secret": secret},
    )
    return urlopen(request, timeout=5)


def test_relay_rejects_wrong_secret(relay):
    from urllib.error import HTTPError

    with pytest.raises(HTTPError) as excinfo:
        _post(relay.port, "wrong-secret", {"chat_id": "g1", "message": "hi"})
    assert excinfo.value.code == 401
    assert relay.dispatched == []


def test_relay_delivers_through_dispatch(relay):
    with _post(relay.port, relay.secret, {"chat_id": "g1", "message": "report"}) as response:
        payload = json.loads(response.read())
    assert payload["success"] is True
    assert payload["message_id"] == "mid-1"
    assert relay.dispatched == [("g1", "report")]


def test_relay_rejects_get(relay):
    from urllib.error import HTTPError
    from urllib.request import Request, urlopen

    request = Request(f"http://127.0.0.1:{relay.port}/notify", method="GET")
    with pytest.raises(HTTPError) as excinfo:
        urlopen(request, timeout=5)
    assert excinfo.value.code == 405


def test_relay_publishes_rendezvous_file(relay, tmp_path):
    doc = json.loads((tmp_path / "wecom-relay.json").read_text())
    assert doc["port"] == relay.port
    assert doc["secret"] == relay.secret
    assert doc["pid"]


def test_relay_binds_loopback_only(relay):
    """The endpoint SENDS messages — it must never bind a routable interface."""
    assert relay.server.server_address[0] == "127.0.0.1"


@pytest.mark.platforms("posix")
def test_rendezvous_file_is_owner_only(relay, tmp_path):
    """fork 2026-10-26 (release fix): the file holds the live bearer secret — any
    world-readable copy lets another local user drive the bot."""
    import os as os_module

    mode = os_module.stat(tmp_path / "wecom-relay.json").st_mode & 0o777
    assert mode == 0o600, f"rendezvous file mode {oct(mode)}, expected 0o600"


def test_client_posts_to_the_published_relay(relay):
    result = relay_client.try_relay_send("g1", "report")
    assert result == {"success": True, "platform": "wecom", "chat_id": "g1", "message_id": "mid-1"}
    assert relay.dispatched == [("g1", "report")]


def test_client_returns_none_without_rendezvous(monkeypatch, tmp_path):
    monkeypatch.setattr(relay_client, "rendezvous_path", lambda: tmp_path / "missing.json")
    assert relay_client.try_relay_send("g1", "report") is None


def test_client_returns_none_when_relay_unreachable():
    # Port 1 is not served — the "no relay" answer, not an error dict.
    assert relay_client.try_relay_send("g1", "report", port=1, secret="x") is None


def test_client_ignores_a_dead_publisher(monkeypatch, tmp_path):
    """fork 2026-10-26 (release fix): a crashed gateway leaves the rendezvous behind —
    posting at the dead port reads as 'no relay', so ignore the file outright."""
    import subprocess
    import sys as sys_module

    child = subprocess.Popen([sys_module.executable, "-c", "pass"])
    child.wait()  # born dead
    (tmp_path / "wecom-relay.json").write_text(
        json.dumps({"port": 7861, "secret": "x", "pid": child.pid}))
    monkeypatch.setattr(relay_client, "rendezvous_path", lambda: tmp_path / "wecom-relay.json")

    assert relay_client.try_relay_send("g1", "report") is None


def test_a_system_proxy_never_blocks_the_loopback_relay(relay, monkeypatch):
    """fork 2026-10-26 (release fix): urlopen honours HTTP_PROXY — on a proxied host the
    loopback POST was dialed through the proxy and the relay went silently missing."""
    monkeypatch.setenv("HTTP_PROXY", "http://192.0.2.1:9")
    monkeypatch.setenv("HTTPS_PROXY", "http://192.0.2.1:9")
    monkeypatch.setenv("ALL_PROXY", "http://192.0.2.1:9")
    monkeypatch.delenv("NO_PROXY", raising=False)
    monkeypatch.delenv("no_proxy", raising=False)

    result = relay_client.try_relay_send("g1", "report")

    assert result is not None and result.get("success") is True


def test_relay_answers_502_with_the_dispatch_error(gateway_loop, tmp_path, monkeypatch):
    """A failed gateway send is a FINAL answer (the client must not fall through to a
    competing connection) — the 502 path turns the dispatch error into the body."""
    from urllib.error import HTTPError

    monkeypatch.setattr(relay_server, "rendezvous_path", lambda: tmp_path / "wecom-relay.json")
    monkeypatch.setattr(relay_client, "rendezvous_path", lambda: tmp_path / "wecom-relay.json")

    async def _failing_dispatch(chat_id: str, message: str) -> dict:
        return {"error": "group policy refused"}

    server = relay_server.start_server(
        dispatch=_failing_dispatch, loop=gateway_loop, port=0,
        secret="s3cret", logger_=relay_server.logger,
    )
    assert server is not None
    try:
        with pytest.raises(HTTPError) as excinfo:
            _post(server.server_address[1], "s3cret", {"chat_id": "g1", "message": "report"})
        assert excinfo.value.code == 502
        assert json.loads(excinfo.value.read()) == {"error": "group policy refused"}
    finally:
        server.shutdown()
        server.server_close()


def test_non_ascii_secret_header_gets_a_401_not_a_crash(relay):
    """fork 2026-10-26 (release fix): str compare_digest raises TypeError on non-ASCII —
    the handler crashed into a traceback instead of answering 401."""
    from urllib.error import HTTPError

    with pytest.raises(HTTPError) as excinfo:
        _post(relay.port, "s3cr\xffet", {"chat_id": "g1", "message": "hi"})
    assert excinfo.value.code == 401
    assert relay.dispatched == []


def test_standalone_send_prefers_the_relay_over_an_ephemeral_connection(monkeypatch):
    """The regression this relay exists for: an out-of-process sender must NOT open a
    second WS for the same bot (it displaces the gateway)."""
    import plugins.platforms.wecom.adapter as wecom_module
    from plugins.platforms.wecom.adapter import _standalone_send

    def _boom(*args, **kwargs):
        raise AssertionError("an ephemeral WeComAdapter was constructed despite a relay answer")

    monkeypatch.setattr(wecom_module, "WeComAdapter", _boom)
    monkeypatch.setattr("tools.send_message_senders._live_adapter", lambda platform: (None, None))
    monkeypatch.setattr(
        relay_client, "try_relay_send",
        lambda chat_id, message, **kwargs: {
            "success": True, "platform": "wecom", "chat_id": chat_id, "message_id": "mid-2"},
    )

    result = asyncio.run(_standalone_send(PlatformConfig(enabled=True), "g1", "report"))

    assert result["success"] is True
    assert result["message_id"] == "mid-2"


def test_standalone_send_returns_the_relay_error_verbatim(monkeypatch):
    """A relay answer is final — a failure must not fall through to a competing send."""
    import plugins.platforms.wecom.adapter as wecom_module
    from plugins.platforms.wecom.adapter import _standalone_send

    def _boom(*args, **kwargs):
        raise AssertionError("an ephemeral WeComAdapter was constructed despite a relay answer")

    monkeypatch.setattr(wecom_module, "WeComAdapter", _boom)
    monkeypatch.setattr("tools.send_message_senders._live_adapter", lambda platform: (None, None))
    monkeypatch.setattr(
        relay_client, "try_relay_send",
        lambda chat_id, message, **kwargs: {"error": "group policy refused"},
    )

    result = asyncio.run(_standalone_send(PlatformConfig(enabled=True), "g1", "report"))

    assert result == {"error": "group policy refused"}


def test_standalone_send_falls_through_when_no_relay_answered(monkeypatch):
    """No relay (the gateway is down) → the ephemeral path still works, and now takes
    the bot lock instead of displacing a live gateway."""
    import plugins.platforms.wecom.adapter as wecom_module
    from plugins.platforms.wecom.adapter import _standalone_send

    sent = {}

    class _EphemeralAdapter:
        def __init__(self, pconfig):
            sent["constructed"] = True

        async def connect(self):
            return True

        async def disconnect(self):
            return None

        async def send(self, chat_id, message):
            sent["args"] = (chat_id, message)
            return SimpleNamespace(success=True, message_id="mid-3")

    monkeypatch.setattr(wecom_module, "WeComAdapter", _EphemeralAdapter)
    monkeypatch.setattr(wecom_module, "check_wecom_requirements", lambda: True)
    monkeypatch.setattr("tools.send_message_senders._live_adapter", lambda platform: (None, None))
    monkeypatch.setattr(relay_client, "try_relay_send", lambda chat_id, message, **kwargs: None)

    result = asyncio.run(_standalone_send(PlatformConfig(enabled=True), "g1", "report"))

    assert result["success"] is True
    assert sent["args"] == ("g1", "report")


@pytest.mark.asyncio
async def test_relay_server_lifecycle_follows_the_adapter(monkeypatch, tmp_path):
    """The relay lives exactly as long as the live adapter that can deliver through it."""
    from plugins.platforms.wecom.adapter import WeComAdapter

    monkeypatch.setenv("WECOM_RELAY_PORT", "0")  # ephemeral port — never 7861 in a test
    monkeypatch.setattr(relay_server, "rendezvous_path", lambda: tmp_path / "wecom-relay.json")
    monkeypatch.setattr(relay_client, "rendezvous_path", lambda: tmp_path / "wecom-relay.json")

    adapter = WeComAdapter(
        PlatformConfig(enabled=True, extra={"bot_id": "bot-1", "secret": "s3cret"})
    )
    adapter._start_relay_server()
    assert adapter._relay_server is not None
    port = adapter._relay_server.server_address[1]
    doc = json.loads((tmp_path / "wecom-relay.json").read_text())
    assert doc["port"] == port
    assert doc["bot_id"] == "bot-1"

    adapter._stop_relay_server()
    assert adapter._relay_server is None
    assert not (tmp_path / "wecom-relay.json").exists()  # unpublished with the server


@pytest.mark.asyncio
async def test_stop_never_unpublishes_another_gateways_rendezvous(monkeypatch, tmp_path):
    """fork 2026-10-26 (release fix): the rendezvous path is MACHINE-scoped — a second
    gateway (another bot) may own it by now. The adapter unlinks the file only while
    it still names THIS process (as release_scoped_lock does for the lock itself)."""
    from plugins.platforms.wecom.adapter import WeComAdapter

    monkeypatch.setenv("WECOM_RELAY_PORT", "0")
    monkeypatch.setattr(relay_server, "rendezvous_path", lambda: tmp_path / "wecom-relay.json")
    monkeypatch.setattr(relay_client, "rendezvous_path", lambda: tmp_path / "wecom-relay.json")

    adapter = WeComAdapter(
        PlatformConfig(enabled=True, extra={"bot_id": "bot-1", "secret": "s3cret"})
    )
    adapter._start_relay_server()
    assert adapter._relay_server is not None
    path = tmp_path / "wecom-relay.json"
    doc = json.loads(path.read_text())
    doc["pid"] = 4_000_000_000  # another gateway's publication
    path.write_text(json.dumps(doc))

    adapter._stop_relay_server()

    assert path.exists()  # not ours to delete


@pytest.mark.asyncio
async def test_a_relay_failure_never_takes_the_platform_down(monkeypatch, tmp_path):
    """fork 2026-10-26 (release fix): the relay is an OPTIONAL side channel — its failure
    must not escape _start_relay_server (a Windows os.fchmod AttributeError did exactly
    that and killed the whole WeCom platform at connect time)."""
    from plugins.platforms.wecom.adapter import WeComAdapter

    monkeypatch.setenv("WECOM_RELAY_PORT", "0")
    monkeypatch.setattr(relay_server, "rendezvous_path", lambda: tmp_path / "wecom-relay.json")

    def _boom(**kwargs):
        raise AttributeError("module 'os' has no attribute 'fchmod'")

    monkeypatch.setattr(relay_server, "start_server", _boom)
    adapter = WeComAdapter(
        PlatformConfig(enabled=True, extra={"bot_id": "bot-1", "secret": "s3cret"})
    )

    adapter._start_relay_server()  # must not raise

    assert adapter._relay_server is None


def test_the_standalone_sender_scopes_the_relay_to_its_own_bot(monkeypatch):
    """fork 2026-10-26 (release fix): the rendezvous file is machine-scoped — the
    sender scopes the lookup to its own bot so it never posts through another bot's
    subscription (the wrong bot would deliver, or the send would fail outright)."""
    import plugins.platforms.wecom.adapter as wecom_module
    from plugins.platforms.wecom.adapter import _standalone_send

    seen = {}

    def _fake_relay(chat_id, message, **kwargs):
        seen.update(kwargs)
        return {"success": True}

    monkeypatch.setattr(wecom_module, "WeComAdapter",
                        lambda *a, **k: (_ for _ in ()).throw(AssertionError("ephemeral adapter built")))
    monkeypatch.setattr("tools.send_message_senders._live_adapter", lambda platform: (None, None))
    monkeypatch.setattr("plugins.platforms.wecom.relay_client.try_relay_send", _fake_relay)

    result = asyncio.run(_standalone_send(
        PlatformConfig(enabled=True, extra={"bot_id": "bot-mine"}), "g1", "report"))

    assert result == {"success": True}
    assert seen["bot_id"] == "bot-mine"


def test_a_typo_port_falls_back_instead_of_killing_the_platform(monkeypatch, tmp_path):
    """fork 2026-10-26 (release fix): a non-numeric WECOM_RELAY_PORT raised ValueError
    out of connect() — the whole WeCom platform died over an optional relay knob."""
    from plugins.platforms.wecom.adapter import WeComAdapter

    monkeypatch.setenv("WECOM_RELAY_PORT", "seven")
    monkeypatch.setattr(relay_server, "DEFAULT_PORT", 0)  # ephemeral port — never 7861 in a test
    monkeypatch.setattr(relay_server, "rendezvous_path", lambda: tmp_path / "wecom-relay.json")
    monkeypatch.setattr(relay_client, "rendezvous_path", lambda: tmp_path / "wecom-relay.json")

    adapter = WeComAdapter(
        PlatformConfig(enabled=True, extra={"bot_id": "bot-1", "secret": "s3cret"})
    )
    adapter._start_relay_server()

    assert adapter._relay_server is not None
    adapter._stop_relay_server()


@pytest.mark.asyncio
async def test_the_adapter_dispatch_maps_a_failed_send_to_an_error_answer(monkeypatch, tmp_path):
    """fork 2026-10-26 (release fix): the production dispatch closure was never
    exercised — a regression that reported success for a failed send would book a
    delivery that never happened (silent message loss)."""
    from unittest.mock import AsyncMock

    from plugins.platforms.wecom.adapter import WeComAdapter

    monkeypatch.setenv("WECOM_RELAY_PORT", "0")
    monkeypatch.setattr(relay_server, "rendezvous_path", lambda: tmp_path / "wecom-relay.json")
    monkeypatch.setattr(relay_client, "rendezvous_path", lambda: tmp_path / "wecom-relay.json")

    adapter = WeComAdapter(
        PlatformConfig(enabled=True, extra={"bot_id": "bot-1", "secret": "s3cret"})
    )
    adapter.send = AsyncMock(return_value=SimpleNamespace(success=False, error="send queue is down"))
    adapter._start_relay_server()
    assert adapter._relay_server is not None
    try:
        # the client call is synchronous — run it off the loop so the dispatch can run
        result = await asyncio.to_thread(relay_client.try_relay_send, "g1", "report")
        assert result == {"error": "send queue is down"}
    finally:
        adapter._stop_relay_server()


def test_publishing_survives_a_platform_without_fchmod(monkeypatch, tmp_path):
    """fork 2026-10-26 (release fix): os.fchmod does not exist on Windows — the publish
    must still work there (the lock dir is already owner-private)."""
    monkeypatch.setattr(relay_server, "rendezvous_path", lambda: tmp_path / "wecom-relay.json")
    monkeypatch.delattr(relay_server.os, "fchmod")

    path = relay_server.publish_rendezvous(7861, "s3cret", relay_server.logger, bot_id="bot-1")

    assert path is not None
    assert json.loads(path.read_text())["secret"] == "s3cret"


@pytest.mark.asyncio
async def test_relay_server_can_be_disabled(monkeypatch, tmp_path):
    from plugins.platforms.wecom.adapter import WeComAdapter

    monkeypatch.setenv("WECOM_RELAY", "0")
    monkeypatch.setattr(relay_server, "rendezvous_path", lambda: tmp_path / "wecom-relay.json")

    adapter = WeComAdapter(
        PlatformConfig(enabled=True, extra={"bot_id": "bot-1", "secret": "s3cret"})
    )
    adapter._start_relay_server()
    assert adapter._relay_server is None
