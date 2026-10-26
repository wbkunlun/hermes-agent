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
        yield SimpleNamespace(port=server.server_address[1], secret="s3cret", dispatched=dispatched)
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

    adapter._stop_relay_server()
    assert adapter._relay_server is None


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
