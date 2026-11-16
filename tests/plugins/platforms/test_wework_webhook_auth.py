"""fork 2026-11-16 (DPMS 561213): optional WeWork webhook authentication.

The WeWork inbound webhook (default ``0.0.0.0:13776``) was unauthenticated:
anyone able to reach the port could forge an inbound message. Auth is now
opt-in via ``WEWORK_WEBHOOK_TOKEN`` / ``platforms.wework.extra.webhookToken``:

- token unset/empty (default) -> behaviour identical to before, no auth work;
- token set -> POST must carry ``X-Webhook-Token: <t>`` or
  ``Authorization: Bearer <t>``, otherwise 401 BEFORE any body parsing;
- ``GET /health`` is exempt (liveness probe, no sensitive data).

These tests drive the real aiohttp routes through ``TestServer`` so the
401-before-JSON-parse ordering is exercised on the wire.
"""

import pytest

aiohttp = pytest.importorskip("aiohttp")
from aiohttp.test_utils import TestClient, TestServer  # noqa: E402

from gateway.config import PlatformConfig  # noqa: E402
from plugins.platforms.wework.adapter import WeWorkAdapter  # noqa: E402

TOKEN = "s3cr3t-token"
PAYLOAD = {
    "cmd": "hi",
    "user": "brycehuang",
    "userFullName": "Bryce Huang",
    "wechatId": "S:1688857642682584_8444250708322274;S:1688851343262740_1688857642682584",
    "fromGroup": "",
    "sendTime": "t1",
    "storeKey": "s1",
}


def _make_adapter(token: str | None) -> WeWorkAdapter:
    extra = {"keyid": "k"}
    if token is not None:
        extra["webhookToken"] = token
    adapter = WeWorkAdapter(PlatformConfig(enabled=True, extra=extra))
    app = aiohttp.web.Application()
    app.router.add_get("/health", adapter._handle_health)
    app.router.add_post("/", adapter._handle_webhook)
    adapter._app = app
    return adapter


def _client(adapter: WeWorkAdapter) -> TestClient:
    return TestClient(TestServer(adapter._app))


@pytest.mark.asyncio
async def test_bearer_token_accepted_and_queued():
    adapter = _make_adapter(TOKEN)
    client = _client(adapter)
    await client.start_server()
    try:
        resp = await client.post("/", json=PAYLOAD, headers={"Authorization": f"Bearer {TOKEN}"})
        assert resp.status == 200
        assert await resp.json() == {"ok": True}
        assert adapter._queue.qsize() == 1, "authorized message must reach the inbound queue"
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_x_webhook_token_header_accepted():
    adapter = _make_adapter(TOKEN)
    client = _client(adapter)
    await client.start_server()
    try:
        resp = await client.post("/", json=PAYLOAD, headers={"X-Webhook-Token": TOKEN})
        assert resp.status == 200
        assert await resp.json() == {"ok": True}
        assert adapter._queue.qsize() == 1
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_wrong_token_rejected():
    adapter = _make_adapter(TOKEN)
    client = _client(adapter)
    await client.start_server()
    try:
        resp = await client.post("/", json=PAYLOAD, headers={"X-Webhook-Token": "wrong"})
        assert resp.status == 401
        assert await resp.json() == {"ok": False, "error": "unauthorized"}
        resp = await client.post("/", json=PAYLOAD, headers={"Authorization": "Bearer wrong"})
        assert resp.status == 401
        assert adapter._queue.empty(), "rejected message must never be queued"
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_missing_token_rejected_before_json_parse():
    adapter = _make_adapter(TOKEN)
    client = _client(adapter)
    await client.start_server()
    try:
        resp = await client.post("/", json=PAYLOAD)
        assert resp.status == 401
        assert await resp.json() == {"ok": False, "error": "unauthorized"}
        # 401 (not 400): auth runs BEFORE the body is parsed, so even invalid
        # JSON from an unauthenticated caller is never parsed.
        resp = await client.post("/", data=b"{not json", headers={"Content-Type": "application/json"})
        assert resp.status == 401
        assert adapter._queue.empty()
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_no_token_configured_keeps_webhook_open():
    """Default-off regression: without a token the webhook behaves exactly as
    before — no header required."""
    adapter = _make_adapter(None)
    assert adapter._webhook_token == ""
    client = _client(adapter)
    await client.start_server()
    try:
        resp = await client.post("/", json=PAYLOAD)
        assert resp.status == 200
        assert await resp.json() == {"ok": True}
        assert adapter._queue.qsize() == 1
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_health_exempt_from_auth():
    adapter = _make_adapter(TOKEN)
    client = _client(adapter)
    await client.start_server()
    try:
        resp = await client.get("/health")
        assert resp.status == 200
        assert await resp.json() == {"status": "ok", "platform": "wework"}
    finally:
        await client.close()
