"""Tests for the control-plane dynamic whitelist client (fork).

Platform semantics: enabled → platform lists REPLACE env lists; fetch
failure falls back to cached lists; no data at all = deny everything
(fail-closed); empty list = that class unrestricted.
"""

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from tools import control_plane_whitelist as cpwl_mod
from tools.control_plane_whitelist import WhitelistSnapshot


@pytest.fixture
def cp_enabled(monkeypatch, tmp_path):
    """Enable the control plane with a tmp cache path; yields the client."""
    monkeypatch.setenv("CONTROL_PLANE_URL", "https://control.example.com")
    monkeypatch.setenv("CONTROL_PLANE_AUTH", "Bearer test-jwt")
    cpwl_mod._reset_for_tests()
    client = cpwl_mod.get_platform_whitelist()
    assert client is not None
    client._cache_path = tmp_path / "whitelist-cache.json"
    client._snapshot = None  # construction may have read the real /opt/data cache
    yield client
    cpwl_mod._reset_for_tests()


def _install(client, *, commands=(), users=()):
    """Install a snapshot directly (tests must not need HTTP)."""
    client._snapshot = WhitelistSnapshot(
        commands=tuple(commands),
        users=tuple(users),
        updated_at="2026-08-27T08:00:00Z",
        fetched_at=1_800_000_000.0,
    )


class TestSingleton:
    def test_disabled_when_either_env_missing(self, monkeypatch):
        """Either env unset → get_platform_whitelist() is None (env paths win)."""
        cpwl_mod._reset_for_tests()
        monkeypatch.delenv("CONTROL_PLANE_URL", raising=False)
        monkeypatch.delenv("CONTROL_PLANE_AUTH", raising=False)
        assert cpwl_mod.get_platform_whitelist() is None

    def test_enabled_builds_client_once(self, cp_enabled):
        """Both envs set → singleton client; second call returns the same one."""
        assert cpwl_mod.get_platform_whitelist() is cp_enabled
        assert cp_enabled._url == "https://control.example.com/api/v1/agent/whitelist"

    def test_url_trailing_slash_stripped(self, monkeypatch):
        """Trailing slash on the base URL must not double up the path."""
        cpwl_mod._reset_for_tests()
        monkeypatch.setenv("CONTROL_PLANE_URL", "https://control.example.com/")
        monkeypatch.setenv("CONTROL_PLANE_AUTH", "Bearer test-jwt")
        client = cpwl_mod.get_platform_whitelist()
        assert client._url == "https://control.example.com/api/v1/agent/whitelist"
        cpwl_mod._reset_for_tests()


class TestUserAllowed:
    def test_no_snapshot_denies(self, cp_enabled):
        """Fail-closed: never-fetched + no disk cache → deny everyone."""
        assert cp_enabled.user_allowed("zhangsan", "Zhang San") is False

    def test_empty_users_allows_all(self, cp_enabled):
        """Empty platform users list = unrestricted."""
        _install(cp_enabled, users=[])
        assert cp_enabled.user_allowed("anyone", "Anyone") is True

    def test_exact_match_on_id_or_name(self, cp_enabled):
        """Exact match on either sender_id or sender_name allows."""
        _install(cp_enabled, users=["zhangsan", "Zhang San"])
        assert cp_enabled.user_allowed("zhangsan", "") is True
        assert cp_enabled.user_allowed("", "Zhang San") is True

    def test_match_is_case_sensitive(self, cp_enabled):
        """Contract: exact username comparison, case-sensitive."""
        _install(cp_enabled, users=["zhangsan"])
        assert cp_enabled.user_allowed("ZhangSan", "") is False


class TestGroupAllowed:
    """Group decisions share the users list; match full chat id or name."""

    def test_no_snapshot_denies(self, cp_enabled):
        """Fail-closed: never-fetched + no disk cache → deny all groups."""
        assert cp_enabled.group_allowed(chat_id="S:1;R:2", chat_name="g") is False

    def test_empty_users_allows_all(self, cp_enabled):
        """Empty platform users list = all groups unrestricted."""
        _install(cp_enabled, users=[])
        assert cp_enabled.group_allowed(chat_id="S:1;R:2", chat_name="g") is True

    def test_match_on_group_name_or_full_chat_id(self, cp_enabled):
        """Same users list as DM; matches group name or the FULL wechatId
        (never a substring / R:-segment extraction)."""
        chat_id = "S:1688858099504500_8444250708322274;R:3284275877"
        _install(cp_enabled, users=["运维群", chat_id])
        assert cp_enabled.group_allowed(chat_id=chat_id, chat_name="") is True
        assert cp_enabled.group_allowed(chat_id="", chat_name="运维群") is True
        assert cp_enabled.group_allowed(chat_id="R:3284275877", chat_name="其他群") is False


class TestDiskCache:
    """Atomic JSON persist + boot-time load fallback (/opt/data)."""

    def test_persist_writes_atomic_json(self, cp_enabled, tmp_path):
        """A successful snapshot lands on disk with both lists."""
        _install(cp_enabled, commands=["ls*"], users=["zhangsan"])
        cp_enabled._persist()
        raw = json.loads(cp_enabled._cache_path.read_text(encoding="utf-8"))
        assert raw["commands"] == ["ls*"]
        assert raw["users"] == ["zhangsan"]
        assert raw["updated_at"] == "2026-08-27T08:00:00Z"
        assert raw["fetched_at"] == 1_800_000_000.0

    def test_persist_failure_is_non_fatal(self, cp_enabled, monkeypatch):
        """Unwritable cache dir must not raise (memory state still serves)."""
        _install(cp_enabled)

        def _raise(*args, **kwargs):
            raise OSError("read-only")

        monkeypatch.setattr(Path, "write_text", _raise)
        cp_enabled._persist()  # must not raise
        assert cp_enabled.snapshot is not None  # memory state still serves

    def test_startup_loads_disk_cache(self, monkeypatch, tmp_path):
        """Fresh process + platform unreachable at boot → disk cache decides."""
        cpwl_mod._reset_for_tests()
        cache = tmp_path / "whitelist-cache.json"
        cache.write_text(json.dumps({
            "commands": ["ls*"], "users": ["zhangsan"],
            "updated_at": None, "fetched_at": 1.0,
        }), encoding="utf-8")
        monkeypatch.setenv("CONTROL_PLANE_URL", "https://control.example.com")
        monkeypatch.setenv("CONTROL_PLANE_AUTH", "Bearer test-jwt")
        client = cpwl_mod.WhitelistClient(
            url="https://control.example.com/api/v1/agent/whitelist",
            auth="Bearer test-jwt", cache_path=cache,
        )
        assert client.user_allowed("zhangsan") is True
        assert client.user_allowed("lisi") is False
        assert client.command_gate("ls -l") == "bypass"
        cpwl_mod._reset_for_tests()

    def test_corrupt_cache_is_ignored_fail_closed(self, monkeypatch, tmp_path):
        """Corrupt/invalid cache files are silently ignored → no snapshot."""
        cpwl_mod._reset_for_tests()
        for bad in ("{not json", '{"commands": "x", "users": []}', '{"commands": []}', "[]",
                    b"\xff\xfe"):
            cache = tmp_path / "whitelist-cache.json"
            if isinstance(bad, bytes):
                cache.write_bytes(bad)  # non-UTF-8 bytes cannot go through write_text
            else:
                cache.write_text(bad, encoding="utf-8")
            client = cpwl_mod.WhitelistClient(
                url="https://x/api", auth="Bearer t", cache_path=cache,
            )
            assert client.snapshot is None, f"corrupt cache {bad!r} must not load"
        cpwl_mod._reset_for_tests()


class _FakeResponse:
    """Minimal response stand-in: .status_code plus a raising/working .json()."""

    def __init__(self, status_code, payload=None):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        if self._payload is None:
            raise ValueError("no json")
        return self._payload


class _FakeAsyncClient:
    """Queue-backed httpx.AsyncClient stand-in; class attrs are the queue."""

    responses = []
    requests = []

    def __init__(self, *args, **kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def get(self, url, headers=None, timeout=None):
        type(self).requests.append({"url": url, "headers": dict(headers or {})})
        item = type(self).responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def _patch_http(monkeypatch):
    """Point cpwl_mod.httpx at the fake client; reset both queues."""
    _FakeAsyncClient.responses = []
    _FakeAsyncClient.requests = []
    monkeypatch.setattr(
        cpwl_mod, "httpx",
        SimpleNamespace(AsyncClient=_FakeAsyncClient, HTTPError=httpx.HTTPError),
    )


def _payload(commands=(), users=(), success=True):
    """Build a platform response envelope (audit-API style wrapper)."""
    return {
        "success": success, "code": 200, "message": "Success",
        "data": {
            "agent_id": "oc-x",
            "commands": list(commands),
            "users": list(users),
            "updated_at": "2026-08-27T08:00:00Z",
        },
        "traceId": "t-1",
    }


class TestRefresh:
    """refresh(): retry ladder, error classification, envelope validation."""

    def test_success_updates_snapshot_and_persists(self, cp_enabled, monkeypatch):
        """200 + valid envelope → snapshot swapped and cache file written."""
        _patch_http(monkeypatch)
        _FakeAsyncClient.responses = [_FakeResponse(200, _payload(["ls*"], ["zhangsan"]))]
        assert asyncio.run(cp_enabled.refresh()) is True
        assert cp_enabled.snapshot.commands == ("ls*",)
        assert cp_enabled.snapshot.users == ("zhangsan",)
        raw = json.loads(cp_enabled._cache_path.read_text(encoding="utf-8"))
        assert raw["users"] == ["zhangsan"]

    def test_auth_header_passthrough_no_extra_bearer(self, cp_enabled, monkeypatch):
        """CONTROL_PLANE_AUTH already has the Bearer prefix — verbatim."""
        _patch_http(monkeypatch)
        _FakeAsyncClient.responses = [_FakeResponse(200, _payload())]
        asyncio.run(cp_enabled.refresh())
        req = _FakeAsyncClient.requests[0]
        assert req["url"].endswith("/api/v1/agent/whitelist")
        assert req["headers"] == {"Authorization": "Bearer test-jwt"}

    def test_401_keeps_cache(self, cp_enabled, monkeypatch):
        """Auth rejection must not retry and must not clear the snapshot."""
        _patch_http(monkeypatch)
        _install(cp_enabled, users=["zhangsan"])
        before = cp_enabled.snapshot
        _FakeAsyncClient.responses = [_FakeResponse(401, {"success": False})]
        assert asyncio.run(cp_enabled.refresh()) is False
        assert len(_FakeAsyncClient.requests) == 1  # auth errors do not retry
        assert cp_enabled.snapshot is before

    def test_5xx_retries_then_succeeds(self, cp_enabled, monkeypatch):
        """Transient server errors retry (3 attempts) and a late 200 wins."""
        _patch_http(monkeypatch)
        _FakeAsyncClient.responses = [
            _FakeResponse(503), _FakeResponse(500),
            _FakeResponse(200, _payload(["ls*"])),
        ]
        assert asyncio.run(cp_enabled.refresh()) is True
        assert len(_FakeAsyncClient.requests) == 3

    def test_network_error_retries_then_succeeds(self, cp_enabled, monkeypatch):
        """Network exceptions are transient: retry, then succeed."""
        _patch_http(monkeypatch)
        _FakeAsyncClient.responses = [
            httpx.ConnectError("boom"),
            _FakeResponse(200, _payload(users=["zhangsan"])),
        ]
        assert asyncio.run(cp_enabled.refresh()) is True
        assert cp_enabled.snapshot.users == ("zhangsan",)

    def test_exhausted_retries_keep_cache(self, cp_enabled, monkeypatch):
        """All 3 attempts failing keeps the previous snapshot intact."""
        _patch_http(monkeypatch)
        _install(cp_enabled, users=["zhangsan"])
        _FakeAsyncClient.responses = [_FakeResponse(503)] * 3
        assert asyncio.run(cp_enabled.refresh()) is False
        assert len(_FakeAsyncClient.requests) == 3
        assert cp_enabled.snapshot.users == ("zhangsan",)

    def test_envelope_success_false_rejected(self, cp_enabled, monkeypatch):
        """200 with success=false is an invalid envelope — keep cache."""
        _patch_http(monkeypatch)
        _FakeAsyncClient.responses = [_FakeResponse(200, _payload(success=False))]
        assert asyncio.run(cp_enabled.refresh()) is False
        assert cp_enabled.snapshot is None

    def test_non_list_field_rejected(self, cp_enabled, monkeypatch):
        """commands/users must be lists; anything else is invalid."""
        _patch_http(monkeypatch)
        bad = _payload()
        bad["data"]["commands"] = "ls*"
        _FakeAsyncClient.responses = [_FakeResponse(200, bad)]
        assert asyncio.run(cp_enabled.refresh()) is False
        assert cp_enabled.snapshot is None

    def test_non_json_body_rejected(self, cp_enabled, monkeypatch):
        """A non-JSON 200 body is invalid — keep cache."""
        _patch_http(monkeypatch)
        _FakeAsyncClient.responses = [_FakeResponse(200, None)]
        assert asyncio.run(cp_enabled.refresh()) is False


class TestCommandGate:
    def test_no_snapshot_is_deny(self, cp_enabled):
        """Fail-closed: no data = every command denied."""
        assert cp_enabled.command_gate("ls") == "deny"

    def test_empty_commands_is_normal(self, cp_enabled):
        """Empty platform commands = unrestricted (normal pipeline)."""
        _install(cp_enabled, commands=[])
        assert cp_enabled.command_gate("rm -rf /tmp/x") == "normal"

    def test_glob_matches_with_args(self, cp_enabled):
        """Wildcard entries match the whole segment, args included."""
        _install(cp_enabled, commands=["git log*", "kubectl get*"])
        assert cp_enabled.command_gate("git log --oneline") == "bypass"
        assert cp_enabled.command_gate("kubectl get pods") == "bypass"

    def test_bare_name_matches_only_itself(self, cp_enabled):
        """Contract: plain `ls` matches ONLY `ls` — use `ls*` for args.
        (Opposite of the env allowlist's bare-name-with-any-args rule.)"""
        _install(cp_enabled, commands=["ls"])
        assert cp_enabled.command_gate("ls") == "bypass"
        assert cp_enabled.command_gate("ls -l") == "deny"

    def test_case_sensitive(self, cp_enabled):
        """fnmatchcase: platform matching is case-sensitive (unlike env)."""
        _install(cp_enabled, commands=["ls"])
        assert cp_enabled.command_gate("LS") == "deny"

    def test_chain_requires_every_segment(self, cp_enabled):
        """A chained tail can never ride in on an allowed first program."""
        _install(cp_enabled, commands=["ls*", "curl*"])
        assert cp_enabled.command_gate("ls -l && curl http://x") == "bypass"
        assert cp_enabled.command_gate("ls -l && rm -rf /tmp/x") == "deny"

    def test_pipe_segment_counts(self, cp_enabled):
        """Pipes split segments too — every stage must be allowed."""
        _install(cp_enabled, commands=["cat*"])
        assert cp_enabled.command_gate("cat f | sh") == "deny"

    def test_substitution_fails_closed(self, cp_enabled):
        """Command substitution / backticks are never allow-matched."""
        _install(cp_enabled, commands=["echo*", "ls*"])
        assert cp_enabled.command_gate("echo $(rm -rf /)") == "deny"
        assert cp_enabled.command_gate("echo `id`") == "deny"

    def test_redirect_ampersand_not_a_separator(self, cp_enabled):
        """`2>&1` is redirection, not a chained command — masked like env path."""
        _install(cp_enabled, commands=["ls*"])
        assert cp_enabled.command_gate("ls -l 2>&1") == "bypass"

    def test_star_entry_allows_anything(self, cp_enabled):
        """A bare `*` entry is an explicit allow-all."""
        _install(cp_enabled, commands=["*"])
        assert cp_enabled.command_gate("anything at all") == "bypass"
