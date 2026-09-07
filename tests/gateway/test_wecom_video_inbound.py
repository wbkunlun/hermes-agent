"""Tests for inbound video extraction (③ gap)."""

import pytest

from gateway.config import Platform
from plugins.platforms.wecom.adapter import WeComAdapter


def _adapter(tmp_path=None):
    adapter = WeComAdapter.__new__(WeComAdapter)
    adapter.platform = Platform.WECOM
    if tmp_path is not None:
        adapter._cache_root = str(tmp_path)
    return adapter


class TestExtractVideoRef:
    @pytest.mark.asyncio
    async def test_top_level_video_collected(self, monkeypatch):
        adapter = _adapter()
        seen = []

        async def fake_cache(kind, media):
            seen.append(kind)
            return None

        monkeypatch.setattr(adapter, "_cache_media", fake_cache)
        await adapter._extract_media({
            "msgtype": "video",
            "video": {"url": "https://cos.example/v.mp4", "aeskey": "k"},
        })
        assert seen == ["video"]

    @pytest.mark.asyncio
    async def test_non_video_untouched(self, monkeypatch):
        adapter = _adapter()
        seen = []

        async def fake_cache(kind, media):
            seen.append(kind)
            return None

        monkeypatch.setattr(adapter, "_cache_media", fake_cache)
        await adapter._extract_media({
            "msgtype": "text",
            "text": {"content": "hi"},
        })
        assert seen == []


class TestCacheVideo:
    @pytest.mark.asyncio
    async def test_video_downloads_with_10mb_cap_and_mp4_sniff(self, monkeypatch, tmp_path):
        adapter = _adapter()
        mp4 = b"\x00\x00\x00\x20ftypisom" + b"0" * 64

        async def fake_download(url, max_bytes=None):
            assert max_bytes == 10 * 1024 * 1024  # VIDEO_MAX_BYTES, 不是 ABSOLUTE
            return mp4, {"content-type": "application/octet-stream"}

        monkeypatch.setattr(adapter, "_download_remote_bytes", fake_download)
        monkeypatch.setattr(
            adapter, "_guess_filename", lambda url, cd, ct: "video_no_ext",
        )
        from plugins.platforms.wecom import adapter as wa
        monkeypatch.setattr(
            wa, "cache_document_from_bytes", lambda raw, filename: str(tmp_path / filename),
        )
        path, _content_type = await adapter._cache_media("video", {"url": "https://x/v"})
        assert path.endswith(".mp4")

    @pytest.mark.asyncio
    async def test_video_webm_sniff(self, monkeypatch, tmp_path):
        adapter = _adapter()
        webm = b"\x1a\x45\xdf\xa3" + b"0" * 32

        async def fake_download(url, max_bytes=None):
            return webm, {"content-type": ""}

        monkeypatch.setattr(adapter, "_download_remote_bytes", fake_download)
        monkeypatch.setattr(
            adapter, "_guess_filename", lambda url, cd, ct: "video_no_ext",
        )
        from plugins.platforms.wecom import adapter as wa
        monkeypatch.setattr(
            wa, "cache_document_from_bytes", lambda raw, filename: str(tmp_path / filename),
        )
        path, _ = await adapter._cache_media("video", {"url": "https://x/v"})
        assert path.endswith(".webm")

    @pytest.mark.asyncio
    async def test_video_with_named_extension_kept(self, monkeypatch, tmp_path):
        adapter = _adapter()
        mp4 = b"\x00\x00\x00\x18ftypmp42" + b"0" * 16

        async def fake_download(url, max_bytes=None):
            return mp4, {"content-type": "video/mp4"}

        monkeypatch.setattr(adapter, "_download_remote_bytes", fake_download)
        monkeypatch.setattr(
            adapter, "_guess_filename", lambda url, cd, ct: "clip.mp4",
        )
        from plugins.platforms.wecom import adapter as wa
        monkeypatch.setattr(
            wa, "cache_document_from_bytes", lambda raw, filename: str(tmp_path / filename),
        )
        path, _ = await adapter._cache_media("video", {"url": "https://x/clip.mp4"})
        # 已有扩展名不再追加魔数后缀
        assert path.endswith("clip.mp4")

    @pytest.mark.asyncio
    async def test_video_oversize_rejected(self, monkeypatch):
        adapter = _adapter()

        async def fake_download(url, max_bytes=None):
            # 超限时 _download_remote_bytes 按 max_bytes 抛错（现有语义）
            raise ValueError("exceeds max_bytes")

        monkeypatch.setattr(adapter, "_download_remote_bytes", fake_download)
        result = await adapter._cache_media("video", {"url": "https://x/v"})
        assert result is None


class TestDetectVideoExt:
    def test_mp4_magic(self):
        assert WeComAdapter._detect_video_ext(b"\x00\x00\x00\x20ftypisom" + b"x" * 8) == ".mp4"

    def test_webm_magic(self):
        assert WeComAdapter._detect_video_ext(b"\x1a\x45\xdf\xa3\x00" * 4) == ".webm"

    def test_unknown_defaults_mp4(self):
        assert WeComAdapter._detect_video_ext(b"\x00" * 16) == ".mp4"
