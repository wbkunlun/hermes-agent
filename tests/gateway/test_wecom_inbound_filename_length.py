"""Inbound WeCom attachments with long percent-encoded (CJK) names must never kill the WS loop.

Live incident (2026-11-05, twice): the CDN URL carried ``doc_<id>_%E4%B8%80%E7%BA%BF...pptx`` and
the cache write raised ``OSError: [Errno 36] File name too long``; the exception escaped
``_on_message`` into the receive loop, the socket closed, and the file was lost.
"""

import errno
import os
from urllib.parse import quote

import pytest

from plugins.platforms.wecom import media as wecom_media
from plugins.platforms.wecom.adapter import WeComAdapter

CJK_NAME = "一线门户子系统架构变更方案+-+新增ECC终端状态数据上报功能 261029.pptx"


def _adapter(tmp_path=None):
    adapter = WeComAdapter.__new__(WeComAdapter)
    if tmp_path is not None:
        adapter._cache_root = str(tmp_path)
    return adapter


class TestBoundedFilename:
    def test_percent_encoded_cjk_name_is_decoded_and_fits_name_max(self):
        encoded = quote("doc_ee8c2ede4484_" + CJK_NAME)
        assert len(encoded.encode()) > 255  # the live shape still overflows before the fix

        name = WeComAdapter._bounded_filename(encoded)

        assert name.startswith("doc_ee8c2ede4484_")
        assert name.endswith(".pptx")
        assert "一线门户" in name
        assert len(name.encode()) <= wecom_media._MAX_FILENAME_BYTES

    def test_short_name_is_unchanged(self):
        assert WeComAdapter._bounded_filename("report.pdf") == "report.pdf"
        assert WeComAdapter._bounded_filename(quote("季度报告.pdf")) == "季度报告.pdf"

    def test_extension_survives_when_stem_is_long(self):
        name = WeComAdapter._bounded_filename("长" * 200 + ".xlsx")
        assert name.endswith(".xlsx")
        assert name != ".xlsx"


class TestInboundLongName:
    @pytest.mark.asyncio
    async def test_long_encoded_name_writes_inside_cache(self, tmp_path, monkeypatch):
        """End to end through `_cache_media`: the cache file is created, so the name is fs-legal."""
        adapter = _adapter(tmp_path)

        async def download(url, max_bytes):
            return b"pptx-bytes", {"content-type": "application/vnd.openxmlformats-officedocument.presentationml.presentation"}

        monkeypatch.setattr(adapter, "_download_remote_bytes", download)
        path, _mime = await adapter._cache_media("file", {"url": "https://cos.example/" + quote("doc_ee8c2ede4484_" + CJK_NAME)})

        assert os.path.isfile(path)
        assert os.path.basename(path).endswith(".pptx")
        assert os.path.getsize(path) == len(b"pptx-bytes")

    @pytest.mark.asyncio
    async def test_name_max_oserror_drops_only_the_attachment(self, monkeypatch):
        """A cache write failing for any reason must not escape `_extract_media`."""
        adapter = _adapter()

        async def download(url, max_bytes):
            return b"x", {"content-type": "application/octet-stream"}

        def ename_toolong(data, filename):
            raise OSError(errno.ENAMETOOLONG, "File name too long")

        monkeypatch.setattr(adapter, "_download_remote_bytes", download)
        monkeypatch.setattr(wecom_media, "cache_document_from_bytes_async", ename_toolong)

        urls, types = await adapter._extract_media({
            "msgtype": "file",
            "file": {"url": "https://cos.example/a", "filename": "a.bin"},
        })

        assert (urls, types) == ([], [])

