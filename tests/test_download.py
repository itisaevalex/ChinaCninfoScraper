"""
test_download.py — Unit tests for downloader.py.

Uses unittest.mock to avoid real HTTP calls.

Covers:
  - _build_filename()  — safe filename construction
  - download_one()     — PDF magic byte validation, atomic write, error handling
  - batch_download()   — parallel orchestration, dedup against DB
"""
from __future__ import annotations

import os
import sqlite3
from unittest.mock import MagicMock, patch

import pytest

from downloader import _build_filename, batch_download, download_one
from db import get_db, upsert_filing, is_downloaded


# ---------------------------------------------------------------------------
# _build_filename()
# ---------------------------------------------------------------------------


class TestBuildFilename:
    def test_includes_ticker_prefix(self):
        filing = {
            "ticker": "000001",
            "filing_id": "1234567890",
            "headline": "年度报告",
            "adjunct_type": "PDF",
        }
        name = _build_filename(filing)
        assert name.startswith("000001_")

    def test_includes_ticker_prefix_legacy_field(self):
        """Backwards compat: sec_code still works."""
        filing = {
            "sec_code": "000001",
            "announcement_id": "1234567890",
            "title": "年度报告",
            "adjunct_type": "PDF",
        }
        name = _build_filename(filing)
        assert name.startswith("000001_")

    def test_includes_truncated_filing_id(self):
        filing = {
            "ticker": "000001",
            "filing_id": "ABCDEFGHIJ",
            "headline": "报告",
            "adjunct_type": "PDF",
        }
        name = _build_filename(filing)
        # filing_id[:8] = "ABCDEFGH"
        assert "ABCDEFGH" in name

    def test_includes_truncated_announcement_id_legacy(self):
        """Backwards compat: announcement_id[:8] still appears in filename."""
        filing = {
            "sec_code": "000001",
            "announcement_id": "ABCDEFGHIJ",
            "title": "报告",
            "adjunct_type": "PDF",
        }
        name = _build_filename(filing)
        assert "ABCDEFGH" in name

    def test_extension_is_uppercased(self):
        filing = {
            "ticker": "000001",
            "filing_id": "12345678",
            "headline": "Report",
            "adjunct_type": "pdf",
        }
        name = _build_filename(filing)
        assert name.endswith(".PDF")

    def test_removes_illegal_filesystem_chars(self):
        filing = {
            "ticker": "000001",
            "filing_id": "12345678",
            "headline": 'bad/name:with"chars',
            "adjunct_type": "PDF",
        }
        name = _build_filename(filing)
        for char in r'<>:"/\\|?*':
            assert char not in name

    def test_title_truncated_to_70_chars(self):
        filing = {
            "ticker": "000001",
            "filing_id": "12345678",
            "headline": "A" * 200,
            "adjunct_type": "PDF",
        }
        name = _build_filename(filing)
        # ticker(6) + _ + id[:8] + _ + title[:70] + .PDF
        parts = name.split("_", 2)
        title_part = parts[2].rsplit(".", 1)[0]
        assert len(title_part) <= 70

    def test_defaults_for_missing_fields(self):
        name = _build_filename({})
        assert name  # Should not raise and should produce a non-empty string


# ---------------------------------------------------------------------------
# download_one() — mocked HTTP
# ---------------------------------------------------------------------------


class TestDownloadOne:
    def test_returns_true_for_valid_pdf(self, tmp_path):
        dest = str(tmp_path / "test.pdf")
        with patch("downloader.requests.get") as mock_get:
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_resp.content = b"%PDF-1.4 fake pdf content"
            mock_get.return_value = mock_resp

            result = download_one("http://example.com/test.pdf", dest, "ann_001")

        assert result is True
        assert os.path.exists(dest)

    def test_file_content_is_correct(self, tmp_path):
        dest = str(tmp_path / "test.pdf")
        expected = b"%PDF-1.4 fake pdf content here"
        with patch("downloader.requests.get") as mock_get:
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_resp.content = expected
            mock_get.return_value = mock_resp

            download_one("http://example.com/test.pdf", dest, "ann_001")

        assert open(dest, "rb").read() == expected

    def test_returns_false_for_non_200_status(self, tmp_path):
        dest = str(tmp_path / "test.pdf")
        with patch("downloader.requests.get") as mock_get:
            mock_resp = MagicMock()
            mock_resp.status_code = 404
            mock_resp.content = b"Not Found"
            mock_get.return_value = mock_resp

            result = download_one("http://example.com/missing.pdf", dest, "ann_002")

        assert result is False
        assert not os.path.exists(dest)

    def test_returns_false_for_html_error_page(self, tmp_path):
        """CDN sometimes returns HTML with HTTP 200 for missing files."""
        dest = str(tmp_path / "test.pdf")
        with patch("downloader.requests.get") as mock_get:
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_resp.content = b"<html><body>Error 404</body></html>"
            mock_get.return_value = mock_resp

            result = download_one("http://example.com/bad.pdf", dest, "ann_003")

        assert result is False

    def test_returns_false_on_network_exception(self, tmp_path):
        import requests as req
        dest = str(tmp_path / "test.pdf")
        with patch("downloader.requests.get", side_effect=req.ConnectionError("timeout")):
            result = download_one("http://example.com/fail.pdf", dest, "ann_004")

        assert result is False

    def test_part_file_cleaned_up_after_disk_error(self, tmp_path):
        """OSError during write should not leave a .part file behind."""
        dest = str(tmp_path / "test.pdf")
        part = dest + ".part"

        with patch("downloader.requests.get") as mock_get, \
             patch("builtins.open", side_effect=OSError("disk full")):
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_resp.content = b"%PDF-1.4 content"
            mock_get.return_value = mock_resp

            with pytest.raises(OSError):
                download_one("http://example.com/test.pdf", dest, "ann_005")

        assert not os.path.exists(part)


# ---------------------------------------------------------------------------
# batch_download() — mocked HTTP + real in-memory DB
# ---------------------------------------------------------------------------


class TestBatchDownload:
    def _make_filing_dict(self, filing_id: str, ticker: str = "000001") -> dict:
        return {
            "filing_id": filing_id,
            "ticker": ticker,
            "headline": "年度报告",
            "adjunct_type": "PDF",
            "direct_download_url": f"http://static.cninfo.com.cn/finalpage/2024/{filing_id}.PDF",
        }

    def test_downloads_new_filings(self, mem_db, tmp_path):
        filings = [self._make_filing_dict("dl_001")]
        with patch("downloader.download_one", return_value=True) as mock_dl:
            count = batch_download(mem_db, filings, str(tmp_path), workers=1)
        assert count == 1

    def test_skips_already_downloaded_filings(
        self, mem_db, tmp_path, sample_filing
    ):
        upsert_filing(mem_db, sample_filing)
        from db import mark_downloaded as db_mark
        db_mark(mem_db, sample_filing.filing_id, "/tmp/already.pdf")

        filing_dict = {
            "filing_id": sample_filing.filing_id,
            "ticker": sample_filing.ticker,
            "headline": sample_filing.headline,
            "adjunct_type": sample_filing.adjunct_type,
            "direct_download_url": sample_filing.direct_download_url,
        }

        with patch("downloader.download_one") as mock_dl:
            count = batch_download(mem_db, [filing_dict], str(tmp_path), workers=1)

        mock_dl.assert_not_called()
        assert count == 0

    def test_marks_successful_download_in_db(self, mem_db, tmp_path):
        from db import Filing, upsert_filing as _upsert

        filing_id = "batch_test_mark"
        filing_obj = Filing(
            filing_id=filing_id,
            ticker="000001",
            company_name="测试",
            org_id="org1",
            org_name="测试公司",
            headline="年度报告",
            filing_date="2024-03-30",
            announcement_time_ms=1711728000000,
            document_url="finalpage/2024-03-30/batch_test_mark.PDF",
            adjunct_type="PDF",
            file_size=100,
            category="category_ndbg_szsh",
            column_id="col_szse",
            direct_download_url=f"http://static.cninfo.com.cn/finalpage/2024-03-30/{filing_id}.PDF",
            filing_type="annual_report",
        )
        _upsert(mem_db, filing_obj)

        filing_dict = {
            "filing_id": filing_id,
            "ticker": "000001",
            "headline": "年度报告",
            "adjunct_type": "PDF",
            "direct_download_url": filing_obj.direct_download_url,
        }

        def fake_download(url, dest, aid):
            open(dest, "wb").write(b"%PDF fake")
            return True

        with patch("downloader.download_one", side_effect=fake_download):
            batch_download(mem_db, [filing_dict], str(tmp_path), workers=1)

        assert is_downloaded(mem_db, filing_id) is True

    def test_empty_list_returns_zero(self, mem_db, tmp_path):
        assert batch_download(mem_db, [], str(tmp_path)) == 0

    def test_returns_zero_on_all_failures(self, mem_db, tmp_path):
        filings = [self._make_filing_dict("fail_001")]
        with patch("downloader.download_one", return_value=False):
            count = batch_download(mem_db, filings, str(tmp_path), workers=1)
        assert count == 0

    def test_parallel_workers_download_multiple_files(self, mem_db, tmp_path):
        filings = [self._make_filing_dict(f"par_{i:03d}") for i in range(5)]
        call_count = []

        def fake_download(url, dest, aid):
            call_count.append(aid)
            return True

        with patch("downloader.download_one", side_effect=fake_download):
            count = batch_download(mem_db, filings, str(tmp_path), workers=3)

        assert count == 5
        assert len(call_count) == 5
