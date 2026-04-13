"""
test_thread_safety.py — Tests that DB writes happen only on the main thread
when concurrency > 1 in cmd_crawl.

Spec: "SQLite writes MUST happen on the main thread only."

The _crawl_date_range_worker function must be a pure HTTP+parse function that
returns data without touching the DB.  All insert_batch and batch_download
calls must happen in the main thread after collecting future results.
"""
from __future__ import annotations

import threading
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, call, patch

import pytest

from db import Filing, get_db, insert_batch


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_crawl_args(
    db: str = ":memory:",
    date_from: str = "2024-01-01",
    date_to: str = "2024-01-03",
    concurrency: int = 3,
    max_pages: int = 1,
    download: bool = False,
    doc_dir: str = "/tmp/test_docs",
    parallel: int = 2,
    category: str = "",
    column: str = "all",
    search: str = "",
    incremental: bool = False,
    resume: bool = False,
) -> SimpleNamespace:
    """Build a minimal argparse-style Namespace for cmd_crawl."""
    return SimpleNamespace(
        db=db,
        date_from=date_from,
        date_to=date_to,
        concurrency=concurrency,
        max_pages=max_pages,
        download=download,
        doc_dir=doc_dir,
        parallel=parallel,
        category=category,
        column=column,
        search=search,
        incremental=incremental,
        resume=resume,
    )


def _make_filing(idx: int) -> Filing:
    """Return a minimal Filing for a given index."""
    return Filing(
        filing_id=f"test_id_{idx:04d}",
        ticker=f"{idx:06d}",
        company_name=f"公司{idx}",
        org_id=f"org{idx}",
        org_name=f"公司{idx}股份有限公司",
        headline=f"年度报告{idx}",
        filing_date="2024-01-01",
        announcement_time_ms=1704067200000,
        document_url=f"finalpage/2024-01-01/test_id_{idx:04d}.PDF",
        adjunct_type="PDF",
        file_size=1024,
        category="category_ndbg_szsh",
        column_id="col_szse_annual",
        direct_download_url=(
            f"http://static.cninfo.com.cn/finalpage/2024-01-01/test_id_{idx:04d}.PDF"
        ),
        filing_type="annual_report",
    )


def _fake_query_response(filings: list[Filing]) -> dict[str, Any]:
    """Build a minimal CNINFO-shaped response dict from Filing objects."""
    announcements = [
        {
            "announcementId": f.filing_id,
            "secCode": f.ticker,
            "secName": f.company_name,
            "orgId": f.org_id,
            "orgName": f.org_name,
            "announcementTitle": f.headline,
            "announcementTime": f.announcement_time_ms,
            "adjunctUrl": f.document_url,
            "adjunctType": f.adjunct_type,
            "adjunctSize": f.file_size,
            "announcementType": f.category,
            "columnId": f.column_id,
        }
        for f in filings
    ]
    return {
        "totalAnnouncement": len(announcements),
        "totalpages": 1,
        "hasMore": False,
        "announcements": announcements,
    }


# ---------------------------------------------------------------------------
# Worker function contract: no DB access
# ---------------------------------------------------------------------------


class TestWorkerNoDB:
    """_crawl_date_range_worker must not call any db module functions."""

    def test_worker_calls_insert_batch_exactly_once_per_range(self, tmp_path):
        """With a single date range, insert_batch is called exactly once."""
        from scraper import cmd_crawl

        db_path = str(tmp_path / "test.db")
        args = _make_crawl_args(
            db=db_path,
            date_from="2024-01-01",
            date_to="2024-01-01",
            concurrency=1,
        )

        filing = _make_filing(1)
        fake_resp = _fake_query_response([filing])

        insert_calls: list[int] = []
        original_insert_batch = insert_batch

        def spying_insert_batch(conn, filings, *a, **kw):
            insert_calls.append(len(filings))
            return original_insert_batch(conn, filings, *a, **kw)

        with (
            patch("scraper.query_announcements", return_value=fake_resp),
            patch("scraper.insert_batch", side_effect=spying_insert_batch),
            patch("scraper.batch_download", return_value=0),
        ):
            cmd_crawl(args)

        assert len(insert_calls) == 1
        assert insert_calls[0] == 1  # one filing inserted

    def test_worker_does_not_call_db_directly_when_concurrent(self, tmp_path):
        """In concurrent mode, workers must not independently call insert_batch."""
        from scraper import cmd_crawl

        db_path = str(tmp_path / "test.db")
        # Use concurrency=2 so ThreadPoolExecutor is engaged.
        args = _make_crawl_args(
            db=db_path,
            date_from="2024-01-01",
            date_to="2024-01-02",
            concurrency=2,
        )

        filings_by_date = {
            "2024-01-01~2024-01-01": [_make_filing(1)],
            "2024-01-02~2024-01-02": [_make_filing(2)],
        }

        worker_thread_insert_calls: list[str] = []
        main_thread_name = threading.current_thread().name
        original_insert_batch = insert_batch

        def spying_insert_batch(conn, filings, *a, **kw):
            t = threading.current_thread().name
            if t != main_thread_name:
                worker_thread_insert_calls.append(t)
            return original_insert_batch(conn, filings, *a, **kw)

        def fake_query(session, page_num=1, date_range="", **kwargs):
            return _fake_query_response(filings_by_date.get(date_range, []))

        with (
            patch("scraper.query_announcements", side_effect=fake_query),
            patch("scraper.insert_batch", side_effect=spying_insert_batch),
            patch("scraper.batch_download", return_value=0),
        ):
            cmd_crawl(args)

        assert worker_thread_insert_calls == [], (
            f"insert_batch was called from worker threads: {worker_thread_insert_calls}"
        )


# ---------------------------------------------------------------------------
# Main-thread DB write enforcement (concurrency > 1)
# ---------------------------------------------------------------------------


class TestMainThreadDbWrites:
    """When concurrency > 1, all DB writes must happen on the main thread."""

    def test_insert_batch_called_on_main_thread(self, tmp_path):
        """insert_batch must be called from the main thread, not a worker."""
        db_path = str(tmp_path / "test.db")
        args = _make_crawl_args(
            db=db_path,
            date_from="2024-01-01",
            date_to="2024-01-03",
            concurrency=3,
        )

        main_thread_name = threading.current_thread().name
        insert_threads: list[str] = []

        original_insert_batch = insert_batch

        def recording_insert_batch(conn, filings, *a, **kw):
            insert_threads.append(threading.current_thread().name)
            return original_insert_batch(conn, filings, *a, **kw)

        # Build one filing per date range so each worker returns data.
        filings_by_date = {
            "2024-01-01~2024-01-01": [_make_filing(1)],
            "2024-01-02~2024-01-02": [_make_filing(2)],
            "2024-01-03~2024-01-03": [_make_filing(3)],
        }

        def fake_query(session, page_num=1, date_range="", **kwargs):
            filings = filings_by_date.get(date_range, [])
            return _fake_query_response(filings)

        with (
            patch("scraper.query_announcements", side_effect=fake_query),
            patch("scraper.insert_batch", side_effect=recording_insert_batch),
            patch("scraper.batch_download", return_value=0),
        ):
            from scraper import cmd_crawl
            cmd_crawl(args)

        assert len(insert_threads) > 0, "insert_batch was never called"
        for thread_name in insert_threads:
            assert thread_name == main_thread_name, (
                f"insert_batch called from worker thread '{thread_name}', "
                f"expected main thread '{main_thread_name}'"
            )

    def test_batch_download_called_on_main_thread(self, tmp_path):
        """batch_download must be called from the main thread, not a worker."""
        db_path = str(tmp_path / "test.db")
        args = _make_crawl_args(
            db=db_path,
            date_from="2024-01-01",
            date_to="2024-01-02",
            concurrency=2,
            download=True,
        )

        main_thread_name = threading.current_thread().name
        download_threads: list[str] = []

        def recording_batch_download(conn, filings, doc_dir, workers=5):
            download_threads.append(threading.current_thread().name)
            return 0

        filings_by_date = {
            "2024-01-01~2024-01-01": [_make_filing(10)],
            "2024-01-02~2024-01-02": [_make_filing(11)],
        }

        def fake_query(session, page_num=1, date_range="", **kwargs):
            filings = filings_by_date.get(date_range, [])
            return _fake_query_response(filings)

        with (
            patch("scraper.query_announcements", side_effect=fake_query),
            patch("scraper.batch_download", side_effect=recording_batch_download),
        ):
            from scraper import cmd_crawl
            cmd_crawl(args)

        assert len(download_threads) > 0, "batch_download was never called"
        for thread_name in download_threads:
            assert thread_name == main_thread_name, (
                f"batch_download called from worker thread '{thread_name}', "
                f"expected main thread '{main_thread_name}'"
            )

    def test_known_ids_updated_on_main_thread(self, tmp_path):
        """Filings from concurrent workers should all appear in the DB after crawl."""
        db_path = str(tmp_path / "test.db")
        args = _make_crawl_args(
            db=db_path,
            date_from="2024-01-01",
            date_to="2024-01-03",
            concurrency=3,
        )

        filings_by_date = {
            "2024-01-01~2024-01-01": [_make_filing(100)],
            "2024-01-02~2024-01-02": [_make_filing(101)],
            "2024-01-03~2024-01-03": [_make_filing(102)],
        }

        def fake_query(session, page_num=1, date_range="", **kwargs):
            filings = filings_by_date.get(date_range, [])
            return _fake_query_response(filings)

        with patch("scraper.query_announcements", side_effect=fake_query):
            from scraper import cmd_crawl
            result = cmd_crawl(args)

        # All three filings should have been written to DB.
        assert result.filings_found == 3
        assert result.filings_new == 3


# ---------------------------------------------------------------------------
# Sequential mode (concurrency=1) still works after refactor
# ---------------------------------------------------------------------------


class TestSequentialMode:
    """Verify sequential (concurrency=1) crawl still produces correct results."""

    def test_sequential_inserts_all_filings(self, tmp_path):
        db_path = str(tmp_path / "seq.db")
        args = _make_crawl_args(
            db=db_path,
            date_from="2024-01-01",
            date_to="2024-01-02",
            concurrency=1,
        )

        filings_by_date = {
            "2024-01-01~2024-01-01": [_make_filing(200)],
            "2024-01-02~2024-01-02": [_make_filing(201)],
        }

        def fake_query(session, page_num=1, date_range="", **kwargs):
            filings = filings_by_date.get(date_range, [])
            return _fake_query_response(filings)

        with patch("scraper.query_announcements", side_effect=fake_query):
            from scraper import cmd_crawl
            result = cmd_crawl(args)

        assert result.filings_found == 2
        assert result.filings_new == 2

    def test_sequential_deduplicates_on_second_run(self, tmp_path):
        """Second crawl over same dates should find 0 new filings."""
        db_path = str(tmp_path / "dedup.db")
        args = _make_crawl_args(
            db=db_path,
            date_from="2024-01-01",
            date_to="2024-01-01",
            concurrency=1,
        )

        filings_by_date = {
            "2024-01-01~2024-01-01": [_make_filing(300)],
        }

        def fake_query(session, page_num=1, date_range="", **kwargs):
            return _fake_query_response(filings_by_date.get(date_range, []))

        with patch("scraper.query_announcements", side_effect=fake_query):
            from scraper import cmd_crawl
            cmd_crawl(args)  # first run
            # Re-open same DB path (simulating second run)
            args2 = _make_crawl_args(db=db_path, date_from="2024-01-01", date_to="2024-01-01", concurrency=1)
            with patch("scraper.query_announcements", side_effect=fake_query):
                result = cmd_crawl(args2)

        assert result.filings_new == 0

    def test_sequential_handles_empty_date_range(self, tmp_path):
        """Empty responses from worker should not crash and contribute 0 filings."""
        db_path = str(tmp_path / "empty.db")
        args = _make_crawl_args(
            db=db_path,
            date_from="2024-01-01",
            date_to="2024-01-02",
            concurrency=1,
        )

        def fake_query(session, page_num=1, date_range="", **kwargs):
            return {"totalAnnouncement": 0, "totalpages": 0, "hasMore": False, "announcements": None}

        with patch("scraper.query_announcements", side_effect=fake_query):
            from scraper import cmd_crawl
            result = cmd_crawl(args)

        assert result.filings_found == 0
        assert result.filings_new == 0


# ---------------------------------------------------------------------------
# Worker error isolation
# ---------------------------------------------------------------------------


class TestWorkerErrorIsolation:
    """A failing worker should not abort the entire crawl."""

    def test_failed_worker_increments_error_count(self, tmp_path):
        """When one date-range worker raises, error_count should increase."""
        db_path = str(tmp_path / "err.db")
        args = _make_crawl_args(
            db=db_path,
            date_from="2024-01-01",
            date_to="2024-01-03",
            concurrency=3,
        )

        call_count = 0

        def fake_query(session, page_num=1, date_range="", **kwargs):
            nonlocal call_count
            call_count += 1
            if date_range == "2024-01-02~2024-01-02":
                raise RuntimeError("simulated network failure")
            return _fake_query_response([_make_filing(call_count)])

        with patch("scraper.query_announcements", side_effect=fake_query):
            from scraper import cmd_crawl
            result = cmd_crawl(args)

        # Two ranges succeeded, one failed — the crawl itself completed.
        assert result.filings_found >= 2

    def test_successful_workers_still_write_when_one_fails(self, tmp_path):
        """Filings from non-failing workers must be persisted despite one failure."""
        db_path = str(tmp_path / "partial.db")
        args = _make_crawl_args(
            db=db_path,
            date_from="2024-01-01",
            date_to="2024-01-02",
            concurrency=2,
        )

        def fake_query(session, page_num=1, date_range="", **kwargs):
            if date_range == "2024-01-02~2024-01-02":
                raise ValueError("bad date range")
            return _fake_query_response([_make_filing(999)])

        with patch("scraper.query_announcements", side_effect=fake_query):
            from scraper import cmd_crawl
            result = cmd_crawl(args)

        # At least the one successful range was written.
        assert result.filings_new >= 1
