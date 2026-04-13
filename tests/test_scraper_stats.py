"""
test_scraper_stats.py — Tests for scraper.py stats command and exit codes.

Covers:
  - cmd_stats() human-readable output
  - cmd_stats() --json output structure and field values
  - Health field presence in JSON output
  - _get_documents_size() utility
  - Exit code semantics
"""
from __future__ import annotations

import json
import sqlite3
import sys
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from db import (
    get_db,
    insert_batch,
    log_crawl_complete,
    log_crawl_start,
    mark_downloaded,
)
from scraper import _get_documents_size, cmd_stats


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_args(
    db: str = ":memory:",
    json_flag: bool = False,
    doc_dir: str = "documents",
) -> SimpleNamespace:
    """Build a minimal argparse.Namespace for cmd_stats."""
    return SimpleNamespace(db=db, json=json_flag, doc_dir=doc_dir)


# ---------------------------------------------------------------------------
# cmd_stats() — human-readable output
# ---------------------------------------------------------------------------


class TestCmdStatsHuman:
    def test_returns_zero_exit_code(self, tmp_path):
        db_path = str(tmp_path / "test.db")
        get_db(db_path).close()
        args = _make_args(db=db_path)
        code = cmd_stats(args)
        assert code == 0

    def test_prints_total_filings(self, tmp_path, capsys, sample_filing):
        db_path = str(tmp_path / "test.db")
        conn = get_db(db_path)
        insert_batch(conn, [sample_filing])
        conn.close()

        args = _make_args(db=db_path)
        cmd_stats(args)
        captured = capsys.readouterr()
        assert "1" in captured.out
        assert "Total filings" in captured.out

    def test_prints_health_field(self, tmp_path, capsys):
        db_path = str(tmp_path / "test.db")
        get_db(db_path).close()
        args = _make_args(db=db_path)
        cmd_stats(args)
        captured = capsys.readouterr()
        assert "Health" in captured.out
        assert "empty" in captured.out

    def test_prints_crawl_runs(self, tmp_path, capsys):
        db_path = str(tmp_path / "test.db")
        conn = get_db(db_path)
        log_id = log_crawl_start(conn)
        log_crawl_complete(conn, log_id, 10, 5)
        conn.close()

        args = _make_args(db=db_path)
        cmd_stats(args)
        captured = capsys.readouterr()
        assert "Crawl runs" in captured.out
        assert "1" in captured.out


# ---------------------------------------------------------------------------
# cmd_stats() --json output
# ---------------------------------------------------------------------------


class TestCmdStatsJson:
    def _get_json_output(self, db_path: str, doc_dir: str = "documents") -> dict:
        """Run cmd_stats --json and return parsed output dict."""
        args = _make_args(db=db_path, json_flag=True, doc_dir=doc_dir)
        captured_output = []

        original_print = __builtins__["print"] if isinstance(__builtins__, dict) else print

        with patch("builtins.print", side_effect=lambda s: captured_output.append(s)):
            code = cmd_stats(args)

        assert code == 0
        assert len(captured_output) == 1
        return json.loads(captured_output[0])

    def test_returns_zero_exit_code(self, tmp_path):
        db_path = str(tmp_path / "test.db")
        get_db(db_path).close()
        args = _make_args(db=db_path, json_flag=True)
        code = cmd_stats(args)
        assert code == 0

    def test_json_output_is_valid_json(self, tmp_path, capsys):
        db_path = str(tmp_path / "test.db")
        get_db(db_path).close()
        args = _make_args(db=db_path, json_flag=True)
        cmd_stats(args)
        captured = capsys.readouterr()
        # Should parse without error
        data = json.loads(captured.out)
        assert isinstance(data, dict)

    def test_json_has_scraper_field(self, tmp_path, capsys):
        db_path = str(tmp_path / "test.db")
        get_db(db_path).close()
        args = _make_args(db=db_path, json_flag=True)
        cmd_stats(args)
        data = json.loads(capsys.readouterr().out)
        assert data["scraper"] == "china-scraper"

    def test_json_has_country_cn(self, tmp_path, capsys):
        db_path = str(tmp_path / "test.db")
        get_db(db_path).close()
        args = _make_args(db=db_path, json_flag=True)
        cmd_stats(args)
        data = json.loads(capsys.readouterr().out)
        assert data["country"] == "CN"

    def test_json_has_sources_list(self, tmp_path, capsys):
        db_path = str(tmp_path / "test.db")
        get_db(db_path).close()
        args = _make_args(db=db_path, json_flag=True)
        cmd_stats(args)
        data = json.loads(capsys.readouterr().out)
        assert "sources" in data
        assert "cninfo" in data["sources"]

    def test_json_has_all_required_fields(self, tmp_path, capsys):
        db_path = str(tmp_path / "test.db")
        get_db(db_path).close()
        args = _make_args(db=db_path, json_flag=True)
        cmd_stats(args)
        data = json.loads(capsys.readouterr().out)
        required_keys = {
            "scraper", "country", "sources",
            "total_filings", "downloaded", "pending_download",
            "unique_companies", "total_crawl_runs",
            "earliest_record", "latest_record",
            "db_size_bytes", "documents_size_bytes",
            "health",
        }
        assert required_keys.issubset(data.keys())

    def test_json_total_filings_correct(self, tmp_path, capsys, sample_filing, sample_filing_2):
        db_path = str(tmp_path / "test.db")
        conn = get_db(db_path)
        insert_batch(conn, [sample_filing, sample_filing_2])
        conn.close()

        args = _make_args(db=db_path, json_flag=True)
        cmd_stats(args)
        data = json.loads(capsys.readouterr().out)
        assert data["total_filings"] == 2

    def test_json_downloaded_and_pending(self, tmp_path, capsys, sample_filing, sample_filing_2):
        db_path = str(tmp_path / "test.db")
        conn = get_db(db_path)
        insert_batch(conn, [sample_filing, sample_filing_2])
        mark_downloaded(conn, sample_filing.filing_id, "/tmp/test.pdf")
        conn.close()

        args = _make_args(db=db_path, json_flag=True)
        cmd_stats(args)
        data = json.loads(capsys.readouterr().out)
        assert data["downloaded"] == 1
        assert data["pending_download"] == 1

    def test_json_health_is_empty_for_fresh_db(self, tmp_path, capsys):
        db_path = str(tmp_path / "test.db")
        get_db(db_path).close()
        args = _make_args(db=db_path, json_flag=True)
        cmd_stats(args)
        data = json.loads(capsys.readouterr().out)
        assert data["health"] == "empty"

    def test_json_health_is_ok_after_successful_crawl(self, tmp_path, capsys):
        db_path = str(tmp_path / "test.db")
        conn = get_db(db_path)
        log_id = log_crawl_start(conn)
        log_crawl_complete(conn, log_id, 100, 50, 0)
        conn.close()

        args = _make_args(db=db_path, json_flag=True)
        cmd_stats(args)
        data = json.loads(capsys.readouterr().out)
        assert data["health"] == "ok"

    def test_json_health_is_error_for_incomplete_crawl(self, tmp_path, capsys):
        db_path = str(tmp_path / "test.db")
        conn = get_db(db_path)
        log_crawl_start(conn)  # never completed
        conn.close()

        args = _make_args(db=db_path, json_flag=True)
        cmd_stats(args)
        data = json.loads(capsys.readouterr().out)
        assert data["health"] == "error"

    def test_json_db_size_bytes_is_integer(self, tmp_path, capsys):
        db_path = str(tmp_path / "test.db")
        get_db(db_path).close()
        args = _make_args(db=db_path, json_flag=True)
        cmd_stats(args)
        data = json.loads(capsys.readouterr().out)
        assert isinstance(data["db_size_bytes"], int)
        assert data["db_size_bytes"] >= 0

    def test_json_documents_size_bytes_is_integer(self, tmp_path, capsys):
        db_path = str(tmp_path / "test.db")
        doc_dir = str(tmp_path / "documents")
        get_db(db_path).close()
        args = _make_args(db=db_path, json_flag=True, doc_dir=doc_dir)
        cmd_stats(args)
        data = json.loads(capsys.readouterr().out)
        assert isinstance(data["documents_size_bytes"], int)

    def test_json_earliest_and_latest_record(
        self, tmp_path, capsys, sample_filing, sample_filing_2
    ):
        db_path = str(tmp_path / "test.db")
        conn = get_db(db_path)
        insert_batch(conn, [sample_filing, sample_filing_2])
        conn.close()

        args = _make_args(db=db_path, json_flag=True)
        cmd_stats(args)
        data = json.loads(capsys.readouterr().out)
        assert data["earliest_record"] == "2023-08-15"
        assert data["latest_record"] == "2024-03-30"

    def test_json_total_crawl_runs(self, tmp_path, capsys):
        db_path = str(tmp_path / "test.db")
        conn = get_db(db_path)
        for _ in range(3):
            log_id = log_crawl_start(conn)
            log_crawl_complete(conn, log_id, 10, 5)
        conn.close()

        args = _make_args(db=db_path, json_flag=True)
        cmd_stats(args)
        data = json.loads(capsys.readouterr().out)
        assert data["total_crawl_runs"] == 3


# ---------------------------------------------------------------------------
# _get_documents_size()
# ---------------------------------------------------------------------------


class TestGetDocumentsSize:
    def test_returns_zero_for_missing_directory(self, tmp_path):
        nonexistent = str(tmp_path / "no_such_dir")
        assert _get_documents_size(nonexistent) == 0

    def test_returns_zero_for_empty_directory(self, tmp_path):
        doc_dir = tmp_path / "empty_docs"
        doc_dir.mkdir()
        assert _get_documents_size(str(doc_dir)) == 0

    def test_counts_file_sizes(self, tmp_path):
        doc_dir = tmp_path / "docs"
        doc_dir.mkdir()
        (doc_dir / "a.pdf").write_bytes(b"A" * 1000)
        (doc_dir / "b.pdf").write_bytes(b"B" * 500)
        total = _get_documents_size(str(doc_dir))
        assert total == 1500

    def test_counts_nested_files(self, tmp_path):
        doc_dir = tmp_path / "docs"
        sub = doc_dir / "sub"
        sub.mkdir(parents=True)
        (doc_dir / "a.pdf").write_bytes(b"X" * 200)
        (sub / "b.pdf").write_bytes(b"Y" * 300)
        total = _get_documents_size(str(doc_dir))
        assert total == 500
