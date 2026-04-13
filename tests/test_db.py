"""
test_db.py — Unit tests for db.py.

Covers:
  - get_db()          — schema creation and migration
  - upsert_filing()   — insert and idempotency
  - insert_batch()    — bulk insert and dedup
  - mark_downloaded() — download tracking
  - is_downloaded()   — read helper
  - get_known_ids()   — read helper
  - get_last_crawl_date() — incremental mode helper
  - stats()           — aggregate stats
  - export_json()     — JSON dump
"""
from __future__ import annotations

import json
import sqlite3
import tempfile
from pathlib import Path

import pytest

from db import (
    export_json,
    get_db,
    get_known_ids,
    get_last_crawl_date,
    insert_batch,
    is_downloaded,
    mark_downloaded,
    stats,
    upsert_filing,
)


# ---------------------------------------------------------------------------
# Schema / get_db()
# ---------------------------------------------------------------------------


class TestGetDb:
    def test_creates_filings_table(self, mem_db):
        tables = {
            row[0]
            for row in mem_db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert "filings" in tables

    def test_filings_table_has_announcement_id_column(self, mem_db):
        cols = {
            row[1].lower()
            for row in mem_db.execute("PRAGMA table_info(filings)").fetchall()
        }
        assert "announcement_id" in cols

    def test_filings_table_has_filing_type_column(self, mem_db):
        cols = {
            row[1].lower()
            for row in mem_db.execute("PRAGMA table_info(filings)").fetchall()
        }
        assert "filing_type" in cols

    def test_filings_table_has_all_required_columns(self, mem_db):
        cols = {
            row[1].lower()
            for row in mem_db.execute("PRAGMA table_info(filings)").fetchall()
        }
        required = {
            "announcement_id", "sec_code", "sec_name", "org_id", "org_name",
            "title", "announcement_date", "announcement_time_ms",
            "adjunct_url", "adjunct_type", "adjunct_size",
            "announcement_type", "column_id", "download_url",
            "filing_type", "downloaded", "local_path", "first_seen",
        }
        assert required.issubset(cols)

    def test_db_is_idempotent_on_repeated_open(self, tmp_path):
        """Opening the DB twice should not raise or corrupt schema."""
        db_path = tmp_path / "test_idempotent.db"
        c1 = get_db(db_path)
        c1.close()
        c2 = get_db(db_path)
        c2.close()


# ---------------------------------------------------------------------------
# upsert_filing()
# ---------------------------------------------------------------------------


class TestUpsertFiling:
    def test_inserts_new_filing_returns_true(self, mem_db, sample_filing):
        assert upsert_filing(mem_db, sample_filing) is True

    def test_duplicate_insert_returns_false(self, mem_db, sample_filing):
        upsert_filing(mem_db, sample_filing)
        assert upsert_filing(mem_db, sample_filing) is False

    def test_inserted_filing_is_retrievable(self, mem_db, sample_filing):
        upsert_filing(mem_db, sample_filing)
        row = mem_db.execute(
            "SELECT * FROM filings WHERE announcement_id = ?",
            (sample_filing.announcement_id,),
        ).fetchone()
        assert row is not None
        assert row["sec_code"] == "000001"
        assert row["filing_type"] == "annual_report"

    def test_inserted_filing_has_first_seen_timestamp(self, mem_db, sample_filing):
        upsert_filing(mem_db, sample_filing)
        row = mem_db.execute(
            "SELECT first_seen FROM filings WHERE announcement_id = ?",
            (sample_filing.announcement_id,),
        ).fetchone()
        assert row is not None
        assert row["first_seen"] is not None

    def test_downloaded_defaults_to_zero(self, mem_db, sample_filing):
        upsert_filing(mem_db, sample_filing)
        row = mem_db.execute(
            "SELECT downloaded FROM filings WHERE announcement_id = ?",
            (sample_filing.announcement_id,),
        ).fetchone()
        assert row["downloaded"] == 0

    def test_second_distinct_filing_is_inserted(
        self, mem_db, sample_filing, sample_filing_2
    ):
        upsert_filing(mem_db, sample_filing)
        assert upsert_filing(mem_db, sample_filing_2) is True

    def test_total_count_after_two_distinct_inserts(
        self, mem_db, sample_filing, sample_filing_2
    ):
        upsert_filing(mem_db, sample_filing)
        upsert_filing(mem_db, sample_filing_2)
        count = mem_db.execute("SELECT COUNT(*) FROM filings").fetchone()[0]
        assert count == 2


# ---------------------------------------------------------------------------
# insert_batch()
# ---------------------------------------------------------------------------


class TestInsertBatch:
    def test_returns_count_of_new_rows(
        self, mem_db, sample_filing, sample_filing_2
    ):
        count = insert_batch(mem_db, [sample_filing, sample_filing_2])
        assert count == 2

    def test_duplicate_batch_returns_zero(self, mem_db, sample_filing):
        insert_batch(mem_db, [sample_filing])
        count = insert_batch(mem_db, [sample_filing])
        assert count == 0

    def test_partial_dedup_returns_correct_count(
        self, mem_db, sample_filing, sample_filing_2
    ):
        insert_batch(mem_db, [sample_filing])
        count = insert_batch(mem_db, [sample_filing, sample_filing_2])
        assert count == 1

    def test_empty_list_returns_zero(self, mem_db):
        assert insert_batch(mem_db, []) == 0


# ---------------------------------------------------------------------------
# mark_downloaded() / is_downloaded()
# ---------------------------------------------------------------------------


class TestDownloadTracking:
    def test_is_downloaded_false_before_marking(self, mem_db, sample_filing):
        upsert_filing(mem_db, sample_filing)
        assert is_downloaded(mem_db, sample_filing.announcement_id) is False

    def test_is_downloaded_true_after_marking(self, mem_db, sample_filing):
        upsert_filing(mem_db, sample_filing)
        mark_downloaded(mem_db, sample_filing.announcement_id, "/tmp/test.pdf")
        assert is_downloaded(mem_db, sample_filing.announcement_id) is True

    def test_mark_downloaded_sets_local_path(self, mem_db, sample_filing):
        upsert_filing(mem_db, sample_filing)
        mark_downloaded(mem_db, sample_filing.announcement_id, "/tmp/report.pdf")
        row = mem_db.execute(
            "SELECT local_path FROM filings WHERE announcement_id = ?",
            (sample_filing.announcement_id,),
        ).fetchone()
        assert row["local_path"] == "/tmp/report.pdf"

    def test_is_downloaded_false_for_unknown_id(self, mem_db):
        assert is_downloaded(mem_db, "nonexistent_id") is False


# ---------------------------------------------------------------------------
# get_known_ids()
# ---------------------------------------------------------------------------


class TestGetKnownIds:
    def test_empty_db_returns_empty_set(self, mem_db):
        assert get_known_ids(mem_db) == set()

    def test_returns_inserted_ids(self, mem_db, sample_filing, sample_filing_2):
        insert_batch(mem_db, [sample_filing, sample_filing_2])
        known = get_known_ids(mem_db)
        assert sample_filing.announcement_id in known
        assert sample_filing_2.announcement_id in known

    def test_returns_set_not_list(self, mem_db, sample_filing):
        upsert_filing(mem_db, sample_filing)
        assert isinstance(get_known_ids(mem_db), set)


# ---------------------------------------------------------------------------
# get_last_crawl_date()
# ---------------------------------------------------------------------------


class TestGetLastCrawlDate:
    def test_returns_none_for_empty_db(self, mem_db):
        assert get_last_crawl_date(mem_db) is None

    def test_returns_most_recent_date(self, mem_db, sample_filing, sample_filing_2):
        # sample_filing: 2024-03-30, sample_filing_2: 2023-08-15
        insert_batch(mem_db, [sample_filing, sample_filing_2])
        last_date = get_last_crawl_date(mem_db)
        assert last_date == "2024-03-30"


# ---------------------------------------------------------------------------
# stats()
# ---------------------------------------------------------------------------


class TestStats:
    def test_empty_db_stats(self, mem_db):
        s = stats(mem_db)
        assert s["total"] == 0
        assert s["downloaded"] == 0
        assert s["pending"] == 0
        assert s["unique_companies"] == 0
        assert s["oldest"] is None
        assert s["newest"] is None

    def test_stats_after_insert(self, mem_db, sample_filing, sample_filing_2):
        insert_batch(mem_db, [sample_filing, sample_filing_2])
        s = stats(mem_db)
        assert s["total"] == 2
        assert s["downloaded"] == 0
        assert s["pending"] == 2
        assert s["unique_companies"] == 2

    def test_stats_after_download(self, mem_db, sample_filing):
        upsert_filing(mem_db, sample_filing)
        mark_downloaded(mem_db, sample_filing.announcement_id, "/tmp/x.pdf")
        s = stats(mem_db)
        assert s["downloaded"] == 1
        assert s["pending"] == 0

    def test_stats_oldest_and_newest(self, mem_db, sample_filing, sample_filing_2):
        insert_batch(mem_db, [sample_filing, sample_filing_2])
        s = stats(mem_db)
        assert s["oldest"] == "2023-08-15"
        assert s["newest"] == "2024-03-30"


# ---------------------------------------------------------------------------
# export_json()
# ---------------------------------------------------------------------------


class TestExportJson:
    def test_creates_json_file(self, mem_db, sample_filing, tmp_path):
        upsert_filing(mem_db, sample_filing)
        out_path = str(tmp_path / "out.json")
        export_json(mem_db, out_path)
        assert Path(out_path).exists()

    def test_json_has_metadata_and_filings_keys(self, mem_db, sample_filing, tmp_path):
        upsert_filing(mem_db, sample_filing)
        out_path = str(tmp_path / "out.json")
        export_json(mem_db, out_path)
        data = json.loads(Path(out_path).read_text(encoding="utf-8"))
        assert "metadata" in data
        assert "filings" in data

    def test_json_filings_count_matches_db(
        self, mem_db, sample_filing, sample_filing_2, tmp_path
    ):
        insert_batch(mem_db, [sample_filing, sample_filing_2])
        out_path = str(tmp_path / "out.json")
        export_json(mem_db, out_path)
        data = json.loads(Path(out_path).read_text(encoding="utf-8"))
        assert len(data["filings"]) == 2
        assert data["metadata"]["total"] == 2

    def test_json_metadata_has_source_url(self, mem_db, sample_filing, tmp_path):
        upsert_filing(mem_db, sample_filing)
        out_path = str(tmp_path / "out.json")
        export_json(mem_db, out_path)
        data = json.loads(Path(out_path).read_text(encoding="utf-8"))
        assert "cninfo" in data["metadata"]["source"]

    def test_json_contains_chinese_text_correctly(
        self, mem_db, sample_filing, tmp_path
    ):
        upsert_filing(mem_db, sample_filing)
        out_path = str(tmp_path / "out.json")
        export_json(mem_db, out_path)
        raw = Path(out_path).read_text(encoding="utf-8")
        assert "平安银行" in raw
