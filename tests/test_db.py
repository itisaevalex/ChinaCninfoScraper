"""
test_db.py — Unit tests for db.py (L3 schema).

Covers:
  - get_db()               — schema creation and migration
  - upsert_filing()        — insert and idempotency
  - insert_batch()         — bulk insert and dedup
  - mark_downloaded()      — download tracking
  - is_downloaded()        — read helper
  - get_known_ids()        — read helper
  - get_last_crawl_date()  — incremental mode helper
  - stats()                — aggregate stats
  - export_json()          — JSON dump
  - log_crawl_start()      — crawl log insertion
  - log_crawl_complete()   — crawl log completion
  - detect_health()        — health state machine
  - schema migration       — pre-L3 column renames
"""
from __future__ import annotations

import json
import sqlite3
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from db import (
    detect_health,
    export_json,
    get_db,
    get_known_ids,
    get_last_crawl_date,
    insert_batch,
    is_downloaded,
    log_crawl_complete,
    log_crawl_start,
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

    def test_filings_table_has_filing_id_column(self, mem_db):
        cols = {
            row[1].lower()
            for row in mem_db.execute("PRAGMA table_info(filings)").fetchall()
        }
        assert "filing_id" in cols

    def test_filings_table_has_filing_type_column(self, mem_db):
        cols = {
            row[1].lower()
            for row in mem_db.execute("PRAGMA table_info(filings)").fetchall()
        }
        assert "filing_type" in cols

    def test_filings_table_has_all_l3_required_columns(self, mem_db):
        cols = {
            row[1].lower()
            for row in mem_db.execute("PRAGMA table_info(filings)").fetchall()
        }
        required = {
            "filing_id", "source", "country", "ticker", "company_name",
            "isin", "lei", "language",
            "filing_date", "filing_time", "headline", "filing_type",
            "category", "document_url", "direct_download_url",
            "file_size", "num_pages", "price_sensitive",
            "downloaded", "download_path", "raw_metadata", "created_at",
        }
        assert required.issubset(cols)

    def test_filings_table_has_isin_column(self, mem_db):
        cols = {
            row[1].lower()
            for row in mem_db.execute("PRAGMA table_info(filings)").fetchall()
        }
        assert "isin" in cols

    def test_filings_table_has_lei_column(self, mem_db):
        cols = {
            row[1].lower()
            for row in mem_db.execute("PRAGMA table_info(filings)").fetchall()
        }
        assert "lei" in cols

    def test_filings_table_has_language_column(self, mem_db):
        cols = {
            row[1].lower()
            for row in mem_db.execute("PRAGMA table_info(filings)").fetchall()
        }
        assert "language" in cols

    def test_isin_index_exists(self, mem_db):
        indexes = {
            row[1]
            for row in mem_db.execute(
                "SELECT type, name FROM sqlite_master WHERE type='index'"
            ).fetchall()
        }
        assert "idx_filings_isin" in indexes

    def test_creates_crawl_log_table(self, mem_db):
        tables = {
            row[0]
            for row in mem_db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert "crawl_log" in tables

    def test_db_is_idempotent_on_repeated_open(self, tmp_path):
        """Opening the DB twice should not raise or corrupt schema."""
        db_path = tmp_path / "test_idempotent.db"
        c1 = get_db(db_path)
        c1.close()
        c2 = get_db(db_path)
        c2.close()

    def test_source_default_is_cninfo(self, mem_db, sample_filing):
        upsert_filing(mem_db, sample_filing)
        row = mem_db.execute(
            "SELECT source FROM filings WHERE filing_id=?",
            (sample_filing.filing_id,),
        ).fetchone()
        assert row["source"] == "cninfo"

    def test_country_default_is_cn(self, mem_db, sample_filing):
        upsert_filing(mem_db, sample_filing)
        row = mem_db.execute(
            "SELECT country FROM filings WHERE filing_id=?",
            (sample_filing.filing_id,),
        ).fetchone()
        assert row["country"] == "CN"


# ---------------------------------------------------------------------------
# Schema migration — pre-L3 column names → L3 names
# ---------------------------------------------------------------------------


class TestSchemaMigration:
    def _make_pre_l3_db(self, db_path: Path) -> sqlite3.Connection:
        """Build a database with the pre-L3 column names."""
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.executescript(
            """
            CREATE TABLE filings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                announcement_id TEXT UNIQUE,
                sec_code TEXT,
                sec_name TEXT,
                org_id TEXT,
                org_name TEXT,
                title TEXT,
                announcement_date TEXT,
                announcement_time_ms INTEGER,
                adjunct_url TEXT,
                adjunct_type TEXT,
                adjunct_size INTEGER,
                announcement_type TEXT,
                column_id TEXT,
                download_url TEXT,
                filing_type TEXT,
                downloaded INTEGER DEFAULT 0,
                local_path TEXT,
                first_seen TEXT
            );
            INSERT INTO filings
                (announcement_id, sec_code, sec_name, announcement_date,
                 title, announcement_type, adjunct_url, download_url,
                 filing_type, first_seen)
            VALUES
                ('PRE_L3_001', '000001', '测试', '2024-01-15',
                 '年度报告', 'category_ndbg_szsh',
                 'finalpage/2024-01-15/PRE_L3_001.PDF',
                 'http://static.cninfo.com.cn/finalpage/2024-01-15/PRE_L3_001.PDF',
                 'annual_report', '2024-01-15T00:00:00');
            """
        )
        conn.commit()
        conn.close()
        return db_path

    def test_migration_renames_announcement_id_to_filing_id(self, tmp_path):
        db_path = self._make_pre_l3_db(tmp_path / "pre_l3.db")
        conn = get_db(db_path)
        cols = {
            row[1].lower()
            for row in conn.execute("PRAGMA table_info(filings)").fetchall()
        }
        conn.close()
        assert "filing_id" in cols
        assert "announcement_id" not in cols

    def test_migration_renames_sec_code_to_ticker(self, tmp_path):
        db_path = self._make_pre_l3_db(tmp_path / "pre_l3.db")
        conn = get_db(db_path)
        cols = {
            row[1].lower()
            for row in conn.execute("PRAGMA table_info(filings)").fetchall()
        }
        conn.close()
        assert "ticker" in cols
        assert "sec_code" not in cols

    def test_migration_renames_title_to_headline(self, tmp_path):
        db_path = self._make_pre_l3_db(tmp_path / "pre_l3.db")
        conn = get_db(db_path)
        cols = {
            row[1].lower()
            for row in conn.execute("PRAGMA table_info(filings)").fetchall()
        }
        conn.close()
        assert "headline" in cols
        assert "title" not in cols

    def test_migration_renames_announcement_date_to_filing_date(self, tmp_path):
        db_path = self._make_pre_l3_db(tmp_path / "pre_l3.db")
        conn = get_db(db_path)
        cols = {
            row[1].lower()
            for row in conn.execute("PRAGMA table_info(filings)").fetchall()
        }
        conn.close()
        assert "filing_date" in cols
        assert "announcement_date" not in cols

    def test_migration_renames_local_path_to_download_path(self, tmp_path):
        db_path = self._make_pre_l3_db(tmp_path / "pre_l3.db")
        conn = get_db(db_path)
        cols = {
            row[1].lower()
            for row in conn.execute("PRAGMA table_info(filings)").fetchall()
        }
        conn.close()
        assert "download_path" in cols
        assert "local_path" not in cols

    def test_migration_preserves_existing_row_data(self, tmp_path):
        db_path = self._make_pre_l3_db(tmp_path / "pre_l3.db")
        conn = get_db(db_path)
        row = conn.execute(
            "SELECT filing_id, ticker, filing_date, headline FROM filings WHERE filing_id='PRE_L3_001'"
        ).fetchone()
        conn.close()
        assert row is not None
        assert row["ticker"] == "000001"
        assert row["filing_date"] == "2024-01-15"
        assert "年度报告" in row["headline"]

    def test_migration_adds_missing_l3_columns(self, tmp_path):
        db_path = self._make_pre_l3_db(tmp_path / "pre_l3.db")
        conn = get_db(db_path)
        cols = {
            row[1].lower()
            for row in conn.execute("PRAGMA table_info(filings)").fetchall()
        }
        conn.close()
        # Columns added by _COLUMN_MIGRATIONS
        assert "source" in cols
        assert "country" in cols
        assert "raw_metadata" in cols

    def test_migration_adds_isin_column(self, tmp_path):
        db_path = self._make_pre_l3_db(tmp_path / "pre_l3.db")
        conn = get_db(db_path)
        cols = {
            row[1].lower()
            for row in conn.execute("PRAGMA table_info(filings)").fetchall()
        }
        conn.close()
        assert "isin" in cols

    def test_migration_adds_lei_column(self, tmp_path):
        db_path = self._make_pre_l3_db(tmp_path / "pre_l3.db")
        conn = get_db(db_path)
        cols = {
            row[1].lower()
            for row in conn.execute("PRAGMA table_info(filings)").fetchall()
        }
        conn.close()
        assert "lei" in cols

    def test_migration_adds_language_column(self, tmp_path):
        db_path = self._make_pre_l3_db(tmp_path / "pre_l3.db")
        conn = get_db(db_path)
        cols = {
            row[1].lower()
            for row in conn.execute("PRAGMA table_info(filings)").fetchall()
        }
        conn.close()
        assert "language" in cols

    def test_migration_is_idempotent(self, tmp_path):
        """Running get_db() twice on the same pre-L3 file must not raise."""
        db_path = self._make_pre_l3_db(tmp_path / "pre_l3.db")
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
            "SELECT * FROM filings WHERE filing_id = ?",
            (sample_filing.filing_id,),
        ).fetchone()
        assert row is not None
        assert row["ticker"] == "000001"
        assert row["filing_type"] == "annual_report"

    def test_inserted_filing_has_created_at_timestamp(self, mem_db, sample_filing):
        upsert_filing(mem_db, sample_filing)
        row = mem_db.execute(
            "SELECT created_at FROM filings WHERE filing_id = ?",
            (sample_filing.filing_id,),
        ).fetchone()
        assert row is not None
        assert row["created_at"] is not None

    def test_downloaded_defaults_to_zero(self, mem_db, sample_filing):
        upsert_filing(mem_db, sample_filing)
        row = mem_db.execute(
            "SELECT downloaded FROM filings WHERE filing_id = ?",
            (sample_filing.filing_id,),
        ).fetchone()
        assert row["downloaded"] == 0

    def test_raw_metadata_is_stored_as_json(self, mem_db, sample_filing):
        upsert_filing(mem_db, sample_filing)
        row = mem_db.execute(
            "SELECT raw_metadata FROM filings WHERE filing_id = ?",
            (sample_filing.filing_id,),
        ).fetchone()
        assert row["raw_metadata"] is not None
        meta = json.loads(row["raw_metadata"])
        assert "org_id" in meta
        assert meta["org_id"] == sample_filing.org_id

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

    def test_isin_is_persisted(self, mem_db, sample_filing):
        upsert_filing(mem_db, sample_filing)
        row = mem_db.execute(
            "SELECT isin FROM filings WHERE filing_id=?",
            (sample_filing.filing_id,),
        ).fetchone()
        assert row["isin"] == sample_filing.isin

    def test_lei_is_persisted_as_none(self, mem_db, sample_filing):
        upsert_filing(mem_db, sample_filing)
        row = mem_db.execute(
            "SELECT lei FROM filings WHERE filing_id=?",
            (sample_filing.filing_id,),
        ).fetchone()
        assert row["lei"] is None

    def test_language_defaults_to_zh(self, mem_db, sample_filing):
        upsert_filing(mem_db, sample_filing)
        row = mem_db.execute(
            "SELECT language FROM filings WHERE filing_id=?",
            (sample_filing.filing_id,),
        ).fetchone()
        assert row["language"] == "zh"


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
        assert is_downloaded(mem_db, sample_filing.filing_id) is False

    def test_is_downloaded_true_after_marking(self, mem_db, sample_filing):
        upsert_filing(mem_db, sample_filing)
        mark_downloaded(mem_db, sample_filing.filing_id, "/tmp/test.pdf")
        assert is_downloaded(mem_db, sample_filing.filing_id) is True

    def test_mark_downloaded_sets_download_path(self, mem_db, sample_filing):
        upsert_filing(mem_db, sample_filing)
        mark_downloaded(mem_db, sample_filing.filing_id, "/tmp/report.pdf")
        row = mem_db.execute(
            "SELECT download_path FROM filings WHERE filing_id = ?",
            (sample_filing.filing_id,),
        ).fetchone()
        assert row["download_path"] == "/tmp/report.pdf"

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
        assert sample_filing.filing_id in known
        assert sample_filing_2.filing_id in known

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
        mark_downloaded(mem_db, sample_filing.filing_id, "/tmp/x.pdf")
        s = stats(mem_db)
        assert s["downloaded"] == 1
        assert s["pending"] == 0

    def test_stats_oldest_and_newest(self, mem_db, sample_filing, sample_filing_2):
        insert_batch(mem_db, [sample_filing, sample_filing_2])
        s = stats(mem_db)
        assert s["oldest"] == "2023-08-15"
        assert s["newest"] == "2024-03-30"

    def test_stats_includes_total_crawl_runs(self, mem_db):
        s = stats(mem_db)
        assert "total_crawl_runs" in s
        assert s["total_crawl_runs"] == 0

    def test_stats_crawl_runs_increments(self, mem_db):
        log_id = log_crawl_start(mem_db)
        log_crawl_complete(mem_db, log_id, 10, 5)
        s = stats(mem_db)
        assert s["total_crawl_runs"] == 1


# ---------------------------------------------------------------------------
# crawl_log helpers: log_crawl_start() / log_crawl_complete()
# ---------------------------------------------------------------------------


class TestCrawlLog:
    def test_log_crawl_start_returns_integer_id(self, mem_db):
        log_id = log_crawl_start(mem_db)
        assert isinstance(log_id, int)
        assert log_id > 0

    def test_log_crawl_start_creates_row(self, mem_db):
        log_crawl_start(mem_db)
        count = mem_db.execute("SELECT COUNT(*) FROM crawl_log").fetchone()[0]
        assert count == 1

    def test_log_crawl_start_sets_started_at(self, mem_db):
        log_crawl_start(mem_db)
        row = mem_db.execute("SELECT started_at FROM crawl_log").fetchone()
        assert row["started_at"] is not None

    def test_log_crawl_start_completed_at_is_null(self, mem_db):
        log_crawl_start(mem_db)
        row = mem_db.execute("SELECT completed_at FROM crawl_log").fetchone()
        assert row["completed_at"] is None

    def test_log_crawl_complete_sets_completed_at(self, mem_db):
        log_id = log_crawl_start(mem_db)
        log_crawl_complete(mem_db, log_id, 50, 10)
        row = mem_db.execute(
            "SELECT completed_at FROM crawl_log WHERE id=?", (log_id,)
        ).fetchone()
        assert row["completed_at"] is not None

    def test_log_crawl_complete_stores_counts(self, mem_db):
        log_id = log_crawl_start(mem_db)
        log_crawl_complete(mem_db, log_id, filings_found=100, filings_new=30, error_count=2)
        row = mem_db.execute(
            "SELECT filings_found, filings_new, error_count FROM crawl_log WHERE id=?",
            (log_id,),
        ).fetchone()
        assert row["filings_found"] == 100
        assert row["filings_new"] == 30
        assert row["error_count"] == 2

    def test_log_crawl_start_stores_parameters(self, mem_db):
        params = {"category": "annual", "column": "all"}
        log_crawl_start(mem_db, parameters=params)
        row = mem_db.execute("SELECT parameters FROM crawl_log").fetchone()
        stored = json.loads(row["parameters"])
        assert stored["category"] == "annual"


# ---------------------------------------------------------------------------
# detect_health()
# ---------------------------------------------------------------------------


class TestDetectHealth:
    def test_empty_crawl_log_returns_empty(self, mem_db):
        assert detect_health(mem_db) == "empty"

    def test_incomplete_crawl_returns_error(self, mem_db):
        """Started but never completed crawl → 'error'."""
        log_crawl_start(mem_db)
        # Do NOT call log_crawl_complete
        assert detect_health(mem_db) == "error"

    def test_recent_successful_crawl_returns_ok(self, mem_db):
        log_id = log_crawl_start(mem_db)
        log_crawl_complete(mem_db, log_id, filings_found=100, filings_new=20, error_count=0)
        assert detect_health(mem_db) == "ok"

    def test_stale_crawl_returns_stale(self, mem_db):
        """Crawl completed more than 48 hours ago → 'stale'."""
        log_id = log_crawl_start(mem_db)
        # Write a completed_at timestamp 72 hours in the past
        old_ts = (datetime.now() - timedelta(hours=72)).isoformat()
        mem_db.execute(
            "UPDATE crawl_log SET completed_at=?, filings_found=100 WHERE id=?",
            (old_ts, log_id),
        )
        mem_db.commit()
        assert detect_health(mem_db) == "stale"

    def test_high_error_rate_returns_degraded(self, mem_db):
        """Error rate > 10% in completed crawl → 'degraded'."""
        log_id = log_crawl_start(mem_db)
        log_crawl_complete(
            mem_db, log_id, filings_found=100, filings_new=80, error_count=20
        )
        assert detect_health(mem_db) == "degraded"

    def test_low_error_rate_returns_ok(self, mem_db):
        """Error rate <= 10% → 'ok'."""
        log_id = log_crawl_start(mem_db)
        log_crawl_complete(
            mem_db, log_id, filings_found=100, filings_new=90, error_count=9
        )
        assert detect_health(mem_db) == "ok"

    def test_zero_filings_found_with_errors_returns_ok(self, mem_db):
        """When filings_found=0, error rate is undefined — should not be degraded."""
        log_id = log_crawl_start(mem_db)
        log_crawl_complete(
            mem_db, log_id, filings_found=0, filings_new=0, error_count=5
        )
        # 0 filings found means we can't compute a ratio — defaults to ok
        assert detect_health(mem_db) == "ok"

    def test_uses_most_recent_crawl_log_entry(self, mem_db):
        """Health should reflect the most recent crawl, not the first."""
        # First crawl: incomplete (error)
        log_crawl_start(mem_db)
        # Second crawl: successful
        log_id2 = log_crawl_start(mem_db)
        log_crawl_complete(mem_db, log_id2, filings_found=50, filings_new=10, error_count=0)
        assert detect_health(mem_db) == "ok"


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

    def test_json_filing_uses_l3_column_names(self, mem_db, sample_filing, tmp_path):
        """Exported filings should expose filing_id, ticker, headline etc."""
        upsert_filing(mem_db, sample_filing)
        out_path = str(tmp_path / "out.json")
        export_json(mem_db, out_path)
        data = json.loads(Path(out_path).read_text(encoding="utf-8"))
        f = data["filings"][0]
        assert "filing_id" in f
        assert "ticker" in f
        assert "headline" in f
        assert "filing_date" in f
