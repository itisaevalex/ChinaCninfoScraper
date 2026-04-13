"""
db.py — SQLite persistence layer for the CNINFO scraper.

Handles schema creation, upserts, download tracking, and JSON export.
All SQLite writes must happen on the thread that owns the connection
(or use check_same_thread=False + external locking for concurrent workers).
"""

from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

log = logging.getLogger("cninfo")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BASE_URL = "http://www.cninfo.com.cn"
DB_FILE = "filings_cache.db"

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS filings (
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

CREATE INDEX IF NOT EXISTS idx_ann_id ON filings(announcement_id);
CREATE INDEX IF NOT EXISTS idx_dl ON filings(downloaded);
CREATE INDEX IF NOT EXISTS idx_date ON filings(announcement_date);
CREATE INDEX IF NOT EXISTS idx_sec_code ON filings(sec_code);
CREATE INDEX IF NOT EXISTS idx_filing_type ON filings(filing_type);
"""

# Incremental migrations applied on every open (idempotent)
MIGRATIONS: list[tuple[str, str]] = [
    (
        "filing_type",
        "ALTER TABLE filings ADD COLUMN filing_type TEXT",
    ),
]


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Filing:
    """Immutable representation of a single CNINFO filing row."""

    announcement_id: str
    sec_code: str
    sec_name: str
    org_id: str
    org_name: str
    title: str
    announcement_date: str
    announcement_time_ms: int
    adjunct_url: str
    adjunct_type: str
    adjunct_size: int
    announcement_type: str
    column_id: str
    download_url: str
    filing_type: str = "other"


@dataclass(frozen=True)
class CrawlResult:
    """Immutable summary of a completed crawl run."""

    filings_found: int
    filings_new: int
    filings_downloaded: int
    elapsed_seconds: float


# ---------------------------------------------------------------------------
# Connection factory
# ---------------------------------------------------------------------------


def get_db(db_path: str | Path = DB_FILE) -> sqlite3.Connection:
    """Open (or create) the SQLite cache and ensure the schema exists.

    Applies incremental migrations on every call so existing databases
    gain new columns without a full schema recreation.

    Args:
        db_path: Path to the SQLite file, or ":memory:" for tests.

    Returns:
        An open sqlite3.Connection with row_factory set to sqlite3.Row.
    """
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.executescript(SCHEMA_SQL)
    conn.commit()
    _apply_migrations(conn)
    return conn


def _apply_migrations(conn: sqlite3.Connection) -> None:
    """Apply ALTER TABLE migrations that may not exist yet (idempotent)."""
    existing_cols = {
        row[1].lower()
        for row in conn.execute("PRAGMA table_info(filings)").fetchall()
    }
    for col_name, sql in MIGRATIONS:
        if col_name.lower() not in existing_cols:
            log.info("DB migration: adding column %r", col_name)
            try:
                conn.execute(sql)
                conn.commit()
            except sqlite3.OperationalError as exc:
                log.warning("Migration skipped (%s): %s", col_name, exc)


# ---------------------------------------------------------------------------
# Write helpers
# ---------------------------------------------------------------------------


def upsert_filing(conn: sqlite3.Connection, filing: Filing) -> bool:
    """Insert a filing if not already present. Returns True when newly inserted.

    Uses INSERT OR IGNORE so existing rows are never overwritten (idempotent).

    Args:
        conn:   SQLite connection.
        filing: Immutable Filing dataclass.

    Returns:
        True if the row was inserted, False if it already existed.
    """
    now = datetime.now().isoformat()
    cur = conn.execute(
        """
        INSERT OR IGNORE INTO filings
            (announcement_id, sec_code, sec_name, org_id, org_name,
             title, announcement_date, announcement_time_ms,
             adjunct_url, adjunct_type, adjunct_size,
             announcement_type, column_id, download_url,
             filing_type, first_seen)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            filing.announcement_id,
            filing.sec_code,
            filing.sec_name,
            filing.org_id,
            filing.org_name,
            filing.title,
            filing.announcement_date,
            filing.announcement_time_ms,
            filing.adjunct_url,
            filing.adjunct_type,
            filing.adjunct_size,
            filing.announcement_type,
            filing.column_id,
            filing.download_url,
            filing.filing_type,
            now,
        ),
    )
    conn.commit()
    return cur.rowcount > 0


def insert_batch(conn: sqlite3.Connection, filings: list[Filing]) -> int:
    """Insert multiple filings, skipping duplicates.

    Args:
        conn:    SQLite connection.
        filings: List of Filing dataclass instances.

    Returns:
        Count of newly inserted rows.
    """
    new_count = 0
    for filing in filings:
        if upsert_filing(conn, filing):
            new_count += 1
    return new_count


def mark_downloaded(
    conn: sqlite3.Connection, announcement_id: str, path: str
) -> None:
    """Set downloaded=1 and local_path for a filing after successful download.

    Args:
        conn:            SQLite connection.
        announcement_id: CNINFO announcement identifier.
        path:            Local file path where the document was saved.
    """
    conn.execute(
        "UPDATE filings SET downloaded=1, local_path=? WHERE announcement_id=?",
        (path, announcement_id),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Read helpers
# ---------------------------------------------------------------------------


def is_downloaded(conn: sqlite3.Connection, announcement_id: str) -> bool:
    """Return True if the filing has already been downloaded.

    Args:
        conn:            SQLite connection.
        announcement_id: CNINFO announcement identifier.

    Returns:
        True if downloaded=1 in the DB, False otherwise.
    """
    row = conn.execute(
        "SELECT downloaded FROM filings WHERE announcement_id=?",
        (announcement_id,),
    ).fetchone()
    return bool(row and row[0])


def get_known_ids(conn: sqlite3.Connection) -> set[str]:
    """Return all announcement_ids currently stored in the cache.

    Used by the monitor command to detect new filings.

    Args:
        conn: SQLite connection.

    Returns:
        A set of announcement_id strings.
    """
    return {
        r[0]
        for r in conn.execute("SELECT announcement_id FROM filings").fetchall()
    }


def get_last_crawl_date(conn: sqlite3.Connection) -> str | None:
    """Return the most recent announcement_date in the cache.

    Used by --incremental mode to skip re-crawling recent dates.

    Args:
        conn: SQLite connection.

    Returns:
        ISO date string "YYYY-MM-DD" or None if the cache is empty.
    """
    row = conn.execute(
        "SELECT MAX(announcement_date) FROM filings"
    ).fetchone()
    return row[0] if row and row[0] else None


def stats(conn: sqlite3.Connection) -> dict[str, Any]:
    """Return aggregate statistics about the filing cache.

    Args:
        conn: SQLite connection.

    Returns:
        Dict with keys: total, downloaded, pending, unique_companies,
        oldest, newest.
    """
    row = conn.execute(
        """
        SELECT
            COUNT(*) AS total,
            COALESCE(SUM(CASE WHEN downloaded=1 THEN 1 ELSE 0 END), 0) AS downloaded,
            COALESCE(SUM(CASE WHEN downloaded=0 THEN 1 ELSE 0 END), 0) AS pending,
            COUNT(DISTINCT sec_code) AS unique_companies,
            MIN(announcement_date) AS oldest,
            MAX(announcement_date) AS newest
        FROM filings
        """
    ).fetchone()
    return dict(row)


def export_json(conn: sqlite3.Connection, path: str) -> None:
    """Dump all filings to a JSON file.

    Args:
        conn: SQLite connection.
        path: Output file path.
    """
    rows = conn.execute(
        "SELECT * FROM filings ORDER BY announcement_date DESC"
    ).fetchall()
    payload = {
        "metadata": {
            "source": BASE_URL,
            "exported_at": datetime.now().isoformat(),
            "total": len(rows),
            "stats": stats(conn),
        },
        "filings": [dict(r) for r in rows],
    }
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, ensure_ascii=False)
    log.info("Exported %d filings to %s", len(rows), path)
