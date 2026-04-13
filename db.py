"""
db.py — SQLite persistence layer for the CNINFO scraper.

Handles schema creation, upserts, download tracking, and JSON export.
All SQLite writes must happen on the thread that owns the connection
(or use check_same_thread=False + external locking for concurrent workers).

L3 schema: table named ``filings`` with standardised column names compatible
with the multi-country scraper spec.  A ``crawl_log`` table records each crawl
run and drives the health-detection logic used by ``stats --json``.
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
    filing_id           TEXT PRIMARY KEY,
    source              TEXT DEFAULT 'cninfo',
    country             TEXT DEFAULT 'CN',
    ticker              TEXT,
    company_name        TEXT,
    filing_date         TEXT,
    filing_time         TEXT,
    headline            TEXT,
    filing_type         TEXT DEFAULT 'other',
    category            TEXT,
    document_url        TEXT,
    direct_download_url TEXT,
    file_size           TEXT,
    num_pages           INTEGER,
    price_sensitive     INTEGER DEFAULT 0,
    downloaded          INTEGER DEFAULT 0,
    download_path       TEXT,
    raw_metadata        TEXT,
    created_at          TEXT
);

CREATE INDEX IF NOT EXISTS idx_filing_id  ON filings(filing_id);
CREATE INDEX IF NOT EXISTS idx_downloaded ON filings(downloaded);
CREATE INDEX IF NOT EXISTS idx_date       ON filings(filing_date);
CREATE INDEX IF NOT EXISTS idx_ticker     ON filings(ticker);
CREATE INDEX IF NOT EXISTS idx_type       ON filings(filing_type);

CREATE TABLE IF NOT EXISTS crawl_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at      TEXT NOT NULL,
    completed_at    TEXT,
    filings_found   INTEGER DEFAULT 0,
    filings_new     INTEGER DEFAULT 0,
    error_count     INTEGER DEFAULT 0,
    parameters      TEXT
);
"""

# ---------------------------------------------------------------------------
# Backwards-compatible migrations
#
# Applied on every get_db() call (idempotent via column-existence check).
# Handles databases created before the L3 rename.
# ---------------------------------------------------------------------------

# Old table name → new table name migration is handled procedurally in
# _apply_migrations() below.  The SQL list here covers column additions only.
_COLUMN_MIGRATIONS: list[tuple[str, str, str]] = [
    # (table, column_name, ADD COLUMN sql)
    ("filings", "source",              "ALTER TABLE filings ADD COLUMN source TEXT DEFAULT 'cninfo'"),
    ("filings", "country",             "ALTER TABLE filings ADD COLUMN country TEXT DEFAULT 'CN'"),
    ("filings", "filing_time",         "ALTER TABLE filings ADD COLUMN filing_time TEXT"),
    ("filings", "num_pages",           "ALTER TABLE filings ADD COLUMN num_pages INTEGER"),
    ("filings", "price_sensitive",     "ALTER TABLE filings ADD COLUMN price_sensitive INTEGER DEFAULT 0"),
    ("filings", "raw_metadata",        "ALTER TABLE filings ADD COLUMN raw_metadata TEXT"),
    ("filings", "file_size",           "ALTER TABLE filings ADD COLUMN file_size TEXT"),
    # crawl_log columns (rare — usually created fresh by SCHEMA_SQL)
]

# Pre-L3 column renames (SQLite 3.25+).
# Each entry: (old_col, new_col, rename sql)
_COLUMN_RENAMES: list[tuple[str, str, str]] = [
    ("announcement_id",    "filing_id",           "ALTER TABLE filings RENAME COLUMN announcement_id TO filing_id"),
    ("sec_code",           "ticker",              "ALTER TABLE filings RENAME COLUMN sec_code TO ticker"),
    ("sec_name",           "company_name",        "ALTER TABLE filings RENAME COLUMN sec_name TO company_name"),
    ("announcement_date",  "filing_date",         "ALTER TABLE filings RENAME COLUMN announcement_date TO filing_date"),
    ("title",              "headline",            "ALTER TABLE filings RENAME COLUMN title TO headline"),
    ("announcement_type",  "category",            "ALTER TABLE filings RENAME COLUMN announcement_type TO category"),
    ("adjunct_url",        "document_url",        "ALTER TABLE filings RENAME COLUMN adjunct_url TO document_url"),
    ("download_url",       "direct_download_url", "ALTER TABLE filings RENAME COLUMN download_url TO direct_download_url"),
    ("adjunct_size",       "file_size",           "ALTER TABLE filings RENAME COLUMN adjunct_size TO file_size"),
    ("local_path",         "download_path",       "ALTER TABLE filings RENAME COLUMN local_path TO download_path"),
    ("first_seen",         "created_at",          "ALTER TABLE filings RENAME COLUMN first_seen TO created_at"),
]


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Filing:
    """Immutable representation of a single CNINFO filing row (L3 schema)."""

    filing_id: str
    ticker: str
    company_name: str
    org_id: str
    org_name: str
    headline: str
    filing_date: str
    announcement_time_ms: int
    document_url: str
    adjunct_type: str
    file_size: int | str
    category: str
    column_id: str
    direct_download_url: str
    filing_type: str = "other"
    source: str = "cninfo"
    country: str = "CN"


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

    Applies backwards-compatible migrations on every call so pre-L3 databases
    gain the new column names and any missing columns without full recreation.

    Args:
        db_path: Path to the SQLite file, or ":memory:" for tests.

    Returns:
        An open sqlite3.Connection with row_factory set to sqlite3.Row.
    """
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    # Migrations must run before SCHEMA_SQL on pre-L3 DBs so that the
    # CREATE TABLE IF NOT EXISTS statement sees the already-renamed table.
    # On fresh databases the filings table doesn't exist yet so migrations
    # are no-ops, then SCHEMA_SQL creates it correctly.
    _apply_migrations(conn)
    conn.executescript(SCHEMA_SQL)
    conn.commit()
    return conn


def _get_table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    """Return the lowercase set of column names for *table*, or empty set if absent."""
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    if table not in tables:
        return set()
    return {
        row[1].lower()
        for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
    }


def _apply_migrations(conn: sqlite3.Connection) -> None:
    """Apply backwards-compatible schema migrations (idempotent).

    Order:
    1. If an ``announcements`` table exists (very old schema), rename to ``filings``.
    2. Rename old column names to L3 names (SQLite 3.25+ RENAME COLUMN).
    3. Add any missing columns with their defaults.
    """
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }

    # Step 1: rename legacy table name
    if "announcements" in tables and "filings" not in tables:
        log.info("DB migration: renaming table 'announcements' → 'filings'")
        try:
            conn.execute("ALTER TABLE announcements RENAME TO filings")
            conn.commit()
        except sqlite3.OperationalError as exc:
            log.warning("Migration skipped (table rename): %s", exc)

    # Step 2: rename columns (pre-L3 → L3)
    cols = _get_table_columns(conn, "filings")
    for old_col, new_col, sql in _COLUMN_RENAMES:
        if old_col.lower() in cols and new_col.lower() not in cols:
            log.info("DB migration: renaming column %r → %r", old_col, new_col)
            try:
                conn.execute(sql)
                conn.commit()
                # Refresh cols after each rename
                cols = _get_table_columns(conn, "filings")
            except sqlite3.OperationalError as exc:
                log.warning("Migration skipped (rename %s → %s): %s", old_col, new_col, exc)

    # Step 3: add missing columns (only when the table already exists — fresh
    # databases will have the columns created by SCHEMA_SQL momentarily)
    for table, col_name, sql in _COLUMN_MIGRATIONS:
        existing = _get_table_columns(conn, table)
        if not existing:
            # Table doesn't exist yet; SCHEMA_SQL will create it with all columns.
            continue
        if col_name.lower() not in existing:
            log.info("DB migration: adding column %r to %r", col_name, table)
            try:
                conn.execute(sql)
                conn.commit()
            except sqlite3.OperationalError as exc:
                log.warning("Migration skipped (%s.%s): %s", table, col_name, exc)


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
    raw = json.dumps(
        {
            "org_id": filing.org_id,
            "org_name": filing.org_name,
            "announcement_time_ms": filing.announcement_time_ms,
            "adjunct_type": filing.adjunct_type,
            "column_id": filing.column_id,
        },
        ensure_ascii=False,
    )
    cur = conn.execute(
        """
        INSERT OR IGNORE INTO filings
            (filing_id, source, country, ticker, company_name,
             headline, filing_date,
             document_url, file_size,
             category, direct_download_url,
             filing_type, raw_metadata, created_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            filing.filing_id,
            filing.source,
            filing.country,
            filing.ticker,
            filing.company_name,
            filing.headline,
            filing.filing_date,
            filing.document_url,
            str(filing.file_size) if filing.file_size else None,
            filing.category,
            filing.direct_download_url,
            filing.filing_type,
            raw,
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
    conn: sqlite3.Connection, filing_id: str, path: str
) -> None:
    """Set downloaded=1 and download_path for a filing after successful download.

    Args:
        conn:      SQLite connection.
        filing_id: CNINFO filing identifier.
        path:      Local file path where the document was saved.
    """
    conn.execute(
        "UPDATE filings SET downloaded=1, download_path=? WHERE filing_id=?",
        (path, filing_id),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Read helpers
# ---------------------------------------------------------------------------


def is_downloaded(conn: sqlite3.Connection, filing_id: str) -> bool:
    """Return True if the filing has already been downloaded.

    Args:
        conn:      SQLite connection.
        filing_id: CNINFO filing identifier.

    Returns:
        True if downloaded=1 in the DB, False otherwise.
    """
    row = conn.execute(
        "SELECT downloaded FROM filings WHERE filing_id=?",
        (filing_id,),
    ).fetchone()
    return bool(row and row[0])


def get_known_ids(conn: sqlite3.Connection) -> set[str]:
    """Return all filing_ids currently stored in the cache.

    Used by the monitor command to detect new filings.

    Args:
        conn: SQLite connection.

    Returns:
        A set of filing_id strings.
    """
    return {
        r[0]
        for r in conn.execute("SELECT filing_id FROM filings").fetchall()
    }


def get_last_crawl_date(conn: sqlite3.Connection) -> str | None:
    """Return the most recent filing_date in the cache.

    Used by --incremental mode to skip re-crawling recent dates.

    Args:
        conn: SQLite connection.

    Returns:
        ISO date string "YYYY-MM-DD" or None if the cache is empty.
    """
    row = conn.execute(
        "SELECT MAX(filing_date) FROM filings"
    ).fetchone()
    return row[0] if row and row[0] else None


def stats(conn: sqlite3.Connection) -> dict[str, Any]:
    """Return aggregate statistics about the filing cache.

    Args:
        conn: SQLite connection.

    Returns:
        Dict with keys: total, downloaded, pending, unique_companies,
        oldest, newest, total_crawl_runs.
    """
    row = conn.execute(
        """
        SELECT
            COUNT(*) AS total,
            COALESCE(SUM(CASE WHEN downloaded=1 THEN 1 ELSE 0 END), 0) AS downloaded,
            COALESCE(SUM(CASE WHEN downloaded=0 THEN 1 ELSE 0 END), 0) AS pending,
            COUNT(DISTINCT ticker) AS unique_companies,
            MIN(filing_date) AS oldest,
            MAX(filing_date) AS newest
        FROM filings
        """
    ).fetchone()
    result = dict(row)

    # crawl_log stats
    crawl_row = conn.execute(
        "SELECT COUNT(*) AS total_runs FROM crawl_log"
    ).fetchone()
    result["total_crawl_runs"] = crawl_row[0] if crawl_row else 0

    return result


def log_crawl_start(conn: sqlite3.Connection, parameters: dict[str, Any] | None = None) -> int:
    """Insert a crawl_log row at the start of a crawl run.

    Args:
        conn:       SQLite connection.
        parameters: Optional dict of crawl parameters to record.

    Returns:
        The rowid of the new crawl_log entry.
    """
    now = datetime.now().isoformat()
    params_json = json.dumps(parameters or {}, ensure_ascii=False)
    cur = conn.execute(
        "INSERT INTO crawl_log (started_at, parameters) VALUES (?, ?)",
        (now, params_json),
    )
    conn.commit()
    return cur.lastrowid  # type: ignore[return-value]


def log_crawl_complete(
    conn: sqlite3.Connection,
    log_id: int,
    filings_found: int,
    filings_new: int,
    error_count: int = 0,
) -> None:
    """Update a crawl_log row when the crawl finishes.

    Args:
        conn:          SQLite connection.
        log_id:        Row ID from log_crawl_start().
        filings_found: Total filings seen in this run.
        filings_new:   New filings inserted.
        error_count:   Number of recoverable errors during the run.
    """
    now = datetime.now().isoformat()
    conn.execute(
        """
        UPDATE crawl_log
        SET completed_at=?, filings_found=?, filings_new=?, error_count=?
        WHERE id=?
        """,
        (now, filings_found, filings_new, error_count, log_id),
    )
    conn.commit()


def detect_health(conn: sqlite3.Connection) -> str:
    """Derive a health label from the most recent crawl_log entry.

    Health states:
    - ``empty``:    no crawl_log entries at all.
    - ``error``:    last crawl started but never completed (completed_at IS NULL).
    - ``stale``:    last completed crawl was more than 48 hours ago.
    - ``degraded``: error_count / filings_found > 10 % in the last completed run.
    - ``ok``:       last crawl completed within 48 h with < 10 % error rate.

    Args:
        conn: SQLite connection.

    Returns:
        One of ``"ok"``, ``"stale"``, ``"degraded"``, ``"error"``, ``"empty"``.
    """
    row = conn.execute(
        """
        SELECT started_at, completed_at, filings_found, error_count
        FROM crawl_log
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()

    if row is None:
        return "empty"

    if row["completed_at"] is None:
        return "error"

    try:
        completed = datetime.fromisoformat(row["completed_at"])
        age_hours = (datetime.now() - completed).total_seconds() / 3600
    except (ValueError, TypeError):
        return "error"

    if age_hours > 48:
        return "stale"

    filings_found: int = row["filings_found"] or 0
    error_count: int = row["error_count"] or 0
    if filings_found > 0 and (error_count / filings_found) > 0.10:
        return "degraded"

    return "ok"


def export_json(conn: sqlite3.Connection, path: str) -> None:
    """Dump all filings to a JSON file.

    Args:
        conn: SQLite connection.
        path: Output file path.
    """
    rows = conn.execute(
        "SELECT * FROM filings ORDER BY filing_date DESC"
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
