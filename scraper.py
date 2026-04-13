"""
CNINFO Chinese Securities Filing Scraper
=========================================

Production scraper for China's CNINFO (巨潮资讯网) filing system.
100% pure Python — no browser required.

Architecture:
  1. Plain requests library — no TLS fingerprinting or WAF to bypass
  2. Stateless JSON API: POST /new/hisAnnouncement/query
  3. Direct PDF download from static.cninfo.com.cn (permanent URLs)
  4. Date-range splitting to bypass 100-page API cap
  5. SQLite cache for dedup and download tracking

Usage:
  python scraper.py crawl --max-pages 10
  python scraper.py crawl --max-pages 50 --download
  python scraper.py crawl --category annual --date-from 2024-01-01 --date-to 2024-12-31
  python scraper.py monitor --interval 300 --download
  python scraper.py export --output filings.json
  python scraper.py stats
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import re
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Any

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_URL = "http://www.cninfo.com.cn"
STATIC_URL = "http://static.cninfo.com.cn"
QUERY_URL = f"{BASE_URL}/new/hisAnnouncement/query"
STOCK_LIST_URL = f"{BASE_URL}/new/data/szse_stock.json"

DB_FILE = "filings_cache.db"
PAGE_SIZE = 30  # Server-enforced max
MAX_API_PAGES = 100  # CNINFO silently caps at ~100 pages

DELAY_BETWEEN_PAGES = 1.0
DELAY_BETWEEN_DOWNLOADS = 0.3
DELAY_JITTER = 0.5  # Random jitter added to delays

REQUEST_HEADERS = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Host": "www.cninfo.com.cn",
    "Origin": "http://www.cninfo.com.cn",
    "Referer": "http://www.cninfo.com.cn/new/commonUrl/pageOfSearch?url=disclosure/list/search",
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "X-Requested-With": "XMLHttpRequest",
}

# Filing category codes
CATEGORIES = {
    "annual": "category_ndbg_szsh",
    "semi_annual": "category_bndbg_szsh",
    "q1": "category_yjdbg_szsh",
    "q3": "category_sjdbg_szsh",
    "ipo": "category_scgkfx_szsh",
    "rights_issue": "category_pg_szsh",
    "additional_offering": "category_zf_szsh",
    "convertible_bond": "category_kzhz_szsh",
    "board_announcement": "category_dshgg_szsh",
    "shareholder_meeting": "category_gddh_szsh",
    "daily_operations": "category_rcjy_szsh",
    "equity_distribution": "category_qyfpxzcs_szsh",
    "corporate_governance": "category_gszl_szsh",
    "earnings_forecast": "category_yjygjxz_szsh",
    "risk_warning": "category_fxts_szsh",
    "delisting": "category_tbclts_szsh",
}

# Exchange/column codes
COLUMNS = {
    "all": "szse",       # Combined: Shenzhen + Shanghai + Beijing
    "shanghai": "sse",
    "hongkong": "hke",
    "third_board": "third",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("cninfo")


# ---------------------------------------------------------------------------
# API Client
# ---------------------------------------------------------------------------


def create_session() -> requests.Session:
    """Create a requests session with retry adapter."""
    session = requests.Session()
    session.headers.update(REQUEST_HEADERS)

    adapter = requests.adapters.HTTPAdapter(
        max_retries=requests.adapters.Retry(
            total=3,
            backoff_factor=1.0,
            status_forcelist=[429, 500, 502, 503, 504],
        ),
        pool_maxsize=10,
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    return session


def query_announcements(
    session: requests.Session,
    page_num: int = 1,
    stock: str = "",
    category: str = "",
    column: str = "szse",
    plate: str = "",
    search_key: str = "",
    date_range: str = "",
    sort_name: str = "",
    sort_type: str = "",
) -> dict[str, Any]:
    """Query the CNINFO announcement API. Returns parsed JSON response."""
    data = {
        "pageNum": page_num,
        "pageSize": PAGE_SIZE,
        "column": column,
        "tabName": "fulltext",
        "plate": plate,
        "stock": stock,
        "searchkey": search_key,
        "secid": "",
        "category": category,
        "trade": "",
        "seDate": date_range,
        "sortName": sort_name,
        "sortType": sort_type,
        "isHLtitle": "false",
    }

    resp = session.post(QUERY_URL, data=data, timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_stock_list(session: requests.Session) -> list[dict]:
    """Fetch the full stock list with orgId mappings."""
    resp = session.get(STOCK_LIST_URL, timeout=30)
    resp.raise_for_status()
    return resp.json().get("stockList", [])


# ---------------------------------------------------------------------------
# Filing Parsing
# ---------------------------------------------------------------------------


def parse_announcements(api_response: dict) -> list[dict[str, Any]]:
    """Extract normalized filing dicts from API response."""
    announcements = api_response.get("announcements") or []
    filings = []

    for ann in announcements:
        adjunct_url = ann.get("adjunctUrl", "")
        if not adjunct_url:
            continue

        # announcementTime is milliseconds since epoch
        ts_ms = ann.get("announcementTime", 0)
        announcement_date = ""
        if ts_ms:
            announcement_date = datetime.fromtimestamp(ts_ms / 1000).strftime("%Y-%m-%d")

        # Clean title: remove <em> highlight tags if present
        title = ann.get("announcementTitle", "")
        title = re.sub(r"</?em>", "", title)

        filing = {
            "sec_code": ann.get("secCode", ""),
            "sec_name": ann.get("secName", ""),
            "org_id": ann.get("orgId", ""),
            "org_name": ann.get("orgName", ""),
            "announcement_id": ann.get("announcementId", ""),
            "title": title,
            "announcement_date": announcement_date,
            "announcement_time_ms": ts_ms,
            "adjunct_url": adjunct_url,
            "adjunct_type": ann.get("adjunctType", "PDF"),
            "adjunct_size": ann.get("adjunctSize", 0),
            "announcement_type": ann.get("announcementType", ""),
            "column_id": ann.get("columnId", ""),
            "download_url": f"{STATIC_URL}/{adjunct_url}",
        }
        filings.append(filing)

    return filings


def get_pagination_info(api_response: dict) -> tuple[int, int, bool]:
    """Extract pagination info: (total_announcements, total_pages, has_more)."""
    total = api_response.get("totalAnnouncement", 0)
    pages = api_response.get("totalpages", 0)
    has_more = api_response.get("hasMore", False)
    return total, pages, has_more


# ---------------------------------------------------------------------------
# Date Range Splitting (bypass 100-page cap)
# ---------------------------------------------------------------------------


def generate_date_ranges(
    date_from: str, date_to: str, interval_days: int = 1
) -> list[str]:
    """Generate date range strings for splitting large queries.

    CNINFO caps results at ~100 pages. Splitting into smaller date
    ranges ensures we get all results.

    Returns list of "YYYY-MM-DD~YYYY-MM-DD" strings.
    """
    start = datetime.strptime(date_from, "%Y-%m-%d")
    end = datetime.strptime(date_to, "%Y-%m-%d")
    ranges = []

    current = start
    while current <= end:
        range_end = min(current + timedelta(days=interval_days - 1), end)
        ranges.append(f"{current.strftime('%Y-%m-%d')}~{range_end.strftime('%Y-%m-%d')}")
        current = range_end + timedelta(days=1)

    return ranges


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------


DOWNLOAD_HEADERS = {
    "Accept": "application/pdf, application/octet-stream, */*",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
}


def download_filings(
    session: requests.Session,
    filings: list[dict],
    doc_dir: str,
    cache: FilingCache,
    parallel: int = 5,
) -> int:
    """Download documents for a list of filings. Returns count downloaded."""
    to_download = [
        f for f in filings
        if f.get("download_url") and not cache.is_downloaded(f["announcement_id"])
    ]
    if not to_download:
        return 0

    results: list[tuple[str, str]] = []  # (announcement_id, filepath) pairs

    def _download_one(filing: dict) -> tuple[str, str] | None:
        url = filing["download_url"]
        ann_id = filing["announcement_id"]
        try:
            # Use clean headers for static CDN — API headers cause 404
            resp = requests.get(url, headers=DOWNLOAD_HEADERS, timeout=120)
            if resp.status_code != 200:
                log.warning("Download failed (%d): %s", resp.status_code, url)
                return None

            # Build filename: secCode_title.ext
            sec_code = filing.get("sec_code", "unknown")
            title = filing.get("title", "doc")
            ext = filing.get("adjunct_type", "PDF").upper()

            # Sanitize filename
            safe_title = re.sub(r'[<>:"/\\|?*\x00-\x1f]', '_', title)
            safe_title = safe_title.replace("*", "＊")[:80]
            filename = f"{sec_code}_{safe_title}.{ext}"

            filepath = os.path.join(doc_dir, filename)
            with open(filepath, "wb") as fh:
                fh.write(resp.content)

            time.sleep(DELAY_BETWEEN_DOWNLOADS + random.uniform(0, DELAY_JITTER))
            return (ann_id, filepath)
        except Exception as e:
            log.warning("Download error for %s: %s", ann_id, e)
            return None

    if parallel > 1 and len(to_download) > 1:
        with ThreadPoolExecutor(max_workers=min(parallel, len(to_download))) as pool:
            futs = {pool.submit(_download_one, f): f for f in to_download}
            for fut in as_completed(futs):
                result = fut.result()
                if result:
                    results.append(result)
    else:
        for f in to_download:
            result = _download_one(f)
            if result:
                results.append(result)

    # Update cache from main thread (SQLite thread safety)
    for ann_id, path in results:
        cache.mark_downloaded(ann_id, path)

    return len(results)


# ---------------------------------------------------------------------------
# SQLite Cache
# ---------------------------------------------------------------------------


class FilingCache:
    def __init__(self, db_path: str = DB_FILE):
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript("""
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
                downloaded INTEGER DEFAULT 0,
                local_path TEXT,
                first_seen TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_ann_id ON filings(announcement_id);
            CREATE INDEX IF NOT EXISTS idx_dl ON filings(downloaded);
            CREATE INDEX IF NOT EXISTS idx_date ON filings(announcement_date);
            CREATE INDEX IF NOT EXISTS idx_sec_code ON filings(sec_code);
        """)
        self.conn.commit()

    def insert_batch(self, filings: list[dict]) -> int:
        """Insert filings, ignoring duplicates. Returns count of new rows."""
        before = self.conn.execute("SELECT COUNT(*) FROM filings").fetchone()[0]
        now = datetime.now().isoformat()
        for f in filings:
            ann_id = f.get("announcement_id", "")
            if not ann_id:
                continue
            self.conn.execute(
                """INSERT OR IGNORE INTO filings
                   (announcement_id, sec_code, sec_name, org_id, org_name,
                    title, announcement_date, announcement_time_ms,
                    adjunct_url, adjunct_type, adjunct_size,
                    announcement_type, column_id, download_url, first_seen)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    ann_id, f.get("sec_code", ""), f.get("sec_name", ""),
                    f.get("org_id", ""), f.get("org_name", ""),
                    f.get("title", ""), f.get("announcement_date", ""),
                    f.get("announcement_time_ms", 0),
                    f.get("adjunct_url", ""), f.get("adjunct_type", ""),
                    f.get("adjunct_size", 0),
                    f.get("announcement_type", ""), f.get("column_id", ""),
                    f.get("download_url", ""), now,
                ),
            )
        self.conn.commit()
        return self.conn.execute("SELECT COUNT(*) FROM filings").fetchone()[0] - before

    def mark_downloaded(self, announcement_id: str, path: str):
        self.conn.execute(
            "UPDATE filings SET downloaded=1, local_path=? WHERE announcement_id=?",
            (path, announcement_id),
        )
        self.conn.commit()

    def is_downloaded(self, announcement_id: str) -> bool:
        row = self.conn.execute(
            "SELECT downloaded FROM filings WHERE announcement_id=?",
            (announcement_id,),
        ).fetchone()
        return bool(row and row[0])

    def get_known_ids(self) -> set[str]:
        return {
            r[0]
            for r in self.conn.execute("SELECT announcement_id FROM filings").fetchall()
        }

    def stats(self) -> dict:
        r = self.conn.execute(
            """SELECT
                COUNT(*) as total,
                SUM(CASE WHEN downloaded=1 THEN 1 ELSE 0 END) as downloaded,
                SUM(CASE WHEN downloaded=0 THEN 1 ELSE 0 END) as pending,
                COUNT(DISTINCT sec_code) as unique_companies,
                MIN(announcement_date) as oldest,
                MAX(announcement_date) as newest
            FROM filings"""
        ).fetchone()
        return dict(r)

    def export_json(self, path: str):
        rows = self.conn.execute(
            "SELECT * FROM filings ORDER BY announcement_date DESC"
        ).fetchall()
        with open(path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "metadata": {
                        "source": BASE_URL,
                        "exported_at": datetime.now().isoformat(),
                        "total": len(rows),
                        "stats": self.stats(),
                    },
                    "filings": [dict(r) for r in rows],
                },
                f,
                indent=2,
                ensure_ascii=False,
            )
        log.info("Exported %d filings to %s", len(rows), path)

    def close(self):
        self.conn.close()


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------


def _polite_delay():
    """Sleep with jitter to be polite to the server."""
    time.sleep(DELAY_BETWEEN_PAGES + random.uniform(0, DELAY_JITTER))


def cmd_crawl(args):
    session = create_session()
    cache = FilingCache(args.db)
    if args.download:
        os.makedirs(args.doc_dir, exist_ok=True)

    # Build query parameters
    category = CATEGORIES.get(args.category, args.category) if args.category else ""
    column = COLUMNS.get(args.column, args.column)

    t_start = time.time()
    total_filings = 0
    total_new = 0
    total_downloaded = 0

    # Determine if we need date-range splitting
    if args.date_from and args.date_to:
        date_ranges = generate_date_ranges(args.date_from, args.date_to)
        concurrency = getattr(args, "concurrency", 1) or 1
        log.info(
            "Crawling %d date ranges (%s to %s), category=%s, column=%s, concurrency=%d",
            len(date_ranges), args.date_from, args.date_to,
            args.category or "all", args.column, concurrency,
        )

        # Lock for thread-safe SQLite and counter updates
        import threading
        counter_lock = threading.Lock()

        def _crawl_date_range(idx_dr: tuple[int, str]) -> tuple[int, int, int]:
            """Crawl a single date range. Returns (filings, new, downloaded)."""
            idx, dr = idx_dr
            range_filings = 0
            range_new = 0
            range_downloaded = 0

            resp = query_announcements(
                session, page_num=1, category=category,
                column=column, date_range=dr,
                search_key=args.search or "",
            )
            filings = parse_announcements(resp)
            total_count, total_pages, _ = get_pagination_info(resp)

            if not filings:
                return (0, 0, 0)

            with counter_lock:
                new = cache.insert_batch(filings)
            range_filings += len(filings)
            range_new += new
            log.info(
                "Range %d/%d [%s]: page 1/%d — %d filings (%d new), %d total in range",
                idx, len(date_ranges), dr, total_pages, len(filings), new, total_count,
            )

            if args.download and filings:
                dl = download_filings(session, filings, args.doc_dir, cache, args.parallel)
                range_downloaded += dl

            # Paginate within this date range
            pages_to_fetch = min(total_pages, args.max_pages)
            for page_num in range(2, pages_to_fetch + 1):
                _polite_delay()
                resp = query_announcements(
                    session, page_num=page_num, category=category,
                    column=column, date_range=dr,
                    search_key=args.search or "",
                )
                filings = parse_announcements(resp)
                if not filings:
                    break

                with counter_lock:
                    new = cache.insert_batch(filings)
                range_filings += len(filings)
                range_new += new
                log.info(
                    "  [%s] Page %d/%d — %d filings (%d new)",
                    dr, page_num, pages_to_fetch, len(filings), new,
                )

                if args.download and filings:
                    dl = download_filings(session, filings, args.doc_dir, cache, args.parallel)
                    range_downloaded += dl

            return (range_filings, range_new, range_downloaded)

        if concurrency > 1:
            with ThreadPoolExecutor(max_workers=concurrency) as pool:
                futs = pool.map(_crawl_date_range, enumerate(date_ranges, 1))
                for r_filings, r_new, r_dl in futs:
                    total_filings += r_filings
                    total_new += r_new
                    total_downloaded += r_dl
        else:
            for idx_dr in enumerate(date_ranges, 1):
                r_filings, r_new, r_dl = _crawl_date_range(idx_dr)
                total_filings += r_filings
                total_new += r_new
                total_downloaded += r_dl
                _polite_delay()

    else:
        # No date range — simple paginated crawl
        date_range = ""
        if args.date_from:
            date_range = f"{args.date_from}~"
        if args.date_to:
            date_range = f"~{args.date_to}" if not date_range else f"{args.date_from}~{args.date_to}"

        log.info(
            "Crawling up to %d pages, category=%s, column=%s, date=%s",
            args.max_pages, args.category or "all", args.column, date_range or "all",
        )

        for page_num in range(1, args.max_pages + 1):
            if page_num > 1:
                _polite_delay()

            resp = query_announcements(
                session, page_num=page_num, category=category,
                column=column, date_range=date_range,
                search_key=args.search or "",
            )
            filings = parse_announcements(resp)
            total_count, total_pages, has_more = get_pagination_info(resp)

            if not filings:
                log.info("No filings on page %d. Stopping.", page_num)
                break

            new = cache.insert_batch(filings)
            total_filings += len(filings)
            total_new += new

            if page_num == 1:
                log.info(
                    "Total available: %d filings across %d pages",
                    total_count, total_pages,
                )
                if total_pages > MAX_API_PAGES:
                    log.warning(
                        "Query exceeds %d-page API cap (%d pages). "
                        "Use --date-from/--date-to to split into date ranges.",
                        MAX_API_PAGES, total_pages,
                    )

            log.info(
                "Page %d/%d — %d filings (%d new)",
                page_num, min(total_pages, args.max_pages), len(filings), new,
            )

            if args.download and filings:
                dl = download_filings(session, filings, args.doc_dir, cache, args.parallel)
                total_downloaded += dl

            if not has_more:
                log.info("No more pages. Stopping.")
                break

    elapsed = time.time() - t_start
    stats = cache.stats()
    log.info(
        "Done: %d filings (%d new), %d downloaded in %.1fs. DB: %d total, %d companies.",
        total_filings, total_new, total_downloaded, elapsed,
        stats["total"] or 0, stats["unique_companies"] or 0,
    )
    cache.close()


def cmd_monitor(args):
    cache = FilingCache(args.db)
    known_ids = cache.get_known_ids()
    if args.download:
        os.makedirs(args.doc_dir, exist_ok=True)

    category = CATEGORIES.get(args.category, args.category) if args.category else ""
    column = COLUMNS.get(args.column, args.column)

    log.info(
        "Monitoring for new filings every %ds. Known: %d. Ctrl+C to stop.",
        args.interval, len(known_ids),
    )

    session = create_session()
    polls = 0

    try:
        while True:
            polls += 1

            resp = query_announcements(
                session, page_num=1, category=category, column=column,
                sort_name="time", sort_type="desc",
            )
            filings = parse_announcements(resp)
            new_filings = [f for f in filings if f["announcement_id"] not in known_ids]

            if new_filings:
                all_new = list(new_filings)
                log.info(
                    "[Poll %d] NEW: %d filings on page 1.", polls, len(new_filings),
                )

                new_count = cache.insert_batch(new_filings)
                for f in new_filings:
                    known_ids.add(f["announcement_id"])

                if args.download and new_filings:
                    dl = download_filings(
                        session, new_filings, args.doc_dir, cache, args.parallel,
                    )
                    log.info("  Downloaded %d/%d docs.", dl, len(new_filings))

                # Paginate to collect all new filings
                page_num = 2
                while page_num <= MAX_API_PAGES:
                    _polite_delay()
                    resp = query_announcements(
                        session, page_num=page_num, category=category,
                        column=column, sort_name="time", sort_type="desc",
                    )
                    page_filings = parse_announcements(resp)
                    if not page_filings:
                        break

                    page_new = [
                        f for f in page_filings
                        if f["announcement_id"] not in known_ids
                    ]
                    if not page_new:
                        log.info("  Page %d: 0 new — caught up.", page_num)
                        break

                    all_new.extend(page_new)
                    cache.insert_batch(page_new)
                    for f in page_new:
                        known_ids.add(f["announcement_id"])

                    log.info("  Page %d: %d new filings", page_num, len(page_new))

                    if args.download:
                        dl = download_filings(
                            session, page_new, args.doc_dir, cache, args.parallel,
                        )
                        log.info("  Downloaded %d/%d docs.", dl, len(page_new))

                    page_num += 1

                log.info(
                    "[Poll %d] Total new: %d filings.",
                    polls, len(all_new),
                )
                for f in all_new[:5]:
                    log.info(
                        "  %s | %s | %s",
                        f.get("sec_code", "")[:8],
                        f.get("title", "")[:40],
                        f.get("announcement_date", ""),
                    )
            else:
                log.info("[Poll %d] No new filings. Known: %d", polls, len(known_ids))

            time.sleep(args.interval)

    except KeyboardInterrupt:
        log.info("Monitor stopped. %d polls, %d filings known.", polls, len(known_ids))
    finally:
        cache.close()


def cmd_export(args):
    cache = FilingCache(args.db)
    cache.export_json(args.output)
    cache.close()


def cmd_stats(args):
    cache = FilingCache(args.db)
    s = cache.stats()
    print(f"Total filings:    {s['total'] or 0}")
    print(f"Downloaded:       {s['downloaded'] or 0}")
    print(f"Pending:          {s['pending'] or 0}")
    print(f"Companies:        {s['unique_companies'] or 0}")
    print(f"Oldest filing:    {s['oldest'] or 'N/A'}")
    print(f"Newest filing:    {s['newest'] or 'N/A'}")
    cache.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    p = argparse.ArgumentParser(
        description="CNINFO Chinese Securities Filing Scraper (巨潮资讯网)"
    )
    sub = p.add_subparsers(dest="command")

    # -- crawl --
    c = sub.add_parser("crawl", help="Crawl filings from CNINFO")
    c.add_argument("--max-pages", type=int, default=10, help="Max pages per query (default: 10)")
    c.add_argument("--download", action="store_true", help="Download documents")
    c.add_argument("--parallel", type=int, default=5, help="Download workers (default: 5)")
    c.add_argument("--doc-dir", default="documents", help="Download directory")
    c.add_argument("--db", default=DB_FILE)
    c.add_argument(
        "--category",
        choices=list(CATEGORIES.keys()),
        default="",
        help="Filing category filter (e.g., annual, semi_annual, ipo)",
    )
    c.add_argument(
        "--column",
        choices=list(COLUMNS.keys()),
        default="all",
        help="Exchange filter (default: all = SH+SZ+BJ)",
    )
    c.add_argument("--date-from", help="Start date YYYY-MM-DD (enables date-range splitting)")
    c.add_argument("--date-to", help="End date YYYY-MM-DD")
    c.add_argument("--search", help="Keyword search in titles")
    c.add_argument(
        "--concurrency", type=int, default=1,
        help="Parallel date-range workers (default: 1, try 3-5 for speed)",
    )

    # -- monitor --
    m = sub.add_parser("monitor", help="Watch for new filings")
    m.add_argument("--interval", type=int, default=300, help="Poll interval seconds (default: 300)")
    m.add_argument("--download", action="store_true", help="Auto-download new filings")
    m.add_argument("--parallel", type=int, default=5, help="Download workers (default: 5)")
    m.add_argument("--doc-dir", default="documents")
    m.add_argument("--db", default=DB_FILE)
    m.add_argument("--category", choices=list(CATEGORIES.keys()), default="")
    m.add_argument("--column", choices=list(COLUMNS.keys()), default="all")

    # -- export --
    e = sub.add_parser("export", help="Export cached filings to JSON")
    e.add_argument("--output", default="filings.json")
    e.add_argument("--db", default=DB_FILE)

    # -- stats --
    sub.add_parser("stats", help="Show cache statistics").add_argument("--db", default=DB_FILE)

    args = p.parse_args()
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    cmds = {
        "crawl": cmd_crawl,
        "monitor": cmd_monitor,
        "export": cmd_export,
        "stats": cmd_stats,
    }
    if args.command in cmds:
        cmds[args.command](args)
    else:
        p.print_help()


if __name__ == "__main__":
    main()
