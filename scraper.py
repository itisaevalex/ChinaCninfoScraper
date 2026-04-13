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
  python scraper.py crawl --date-from 2024-01-01 --date-to 2024-03-31 --incremental
  python scraper.py crawl --date-from 2024-01-01 --date-to 2024-03-31 --resume
  python scraper.py monitor --interval 300 --download
  python scraper.py export --output filings.json
  python scraper.py stats
"""

from __future__ import annotations

import argparse
import logging
import logging.handlers
import os
import random
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Any

import requests

from db import (
    DB_FILE,
    CrawlResult,
    export_json,
    get_db,
    get_known_ids,
    get_last_crawl_date,
    insert_batch,
    stats,
)
from downloader import batch_download
from http_utils import make_session, safe_post
from parsers import get_pagination_info, parse_announcements

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

QUERY_URL = "http://www.cninfo.com.cn/new/hisAnnouncement/query"
STOCK_LIST_URL = "http://www.cninfo.com.cn/new/data/szse_stock.json"

PAGE_SIZE = 30        # Server-enforced maximum
MAX_API_PAGES = 100   # CNINFO silently caps at ~100 pages

DELAY_BETWEEN_PAGES = 1.0
DELAY_JITTER = 0.5

# Filing category codes (API parameter values)
CATEGORIES: dict[str, str] = {
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

# Exchange / column codes
COLUMNS: dict[str, str] = {
    "all": "szse",
    "shanghai": "sse",
    "hongkong": "hke",
    "third_board": "third",
}

log = logging.getLogger("cninfo")


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------


def _configure_logging(log_file: str | None) -> None:
    """Configure root and cninfo loggers.

    Always writes to stderr. Optionally also writes to a rotating file.

    Args:
        log_file: Path for an optional rotating log file, or None.
    """
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S"
    )
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    if not root.handlers:
        stderr_handler = logging.StreamHandler(sys.stderr)
        stderr_handler.setFormatter(fmt)
        root.addHandler(stderr_handler)

    if log_file:
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,  # 10 MB
            backupCount=5,
            encoding="utf-8",
        )
        file_handler.setFormatter(fmt)
        root.addHandler(file_handler)
        log.info("Logging to file: %s", log_file)


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------


def _build_query_data(
    page_num: int,
    category: str,
    column: str,
    date_range: str,
    search_key: str,
    sort_name: str = "",
    sort_type: str = "",
    plate: str = "",
    stock: str = "",
) -> dict[str, Any]:
    """Build the form-encoded POST body for the CNINFO query endpoint.

    Args:
        page_num:   1-based page number.
        category:   Filing category code (empty string = all categories).
        column:     Exchange code (e.g. "szse", "sse").
        date_range: Date range string "YYYY-MM-DD~YYYY-MM-DD" or "".
        search_key: Keyword search string.
        sort_name:  Sort field name (e.g. "time").
        sort_type:  Sort direction (e.g. "desc").
        plate:      Optional plate filter.
        stock:      Optional stock code filter.

    Returns:
        Dict suitable for requests.post(data=...).
    """
    return {
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
    """Query the CNINFO announcement API. Returns parsed JSON response.

    Raises ValueError if the API returns None (all retries failed).

    Args:
        session:    requests.Session from make_session().
        page_num:   Page number (1-based).
        stock:      Optional stock code filter.
        category:   Filing category code.
        column:     Exchange column code.
        plate:      Plate filter.
        search_key: Keyword search string.
        date_range: Date range "YYYY-MM-DD~YYYY-MM-DD".
        sort_name:  Sort field.
        sort_type:  Sort direction.

    Returns:
        Parsed JSON response dict.

    Raises:
        ValueError: If safe_post returns None after all retries.
    """
    data = _build_query_data(
        page_num=page_num,
        category=category,
        column=column,
        date_range=date_range,
        search_key=search_key,
        sort_name=sort_name,
        sort_type=sort_type,
        plate=plate,
        stock=stock,
    )
    result = safe_post(session, QUERY_URL, data)
    if result is None:
        raise ValueError(f"API request failed for page {page_num}, date_range={date_range!r}")
    return result


# ---------------------------------------------------------------------------
# Date range splitting (bypass 100-page cap)
# ---------------------------------------------------------------------------


def generate_date_ranges(
    date_from: str,
    date_to: str,
    interval_days: int = 1,
) -> list[str]:
    """Split a date span into smaller query ranges to stay under the API cap.

    CNINFO caps results at ~100 pages (3,000 filings) per query. Splitting
    the date range into daily windows ensures all filings are captured.

    Args:
        date_from:     Start date "YYYY-MM-DD".
        date_to:       End date "YYYY-MM-DD" (inclusive).
        interval_days: Window size in days (default 1 = daily splits).

    Returns:
        List of "YYYY-MM-DD~YYYY-MM-DD" range strings.
    """
    start = datetime.strptime(date_from, "%Y-%m-%d")
    end = datetime.strptime(date_to, "%Y-%m-%d")
    ranges: list[str] = []

    current = start
    while current <= end:
        range_end = min(current + timedelta(days=interval_days - 1), end)
        ranges.append(
            f"{current.strftime('%Y-%m-%d')}~{range_end.strftime('%Y-%m-%d')}"
        )
        current = range_end + timedelta(days=1)

    return ranges


def _polite_delay() -> None:
    """Sleep with jitter between page requests."""
    time.sleep(DELAY_BETWEEN_PAGES + random.uniform(0, DELAY_JITTER))


# ---------------------------------------------------------------------------
# Crawl command
# ---------------------------------------------------------------------------


def cmd_crawl(args: argparse.Namespace) -> None:
    """Crawl CNINFO filings with optional date-range splitting."""
    conn = get_db(args.db)
    session = make_session()

    if args.download:
        os.makedirs(args.doc_dir, exist_ok=True)

    category = CATEGORIES.get(args.category, args.category) if args.category else ""
    column = COLUMNS.get(args.column, args.column)

    # --incremental: skip date ranges already covered in the cache
    incremental_cutoff: str | None = None
    if getattr(args, "incremental", False):
        incremental_cutoff = get_last_crawl_date(conn)
        if incremental_cutoff:
            log.info(
                "--incremental: cache has data through %s. "
                "Skipping date ranges on or before this date.",
                incremental_cutoff,
            )

    t_start = time.time()
    total_filings = 0
    total_new = 0
    total_downloaded = 0

    try:
        if args.date_from and args.date_to:
            all_ranges = generate_date_ranges(args.date_from, args.date_to)

            # --incremental: drop ranges already seen
            if incremental_cutoff:
                all_ranges = [
                    dr for dr in all_ranges
                    if dr.split("~")[0] > incremental_cutoff
                ]
                if not all_ranges:
                    log.info(
                        "--incremental: all date ranges already in cache. Nothing to do."
                    )
                    return

            # --resume: load already-seen IDs to detect genuinely new items
            known_ids: set[str] = set()
            if getattr(args, "resume", False):
                known_ids = get_known_ids(conn)
                log.info(
                    "--resume: %d known IDs loaded. Will skip already-crawled filings.",
                    len(known_ids),
                )

            concurrency = getattr(args, "concurrency", 1) or 1
            log.info(
                "Crawling %d date ranges (%s to %s), category=%s, column=%s, "
                "concurrency=%d",
                len(all_ranges),
                args.date_from,
                args.date_to,
                args.category or "all",
                args.column,
                concurrency,
            )

            counter_lock = threading.Lock()

            def _crawl_date_range(
                idx_dr: tuple[int, str],
            ) -> tuple[int, int, int]:
                """Crawl a single date range. Returns (filings, new, downloaded)."""
                idx, dr = idx_dr
                local_session = make_session() if concurrency > 1 else session
                range_filings = 0
                range_new = 0
                range_downloaded = 0

                resp = query_announcements(
                    local_session,
                    page_num=1,
                    category=category,
                    column=column,
                    date_range=dr,
                    search_key=args.search or "",
                )
                filings = parse_announcements(resp)
                total_count, total_pages, _ = get_pagination_info(resp)

                if not filings:
                    return (0, 0, 0)

                # Filter already-known IDs when resuming
                if known_ids:
                    filings = [f for f in filings if f.announcement_id not in known_ids]

                if filings:
                    with counter_lock:
                        filing_dicts = [f.__dict__ for f in filings]
                        new = insert_batch(conn, filings)
                        for f in filings:
                            known_ids.add(f.announcement_id)
                    range_filings += len(filings)
                    range_new += new
                    log.info(
                        "Range %d/%d [%s]: page 1/%d — %d filings (%d new), "
                        "%d total in range",
                        idx,
                        len(all_ranges),
                        dr,
                        total_pages,
                        len(filings),
                        new,
                        total_count,
                    )
                    if args.download:
                        dl = batch_download(conn, filing_dicts, args.doc_dir, args.parallel)
                        range_downloaded += dl

                pages_to_fetch = min(total_pages, args.max_pages)
                for page_num in range(2, pages_to_fetch + 1):
                    _polite_delay()
                    resp = query_announcements(
                        local_session,
                        page_num=page_num,
                        category=category,
                        column=column,
                        date_range=dr,
                        search_key=args.search or "",
                    )
                    page_filings = parse_announcements(resp)
                    if not page_filings:
                        break

                    if known_ids:
                        page_filings = [
                            f for f in page_filings
                            if f.announcement_id not in known_ids
                        ]

                    if page_filings:
                        with counter_lock:
                            filing_dicts = [f.__dict__ for f in page_filings]
                            new = insert_batch(conn, page_filings)
                            for f in page_filings:
                                known_ids.add(f.announcement_id)
                        range_filings += len(page_filings)
                        range_new += new
                        log.info(
                            "  [%s] Page %d/%d — %d filings (%d new)",
                            dr,
                            page_num,
                            pages_to_fetch,
                            len(page_filings),
                            new,
                        )
                        if args.download:
                            dl = batch_download(
                                conn, filing_dicts, args.doc_dir, args.parallel
                            )
                            range_downloaded += dl

                return (range_filings, range_new, range_downloaded)

            if concurrency > 1:
                with ThreadPoolExecutor(max_workers=concurrency) as pool:
                    futs = {
                        pool.submit(_crawl_date_range, idx_dr): idx_dr
                        for idx_dr in enumerate(all_ranges, 1)
                    }
                    for fut in as_completed(futs):
                        try:
                            r_f, r_n, r_d = fut.result()
                            total_filings += r_f
                            total_new += r_n
                            total_downloaded += r_d
                        except Exception as exc:
                            log.error("Date range %s failed: %s", futs[fut], exc)
            else:
                for idx_dr in enumerate(all_ranges, 1):
                    r_f, r_n, r_d = _crawl_date_range(idx_dr)
                    total_filings += r_f
                    total_new += r_n
                    total_downloaded += r_d
                    _polite_delay()

        else:
            # No date range — simple paginated crawl
            date_range = ""
            if args.date_from:
                date_range = f"{args.date_from}~"
            if args.date_to:
                if date_range:
                    date_range = f"{args.date_from}~{args.date_to}"
                else:
                    date_range = f"~{args.date_to}"

            log.info(
                "Crawling up to %d pages, category=%s, column=%s, date=%s",
                args.max_pages,
                args.category or "all",
                args.column,
                date_range or "all",
            )

            for page_num in range(1, args.max_pages + 1):
                if page_num > 1:
                    _polite_delay()

                resp = query_announcements(
                    session,
                    page_num=page_num,
                    category=category,
                    column=column,
                    date_range=date_range,
                    search_key=args.search or "",
                )
                filings = parse_announcements(resp)
                total_count, total_pages, has_more = get_pagination_info(resp)

                if not filings:
                    log.info("No filings on page %d. Stopping.", page_num)
                    break

                filing_dicts = [f.__dict__ for f in filings]
                new = insert_batch(conn, filings)
                total_filings += len(filings)
                total_new += new

                if page_num == 1:
                    log.info(
                        "Total available: %d filings across %d pages",
                        total_count,
                        total_pages,
                    )
                    if total_pages > MAX_API_PAGES:
                        log.warning(
                            "Query exceeds %d-page API cap (%d pages). "
                            "Use --date-from/--date-to to split into date ranges.",
                            MAX_API_PAGES,
                            total_pages,
                        )

                log.info(
                    "Page %d/%d — %d filings (%d new)",
                    page_num,
                    min(total_pages, args.max_pages),
                    len(filings),
                    new,
                )

                if args.download and filing_dicts:
                    dl = batch_download(
                        conn, filing_dicts, args.doc_dir, args.parallel
                    )
                    total_downloaded += dl

                if not has_more:
                    log.info("No more pages. Stopping.")
                    break

        elapsed = time.time() - t_start
        db_stats = stats(conn)
        log.info(
            "Done: %d filings (%d new), %d downloaded in %.1fs. "
            "DB: %d total, %d companies.",
            total_filings,
            total_new,
            total_downloaded,
            elapsed,
            db_stats.get("total") or 0,
            db_stats.get("unique_companies") or 0,
        )
        result = CrawlResult(
            filings_found=total_filings,
            filings_new=total_new,
            filings_downloaded=total_downloaded,
            elapsed_seconds=elapsed,
        )
        return result

    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Monitor command
# ---------------------------------------------------------------------------


def cmd_monitor(args: argparse.Namespace) -> None:
    """Watch for new filings on a polling interval."""
    conn = get_db(args.db)
    known_ids = get_known_ids(conn)
    if args.download:
        os.makedirs(args.doc_dir, exist_ok=True)

    category = CATEGORIES.get(args.category, args.category) if args.category else ""
    column = COLUMNS.get(args.column, args.column)

    log.info(
        "Monitoring for new filings every %ds. Known: %d. Ctrl+C to stop.",
        args.interval,
        len(known_ids),
    )

    session = make_session()
    polls = 0

    try:
        while True:
            polls += 1

            resp = query_announcements(
                session,
                page_num=1,
                category=category,
                column=column,
                sort_name="time",
                sort_type="desc",
            )
            filings = parse_announcements(resp)
            new_filings = [f for f in filings if f.announcement_id not in known_ids]

            if new_filings:
                all_new = list(new_filings)
                log.info("[Poll %d] NEW: %d filings on page 1.", polls, len(new_filings))

                insert_batch(conn, new_filings)
                for f in new_filings:
                    known_ids.add(f.announcement_id)

                if args.download and new_filings:
                    dl = batch_download(
                        conn,
                        [f.__dict__ for f in new_filings],
                        args.doc_dir,
                        args.parallel,
                    )
                    log.info("  Downloaded %d/%d docs.", dl, len(new_filings))

                # Paginate to collect all new filings
                page_num = 2
                while page_num <= MAX_API_PAGES:
                    _polite_delay()
                    resp = query_announcements(
                        session,
                        page_num=page_num,
                        category=category,
                        column=column,
                        sort_name="time",
                        sort_type="desc",
                    )
                    page_filings = parse_announcements(resp)
                    if not page_filings:
                        break

                    page_new = [
                        f for f in page_filings
                        if f.announcement_id not in known_ids
                    ]
                    if not page_new:
                        log.info("  Page %d: 0 new — caught up.", page_num)
                        break

                    all_new.extend(page_new)
                    insert_batch(conn, page_new)
                    for f in page_new:
                        known_ids.add(f.announcement_id)

                    log.info("  Page %d: %d new filings", page_num, len(page_new))

                    if args.download:
                        dl = batch_download(
                            conn,
                            [f.__dict__ for f in page_new],
                            args.doc_dir,
                            args.parallel,
                        )
                        log.info("  Downloaded %d/%d docs.", dl, len(page_new))

                    page_num += 1

                log.info("[Poll %d] Total new: %d filings.", polls, len(all_new))
                for f in all_new[:5]:
                    log.info(
                        "  %s | %s | %s",
                        f.sec_code[:8],
                        f.title[:40],
                        f.announcement_date,
                    )
            else:
                log.info("[Poll %d] No new filings. Known: %d", polls, len(known_ids))

            time.sleep(args.interval)

    except KeyboardInterrupt:
        log.info("Monitor stopped. %d polls, %d filings known.", polls, len(known_ids))
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Export / stats commands
# ---------------------------------------------------------------------------


def cmd_export(args: argparse.Namespace) -> None:
    """Export cached filings to JSON."""
    conn = get_db(args.db)
    export_json(conn, args.output)
    conn.close()


def cmd_stats(args: argparse.Namespace) -> None:
    """Print filing cache statistics."""
    conn = get_db(args.db)
    s = stats(conn)
    print(f"Total filings:    {s.get('total') or 0}")
    print(f"Downloaded:       {s.get('downloaded') or 0}")
    print(f"Pending:          {s.get('pending') or 0}")
    print(f"Companies:        {s.get('unique_companies') or 0}")
    print(f"Oldest filing:    {s.get('oldest') or 'N/A'}")
    print(f"Newest filing:    {s.get('newest') or 'N/A'}")
    conn.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    """Entry point — parse arguments and dispatch to the appropriate command."""
    p = argparse.ArgumentParser(
        description="CNINFO Chinese Securities Filing Scraper (巨潮资讯网)"
    )
    p.add_argument(
        "--log-file",
        default=None,
        help="Optional path for a rotating log file (in addition to stderr)",
    )
    sub = p.add_subparsers(dest="command")

    # -- crawl --
    c = sub.add_parser("crawl", help="Crawl filings from CNINFO")
    c.add_argument(
        "--max-pages", type=int, default=10, help="Max pages per query (default: 10)"
    )
    c.add_argument("--download", action="store_true", help="Download documents")
    c.add_argument(
        "--parallel", type=int, default=5, help="Download workers (default: 5)"
    )
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
    c.add_argument(
        "--date-from", help="Start date YYYY-MM-DD (enables date-range splitting)"
    )
    c.add_argument("--date-to", help="End date YYYY-MM-DD")
    c.add_argument("--search", help="Keyword search in titles")
    c.add_argument(
        "--concurrency",
        type=int,
        default=1,
        help="Parallel date-range workers (default: 1, try 3-5 for speed)",
    )
    c.add_argument(
        "--incremental",
        action="store_true",
        help=(
            "Skip date ranges already in the cache. "
            "Uses the newest announcement_date as the cutoff."
        ),
    )
    c.add_argument(
        "--resume",
        action="store_true",
        help=(
            "Continue an interrupted crawl by skipping already-cached "
            "announcement IDs within the target date range."
        ),
    )

    # -- monitor --
    m = sub.add_parser("monitor", help="Watch for new filings")
    m.add_argument(
        "--interval",
        type=int,
        default=300,
        help="Poll interval seconds (default: 300)",
    )
    m.add_argument("--download", action="store_true", help="Auto-download new filings")
    m.add_argument(
        "--parallel", type=int, default=5, help="Download workers (default: 5)"
    )
    m.add_argument("--doc-dir", default="documents")
    m.add_argument("--db", default=DB_FILE)
    m.add_argument("--category", choices=list(CATEGORIES.keys()), default="")
    m.add_argument("--column", choices=list(COLUMNS.keys()), default="all")

    # -- export --
    e = sub.add_parser("export", help="Export cached filings to JSON")
    e.add_argument("--output", default="filings.json")
    e.add_argument("--db", default=DB_FILE)

    # -- stats --
    st = sub.add_parser("stats", help="Show cache statistics")
    st.add_argument("--db", default=DB_FILE)

    args = p.parse_args()
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    _configure_logging(getattr(args, "log_file", None))

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
