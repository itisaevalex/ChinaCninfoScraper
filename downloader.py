"""
downloader.py — PDF download helpers for the CNINFO scraper.

CNINFO PDFs are served from a static CDN at static.cninfo.com.cn.
URLs are permanent and require no authentication or token resolution —
they are constructed directly from the ``adjunctUrl`` API field.

Key design decisions:
  - Uses DOWNLOAD_HEADERS (not API headers) — the CDN returns 404 if the
    API headers (X-Requested-With, Content-Type) are sent.
  - Validates the ``%PDF`` magic bytes before writing to disk (CDN can
    return HTML error pages with HTTP 200 for missing files).
  - Atomic writes via .part file to avoid partial downloads surviving a crash.
  - Thread workers create their own requests.Session; all SQLite writes
    are deferred to the calling thread.
"""

from __future__ import annotations

import logging
import os
import random
import re
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

from db import is_downloaded, mark_downloaded
from http_utils import DOWNLOAD_HEADERS

log = logging.getLogger("cninfo")

DELAY_BETWEEN_DOWNLOADS = 0.3
DELAY_JITTER = 0.5


def _build_filename(filing: dict) -> str:
    """Construct a safe, collision-resistant filename for a filing.

    Uses ``announcement_id`` prefix to prevent title collisions.

    Args:
        filing: Dict with keys sec_code, announcement_id, title, adjunct_type.

    Returns:
        A filesystem-safe filename string.
    """
    sec_code = filing.get("sec_code", "unknown")
    ann_id = filing.get("announcement_id", "unknown")
    title = filing.get("title", "doc")
    ext = filing.get("adjunct_type", "PDF").upper()

    safe_title = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", title)
    safe_title = safe_title.replace("*", "＊")[:70]
    return f"{sec_code}_{ann_id[:8]}_{safe_title}.{ext}"


def download_one(
    url: str,
    dest_path: str,
    ann_id: str,
) -> bool:
    """Download a single PDF from the CNINFO CDN.

    Uses a fresh requests.get (not a Session) with CDN-appropriate headers.
    Validates the ``%PDF`` magic bytes before committing the write.
    Uses atomic .part file pattern to avoid partial downloads.

    Args:
        url:       Full CDN URL (e.g. http://static.cninfo.com.cn/...).
        dest_path: Absolute destination path for the saved file.
        ann_id:    Announcement ID for log messages.

    Returns:
        True if the file was saved successfully, False otherwise.
    """
    try:
        resp = requests.get(url, headers=DOWNLOAD_HEADERS, timeout=120)
        if resp.status_code != 200:
            log.warning("Download failed (%d): %s", resp.status_code, url)
            return False

        content = resp.content
        if not content.startswith(b"%PDF"):
            log.warning("Not a PDF (got %r...): %s", content[:30], url)
            return False

        part_path = dest_path + ".part"
        try:
            with open(part_path, "wb") as fh:
                fh.write(content)
            os.replace(part_path, dest_path)
        except OSError as exc:
            log.error("Disk write failed (%s). Aborting download.", exc)
            try:
                os.unlink(part_path)
            except OSError:
                pass
            raise

        time.sleep(DELAY_BETWEEN_DOWNLOADS + random.uniform(0, DELAY_JITTER))
        return True

    except requests.RequestException as exc:
        log.warning("Network error for %s: %s", ann_id, exc)
        return False


def _worker(filing: dict, doc_dir: str) -> tuple[str, str] | None:
    """Thread worker: download one filing document.

    Creates no database connections — all DB writes happen on the calling thread.

    Args:
        filing:  Dict with download_url, announcement_id, sec_code, title,
                 adjunct_type fields.
        doc_dir: Directory path where documents are saved.

    Returns:
        (announcement_id, local_path) on success, None on failure.
    """
    url = filing.get("download_url", "")
    ann_id = filing.get("announcement_id", "")
    if not url or not ann_id:
        return None

    filename = _build_filename(filing)
    dest_path = os.path.join(doc_dir, filename)

    success = download_one(url, dest_path, ann_id)
    if success:
        return (ann_id, dest_path)
    return None


def batch_download(
    conn: sqlite3.Connection,
    filings: list[dict],
    doc_dir: str,
    workers: int = 5,
) -> int:
    """Download PDFs for a list of filings in parallel.

    Skips filings that have already been downloaded (idempotent).
    Workers do HTTP only; all SQLite writes happen on the calling thread.

    Args:
        conn:     Main-thread SQLite connection for marking downloads.
        filings:  List of filing dicts (each must have download_url,
                  announcement_id, sec_code, title, adjunct_type).
        doc_dir:  Directory where PDFs are saved.
        workers:  Number of parallel download threads.

    Returns:
        Count of successfully downloaded files.
    """
    to_download = [
        f for f in filings
        if f.get("download_url") and not is_downloaded(conn, f["announcement_id"])
    ]
    if not to_download:
        return 0

    os.makedirs(doc_dir, exist_ok=True)
    completed: list[tuple[str, str]] = []

    if workers > 1 and len(to_download) > 1:
        with ThreadPoolExecutor(max_workers=min(workers, len(to_download))) as pool:
            futs = {
                pool.submit(_worker, f, doc_dir): f["announcement_id"]
                for f in to_download
            }
            for fut in as_completed(futs):
                ann_id = futs[fut]
                try:
                    result = fut.result()
                    if result:
                        completed.append(result)
                except Exception as exc:
                    log.error("Unexpected error downloading %s: %s", ann_id, exc)
    else:
        for f in to_download:
            result = _worker(f, doc_dir)
            if result:
                completed.append(result)

    # Write all DB updates from the calling (main) thread
    for ann_id, path in completed:
        mark_downloaded(conn, ann_id, path)

    return len(completed)
