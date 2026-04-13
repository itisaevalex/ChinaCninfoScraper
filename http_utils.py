"""
http_utils.py — HTTP session factory and safe_post helper for the CNINFO scraper.

CNINFO uses a POST JSON API with form-encoded parameters.
No TLS fingerprinting or WAF bypass is needed — plain requests works perfectly.
"""

from __future__ import annotations

import logging
import time
from typing import Any

import requests

log = logging.getLogger("cninfo")

# ---------------------------------------------------------------------------
# Headers
# ---------------------------------------------------------------------------

REQUEST_HEADERS: dict[str, str] = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Host": "www.cninfo.com.cn",
    "Origin": "http://www.cninfo.com.cn",
    "Referer": (
        "http://www.cninfo.com.cn/new/commonUrl/pageOfSearch"
        "?url=disclosure/list/search"
    ),
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "X-Requested-With": "XMLHttpRequest",
}

DOWNLOAD_HEADERS: dict[str, str] = {
    "Accept": "application/pdf, application/octet-stream, */*",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
}

# Retry configuration
_RETRY_STATUS_CODES = [429, 500, 502, 503, 504]
_MAX_RETRIES = 3
_BACKOFF_FACTOR = 1.0
_POOL_MAXSIZE = 10


def make_session() -> requests.Session:
    """Create a requests.Session configured for the CNINFO API.

    Sets browser-like headers and an HTTPAdapter with automatic retries
    on transient server errors (429, 5xx).

    Returns:
        A fully configured requests.Session ready for CNINFO POST calls.
    """
    session = requests.Session()
    session.headers.update(REQUEST_HEADERS)

    adapter = requests.adapters.HTTPAdapter(
        max_retries=requests.adapters.Retry(
            total=_MAX_RETRIES,
            backoff_factor=_BACKOFF_FACTOR,
            status_forcelist=_RETRY_STATUS_CODES,
        ),
        pool_maxsize=_POOL_MAXSIZE,
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    return session


def safe_post(
    session: requests.Session,
    url: str,
    data: dict[str, Any],
    retries: int = _MAX_RETRIES,
    timeout: int = 30,
) -> dict[str, Any] | None:
    """POST form-encoded data to *url* with manual retry logic.

    CNINFO returns JSON for all API calls. Returns the parsed dict or None
    if all attempts fail.

    Args:
        session: An active requests.Session (should use make_session()).
        url:     Target URL.
        data:    Form-encoded POST body as a dict.
        retries: Maximum number of attempts.
        timeout: Per-attempt timeout in seconds.

    Returns:
        Parsed JSON dict on success, None after all retries are exhausted.
    """
    for attempt in range(1, retries + 1):
        try:
            resp = session.post(url, data=data, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else 0
            log.warning(
                "HTTP %s on POST %s (attempt %d/%d)", status, url, attempt, retries
            )
            if status in (403, 404, 410):
                return None
        except (requests.RequestException, ValueError) as exc:
            log.warning(
                "Request/parse error on POST %s (attempt %d/%d): %s",
                url,
                attempt,
                retries,
                exc,
            )
        if attempt < retries:
            time.sleep(attempt * _BACKOFF_FACTOR)
    return None
