"""
parsers.py — JSON response parsing and filing type classification for CNINFO.

CNINFO returns JSON (not HTML) from its POST API. This module:
  1. Parses the raw JSON response into Filing dataclass instances.
  2. Classifies filings into a standard taxonomy using Chinese-language patterns.
  3. Extracts pagination metadata from the response envelope.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Any

from db import Filing

log = logging.getLogger("cninfo")

STATIC_URL = "http://static.cninfo.com.cn"

# ---------------------------------------------------------------------------
# Filing type classification — Chinese → taxonomy
# ---------------------------------------------------------------------------

# Ordered list of (taxonomy_label, compiled_regex) pairs. First match wins.
# Patterns match against the Chinese `announcementType` category codes AND
# the human-readable `announcementTitle` field.
TYPE_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    # Semi-annual / half-year — 半年度报告  (MUST precede annual_report to avoid
    # "年度" inside "半年度" matching the annual pattern first)
    ("half_yearly", re.compile(r"半年(度)?报告|bndbg|half[\s_-]?year", re.I)),
    # Annual reports — 年度报告
    ("annual_report", re.compile(r"(?<!半)年度报告|ndbg|annual[\s_-]?report", re.I)),
    # Q1 — 一季度报告
    ("quarterly_q1", re.compile(r"一季度报告|yjdbg|first[\s_-]?quarter", re.I)),
    # Q3 — 三季度报告
    ("quarterly_q3", re.compile(r"三季度报告|sjdbg|third[\s_-]?quarter", re.I)),
    # Generic quarterly
    ("quarterly", re.compile(r"季度报告|quarterly", re.I)),
    # Earnings forecast / flash — 业绩快报 / 业绩预告
    ("earnings_forecast", re.compile(r"业绩(快报|预告|预增|预降)|yjygjxz|earnings[\s_-]?fore", re.I)),
    # Dividend / profit distribution — 分红派息 / 权益分派
    ("dividend", re.compile(r"分红|派息|权益分派|利润分配|qyfpxzcs|dividend", re.I)),
    # IPO / initial public offering — 首次公开发行
    ("prospectus", re.compile(r"(首次)?公开发行|招股说明书|scgkfx|prospectus|ipo", re.I)),
    # Rights issue / allotment — 配股
    ("rights_issue", re.compile(r"配股|权益发行|category_pg", re.I)),
    # Additional share offering — 增发
    ("additional_offering", re.compile(r"增发|非公开发行|category_zf", re.I)),
    # Convertible bond — 可转债
    ("convertible_bond", re.compile(r"可转(换)?债|convertible[\s_-]?bond|kzhz", re.I)),
    # Board announcements — 董事会公告 / 董事会决议 / 董事会会议
    ("board_announcement", re.compile(r"董事会|dshgg", re.I)),
    # Shareholder meeting — 股东大会
    ("shareholder_meeting", re.compile(r"股东(大)?会|gddh|shareholder[\s_-]?meet", re.I)),
    # Risk warning — 风险提示
    ("risk_warning", re.compile(r"风险提示|fxts|risk[\s_-]?warn", re.I)),
    # Delisting / major event — 退市
    ("delisting", re.compile(r"退市|tbclts|delist", re.I)),
    # Corporate governance — 公司治理
    ("corporate_governance", re.compile(r"公司治理|章程|gszl", re.I)),
    # Daily operations / general — 日常经营
    ("daily_operations", re.compile(r"日常经营|rcjy", re.I)),
    # Equity distribution — 权益分派
    ("equity_distribution", re.compile(r"qyfpxzcs|equity[\s_-]?distrib", re.I)),
]


def classify_filing_type(headline: str, announcement_type: str = "") -> str:
    """Map a CNINFO filing to a standard taxonomy label.

    Checks both the human-readable title and the API category code field
    against Chinese and English patterns. First match wins.

    Args:
        headline:          The filing title (``announcementTitle``).
        announcement_type: The CNINFO category code (``announcementType``),
                           e.g. ``"category_ndbg_szsh"``.

    Returns:
        A lowercase taxonomy string such as ``"annual_report"`` or ``"other"``.
    """
    combined = f"{headline} {announcement_type}"
    for label, pattern in TYPE_PATTERNS:
        if pattern.search(combined):
            return label
    return "other"


# ---------------------------------------------------------------------------
# JSON response parsing
# ---------------------------------------------------------------------------


def parse_announcements(api_response: dict[str, Any]) -> list[Filing]:
    """Extract Filing instances from a CNINFO API JSON response.

    Handles the ``announcements`` array inside the response envelope.
    Skips entries that lack an ``adjunctUrl`` (no downloadable document).
    Strips ``<em>`` highlight tags injected by the search engine.

    Args:
        api_response: Parsed JSON dict returned by the CNINFO query endpoint.

    Returns:
        A list of Filing dataclass instances (may be empty).
    """
    raw_announcements = api_response.get("announcements") or []
    filings: list[Filing] = []

    for ann in raw_announcements:
        adjunct_url = ann.get("adjunctUrl", "")
        if not adjunct_url:
            continue

        # announcementTime is milliseconds since epoch
        ts_ms: int = ann.get("announcementTime", 0) or 0
        announcement_date = ""
        if ts_ms:
            announcement_date = datetime.fromtimestamp(ts_ms / 1000).strftime(
                "%Y-%m-%d"
            )

        # Strip <em> highlight tags inserted by the search engine
        raw_title: str = ann.get("announcementTitle", "")
        title = re.sub(r"</?em>", "", raw_title)

        announcement_type_code: str = ann.get("announcementType", "")
        filing_type = classify_filing_type(title, announcement_type_code)

        filings.append(
            Filing(
                filing_id=ann.get("announcementId", ""),
                ticker=ann.get("secCode", ""),
                company_name=ann.get("secName", ""),
                org_id=ann.get("orgId", ""),
                org_name=ann.get("orgName", ""),
                headline=title,
                filing_date=announcement_date,
                announcement_time_ms=ts_ms,
                document_url=adjunct_url,
                adjunct_type=ann.get("adjunctType", "PDF"),
                file_size=ann.get("adjunctSize", 0) or 0,
                category=announcement_type_code,
                column_id=ann.get("columnId", ""),
                direct_download_url=f"{STATIC_URL}/{adjunct_url}",
                filing_type=filing_type,
            )
        )

    return filings


def get_pagination_info(
    api_response: dict[str, Any],
) -> tuple[int, int, bool]:
    """Extract pagination metadata from a CNINFO API response.

    Args:
        api_response: Parsed JSON dict from the query endpoint.

    Returns:
        A 3-tuple of (total_announcements, total_pages, has_more).
    """
    total: int = api_response.get("totalAnnouncement", 0) or 0
    pages: int = api_response.get("totalpages", 0) or 0
    has_more: bool = bool(api_response.get("hasMore", False))
    return total, pages, has_more
