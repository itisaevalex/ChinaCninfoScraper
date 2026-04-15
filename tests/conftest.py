"""
conftest.py — shared pytest fixtures for the CNINFO scraper test suite.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

FIXTURES_DIR = Path(__file__).parent / "fixtures"


# ---------------------------------------------------------------------------
# JSON fixture loaders
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def cninfo_query_response() -> dict:
    """Real-world-style CNINFO API response with 2 filings."""
    path = FIXTURES_DIR / "cninfo_query_response.json"
    return json.loads(path.read_text(encoding="utf-8"))


@pytest.fixture(scope="session")
def cninfo_empty_response() -> dict:
    """CNINFO API response with zero results (null announcements)."""
    path = FIXTURES_DIR / "cninfo_empty_response.json"
    return json.loads(path.read_text(encoding="utf-8"))


@pytest.fixture(scope="session")
def cninfo_multi_type_response() -> dict:
    """CNINFO API response with one filing per major type + no-url entry."""
    path = FIXTURES_DIR / "cninfo_multi_type_response.json"
    return json.loads(path.read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# In-memory database
# ---------------------------------------------------------------------------


@pytest.fixture
def mem_db() -> sqlite3.Connection:
    """Fresh in-memory SQLite connection with the full CNINFO schema applied."""
    import sys
    import os
    # Ensure the china-scraper root is on sys.path
    root = str(Path(__file__).parent.parent)
    if root not in sys.path:
        sys.path.insert(0, root)

    from db import get_db

    conn = get_db(db_path=":memory:")
    yield conn
    conn.close()


# ---------------------------------------------------------------------------
# Sample Filing dataclass
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_filing():
    """A valid Filing dataclass instance for DB tests (L3 schema)."""
    import sys
    import os
    root = str(Path(__file__).parent.parent)
    if root not in sys.path:
        sys.path.insert(0, root)

    from db import Filing
    from parsers import derive_isin_from_stock_code

    return Filing(
        filing_id="1219488813",
        ticker="000001",
        company_name="平安银行",
        org_id="gssz0000001",
        org_name="平安银行股份有限公司",
        headline="平安银行股份有限公司2023年年度报告",
        filing_date="2024-03-30",
        announcement_time_ms=1711728000000,
        document_url="finalpage/2024-03-30/1219488813.PDF",
        adjunct_type="PDF",
        file_size=8543,
        category="category_ndbg_szsh",
        column_id="col_szse_annual",
        direct_download_url="http://static.cninfo.com.cn/finalpage/2024-03-30/1219488813.PDF",
        filing_type="annual_report",
        isin=derive_isin_from_stock_code("000001"),
        lei=None,
        language="zh",
    )


@pytest.fixture
def sample_filing_2():
    """A second distinct Filing dataclass instance for batch/dedup tests."""
    import sys
    root = str(Path(__file__).parent.parent)
    if root not in sys.path:
        sys.path.insert(0, root)

    from db import Filing
    from parsers import derive_isin_from_stock_code

    return Filing(
        filing_id="2300145722",
        ticker="600519",
        company_name="贵州茅台",
        org_id="gssh0600519",
        org_name="贵州茅台酒股份有限公司",
        headline="贵州茅台酒股份有限公司2023年半年度报告",
        filing_date="2023-08-15",
        announcement_time_ms=1692057600000,
        document_url="finalpage/2023-08-15/2300145722.PDF",
        adjunct_type="PDF",
        file_size=6100,
        category="category_bndbg_szsh",
        column_id="col_sse_semi",
        direct_download_url="http://static.cninfo.com.cn/finalpage/2023-08-15/2300145722.PDF",
        filing_type="half_yearly",
        isin=derive_isin_from_stock_code("600519"),
        lei=None,
        language="zh",
    )
