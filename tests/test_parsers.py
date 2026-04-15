"""
test_parsers.py — Unit tests for parsers.py.

Covers:
  - classify_filing_type()  — Chinese category code + title → taxonomy label
  - parse_announcements()   — JSON response → Filing list
  - get_pagination_info()   — envelope extraction
"""
from __future__ import annotations

import re

import pytest

from parsers import (
    classify_filing_type,
    derive_isin_from_stock_code,
    get_pagination_info,
    parse_announcements,
)


# ---------------------------------------------------------------------------
# classify_filing_type()
# ---------------------------------------------------------------------------


class TestClassifyFilingType:
    """Unit tests for the Chinese filing type classifier."""

    # --- Annual reports ---
    def test_annual_report_from_title_chinese(self):
        assert classify_filing_type("2023年年度报告") == "annual_report"

    def test_annual_report_from_category_code(self):
        assert classify_filing_type("", "category_ndbg_szsh") == "annual_report"

    def test_annual_report_from_english_title(self):
        assert classify_filing_type("Annual Report 2023", "") == "annual_report"

    # --- Half-yearly ---
    def test_half_yearly_from_title_chinese(self):
        assert classify_filing_type("2023年半年度报告") == "half_yearly"

    def test_half_yearly_from_category_code(self):
        assert classify_filing_type("", "category_bndbg_szsh") == "half_yearly"

    # --- Quarterly Q1 ---
    def test_quarterly_q1_from_title_chinese(self):
        assert classify_filing_type("2024年一季度报告") == "quarterly_q1"

    def test_quarterly_q1_from_category_code(self):
        assert classify_filing_type("", "category_yjdbg_szsh") == "quarterly_q1"

    # --- Quarterly Q3 ---
    def test_quarterly_q3_from_title_chinese(self):
        assert classify_filing_type("2023年三季度报告") == "quarterly_q3"

    def test_quarterly_q3_from_category_code(self):
        assert classify_filing_type("", "category_sjdbg_szsh") == "quarterly_q3"

    # --- Earnings forecast ---
    def test_earnings_forecast_from_title_yujingao(self):
        assert classify_filing_type("2023年度业绩预告") == "earnings_forecast"

    def test_earnings_forecast_from_title_kuaibao(self):
        assert classify_filing_type("业绩快报") == "earnings_forecast"

    def test_earnings_forecast_from_category_code(self):
        assert classify_filing_type("", "category_yjygjxz_szsh") == "earnings_forecast"

    # --- Dividend ---
    def test_dividend_from_title_fenghong(self):
        assert classify_filing_type("2023年度利润分配预案") == "dividend"

    def test_dividend_from_title_paixi(self):
        assert classify_filing_type("分红派息方案公告") == "dividend"

    def test_dividend_from_category_code(self):
        assert classify_filing_type("", "category_qyfpxzcs_szsh") == "dividend"

    # --- IPO / Prospectus ---
    def test_prospectus_from_title_zhaogushuoming(self):
        assert classify_filing_type("首次公开发行股票招股说明书") == "prospectus"

    def test_prospectus_from_title_scgkfx(self):
        assert classify_filing_type("", "category_scgkfx_szsh") == "prospectus"

    # --- Board announcement ---
    def test_board_announcement_from_title_chinese(self):
        assert classify_filing_type("第十届董事会第三次会议决议公告") == "board_announcement"

    def test_board_announcement_from_category_code(self):
        assert classify_filing_type("", "category_dshgg_szsh") == "board_announcement"

    # --- Risk warning ---
    def test_risk_warning_from_title_chinese(self):
        assert classify_filing_type("风险提示公告") == "risk_warning"

    def test_risk_warning_from_category_code(self):
        assert classify_filing_type("", "category_fxts_szsh") == "risk_warning"

    # --- Convertible bond ---
    def test_convertible_bond_from_title_chinese(self):
        assert classify_filing_type("可转换债券募集说明书") == "convertible_bond"

    # --- Fallback ---
    def test_unknown_returns_other(self):
        assert classify_filing_type("关于本公司的其他公告") == "other"

    def test_empty_inputs_return_other(self):
        assert classify_filing_type("", "") == "other"

    def test_case_insensitive_english(self):
        assert classify_filing_type("ANNUAL REPORT 2023") == "annual_report"

    # --- Combined title + code matching ---
    def test_title_takes_precedence_over_code(self):
        # Title is annual, code says earnings — title match should win (first match)
        result = classify_filing_type("年度报告", "category_yjygjxz_szsh")
        assert result == "annual_report"


# ---------------------------------------------------------------------------
# parse_announcements()
# ---------------------------------------------------------------------------


class TestParseAnnouncements:
    def test_returns_filings_list_from_valid_response(self, cninfo_query_response):
        filings = parse_announcements(cninfo_query_response)
        assert len(filings) == 2

    def test_empty_response_returns_empty_list(self, cninfo_empty_response):
        filings = parse_announcements(cninfo_empty_response)
        assert filings == []

    def test_filings_have_correct_tickers(self, cninfo_query_response):
        filings = parse_announcements(cninfo_query_response)
        tickers = {f.ticker for f in filings}
        assert "000001" in tickers
        assert "600519" in tickers

    def test_direct_download_url_uses_static_cdn(self, cninfo_query_response):
        filings = parse_announcements(cninfo_query_response)
        for f in filings:
            assert f.direct_download_url.startswith("http://static.cninfo.com.cn/")

    def test_direct_download_url_contains_document_url(self, cninfo_query_response):
        filings = parse_announcements(cninfo_query_response)
        for f in filings:
            assert f.document_url in f.direct_download_url

    def test_filing_date_is_formatted_correctly(self, cninfo_query_response):
        filings = parse_announcements(cninfo_query_response)
        for f in filings:
            # Should be YYYY-MM-DD
            parts = f.filing_date.split("-")
            assert len(parts) == 3
            assert len(parts[0]) == 4

    def test_first_filing_is_annual_report(self, cninfo_query_response):
        filings = parse_announcements(cninfo_query_response)
        annual = next(f for f in filings if f.ticker == "000001")
        assert annual.filing_type == "annual_report"

    def test_second_filing_is_half_yearly(self, cninfo_query_response):
        filings = parse_announcements(cninfo_query_response)
        semi = next(f for f in filings if f.ticker == "600519")
        assert semi.filing_type == "half_yearly"

    def test_em_tags_stripped_from_headline(self):
        response = {
            "totalAnnouncement": 1,
            "totalpages": 1,
            "hasMore": False,
            "announcements": [
                {
                    "announcementId": "test_em_001",
                    "secCode": "000099",
                    "secName": "测试公司",
                    "orgId": "org001",
                    "orgName": "测试公司",
                    "announcementTitle": "<em>年度</em>报告",
                    "announcementTime": 1711728000000,
                    "adjunctUrl": "finalpage/2024-03-30/test_em_001.PDF",
                    "adjunctType": "PDF",
                    "adjunctSize": 100,
                    "announcementType": "category_ndbg_szsh",
                    "columnId": "col_szse",
                }
            ],
        }
        filings = parse_announcements(response)
        assert len(filings) == 1
        assert "<em>" not in filings[0].headline
        assert "</em>" not in filings[0].headline
        assert filings[0].headline == "年度报告"

    def test_entry_without_adjunct_url_is_skipped(self, cninfo_multi_type_response):
        filings = parse_announcements(cninfo_multi_type_response)
        # ann_no_url_001 has empty adjunctUrl and should be skipped
        ids = {f.filing_id for f in filings}
        assert "ann_no_url_001" not in ids

    def test_multi_type_response_produces_correct_count(self, cninfo_multi_type_response):
        filings = parse_announcements(cninfo_multi_type_response)
        # 8 in fixture, 1 has no adjunct_url — expect 7
        assert len(filings) == 7

    def test_multi_type_all_filing_types_present(self, cninfo_multi_type_response):
        filings = parse_announcements(cninfo_multi_type_response)
        types = {f.filing_type for f in filings}
        assert "annual_report" in types
        assert "half_yearly" in types
        assert "quarterly_q1" in types
        assert "earnings_forecast" in types
        assert "prospectus" in types
        assert "board_announcement" in types

    def test_filings_are_frozen_dataclasses(self, cninfo_query_response):
        filings = parse_announcements(cninfo_query_response)
        for f in filings:
            with pytest.raises((AttributeError, TypeError)):
                f.title = "mutated"  # type: ignore[misc]

    def test_zero_timestamp_produces_empty_date(self):
        response = {
            "totalAnnouncement": 1,
            "totalpages": 1,
            "hasMore": False,
            "announcements": [
                {
                    "announcementId": "ts_zero_001",
                    "secCode": "000099",
                    "secName": "测试",
                    "orgId": "org1",
                    "orgName": "测试公司",
                    "announcementTitle": "公告",
                    "announcementTime": 0,
                    "adjunctUrl": "finalpage/test.PDF",
                    "adjunctType": "PDF",
                    "adjunctSize": 100,
                    "announcementType": "",
                    "columnId": "",
                }
            ],
        }
        filings = parse_announcements(response)
        assert filings[0].filing_date == ""

    def test_none_timestamp_produces_empty_date(self):
        response = {
            "totalAnnouncement": 1,
            "totalpages": 1,
            "hasMore": False,
            "announcements": [
                {
                    "announcementId": "ts_none_001",
                    "secCode": "000099",
                    "secName": "测试",
                    "orgId": "org1",
                    "orgName": "测试公司",
                    "announcementTitle": "公告",
                    "announcementTime": None,
                    "adjunctUrl": "finalpage/test.PDF",
                    "adjunctType": "PDF",
                    "adjunctSize": None,
                    "announcementType": "",
                    "columnId": "",
                }
            ],
        }
        filings = parse_announcements(response)
        assert filings[0].filing_date == ""
        assert filings[0].file_size == 0

    def test_language_is_zh_for_all_filings(self, cninfo_query_response):
        filings = parse_announcements(cninfo_query_response)
        for f in filings:
            assert f.language == "zh"

    def test_lei_is_none_for_all_filings(self, cninfo_query_response):
        """LEI is not available in CNINFO API — must always be None."""
        filings = parse_announcements(cninfo_query_response)
        for f in filings:
            assert f.lei is None

    def test_isin_is_derived_for_known_stock_code(self, cninfo_query_response):
        """Filings with 6-digit stock codes should have a non-None ISIN."""
        filings = parse_announcements(cninfo_query_response)
        for f in filings:
            assert f.isin is not None
            # CN(2) + NSIN(10) + check(2) = 14 chars
            assert len(f.isin) == 14
            assert f.isin.startswith("CN")

    def test_isin_is_none_for_empty_stock_code(self):
        response = {
            "totalAnnouncement": 1,
            "totalpages": 1,
            "hasMore": False,
            "announcements": [
                {
                    "announcementId": "no_code_001",
                    "secCode": "",
                    "secName": "测试",
                    "orgId": "org1",
                    "orgName": "测试公司",
                    "announcementTitle": "公告",
                    "announcementTime": 1711728000000,
                    "adjunctUrl": "finalpage/test.PDF",
                    "adjunctType": "PDF",
                    "adjunctSize": 100,
                    "announcementType": "",
                    "columnId": "",
                }
            ],
        }
        filings = parse_announcements(response)
        assert filings[0].isin is None

    def test_language_is_zh_even_for_empty_code_filing(self):
        response = {
            "totalAnnouncement": 1,
            "totalpages": 1,
            "hasMore": False,
            "announcements": [
                {
                    "announcementId": "lang_001",
                    "secCode": "",
                    "secName": "测试",
                    "orgId": "org1",
                    "orgName": "测试",
                    "announcementTitle": "公告",
                    "announcementTime": 1711728000000,
                    "adjunctUrl": "finalpage/lang_001.PDF",
                    "adjunctType": "PDF",
                    "adjunctSize": 50,
                    "announcementType": "",
                    "columnId": "",
                }
            ],
        }
        filings = parse_announcements(response)
        assert filings[0].language == "zh"


# ---------------------------------------------------------------------------
# derive_isin_from_stock_code()
# ---------------------------------------------------------------------------


class TestDeriveIsinFromStockCode:
    """Unit tests for the ISO 6166 ISIN derivation helper.

    The algorithm follows ISO 6166: country prefix ``CN`` + 10-char NSIN
    (6-digit stock code left-padded with four zeros) + 2 check digits, giving
    a 14-character string.  Note: real published Chinese ISINs use a different
    NSIN encoding (exchange letter prefix + compressed code) and are 12 chars.
    This implementation produces a consistent, deterministic identifier from
    the stock code using the ISO 6166 check-digit formula; it is structurally
    valid but may differ from exchange-published ISINs which use proprietary
    NSIN encoding.  Consumers requiring exchange-canonical ISINs must enrich
    via an external registry (e.g. CSRC, Wind, or Bloomberg).
    """

    def test_000001_produces_correct_length_isin(self):
        isin = derive_isin_from_stock_code("000001")
        assert isin is not None
        # CN(2) + NSIN(10) + check(2) = 14 chars
        assert len(isin) == 14

    def test_isin_starts_with_cn(self):
        isin = derive_isin_from_stock_code("000001")
        assert isin is not None
        assert isin.startswith("CN")

    def test_isin_contains_nsin_padded_to_10_digits(self):
        isin = derive_isin_from_stock_code("000001")
        assert isin is not None
        # Positions 2–11 (0-indexed) = NSIN = "0000000001"
        assert isin[2:12] == "0000000001"

    def test_check_digits_are_numeric(self):
        isin = derive_isin_from_stock_code("000001")
        assert isin is not None
        assert isin[12:].isdigit()

    def test_returns_none_for_empty_string(self):
        assert derive_isin_from_stock_code("") is None

    def test_returns_none_for_non_digit_code(self):
        assert derive_isin_from_stock_code("ABCDEF") is None

    def test_returns_none_for_short_code(self):
        assert derive_isin_from_stock_code("00001") is None

    def test_returns_none_for_long_code(self):
        assert derive_isin_from_stock_code("0000001") is None

    def test_returns_none_for_none_input(self):
        assert derive_isin_from_stock_code(None) is None  # type: ignore[arg-type]

    def test_different_codes_produce_different_isins(self):
        isin1 = derive_isin_from_stock_code("000001")
        isin2 = derive_isin_from_stock_code("600519")
        assert isin1 != isin2

    def test_deterministic_for_same_input(self):
        assert derive_isin_from_stock_code("000001") == derive_isin_from_stock_code("000001")

    def test_result_matches_cn_plus_digits_pattern(self):
        isin = derive_isin_from_stock_code("123456")
        assert isin is not None
        assert re.fullmatch(r"CN\d{12}", isin) is not None

    def test_check_digit_is_valid_iso6166(self):
        """Verify the check digits satisfy 98 - (number mod 97) = check."""
        stock_code = "000001"
        isin = derive_isin_from_stock_code(stock_code)
        assert isin is not None
        # Re-derive manually: replace check digits with 00, compute mod 97
        body_with_zeros = isin[:12] + "00"
        digit_str = "".join(
            str(ord(ch) - ord("A") + 10) if ch.isalpha() else ch
            for ch in body_with_zeros
        )
        expected_check = 98 - (int(digit_str) % 97)
        assert int(isin[12:]) == expected_check


# ---------------------------------------------------------------------------
# get_pagination_info()
# ---------------------------------------------------------------------------


class TestGetPaginationInfo:
    def test_extracts_total_and_pages(self, cninfo_query_response):
        total, pages, has_more = get_pagination_info(cninfo_query_response)
        assert total == 2
        assert pages == 1
        assert has_more is False

    def test_empty_response_returns_zeros(self, cninfo_empty_response):
        total, pages, has_more = get_pagination_info(cninfo_empty_response)
        assert total == 0
        assert pages == 0
        assert has_more is False

    def test_has_more_true_when_set(self):
        resp = {"totalAnnouncement": 300, "totalpages": 10, "hasMore": True, "announcements": []}
        total, pages, has_more = get_pagination_info(resp)
        assert total == 300
        assert pages == 10
        assert has_more is True

    def test_missing_keys_return_defaults(self):
        total, pages, has_more = get_pagination_info({})
        assert total == 0
        assert pages == 0
        assert has_more is False

    def test_none_values_coerce_to_zero(self):
        resp = {"totalAnnouncement": None, "totalpages": None, "hasMore": None}
        total, pages, has_more = get_pagination_info(resp)
        assert total == 0
        assert pages == 0
        assert has_more is False
