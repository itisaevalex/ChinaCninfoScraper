"""
test_parsers.py — Unit tests for parsers.py.

Covers:
  - classify_filing_type()  — Chinese category code + title → taxonomy label
  - parse_announcements()   — JSON response → Filing list
  - get_pagination_info()   — envelope extraction
"""
from __future__ import annotations

import pytest

from parsers import classify_filing_type, get_pagination_info, parse_announcements


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

    def test_filings_have_correct_sec_codes(self, cninfo_query_response):
        filings = parse_announcements(cninfo_query_response)
        codes = {f.sec_code for f in filings}
        assert "000001" in codes
        assert "600519" in codes

    def test_download_url_uses_static_cdn(self, cninfo_query_response):
        filings = parse_announcements(cninfo_query_response)
        for f in filings:
            assert f.download_url.startswith("http://static.cninfo.com.cn/")

    def test_download_url_contains_adjunct_url(self, cninfo_query_response):
        filings = parse_announcements(cninfo_query_response)
        for f in filings:
            assert f.adjunct_url in f.download_url

    def test_announcement_date_is_formatted_correctly(self, cninfo_query_response):
        filings = parse_announcements(cninfo_query_response)
        for f in filings:
            # Should be YYYY-MM-DD
            parts = f.announcement_date.split("-")
            assert len(parts) == 3
            assert len(parts[0]) == 4

    def test_first_filing_is_annual_report(self, cninfo_query_response):
        filings = parse_announcements(cninfo_query_response)
        annual = next(f for f in filings if f.sec_code == "000001")
        assert annual.filing_type == "annual_report"

    def test_second_filing_is_half_yearly(self, cninfo_query_response):
        filings = parse_announcements(cninfo_query_response)
        semi = next(f for f in filings if f.sec_code == "600519")
        assert semi.filing_type == "half_yearly"

    def test_em_tags_stripped_from_title(self):
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
        assert "<em>" not in filings[0].title
        assert "</em>" not in filings[0].title
        assert filings[0].title == "年度报告"

    def test_entry_without_adjunct_url_is_skipped(self, cninfo_multi_type_response):
        filings = parse_announcements(cninfo_multi_type_response)
        # ann_no_url_001 has empty adjunctUrl and should be skipped
        ids = {f.announcement_id for f in filings}
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
        assert filings[0].announcement_date == ""

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
        assert filings[0].announcement_date == ""
        assert filings[0].adjunct_size == 0


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
