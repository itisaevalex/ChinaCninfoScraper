"""
test_http_utils.py — Unit tests for http_utils.py.

Covers:
  - make_session()  — session configuration
  - safe_post()     — retry logic, error handling, JSON parsing
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests

from http_utils import DOWNLOAD_HEADERS, REQUEST_HEADERS, make_session, safe_post


# ---------------------------------------------------------------------------
# make_session()
# ---------------------------------------------------------------------------


class TestMakeSession:
    def test_returns_requests_session(self):
        sess = make_session()
        assert isinstance(sess, requests.Session)

    def test_session_has_user_agent_header(self):
        sess = make_session()
        assert "User-Agent" in sess.headers
        assert "Mozilla" in sess.headers["User-Agent"]

    def test_session_has_accept_language_chinese(self):
        sess = make_session()
        assert "zh-CN" in sess.headers.get("Accept-Language", "")

    def test_session_has_x_requested_with(self):
        sess = make_session()
        assert sess.headers.get("X-Requested-With") == "XMLHttpRequest"

    def test_session_has_correct_content_type(self):
        sess = make_session()
        assert "application/x-www-form-urlencoded" in sess.headers.get(
            "Content-Type", ""
        )

    def test_session_has_http_adapter(self):
        sess = make_session()
        adapter = sess.get_adapter("http://www.cninfo.com.cn")
        assert adapter is not None

    def test_session_has_https_adapter(self):
        sess = make_session()
        adapter = sess.get_adapter("https://www.cninfo.com.cn")
        assert adapter is not None

    def test_different_calls_return_distinct_sessions(self):
        s1 = make_session()
        s2 = make_session()
        assert s1 is not s2


# ---------------------------------------------------------------------------
# REQUEST_HEADERS / DOWNLOAD_HEADERS constants
# ---------------------------------------------------------------------------


class TestHeaders:
    def test_request_headers_has_required_keys(self):
        required = {"Accept", "Content-Type", "User-Agent", "X-Requested-With"}
        assert required.issubset(REQUEST_HEADERS.keys())

    def test_download_headers_does_not_have_x_requested_with(self):
        # CDN returns 404 if XHR headers are sent; download headers must be clean
        assert "X-Requested-With" not in DOWNLOAD_HEADERS

    def test_download_headers_has_user_agent(self):
        assert "User-Agent" in DOWNLOAD_HEADERS

    def test_download_headers_accepts_pdf(self):
        assert "pdf" in DOWNLOAD_HEADERS.get("Accept", "").lower()


# ---------------------------------------------------------------------------
# safe_post()
# ---------------------------------------------------------------------------


class TestSafePost:
    def _make_mock_response(self, status: int, json_data: dict) -> MagicMock:
        resp = MagicMock()
        resp.status_code = status
        resp.json.return_value = json_data
        if status >= 400:
            resp.raise_for_status.side_effect = requests.HTTPError(
                response=resp
            )
        else:
            resp.raise_for_status.return_value = None
        return resp

    def test_returns_parsed_json_on_success(self):
        sess = make_session()
        expected = {"totalAnnouncement": 5, "announcements": []}
        with patch.object(sess, "post", return_value=self._make_mock_response(200, expected)):
            result = safe_post(sess, "http://example.com/api", {})
        assert result == expected

    def test_returns_none_after_all_retries_exhausted(self):
        sess = make_session()
        with patch.object(
            sess,
            "post",
            side_effect=requests.ConnectionError("refused"),
        ):
            result = safe_post(sess, "http://example.com/api", {}, retries=2)
        assert result is None

    def test_returns_none_on_404(self):
        sess = make_session()
        with patch.object(
            sess, "post", return_value=self._make_mock_response(404, {})
        ):
            result = safe_post(sess, "http://example.com/api", {})
        assert result is None

    def test_returns_none_on_403(self):
        sess = make_session()
        with patch.object(
            sess, "post", return_value=self._make_mock_response(403, {})
        ):
            result = safe_post(sess, "http://example.com/api", {})
        assert result is None

    def test_retries_on_500(self):
        sess = make_session()
        call_count = []

        def fake_post(*args, **kwargs):
            call_count.append(1)
            return self._make_mock_response(500, {})

        with patch.object(sess, "post", side_effect=fake_post):
            result = safe_post(
                sess, "http://example.com/api", {}, retries=3, timeout=5
            )

        assert result is None
        assert len(call_count) == 3

    def test_succeeds_on_second_attempt(self):
        sess = make_session()
        responses = [
            requests.ConnectionError("transient"),
            self._make_mock_response(200, {"ok": True}),
        ]
        responses_iter = iter(responses)

        def fake_post(*args, **kwargs):
            r = next(responses_iter)
            if isinstance(r, Exception):
                raise r
            return r

        with patch.object(sess, "post", side_effect=fake_post), \
             patch("http_utils.time.sleep"):
            result = safe_post(sess, "http://example.com/api", {}, retries=3)

        assert result == {"ok": True}

    def test_returns_none_on_invalid_json(self):
        sess = make_session()
        resp = MagicMock()
        resp.status_code = 200
        resp.raise_for_status.return_value = None
        resp.json.side_effect = ValueError("not json")
        with patch.object(sess, "post", return_value=resp):
            result = safe_post(sess, "http://example.com/api", {}, retries=1)
        assert result is None

    def test_passes_data_to_post(self):
        sess = make_session()
        posted_data = {}

        def capture_post(*args, **kwargs):
            posted_data.update(kwargs.get("data", {}))
            return self._make_mock_response(200, {"ok": True})

        payload = {"pageNum": 1, "pageSize": 30}
        with patch.object(sess, "post", side_effect=capture_post):
            safe_post(sess, "http://example.com/api", payload)

        assert posted_data.get("pageNum") == 1
        assert posted_data.get("pageSize") == 30
