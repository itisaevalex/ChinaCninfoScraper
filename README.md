<div align="center">

```
 ██████╗███╗   ██╗██╗███╗   ██╗███████╗ ██████╗ 
██╔════╝████╗  ██║██║████╗  ██║██╔════╝██╔═══██╗
██║     ██╔██╗ ██║██║██╔██╗ ██║█████╗  ██║   ██║
██║     ██║╚██╗██║██║██║╚██╗██║██╔══╝  ██║   ██║
╚██████╗██║ ╚████║██║██║ ╚████║██║     ╚██████╔╝
 ╚═════╝╚═╝  ╚═══╝╚═╝╚═╝  ╚═══╝╚═╝      ╚═════╝ 
      ███████╗ ██████╗██████╗  █████╗ ██████╗ ███████╗██████╗ 
      ██╔════╝██╔════╝██╔══██╗██╔══██╗██╔══██╗██╔════╝██╔══██╗
      ███████╗██║     ██████╔╝███████║██████╔╝█████╗  ██████╔╝
      ╚════██║██║     ██╔══██╗██╔══██║██╔═══╝ ██╔══╝  ██╔══██╗
      ███████║╚██████╗██║  ██║██║  ██║██║     ███████╗██║  ██║
      ╚══════╝ ╚═════╝╚═╝  ╚═╝╚═╝  ╚═╝╚═╝     ╚══════╝╚═╝  ╚═╝
```

**China's EDGAR — 巨潮资讯网 scraped with plain HTTP requests. No browser needed.**

*380K+ filings across all Chinese exchanges. Pure `requests`. One dependency. Zero bot protection.*

[![License: Proprietary](https://img.shields.io/badge/License-Proprietary-red.svg)](#license)

**Created by Alexander Isaev | [Data Alchemy Labs](https://github.com/itisaevalex)**

</div>

---

Production scraper for China's primary securities filing disclosure system. Extracts filing metadata and downloads documents (annual reports, prospectuses, financial statements) from [CNINFO](http://www.cninfo.com.cn) into structured JSON + PDF files.

Part of a multi-country financial filings scraper project alongside [Canada (SEDAR+)](https://github.com/itisaevalex/SedarPlusScraper) and [Mexico (CNBV)](https://github.com/itisaevalex/MexicanReportsScraperExtended).

## Quick Start

```bash
pip install -r requirements.txt

# Crawl latest 10 pages of all filings
python scraper.py crawl --max-pages 10

# Crawl annual reports for a specific date range, with downloads
python scraper.py crawl --category annual --date-from 2024-03-01 --date-to 2024-03-31 --download

# Monitor for new filings every 5 minutes
python scraper.py monitor --interval 300 --download

# Export cached filings to JSON
python scraper.py export --output filings.json

# View cache stats
python scraper.py stats
```

## Architecture

```
Python requests (plain HTTP, no browser)
  → POST /new/hisAnnouncement/query (JSON API)
    → Parse announcement metadata
      → GET static.cninfo.com.cn/{adjunctUrl} (direct PDF download)
        → Cache everything in SQLite (dedup + download tracking)
```

**Single dependency:** `requests`. No browser automation, no TLS fingerprint spoofing, no session management needed.

## How It Works

### API

CNINFO exposes an undocumented but stable JSON API at `POST /new/hisAnnouncement/query`. It accepts form-encoded parameters (page number, category, date range, stock code, keyword) and returns paginated JSON with filing metadata including direct download URLs.

### Downloads

PDFs are served from a static CDN at `static.cninfo.com.cn`. URLs are permanent — no tokens, no expiration, no authentication. Download URLs are constructed by prepending the CDN base to the `adjunctUrl` field from the API response.

### 100-Page Cap

The API silently caps results at ~100 pages. For comprehensive scraping, the `--date-from` / `--date-to` flags split queries into daily date ranges, keeping each query under the cap.

### SQLite Cache

All filing metadata is cached in `filings_cache.db` with:
- Deduplication via `announcement_id` (unique key)
- Download tracking (`downloaded` flag + `local_path`)
- Indexes on date, stock code, and download status

## Commands

| Command | Description |
|---------|-------------|
| `crawl` | Crawl filings with pagination. Supports category/date/keyword filters |
| `monitor` | Poll for new filings on an interval. Smart dedup against known IDs |
| `export` | Export SQLite cache to structured JSON |
| `stats` | Show filing count, download status, date range, company count |

### Crawl Options

```
--max-pages N       Max pages per query (default: 10)
--download          Download PDF documents
--parallel N        Download workers (default: 5)
--category TYPE     Filter: annual, semi_annual, q1, q3, ipo, etc.
--column EXCHANGE   Filter: all (default), shanghai, hongkong, third_board
--date-from DATE    Start date (YYYY-MM-DD) — enables date-range splitting
--date-to DATE      End date (YYYY-MM-DD)
--search KEYWORD    Keyword search in titles
```

### Filing Categories

| Key | Chinese | Description |
|-----|---------|-------------|
| `annual` | 年度报告 | Annual reports |
| `semi_annual` | 半年度报告 | Semi-annual reports |
| `q1` | 一季度报告 | Q1 reports |
| `q3` | 三季度报告 | Q3 reports |
| `ipo` | 首次公开发行 | IPO filings |
| `earnings_forecast` | 业绩预告 | Earnings forecasts |
| `board_announcement` | 董事会公告 | Board announcements |
| `shareholder_meeting` | 股东大会 | Shareholder meetings |
| `risk_warning` | 风险提示 | Risk warnings |

## Reverse-Engineering Journey

This scraper was built as the third in a series (after Canada SEDAR+ and Mexico CNBV). The methodology was refined across all three projects.

### Phase 1: Reconnaissance

**Objective:** Identify CNINFO's tech stack, bot protection, and API surface.

**Findings:**
- **Server:** OpenResty (Nginx-based) with Java backend (Spring MVC)
- **Frontend:** Vue.js 2.x + axios + Element UI
- **CDN:** Huawei Cloud CDN for static assets
- **Bot protection:** **None detected.** No Cloudflare, Radware, Akamai, or TLS fingerprinting. Plain `curl` with a basic User-Agent works. This was a dramatic contrast to SEDAR+ (Radware Bot Manager with JA3/JA4 TLS fingerprinting) and CNBV (Azure WAF with DevExpress ViewState).
- **robots.txt:** Returns 404 (not found)
- **Authentication:** None required for API or downloads

### Phase 2: GitHub Research

Searched GitHub for existing CNINFO scrapers before writing any code. Found 8+ repos with partial implementations, all using the same `hisAnnouncement/query` endpoint.

**Key repos studied:**
- `legeling/Annualreport_tools` — best structured, daily date range pattern
- `tr1s7an/CnInfoReports` — revealed stock list JSON endpoints
- `jiwooshim/cninfo_scraper` — closest to our target (SQLite + metadata tracking)
- `Shih-yenh-suan/scrape-cop-reports-CnInfo` — best category coverage, documented 100-page cap

**Critical finding:** Every repo uses the same API with the same parameters. The protocol is stable and well-understood by the community.

### Phase 3: HTTP Library Selection

Tested plain `requests` against the API — worked immediately. No TLS fingerprinting, no JavaScript challenges, no cookie requirements.

**Comparison across projects:**
| Project | Library Needed | Why |
|---------|---------------|-----|
| Canada (SEDAR+) | `curl_cffi` with Chrome impersonation | Radware Bot Manager checks JA3/JA4 TLS fingerprints |
| Mexico (CNBV) | `requests` (with careful headers) | Azure WAF checks browser headers but not TLS |
| **China (CNINFO)** | **`requests` (minimal headers)** | **No bot protection at all** |

### Phase 4: Download URL Discovery

The API returns an `adjunctUrl` field (e.g., `finalpage/2024-03-30/1219488813.PDF`) which maps directly to a CDN URL: `http://static.cninfo.com.cn/{adjunctUrl}`.

**Gotcha discovered:** The static CDN returns 404 if you send API-style headers (`X-Requested-With: XMLHttpRequest`, `Content-Type: application/x-www-form-urlencoded`). Downloads require clean browser-like headers (just `Accept` and `User-Agent`). This was caught during testing when all downloads failed despite the API queries working.

### Phase 5: Pagination Cap Workaround

The API caps results at ~100 pages (3,000 filings). Beyond that, it silently returns duplicate data. This is documented by multiple GitHub repos.

**Solution:** Split queries into daily date ranges. A single day rarely exceeds 100 pages even for broad queries. The scraper generates `YYYY-MM-DD~YYYY-MM-DD` range strings and iterates through them.

### What We Didn't Need (Lessons from SEDAR+ and CNBV)

The methodology from CLAUDE.md includes 8 phases for reverse-engineering government filing portals. For CNINFO, most were unnecessary:

- **Phase 4 (Playwright as debugging tool):** Not needed — the API responds to plain HTTP
- **Phase 5 (State machine understanding):** Not needed — the API is stateless
- **Phase 6 (Download-before-paginate):** Not needed — download URLs are permanent
- **Phase 7 (Bot protection bypass):** Not needed — no bot protection exists

This is documented as a data point for future scrapers: Chinese government portals may have lighter bot protection than North American ones.

### Phase 6: Code Review & Hardening

Ran an automated code review which found 5 HIGH-severity issues. All were fixed before declaring production-ready:

1. **SQLite thread safety** — `check_same_thread=False` for `--concurrency` mode
2. **Session sharing** — per-worker `requests.Session` in concurrent date-range processing
3. **Disk-full handling** — atomic `.part` file writes, propagate `OSError` instead of swallowing it (this bug was triggered during the 5.5 GB test run)
4. **PDF validation** — verify `%PDF-` magic bytes before saving (CDN can return HTML error pages with HTTP 200)
5. **Filename collisions** — `announcement_id` prefix prevents overwrites from duplicate titles

### Validation Results

| Metric | Result |
|--------|--------|
| Filings scraped | 2,055 (March 2024 annual reports) |
| PDFs downloaded | 2,055 (5.5 GB) |
| API errors | 0 |
| Download failures | 0 (after header fix) |
| Rate limiting | None observed |
| Today's filings (Apr 2026) | Confirmed working (910 annual reports) |

## Output Format

### SQLite Schema (`filings_cache.db`)

```sql
CREATE TABLE filings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    announcement_id TEXT UNIQUE,    -- CNINFO's internal ID
    sec_code TEXT,                  -- Stock code (e.g., "000001")
    sec_name TEXT,                  -- Company short name (e.g., "平安银行")
    org_id TEXT,                    -- Organization ID (e.g., "gssz0000001")
    org_name TEXT,                  -- Full company name
    title TEXT,                     -- Filing title
    announcement_date TEXT,         -- Date (YYYY-MM-DD)
    announcement_time_ms INTEGER,   -- Epoch milliseconds
    adjunct_url TEXT,               -- Relative path on CDN
    adjunct_type TEXT,              -- File type (usually "PDF")
    adjunct_size INTEGER,           -- File size in KB
    announcement_type TEXT,         -- CNINFO category codes
    column_id TEXT,                 -- Exchange identifier
    download_url TEXT,              -- Full CDN URL
    downloaded INTEGER DEFAULT 0,   -- Download tracking flag
    local_path TEXT,                -- Local file path after download
    first_seen TEXT                 -- When we first scraped this filing
);
```

### JSON Export

```json
{
  "metadata": {
    "source": "http://www.cninfo.com.cn",
    "exported_at": "2026-04-13T...",
    "total": 1234,
    "stats": { "total": 1234, "downloaded": 1000, "pending": 234, ... }
  },
  "filings": [ ... ]
}
```

### Downloaded Files

PDFs are saved to `documents/` with the naming pattern: `{sec_code}_{title}.PDF`

## Project Structure

```
china-scraper/
├── scraper.py              # Main scraper (single file, ~550 lines)
├── requirements.txt        # Just: requests
├── CLAUDE.md               # AI context + full reasoning trace
├── README.md               # This file
├── _investigation/         # Reverse-engineering artifacts
│   ├── phase1_api_reference.md
│   └── phase1_reconnaissance.txt
├── filings_cache.db        # SQLite cache (auto-generated)
├── documents/              # Downloaded PDFs (auto-generated)
└── filings.json            # Exported JSON (via export command)
```

## Sister Projects

| Country | Portal | Repo |
|---------|--------|------|
| Canada | SEDAR+ | [SedarPlusScraper](https://github.com/itisaevalex/SedarPlusScraper) |
| Mexico | CNBV STIV-2 | [MexicanReportsScraperExtended](https://github.com/itisaevalex/MexicanReportsScraperExtended) |
| China | CNINFO 巨潮资讯网 | **This repo** |

## License

Copyright (c) 2026 Alexander Isaev / Data Alchemy Labs. All rights reserved.

This software is proprietary. See [LICENSE](LICENSE) for details. Commercial use, redistribution, or derivative works require explicit written authorization.
