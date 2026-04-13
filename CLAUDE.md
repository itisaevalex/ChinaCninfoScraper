# CLAUDE.md — China Securities Filing Scraper

## Mission

Reverse-engineer and scrape China's securities filing systems. Extract filings (annual reports, prospectuses, financial statements) from Chinese regulatory portals into structured JSON + downloaded documents.

This is part of a multi-country financial filings scraper project. Sibling scrapers exist for Canada (SEDAR+, task2/) and Mexico (CNBV STIV-2, task1/).

## Target Portals

### Primary Target: CNINFO (巨潮资讯网)
- **URL:** https://www.cninfo.com.cn
- **Operator:** Shenzhen Securities Information Co. (subsidiary of Shenzhen Stock Exchange)
- **Coverage:** Most comprehensive — covers Shenzhen, Shanghai, and Beijing exchanges
- **Language:** Chinese (Simplified) only
- **Access:** Free, no registration required for viewing
- **Search:** Full-text search at http://www.cninfo.com.cn/new/fulltextSearch
- **API:** No official API — reverse-engineering required
- **Prior art:** GitHub repos exist (CnInfoReports, cninfo) with partial scraping

### Secondary Targets (if CNINFO is insufficient)
| Portal | URL | Coverage | English? |
|--------|-----|----------|----------|
| SSE (Shanghai) | https://english.sse.com.cn | Shanghai Main Board + STAR Market | Yes |
| SZSE (Shenzhen) | https://www.szse.cn/English/ | Shenzhen + ChiNext | Yes |
| CSRC EID | http://eid.csrc.gov.cn | Regulatory filings, IPOs | Chinese only |
| BSE (Beijing) | https://www.bse.com.cn | SMEs | Limited |

## Methodology — Lessons from Previous Scrapers

The following methodology was developed across two successful scraper projects (Canada SEDAR+ and Mexico CNBV). Follow this playbook — it saves days of wasted effort.

### Phase 1: Reconnaissance (DO THIS FIRST)

1. **Identify the tech stack** — what backend (ASP.NET? Java? Oracle? React SPA?) and what WAF/bot protection (Cloudflare, Radware, Akamai, Azure WAF, etc.)
2. **Check response headers** — `Server`, `X-Powered-By`, cookie names reveal the stack
3. **Inspect the page source** — look for framework-specific patterns:
   - ASP.NET: `__VIEWSTATE`, `__EVENTVALIDATION`, `ScriptManager`
   - Oracle Catalyst: `viewInstanceKey`, `_CBNAME_`, `_VIKEY_`
   - DevExpress: `dxgvDataRow`, `ASPxClientCallbackPanel`, `WebForm_DoCallback`
   - React/Vue SPA: API calls in Network tab, no server-rendered HTML
4. **Test plain curl** — does it work? Does it redirect to a captcha/challenge page?
5. **Check robots.txt** — what's disallowed?

### Phase 2: GitHub Research (BEFORE writing any code)

1. Run `gh search repos` and `gh search code` for the target site
2. Look for existing scrapers, partial reverse-engineering, API documentation
3. Check if anyone has documented the protocol or found bypasses
4. **This saved days on both SEDAR+ and CNBV** — prior art existed for both

### Phase 3: HTTP Library Selection

Test different HTTP libraries against the target. Results vary dramatically by site:

| Library | When it works | When it fails |
|---------|--------------|---------------|
| `requests` | No TLS fingerprinting (Mexico CNBV worked with just `requests`) | Radware, Cloudflare, Akamai |
| `curl_cffi` | TLS fingerprint-sensitive WAFs (Canada SEDAR+ needed this) | Some sites reject impersonation |
| `httpx` | HTTP/2 required sites | Same TLS issues as requests |
| `tls_client` | Theoretically good TLS, Go-based | Radware rejected it despite browser TLS |

**Key insight from SEDAR+:** If plain requests gets blocked, try `curl_cffi` with `impersonate="chrome120"` before jumping to browser automation. It bypasses JA3/JA4 TLS fingerprinting which is the most common first layer.

### Phase 4: Use Playwright as a DEBUGGING TOOL, not the scraper

**Critical lesson from both projects:** Use Playwright to capture real browser traffic, then replicate with raw HTTP.

```python
# Connect to real Chrome via CDP for maximum trust
google-chrome --remote-debugging-port=9222
browser = pw.chromium.connect_over_cdp("http://127.0.0.1:9222")

# Or launch headless for quick capture
browser = pw.chromium.launch(headless=True)
```

What to capture:
- **Network requests** — exact headers, POST bodies, cookie values
- **XHR/Fetch calls** — these reveal the actual API the frontend uses
- **JavaScript callbacks** — frameworks often prepend prefixes or transform data before sending

**Mexico example:** DevExpress silently prepends `c0:` to callback params. Without Playwright capture, this was invisible and caused a .NET exception. One Playwright session revealed the exact format.

**Canada example:** CDP connection to real Chrome revealed that stormcaster.js cookies from real Chrome sessions enable pure HTTP pagination. Headless Playwright's cookies were rejected.

### Phase 5: Understand the State Machine

Most government portals use server-side state machines. Key patterns:

**ASP.NET WebForms (Mexico):**
- `__VIEWSTATE` and `__EVENTVALIDATION` must be sent with every POST
- ViewState encodes the server's UI state — wrong ViewState = wrong results
- Async postback (`ScriptManager`) vs sync POST produce different ViewStates
- ViewState from sync POST may not support subsequent AJAX operations

**Oracle Catalyst (Canada):**
- `_VIKEY_`, `_CBNAME_`, `_CBVALUE_` control the state machine
- Pagination is sequential only (1→2→3, no jumping)
- Node IDs change between responses — must re-extract from each response
- **State invalidation:** paginating destroys previous page's resource URLs

**General rule:** The server remembers what page you're on. If you skip steps or send stale state tokens, you get garbage back.

### Phase 6: Download Pattern

**Download-before-paginate** (learned the hard way on SEDAR+):
- Some frameworks invalidate resource URLs when you navigate away
- Always download documents from the current page BEFORE moving to the next
- Within a single page, downloads CAN be parallelized (thread pool)
- Cross-page parallelism often DOES NOT work

**Enc/token caching** (learned on CNBV):
- If download URLs use encrypted tokens, cache them in SQLite
- Tokens are often deterministic and permanent — resolve once, use forever
- This turns a 2-request-per-file flow into a 1-request-per-file flow

### Phase 7: Rate Limiting & Bot Protection

| Protection | Detection | Bypass |
|-----------|-----------|--------|
| TLS fingerprinting | 403/redirect on plain requests | curl_cffi with browser impersonation |
| JavaScript challenge | Redirect to challenge page | Real browser cookies via CDP |
| IP reputation | Datacenter IPs blocked | Residential IP required |
| Cookie validation | Requests without cookies blocked | Harvest from real browser session |
| Rate limiting | 429 or connection pool exhaustion | Add delays, limit concurrency |
| WAF headers | Missing Sec-Fetch-* etc. | Copy exact browser headers |

**Key insight:** Bot protection layers are cumulative. You may pass TLS but fail cookie validation. Test each layer independently.

### Phase 8: Production Architecture

Target architecture (proven on both projects):
```
Session init (one-time, <10s)
  → Pure HTTP crawl (requests or curl_cffi)
    → Parse HTML (BeautifulSoup + lxml)
      → Download documents (parallel within page)
        → Cache to SQLite (dedup + tracking)
```

- **No browser in the loop** — browsers use 2-3GB RAM each, HTTP uses ~5MB
- **SQLite for everything** — filings cache, download tracking, enc token cache
- **Headless Chrome as fallback only** — for when IP gets flagged or session init fails

## Output Format

Match the existing project structure:
```
scraper.py              # Main scraper (crawl, monitor, export, stats)
requirements.txt        # Python dependencies
filings_cache.db        # SQLite cache (auto-generated)
documents/              # Downloaded files (auto-generated)
filings.json            # Exported filings (via export command)
_investigation/         # Reverse-engineering artifacts
README.md               # Documentation with full RE journey
```

## Commands to Support

```bash
python scraper.py crawl --max-pages 10 --download
python scraper.py monitor --interval 300 --download
python scraper.py export --output filings.json
python scraper.py stats
```

## Investigation Artifacts

Save ALL reverse-engineering work in `_investigation/`:
- Network captures, decoded responses
- Hypothesis test scripts (`exp_*.py`, `h1-h5_*.py`)
- Deobfuscated JavaScript if relevant
- Protocol documentation

This evidence is invaluable for debugging when things break later.

---

## What Actually Happened (Reasoning Trace — 2026-04-13)

### Decision: Parallel-first investigation

Launched 4 parallel agents simultaneously rather than sequentially:
1. **Recon agent** — probed CNINFO headers, robots.txt, tried the API with WebFetch
2. **GitHub research agent** — `gh search repos/code` for existing scrapers
3. **Sibling project review agent** — read task1/ and task2/ scraper code to extract patterns
4. **Web research agent** — searched for blog posts and API documentation

**Reasoning:** Each agent costs ~30-60s. Running 4 sequentially = 2-4 min. Running in parallel = same wall time as 1.

### Key discovery: CNINFO has ZERO bot protection

The recon agent came back with the best possible news:
- **No WAF** (no Cloudflare, Radware, Akamai, Azure)
- **No TLS fingerprinting** — plain `requests` works
- **No JavaScript challenges**, no captchas
- **No required cookies/sessions** for API access
- **robots.txt returns 404**

This made Phases 3-7 of the methodology unnecessary. We skipped straight from Phase 2 to Phase 8.

### Key discovery: GitHub prior art is extensive

Found 8+ repos all using the same endpoint: `POST /new/hisAnnouncement/query`. Every repo agreed on:
- Same API parameters
- Same response format
- Same download URL pattern
- Same 100-page pagination cap

This saved hours of reverse-engineering. The protocol was already fully documented by the community.

### Architecture decision: Copy task2 (SEDAR+) patterns, strip complexity

Task2's architecture was closest to what we needed. We kept:
- argparse subcommand CLI (crawl, monitor, export, stats)
- FilingCache class with SQLite
- ThreadPoolExecutor for parallel downloads
- Logging setup
- JSON export envelope format

We removed:
- curl_cffi (plain requests instead — no TLS bypass needed)
- Headless Chrome fallback (no bot protection to bypass)
- Catalyst state machine logic (CNINFO API is stateless)
- Download-before-paginate constraint (CNINFO URLs are permanent)

### Critical bug: Static CDN returns 404 with API headers

**First test run:** All downloads returned 404 despite API queries working.

**Root cause:** The scraper's `requests.Session` had API headers set globally (including `X-Requested-With: XMLHttpRequest` and `Content-Type: application/x-www-form-urlencoded`). The static CDN at `static.cninfo.com.cn` rejected requests with these headers.

**Fix:** Created separate `DOWNLOAD_HEADERS` with only `Accept` and `User-Agent` for GET requests to the CDN. Used `requests.get()` with explicit headers instead of the session.

**Verification:** HEAD request test confirmed 2024 filing URLs return 200 with clean headers. Full download test confirmed 30/30 PDFs downloaded successfully.

### 100-page cap workaround

CNINFO caps API results at ~100 pages. Beyond that, it silently returns duplicate data.

**Solution:** `--date-from`/`--date-to` flags split queries into daily date ranges using `generate_date_ranges()`. Each day rarely exceeds 100 pages even during peak filing season (March-April for annual reports).

**Added --concurrency flag** to process multiple date ranges in parallel, since the API is stateless and downloads go to a separate CDN.

### Test results (March 2024 annual reports)

Full month test (31 date ranges, --max-pages 20, --download):
- **2,055 filings scraped** across all March 2024 dates
- **2,055 PDFs downloaded** (5.5 GB) before hitting local disk limit
- **Zero API errors** — no rate limiting, no blocking, no authentication failures
- **Zero download failures** after the header fix (all 404s resolved)
- Peak days: March 30 (575 filings, 19 pages), March 29 (486 filings, 16 pages), March 28 (241 filings, 8 pages)
- April 2026 (today's filings) also confirmed working — 910 annual reports available

### Code review and hardening

Ran automated code review after initial test. Found 5 HIGH issues, all fixed:

| Issue | Problem | Fix |
|-------|---------|-----|
| HIGH-1 | SQLite `ProgrammingError` under `--concurrency` | `check_same_thread=False` |
| HIGH-2 | `requests.Session` not thread-safe across workers | Per-worker session in concurrent mode |
| HIGH-3 | Disk full leaves corrupt `.PDF` files marked as downloaded | Atomic `.part` write + propagate `OSError` |
| HIGH-4 | CDN HTML error pages saved as valid `.PDF` | Validate `%PDF-` magic bytes before writing |
| HIGH-5 | Duplicate filenames silently overwrite each other | `announcement_id` prefix in filename |

The disk-full issue (HIGH-3) was discovered during the large test run when 5.5 GB of annual reports exhausted local disk space. The fix ensures partial writes are cleaned up and the error propagates instead of being silently swallowed.

### Complexity comparison across all 3 projects

| Aspect | Mexico (CNBV) | Canada (SEDAR+) | China (CNINFO) |
|--------|--------------|-----------------|----------------|
| Bot protection | Azure WAF | Radware Bot Manager | **None** |
| HTTP library | requests | curl_cffi | **requests** |
| State management | ViewState + DevExpress callbacks | Oracle Catalyst state machine | **Stateless JSON API** |
| Download URLs | 3-step enc token chain | Session-bound, expire on paginate | **Permanent CDN URLs** |
| Pagination | Complex callback format | Sequential only | **Random-access page numbers** |
| Dependencies | 5 | 5 | **1** (just requests) |
| Lines of code | 1,188 | 765 | **~550** |
| Difficulty | Hard | Very Hard | **Easy** |

## Production Status (2026-04-13)

**Status: PRODUCTION READY**

Repo: https://github.com/itisaevalex/ChinaCninfoScraper

Sibling projects:
- Canada: https://github.com/itisaevalex/SedarPlusScraper
- Mexico: https://github.com/itisaevalex/MexicanReportsScraperExtended
- India: separate repo (built in parallel)

### For the team

Quick start:
```bash
git clone git@github.com:itisaevalex/ChinaCninfoScraper.git
cd ChinaCninfoScraper
pip install -r requirements.txt

# Crawl all annual reports for a date range
python scraper.py crawl --category annual --date-from 2024-01-01 --date-to 2024-12-31 --download --max-pages 20

# Speed up with concurrent date ranges (4 parallel workers)
python scraper.py crawl --category annual --date-from 2024-01-01 --date-to 2024-12-31 --download --concurrency 4

# Monitor for new filings in real time
python scraper.py monitor --interval 300 --download

# Export to JSON
python scraper.py export --output filings.json
```

### Known constraints
- **100-page API cap** — always use `--date-from`/`--date-to` for comprehensive scraping
- **Disk space** — annual reports are large (avg ~2.5 MB each). A full year can be 10+ GB
- **Chinese-only** — all titles, company names, and categories are in Simplified Chinese
- **No official API** — this uses a reverse-engineered internal API that could change without notice
