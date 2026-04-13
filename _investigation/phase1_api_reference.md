# CNINFO API Reference (Reverse-Engineered)

## Date: 2026-04-13

## Primary Endpoint: Historical Announcement Query

```
POST http://www.cninfo.com.cn/new/hisAnnouncement/query
Content-Type: application/x-www-form-urlencoded; charset=UTF-8
X-Requested-With: XMLHttpRequest
```

### POST Parameters (form-encoded)

| Parameter | Type | Description | Example |
|-----------|------|-------------|---------|
| pageNum | int | Page number (1-based) | 1 |
| pageSize | int | Results per page (fixed at 30) | 30 |
| column | str | Exchange group | "szse" (SH+SZ+BJ) |
| tabName | str | Always "fulltext" | "fulltext" |
| plate | str | Board filter | "sz", "sh", "szmb", "szcy", "shkcp", "bj" |
| stock | str | "CODE,ORGID" | "000001,gssz0000001" |
| searchkey | str | Title search | "招股说明书" |
| secid | str | Always empty | "" |
| category | str | Filing type (semicolon-separated) | "category_ndbg_szsh" |
| trade | str | Industry filter | "" |
| seDate | str | Date range | "2024-01-01~2024-12-31" |
| sortName | str | Sort field | "time" or "code" |
| sortType | str | Sort direction | "asc" or "desc" |
| isHLtitle | str | Highlight in titles | "true" or "false" |

### Response Format

```json
{
  "announcements": [{
    "secCode": "000001",
    "secName": "平安银行",
    "orgId": "gssz0000001",
    "announcementId": "1216072952",
    "announcementTitle": "2022年年度报告",
    "announcementTime": 1678320000000,
    "adjunctUrl": "finalpage/2023-03-09/1216072952.PDF",
    "adjunctSize": 12345,
    "adjunctType": "PDF",
    "announcementType": "01010503",
    "orgName": "平安银行股份有限公司",
    "columnId": "szse"
  }],
  "totalAnnouncement": 5000,
  "totalpages": 167,
  "hasMore": true
}
```

### Download URL Pattern

```
http://static.cninfo.com.cn/{adjunctUrl}
```

No authentication, no tokens, no referrer check. CDN-cached for 360 days.

## Stock List Endpoint

```
GET http://www.cninfo.com.cn/new/data/szse_stock.json
GET http://www.cninfo.com.cn/new/data/hke_stock.json
```

Returns: `{"stockList": [{"orgId": "gssz0000001", "code": "000001", "zwjc": "平安银行", ...}]}`

## Known Limitations

- **100-page cap**: API silently returns duplicate data beyond page ~100. Split with date ranges.
- **pageSize fixed at 30**: Server ignores other values.
- **announcementTime in milliseconds**: Divide by 1000 for Unix timestamp.

## Tech Stack

- Server: OpenResty (Nginx-based)
- Backend: Java (Spring MVC, JSESSIONID cookies)
- Frontend: Vue.js 2.x + axios + Element UI
- CDN: Huawei Cloud CDN
- Bot protection: NONE detected

## Confirmed Working Libraries

- Python `requests` — works with basic headers
- Python `httpx` — also works
- No need for curl_cffi, Playwright, or browser automation
