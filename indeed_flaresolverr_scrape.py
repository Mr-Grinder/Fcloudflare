import json, time, random
from pathlib import Path
from urllib.parse import urlparse, urlunparse

import requests
import pandas as pd
from parsel import Selector

# ===== Налаштування =====
FS_URL = "http://localhost:8191/v1"   # FlareSolverr API
EXCEL_PATH = "Indeed_UK_Jobs_Links 1.xlsx"
NAME_COL = "Company Names"
URL_COL  = "Indeed UK Jobs Link"

SESSION_NAME = "indeed_uk_session"
MAX_TIMEOUT_MS = 90000
SLEEP_BETWEEN = (1.5, 3.2)

DEBUG_SAVE_HTML = True
LIMIT_ROWS = False  # 👉 тестовий зріз: перші 10 рядків; постав None щоб йти по всіх

CARD_SELECTORS = [
    "li[data-testid='cmpJobListItem']",
    "div.job_seen_beacon",
    "div[data-testid='jobCard']",
    "a.tapItem"
]
TITLE_SELECTORS = [
    "[data-testid='jobTitle']::text",
    "h2.jobTitle span::text",
    "a[aria-label]::attr(aria-label)",
    "a[title]::attr(title)"
]
HREF_SELECTORS = [
    "a[href*='/viewjob']::attr(href)",
    "a.tapItem::attr(href)",
    "a::attr(href)"
]
LOC_SELECTORS = [
    "[data-testid='text-location']::text",
    "div.companyLocation::text"
]


def normalize_jobs_url(url: str) -> str:
    p = urlparse(url)
    path = p.path.rstrip("/")
    if not path.endswith("/jobs"):
        path = path + "/jobs"
    return urlunparse((p.scheme, p.netloc, path, p.params, p.query, p.fragment))


def fs_request_get(url: str, session: str) -> dict:
    payload = {
        "cmd": "request.get",
        "url": url,
        "maxTimeout": MAX_TIMEOUT_MS,
        "session": session,
        "followRedirects": True,
        "headers": {
            "Accept-Language": "en-GB,en;q=0.9"
        }
    }
    try:
        r = requests.post(FS_URL, json=payload, timeout=(30, 180))
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        return {"ok": False, "url": url, "status": 0, "html": "", "error": str(e)}

    if data.get("status") != "ok":
        return {"ok": False, "url": url, "status": 0, "html": "", "error": str(data)}

    sol = data.get("solution", {})
    return {
        "ok": True,
        "url": sol.get("url", url),
        "status": int(sol.get("status", 0) or 0),
        "html": sol.get("response", ""),
        "error": None
    }


def quick_no_jobs_or_missing(html: str, title: str) -> str | None:
    title_l = (title or "").lower()
    body_l = " ".join(Selector(text=html).xpath("//body//text()").getall() or []).lower()

    if any(x in title_l for x in ["404", "page not found", "not found"]):
        return "not_found"

    phrases = [
        "no jobs", "no job", "no open jobs", "no openings",
        "no results", "did not match any jobs", "we couldn't find any jobs",
        "this company doesn't have any jobs", "currently, there are no jobs"
    ]
    if any(p in body_l for p in phrases):
        return "no_jobs"

    if "company not found" in body_l or "we can't find this company" in body_l:
        return "not_found"

    return None


def parse_jobs(html: str, base_url: str) -> list[dict]:
    sel = Selector(text=html)
    jobs, seen = [], set()

    cards = []
    for cs in CARD_SELECTORS:
        cards.extend(sel.css(cs))

    for c in cards:
        title, href, location = None, None, ""

        for ts in TITLE_SELECTORS:
            val = c.css(ts).get()
            if val:
                title = val.strip()
                break

        for hs in HREF_SELECTORS:
            val = c.css(hs).get()
            if val:
                href = requests.compat.urljoin(base_url, val.strip())
                break

        for ls in LOC_SELECTORS:
            val = c.css(ls).get()
            if val:
                location = val.strip()
                break

        if title and href and href not in seen:
            seen.add(href)
            jobs.append({"title": title, "url": href, "location": location})

    return jobs


def scrape_company(company_name: str, company_url: str) -> dict:
    start_url = normalize_jobs_url(company_url)
    out = {
        "company_name": company_name,
        "company_url": company_url,
        "status": "ok",
        "jobs": [],
        "jobs_count": 0     # 👉 нове поле
    }

    page_url = start_url
    seen_urls = set()
    pages_crawled = 0
    MAX_PAGES = 20

    while page_url and pages_crawled < MAX_PAGES:
        res = fs_request_get(page_url, SESSION_NAME)
        pages_crawled += 1

        if not res["ok"] or res["status"] != 200:
            out["status"] = "error"
            break

        html, final_url = res["html"], res["url"]

        if "verify you are human" in (html or "").lower():
            out["status"] = "blocked"
            break

        jobs = parse_jobs(html, final_url)
        for j in jobs:
            if j["url"] not in seen_urls:
                seen_urls.add(j["url"])
                out["jobs"].append(j)

        if not jobs and pages_crawled == 1:
            flag = quick_no_jobs_or_missing(html, Selector(text=html).xpath("//title/text()").get() or "")
            out["status"] = flag or "no_jobs"
            break

        sel = Selector(text=html)
        next_href = sel.css("a[aria-label='Next']::attr(href)").get() \
                    or sel.css("a[data-testid='pagination-page-next']::attr(href)").get()
        if next_href:
            page_url = requests.compat.urljoin(final_url, next_href.strip())
            time.sleep(random.uniform(1.0, 2.5))
        else:
            break

    # 🔹 оновлюємо лічильник
    out["jobs_count"] = len(out["jobs"])

    if out["jobs_count"] == 0 and out["status"] == "ok":
        out["status"] = "no_jobs"

    return out



def main():
    df = pd.read_excel(EXCEL_PATH, engine="openpyxl")
    if NAME_COL not in df.columns or URL_COL not in df.columns:
        raise ValueError(f"Excel має містити колонки: '{NAME_COL}' і '{URL_COL}'")

    rows = [(str(r[NAME_COL]).strip(), str(r[URL_COL]).strip())
            for _, r in df.iterrows() if pd.notna(r[URL_COL])]

    if LIMIT_ROWS:
        rows = rows[:LIMIT_ROWS]

    results = []
    for i, (name, url) in enumerate(rows, 1):
        print(f"[{i}/{len(rows)}] {name} -> {url}")
        data = scrape_company(name, url)
        results.append(data)
        time.sleep(random.uniform(*SLEEP_BETWEEN))

    Path("out.json").write_text(json.dumps(results, ensure_ascii=False, indent=2))
    print("✅ Saved to out.json")


if __name__ == "__main__":
    main()
