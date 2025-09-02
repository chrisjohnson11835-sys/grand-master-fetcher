# -*- coding: utf-8 -*-
import requests, io, re, time
from datetime import datetime
from .rate_limiter import RateLimiter

BASE = "https://www.sec.gov/Archives/edgar/daily-index"

def fetch_master_idx(year: int, qtr: int, yyyymmdd: str, ua: str, timeout: int, rl: RateLimiter):
    url = f"{BASE}/{year}/QTR{qtr}/master.{yyyymmdd}.idx"
    headers = {"User-Agent": ua, "Accept-Encoding": "gzip, deflate"}
    for attempt in range(5):
        try:
            resp = requests.get(url, headers=headers, timeout=timeout)
            if resp.status_code == 200:
                return resp.text
            if resp.status_code in (429, 503):
                retry_after = int(resp.headers.get("Retry-After", "3"))
                time.sleep(max(3, retry_after))
                continue
        except requests.RequestException:
            time.sleep(2 * (attempt + 1))
        finally:
            rl.wait()
    return None

def parse_master_idx(text: str):
    # Skip header lines; entries start after a line with "-----"
    lines = text.splitlines()
    start = 0
    for i, ln in enumerate(lines):
        if ln.startswith("-----"):
            start = i + 1
            break
    entries = []
    for ln in lines[start:]:
        parts = ln.split("|")
        if len(parts) != 5:
            continue
        company, form, cik, date_filed, filename = parts
        entries.append({
            "company": company.strip(),
            "form": form.strip().upper(),
            "cik": cik.strip().lstrip("0"),
            "date_filed": date_filed.strip(),
            "filename": filename.strip(),
        })
    return entries
