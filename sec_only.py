#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
sec_only.py — Step 6 strict
- Scrape SEC Atom feed
- Keep ONLY previous Eastern Time (ET) calendar day
- Track ONLY: 8-K, 6-K, 10-Q, 10-K, Form 3, Form 4, SC 13D/G (and amendments)
- Score by form + simple keyword context
- Output: data/sec_filings_snapshot.json and data/sec_filings_snapshot.csv
Columns: ticker, company, industry, form, score, recommended
"""
import os, re, json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from bs4 import BeautifulSoup

UA = os.getenv("FETCH_UA", "Mozilla/5.0")
SEC_ATOM = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&output=atom&count=100&start={}"

# Allowed forms (prefix match covers amendments like 'SC 13D/A', '8-K/A')
FORM_PREFIXES = ("8-K","6-K","10-Q","10-K","3","4","SC 13D","SC 13G")

FORM_BASE_SCORES = {
    "8-K": 5, "6-K": 4, "10-Q": 5, "10-K": 5, "3": 2, "4": 3, "SC 13D": 4, "SC 13G": 4
}
KEYWORDS_POS = ("merger","acquire","guidance","partnership","contract","approval","up-list","uplist","spin-off","spinoff")
KEYWORDS_NEG = ("dilution","offering","register","shelf","atm","warrant","convertible","discount")

NY = ZoneInfo("America/New_York")

def previous_et_date() -> datetime.date:
    now_et = datetime.now(NY)
    return (now_et - timedelta(days=1)).date()

def is_allowed_form(form: str) -> bool:
    return any(form.startswith(p) for p in FORM_PREFIXES)

def base_score(form: str) -> int:
    for p, s in FORM_BASE_SCORES.items():
        if form.startswith(p):
            return s
    return 1

def parse_company(title: str) -> str:
    # Remove leading "FORM - " if present, then strip "(CIK)" part
    t = re.sub(r"^\s*[^-]+-\s*", "", title).strip()
    t = t.split("(")[0].strip()
    return t

def extract_ticker(title: str) -> str:
    # Some titles include "(TICKER)" but many don't. Best-effort regex for 1–5 uppercase letters.
    m = re.search(r"\(([A-Z]{1,5})\)", title)
    return m.group(1) if m else ""

def fetch_sec_filings(max_pages: int = 25):
    rows = []
    for p in range(max_pages):
        url = SEC_ATOM.format(p*100)
        try:
            r = requests.get(url, headers={"User-Agent": UA}, timeout=15)
            r.raise_for_status()
        except Exception:
            break
        soup = BeautifulSoup(r.text, "lxml-xml")
        entries = soup.find_all("entry")
        if not entries:
            break
        for e in entries:
            form = (e.find("category").get("term","") or "").strip()
            if not is_allowed_form(form):
                continue
            updated_raw = (e.findtext("updated") or "").strip()
            try:
                # updated has TZ offset already; interpret and convert to ET for date comparison
                updated_dt = datetime.fromisoformat(updated_raw)
            except Exception:
                continue
            et_dt = updated_dt.astimezone(NY)
            rows.append({
                "form": form,
                "title": (e.findtext("title") or "").strip(),
                "link": (e.find("link").get("href") if e.find("link") else "").strip(),
                "updated_et": et_dt.isoformat(),
                "updated_date_et": et_dt.date().isoformat()
            })
    return rows

def score_row(row: dict) -> dict:
    s = base_score(row["form"])
    tl = row["title"].lower()
    if any(k in tl for k in KEYWORDS_POS): s += 2
    if any(k in tl for k in KEYWORDS_NEG): s -= 2
    row["score"] = s
    row["recommended"] = s >= 5
    row["ticker"] = extract_ticker(row["title"])
    row["company"] = parse_company(row["title"])
    row["industry"] = ""  # placeholder for later enrichment
    return row

def main():
    target_date = previous_et_date()
    data = fetch_sec_filings()
    # Strict date filter: keep ONLY rows where updated ET date == previous ET date
    data = [r for r in data if r.get("updated_date_et") == target_date.isoformat()]
    # Score & project
    out = [score_row(r) for r in data]
    # Keep only required columns in CSV
    csv_rows = [
        {
            "ticker": r["ticker"],
            "company": r["company"],
            "industry": r["industry"],
            "form": r["form"],
            "score": r["score"],
            "recommended": r["recommended"],
        }
        for r in out
    ]

    os.makedirs("data", exist_ok=True)
    with open("data/sec_filings_snapshot.json","w") as fp:
        json.dump(out, fp, indent=2)
    pd.DataFrame(csv_rows, columns=["ticker","company","industry","form","score","recommended"]).to_csv(
        "data/sec_filings_snapshot.csv", index=False
    )
    print(f"Wrote {len(out)} filings for previous ET date {target_date}")

if __name__ == "__main__":
    main()
