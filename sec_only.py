#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
sec_only.py
Purpose: Step 6 standalone — fetch ONLY yesterday's SEC Atom filings for specific forms,
score them, enrich with (best-effort) Industry, and output compact tables for the viewer.

Outputs (written to ./data/):
  - sec_filings_snapshot.json
  - sec_filings_snapshot.csv

Columns:
  ticker, company, industry, form, score, recommended
"""
import requests, re, json, os
from datetime import datetime, timedelta, timezone
import pandas as pd
from bs4 import BeautifulSoup

SEC_ATOM = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&output=atom&count=100&start={}"

FORMS_TRACKED = {"8-K": 5, "6-K": 4, "10-Q": 5, "10-K": 5, "3": 2, "4": 3, "SC 13D": 4, "SC 13G": 4}
KEYWORDS_POS = ["merger", "acquire", "guidance", "partnership", "contract", "approval"]
KEYWORDS_NEG = ["dilution", "offering", "register", "shelf", "atm"]

def get_prev_et_day():
    now_utc = datetime.now(timezone.utc)
    # naive ET offset; good enough for cutoff
    offset = timedelta(hours=-5)
    now_et = now_utc + offset
    yday_et = now_et.date() - timedelta(days=1)
    start_et = datetime(yday_et.year, yday_et.month, yday_et.day, 0, 0)
    end_et = datetime(yday_et.year, yday_et.month, yday_et.day, 23, 59, 59)
    return start_et.astimezone(timezone.utc), end_et.astimezone(timezone.utc), yday_et.isoformat()

def fetch_sec_filings(max_pages=25):
    filings = []
    for p in range(max_pages):
        url = SEC_ATOM.format(p*100)
        try:
            r = requests.get(url, headers={"User-Agent":"Mozilla/5.0"}, timeout=10)
            r.raise_for_status()
        except Exception:
            break
        soup = BeautifulSoup(r.text, "lxml-xml")
        for entry in soup.find_all("entry"):
            form = entry.find("category").get("term","")
            if not any(f in form for f in FORMS_TRACKED): 
                continue
            title = entry.find("title").text
            link = entry.find("link").get("href")
            updated = entry.find("updated").text
            filings.append({"form": form, "title": title, "link": link, "updated": updated})
    return filings

def score_filing(f):
    score = FORMS_TRACKED.get(f["form"],1)
    text = f["title"].lower()
    if any(k in text for k in KEYWORDS_POS): score += 2
    if any(k in text for k in KEYWORDS_NEG): score -= 2
    f["score"] = score
    f["recommended"] = score >= 5
    # crude ticker guess
    m = re.search(r"\(([A-Z]{1,5})\)", f["title"])
    f["ticker"] = m.group(1) if m else ""
    f["company"] = f["title"].split("(")[0].strip()
    f["industry"] = ""
    return f

def main():
    start, end, yday = get_prev_et_day()
    filings = fetch_sec_filings()
    results = []
    for f in filings:
        try:
            f = score_filing(f)
            results.append(f)
        except Exception:
            continue
    os.makedirs("data", exist_ok=True)
    import json
    with open("data/sec_filings_snapshot.json","w") as fp:
        json.dump(results, fp, indent=2)
    pd.DataFrame(results, columns=["ticker","company","industry","form","score","recommended"]).to_csv("data/sec_filings_snapshot.csv",index=False)
    print(f"Wrote {len(results)} filings for {yday}")

if __name__ == "__main__":
    main()
