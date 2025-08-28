#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
sec_only.py — Step 6 strict (v13)
Adds rich debug so we can see *why* zero rows:
- Saves raw allowed-form rows (unfiltered by date): data/sec_filings_raw.json
- Saves date histogram + form histogram: data/sec_debug_stats.json
- Still writes strict previous-ET snapshot: data/sec_filings_snapshot.json / .csv
"""
import os, re, json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from bs4 import BeautifulSoup

UA = os.getenv("FETCH_UA", "Mozilla/5.0")
SEC_ATOM = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&output=atom&count=100&start={}"

FORM_PREFIXES = ("8-K","6-K","10-Q","10-K","3","4","SC 13D","SC 13G")
FORM_BASE_SCORES = {"8-K":5, "6-K":4, "10-Q":5, "10-K":5, "3":2, "4":3, "SC 13D":4, "SC 13G":4}
KEYWORDS_POS = ("merger","acquire","guidance","partnership","contract","approval","up-list","uplist","spin-off","spinoff")
KEYWORDS_NEG = ("dilution","offering","register","shelf","atm","warrant","convertible","discount")

NY = ZoneInfo("America/New_York")

def previous_et_date():
    return (datetime.now(NY) - timedelta(days=1)).date()

def is_allowed_form(form: str) -> bool:
    return any(form.startswith(p) for p in FORM_PREFIXES)

def base_score(form: str) -> int:
    for p, s in FORM_BASE_SCORES.items():
        if form.startswith(p):
            return s
    return 1

def parse_company(title: str) -> str:
    t = re.sub(r"^\s*[^-]+-\s*", "", title).strip()
    t = t.split("(")[0].strip()
    return t

def extract_ticker(title: str) -> str:
    m = re.search(r"\(([A-Z]{1,5})\)", title)
    return m.group(1) if m else ""

def parse_iso_to_dt(s: str) -> datetime:
    s = (s or "").strip()
    if not s:
        raise ValueError("no timestamp")
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s)

def fetch_sec_filings(max_pages: int = 40):
    rows = []
    for p in range(max_pages):
        url = SEC_ATOM.format(p*100)
        try:
            r = requests.get(url, headers={"User-Agent": UA}, timeout=20)
            r.raise_for_status()
        except Exception:
            break
        soup = BeautifulSoup(r.text, "lxml-xml")
        entries = soup.find_all("entry")
        if not entries:
            break
        for e in entries:
            form_tag = e.find("category")
            form = (form_tag.get("term","") if form_tag else "").strip()
            if not is_allowed_form(form):
                continue
            upd_tag = e.find("updated")
            updated_raw = upd_tag.text.strip() if (upd_tag and upd_tag.text) else ""
            try:
                updated_dt = parse_iso_to_dt(updated_raw)  # aware
            except Exception:
                continue
            et_dt = updated_dt.astimezone(NY)
            title_tag = e.find("title")
            link_tag = e.find("link")
            rows.append({
                "form": form,
                "title": (title_tag.text if title_tag else "").strip(),
                "link": (link_tag.get("href") if link_tag else "").strip(),
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
    row["industry"] = ""  # enrich later
    return row

def main():
    target_date = previous_et_date().isoformat()
    raw_rows = fetch_sec_filings()

    # Write raw allowed-form rows for debugging
    os.makedirs("data", exist_ok=True)
    with open("data/sec_filings_raw.json","w") as fp:
        json.dump(raw_rows, fp, indent=2)

    # Histograms
    by_date = {}
    by_form = {}
    for r in raw_rows:
        by_date[r["updated_date_et"]] = by_date.get(r["updated_date_et"], 0) + 1
        by_form[r["form"]] = by_form.get(r["form"], 0) + 1
    debug = {"target_prev_et": target_date, "counts_by_date": by_date, "counts_by_form": by_form, "total_allowed_rows": len(raw_rows)}
    with open("data/sec_debug_stats.json","w") as fp:
        json.dump(debug, fp, indent=2)

    # Strict filter
    filt = [r for r in raw_rows if r.get("updated_date_et") == target_date]
    out = [score_row(r) for r in filt]

    # CSV projection
    csv_rows = [{"ticker":r["ticker"],"company":r["company"],"industry":r["industry"],"form":r["form"],"score":r["score"],"recommended":r["recommended"]} for r in out]

    with open("data/sec_filings_snapshot.json","w") as fp:
        json.dump(out, fp, indent=2)
    pd.DataFrame(csv_rows, columns=["ticker","company","industry","form","score","recommended"]).to_csv("data/sec_filings_snapshot.csv", index=False)
    print(f"[Step6] target_prev_et={target_date} raw={len(raw_rows)} snapshot={len(out)}")

if __name__ == "__main__":
    main()
