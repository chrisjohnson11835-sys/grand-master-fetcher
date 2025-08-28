#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fetch_sec_only.py
Step 6 only — SEC Atom (previous ET day) → data/step6_full.json
Forms: 8-K, 6-K, 10-Q, 10-K, 3, 4, SC 13D/G (+/A)
Backoff + fallbacks for form parsing; ticker extraction from filing detail page.
"""

import os, re, json, time, html, random
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import requests
from xml.etree import ElementTree as ET

DATA_DIR = os.path.join(os.getcwd(), "data"); os.makedirs(DATA_DIR, exist_ok=True)
OUT_JSON = os.path.join(DATA_DIR, "step6_full.json")

SEC_ATOM_URL = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&start={start}&count={count}&output=atom"
COUNT_PER_PAGE = int(os.environ.get("COUNT_PER_PAGE", "100"))
MAX_PAGES = int(os.environ.get("MAX_PAGES", "24"))
REQUEST_TIMEOUT = 25
MAX_RETRIES = 6

TRACK_FORMS = {
    "8-K", "6-K", "10-Q", "10-K", "3", "4", "SC 13D", "SC 13G",
    "SC 13D/A", "SC 13G/A", "3/A", "4/A"
}

HEADERS = {
    "User-Agent": os.environ.get("SEC_USER_AGENT", "GrandMasterScript/1.2 (contact: you@example.com)"),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "close",
}

FORM_PATTERNS = [
    r"\b8-K\b", r"\b6-K\b", r"\b10-Q\b", r"\b10-K\b",
    r"\bForm\s+3\b", r"\bForm\s+4\b", r"\b3\b", r"\b4\b",
    r"\bSC\s*13D\b", r"\bSC\s*13G\b", r"\bSC\s*13D/A\b", r"\bSC\s*13G/A\b",
    r"\bSCHEDULE\s*13D\b", r"\bSCHEDULE\s*13G\b", r"\bSCHEDULE\s*13D/A\b", r"\bSCHEDULE\s*13G/A\b",
    r"\b3/A\b", r"\b4/A\b"
]
FORM_REGEX = re.compile("|".join(FORM_PATTERNS), re.IGNORECASE)

POSITIVE_TERMS = [
    "guidance raise", "raises guidance", "boosts guidance",
    "merger", "acquisition", "acquire", "acquiring", "buyout",
    "buy-back", "buyback", "repurchase", "dividend",
    "approval", "fda approval", "clearance", "contract", "award",
    "strategic partnership", "partnership", "collaboration",
    "upgrade", "upgrades", "added to index", "included in index",
    "secures funding", "non-dilutive", "grant", "license", "licensing",
    "reaffirms guidance", "outlook raised", "surpasses expectations",
]
DILUTION_TERMS = [
    "offering", "equity offering", "registered direct",
    "pipe", "shelf", "s-3", "at-the-market", "atm offering",
    "warrant", "convertible", "preferred stock", "rights offering",
    "pricing of", "securities purchase agreement", "unit offering",
]

def now_et():
    return datetime.now(ZoneInfo("America/New_York"))

def prev_day_bounds_et():
    et_date = now_et().date()
    prev = et_date - timedelta(days=1)
    start_et = datetime(prev.year, prev.month, prev.day, 0, 0, 0, tzinfo=ZoneInfo("America/New_York"))
    end_et = datetime(prev.year, prev.month, prev.day, 23, 59, 59, tzinfo=ZoneInfo("America/New_York"))
    return start_et.astimezone(timezone.utc), end_et.astimezone(timezone.utc), prev.isoformat()

def fetch(url, **kwargs):
    for attempt in range(1, MAX_RETRIES+1):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT, **kwargs)
            status = resp.status_code
            if status in (429, 403) or status >= 500:
                raise requests.HTTPError(f"{status}")
            resp.raise_for_status()
            return resp
        except Exception as e:
            wait = min(6.0, 0.6*(2**(attempt-1))) + random.uniform(0.0, 0.5)
            print(f"[WARN] GET failed ({attempt}/{MAX_RETRIES}): {url} -> {e}; sleep {wait:.2f}s")
            time.sleep(wait)
    print(f"[ERROR] Giving up on {url}")
    return None

def parse_atom(xml_text):
    ns = {"atom": "http://www.w3.org/2005/Atom"}
    root = ET.fromstring(xml_text)
    out = []
    for e in root.findall("atom:entry", ns):
        entry = {}
        updated = e.findtext("atom:updated", default="", namespaces=ns) or e.findtext("atom:published", default="", namespaces=ns)
        entry["updated_raw"] = updated
        try:
            entry["updated_dt"] = datetime.fromisoformat(updated.replace("Z", "+00:00"))
        except Exception:
            entry["updated_dt"] = None
        entry["title"] = e.findtext("atom:title", default="", namespaces=ns) or ""
        link_el = e.find("atom:link", ns)
        entry["link"] = link_el.get("href") if link_el is not None else ""
        cats = []
        for c in e.findall("atom:category", ns):
            term = (c.get("term") or "").strip()
            label = (c.get("label") or "").strip()
            if term: cats.append(term)
            if label and label not in cats: cats.append(label)
        entry["categories"] = cats
        out.append(entry)
    return out

def normalize_form(text):
    s = (text or "").upper().replace("FORM ", "").strip()
    s = re.sub(r"\s+", " ", s)
    s = s.replace("SCHEDULE 13D", "SC 13D").replace("SCHEDULE 13G", "SC 13G")
    s = s.replace("SC13D", "SC 13D").replace("SC13G", "SC 13G").replace("SC 13 D", "SC 13D").replace("SC 13 G", "SC 13G")
    if s in {"3/A", "FORM 3/A"}: return "3/A"
    if s in {"4/A", "FORM 4/A"}: return "4/A"
    for v in ["8-K", "6-K", "10-Q", "10-K", "3", "4", "SC 13D", "SC 13G", "SC 13D/A", "SC 13G/A"]:
        if s == v: return v
    return s if s in TRACK_FORMS else None

def extract_form(entry):
    for c in entry.get("categories", []):
        m = FORM_REGEX.search(c)
        if m:
            f = normalize_form(m.group(0))
            if f: return f
    t = entry.get("title", "")
    m = FORM_REGEX.search(t)
    if m:
        f = normalize_form(m.group(0))
        if f: return f
    return None

def extract_company(entry):
    t = entry.get("title","")
    t = re.sub(r"^Form\s+[\w\s/.-]+-\s*", "", t, flags=re.IGNORECASE)
    t = re.sub(r"\(.*?CIK.*?\)", "", t, flags=re.IGNORECASE)
    t = re.sub(r"\(CIK:.*?\)", "", t, flags=re.IGNORECASE)
    t = re.sub(r"\(000\d+\)", "", t)
    return t.strip(" -\u2013").strip()

TICKER_PATTERNS = [
    r"Trading Symbol(?:\(s\))?\s*[:\-]\s*([A-Z.\-]{1,5})",
    r"Ticker(?: Symbol)?\s*[:\-]\s*([A-Z.\-]{1,5})",
    r"(?i)NASDAQ:\s*([A-Z.\-]{1,5})",
    r"(?i)NYSE:\s*([A-Z.\-]{1,5})",
    r"(?i)NYSE\s+MKT:\s*([A-Z.\-]{1,5})",
    r"(?i)AMEX:\s*([A-Z.\-]{1,5})"
]
TICKER_REGEXES = [re.compile(p) for p in TICKER_PATTERNS]

def guess_ticker_from_detail(url):
    if not url: return None
    resp = fetch(url)
    if not resp: return None
    text = resp.text
    for rx in TICKER_REGEXES:
        m = rx.search(text)
        if m:
            sym = m.group(1).upper().strip(".- ")
            if 1 <= len(sym) <= 5: return sym
    m = re.search(r"Trading Symbol.*?([A-Z]{1,5})", text, flags=re.DOTALL)
    if m: return m.group(1).upper()
    return None

def score_filing(form, title_text):
    base = {
        "8-K": 10, "6-K": 7, "10-Q": 8, "10-K": 6, "3": 5, "4": 9,
        "SC 13D": 9, "SC 13G": 7, "SC 13D/A": 8, "SC 13G/A": 6, "3/A": 4, "4/A": 8
    }.get(form, 0)
    t = (title_text or "").lower()
    pos_hits = [k for k in POSITIVE_TERMS if k in t]
    neg_hits = [k for k in DILUTION_TERMS if k in t]
    score = base + 2*len(pos_hits) - 3*len(neg_hits)
    flags = []
    if pos_hits: flags.append({"positive": pos_hits})
    if neg_hits: flags.append({"dilution": neg_hits})
    return max(score, 0), flags

def within_prev_day(updated_dt_utc, start_utc, end_utc):
    if updated_dt_utc is None: return False
    return start_utc <= updated_dt_utc <= end_utc

def main():
    start_utc, end_utc, prev_date_str = prev_day_bounds_et()
    print(f"[INFO] Previous day (ET): {prev_date_str} | UTC window: {start_utc} -> {end_utc}")
    collected, older_seen = [], 0

    for page in range(MAX_PAGES):
        start = page * COUNT_PER_PAGE
        url = SEC_ATOM_URL.format(start=start, count=COUNT_PER_PAGE)
        resp = fetch(url)
        if not resp:
            print(f"[WARN] Skipping page {page} due to fetch error")
            continue

        try:
            entries = parse_atom(resp.text)
        except Exception as e:
            print(f"[WARN] XML parse failed page {page}: {e}")
            continue

        if not entries:
            print(f"[INFO] No entries on page {page}, stopping.")
            break

        for en in entries:
            upd = en.get("updated_dt")
            if upd is None: 
                continue
            upd_utc = upd.astimezone(timezone.utc)

            if upd_utc < start_utc:
                older_seen += 1; continue
            if upd_utc > end_utc:
                continue

            form = extract_form(en)
            if not form or form not in TRACK_FORMS:
                continue

            company = extract_company(en)
            filing_url = en.get("link","")
            ticker = guess_ticker_from_detail(filing_url) or ""

            score, flags = score_filing(form, en.get("title",""))

            collected.append({
                "ticker": ticker,
                "company": company,
                "form": form,
                "filed_utc": upd_utc.isoformat(),
                "filing_url": filing_url,
                "score": score,
                "flags": flags
            })

        if older_seen >= COUNT_PER_PAGE:
            print(f"[INFO] Many entries older than window; stopping at page {page}.")
            break

        time.sleep(1.0)

    filtered = [
        r for r in collected
        if r["form"] in TRACK_FORMS and within_prev_day(datetime.fromisoformat(r["filed_utc"]), start_utc, end_utc)
    ]

    seen, unique = set(), []
    for r in filtered:
        k = r["filing_url"]
        if k in seen: continue
        seen.add(k); unique.append(r)

    unique.sort(key=lambda x: (x["score"], x["filed_utc"]), reverse=True)

    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump({"date_et": prev_date_str, "count": len(unique), "records": unique}, f, indent=2)

    print(f"[SEC] Wrote {len(unique)} records -> {OUT_JSON}")

if __name__ == "__main__":
    main()
