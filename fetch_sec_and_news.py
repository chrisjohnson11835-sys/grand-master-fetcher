#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fetch_sec_and_news.py — Grand Master Script (Step 6 + Step 7)
- Atom feed only (SEC current filings)
- STRICT previous US/Eastern calendar day window
- Forms tracked: 8-K, 6-K, 10-Q, 10-K, 3, 4, SC 13D/G (+/A)
- Robust form extraction via category term/label → title fallback
- Detail-page ticker extraction (Trading Symbol, etc.)
- Heuristic scoring (positive vs dilution keywords)
- News overlay via DuckDuckGo HTML (portal hints)
- Exponential backoff on 429/5xx, polite pacing
- Outputs:
    data/step6_full.json
    data/step7_overlay.json
Env:
    SEC_USER_AGENT="YourApp/1.0 (contact: you@example.com)"
Optional env:
    MAX_PAGES (default 24 => 2400 entries), COUNT_PER_PAGE (default 100)
"""

import os
import re
import json
import time
import html
import random
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import requests
from xml.etree import ElementTree as ET

# --------------------------- Config ---------------------------

DATA_DIR = os.path.join(os.getcwd(), "data")
os.makedirs(DATA_DIR, exist_ok=True)

STEP6_JSON = os.path.join(DATA_DIR, "step6_full.json")
STEP7_JSON = os.path.join(DATA_DIR, "step7_overlay.json")

COUNT_PER_PAGE = int(os.environ.get("COUNT_PER_PAGE", "100"))
MAX_PAGES = int(os.environ.get("MAX_PAGES", "24"))  # 0..23 (2400 entries) default
REQUEST_TIMEOUT = 25
MAX_RETRIES = 6

SEC_ATOM_URL = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&start={start}&count={count}&output=atom"

TRACK_FORMS = {
    "8-K", "6-K", "10-Q", "10-K", "3", "4",
    "SC 13D", "SC 13G", "SC 13D/A", "SC 13G/A",
    "3/A", "4/A",
}

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

HEADERS = {
    "User-Agent": os.environ.get("SEC_USER_AGENT", "GrandMasterScript/1.2 (contact: you@example.com)"),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "close",
}

PORTAL_HINTS = [
    "yahoo finance", "benzinga", "cnbc", "prnewswire", "globenewswire",
    "business wire", "tradingview", "investopedia", "forbes", "mitrade",
    "cointelegraph", "coincentral", "ainvest",
]

# --------------------------- Time helpers ---------------------------

def now_et() -> datetime:
    return datetime.now(ZoneInfo("America/New_York"))

def prev_day_bounds_et():
    et_date = now_et().date()
    prev = et_date - timedelta(days=1)
    start_et = datetime(prev.year, prev.month, prev.day, 0, 0, 0, tzinfo=ZoneInfo("America/New_York"))
    end_et = datetime(prev.year, prev.month, prev.day, 23, 59, 59, tzinfo=ZoneInfo("America/New_York"))
    return start_et.astimezone(timezone.utc), end_et.astimezone(timezone.utc), prev.isoformat()

# --------------------------- HTTP helpers ---------------------------

def fetch(url: str, **kwargs):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT, **kwargs)
            status = resp.status_code
            if status in (429, 403) or status >= 500:
                raise requests.HTTPError(f"{status} slow down")
            resp.raise_for_status()
            return resp
        except Exception as e:
            wait = min(6.0, 0.6 * (2 ** (attempt - 1))) + random.uniform(0.0, 0.5)
            print(f"[WARN] GET failed (attempt {attempt}/{MAX_RETRIES}): {url} -> {e}; sleeping {wait:.2f}s")
            time.sleep(wait)
    print(f"[ERROR] Giving up on {url}")
    return None

# --------------------------- Atom parsing ---------------------------

FORM_PATTERNS = [
    r"\b8-K\b", r"\b6-K\b", r"\b10-Q\b", r"\b10-K\b",
    r"\bForm\s+3\b", r"\bForm\s+4\b", r"\b3\b", r"\b4\b",
    r"\bSC\s*13D\b", r"\bSC\s*13G\b", r"\bSC\s*13D/A\b", r"\bSC\s*13G/A\b",
    r"\bSCHEDULE\s*13D\b", r"\bSCHEDULE\s*13G\b", r"\bSCHEDULE\s*13D/A\b", r"\bSCHEDULE\s*13G/A\b",
    r"\b3/A\b", r"\b4/A\b",
]
FORM_REGEX = re.compile("|".join(FORM_PATTERNS), re.IGNORECASE)

def parse_atom(xml_text: str):
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

def normalize_form(text: str):
    s = (text or "").upper().replace("FORM ", "").strip()
    s = re.sub(r"\s+", " ", s)
    s = s.replace("SCHEDULE 13D", "SC 13D").replace("SCHEDULE 13G", "SC 13G")
    s = s.replace("SC13D", "SC 13D").replace("SC13G", "SC 13G").replace("SC 13 D", "SC 13D").replace("SC 13 G", "SC 13G")
    if s in {"3/A", "FORM 3/A"}: return "3/A"
    if s in {"4/A", "FORM 4/A"}: return "4/A"
    for v in ["8-K", "6-K", "10-Q", "10-K", "3", "4", "SC 13D", "SC 13G", "SC 13D/A", "SC 13G/A"]:
        if s == v: return v
    return s if s in TRACK_FORMS else None

def extract_form(entry: dict):
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

def extract_company(entry: dict):
    t = entry.get("title", "")
    t = re.sub(r"^Form\s+[\w\s/.-]+-\s*", "", t, flags=re.IGNORECASE)
    t = re.sub(r"\(.*?CIK.*?\)", "", t, flags=re.IGNORECASE)
    t = re.sub(r"\(CIK:.*?\)", "", t, flags=re.IGNORECASE)
    t = re.sub(r"\(000\d+\)", "", t)
    return t.strip(" -\u2013").strip()

# --------------------------- Ticker from detail page ---------------------------

TICKER_PATTERNS = [
    r"Trading Symbol(?:\(s\))?\s*[:\-]\s*([A-Z.\-]{1,5})",
    r"Ticker(?: Symbol)?\s*[:\-]\s*([A-Z.\-]{1,5})",
    r"(?i)NASDAQ:\s*([A-Z.\-]{1,5})",
    r"(?i)NYSE:\s*([A-Z.\-]{1,5})",
    r"(?i)NYSE\s+MKT:\s*([A-Z.\-]{1,5})",
    r"(?i)AMEX:\s*([A-Z.\-]{1,5})",
]
TICKER_REGEXES = [re.compile(p) for p in TICKER_PATTERNS]

def guess_ticker_from_detail(url: str):
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

# --------------------------- Scoring ---------------------------

def score_filing(form: str, title_text: str):
    base = {
        "8-K": 10, "6-K": 7, "10-Q": 8, "10-K": 6, "3": 5, "4": 9,
        "SC 13D": 9, "SC 13G": 7, "SC 13D/A": 8, "SC 13G/A": 6, "3/A": 4, "4/A": 8,
    }.get(form, 0)
    t = (title_text or "").lower()
    pos_hits = [k for k in POSITIVE_TERMS if k in t]
    neg_hits = [k for k in DILUTION_TERMS if k in t]
    score = base + 2 * len(pos_hits) - 3 * len(neg_hits)
    flags = []
    if pos_hits: flags.append({"positive": pos_hits})
    if neg_hits: flags.append({"dilution": neg_hits})
    return max(score, 0), flags

def within_prev_day(updated_dt_utc: datetime, start_utc: datetime, end_utc: datetime):
    if updated_dt_utc is None: return False
    return start_utc <= updated_dt_utc <= end_utc

# --------------------------- News overlay ---------------------------

def ddg_search(query: str, max_results=5):
    url = "https://html.duckduckgo.com/html/"
    params = {"q": query}
    resp = fetch(url, params=params)
    results = []
    if not resp: return results
    html_text = resp.text
    blocks = re.split(r'<div class="result">', html_text)[1:]
    for b in blocks[:max_results]:
        a = re.search(r'<a[^>]+class="result__a"[^>]+href="([^"]+)"[^>]*>(.*?)</a>', b, flags=re.DOTALL)
        if not a: continue
        href = html.unescape(a.group(1))
        title = re.sub("<.*?>", "", html.unescape(a.group(2))).strip()
        sn = re.search(r'<a[^>]*class="result__snippet"[^>]*>(.*?)</a>', b, flags=re.DOTALL)
        snippet = re.sub("<.*?>", "", html.unescape(sn.group(1))).strip() if sn else ""
        results.append({"title": title, "url": href, "snippet": snippet})
    return results

def classify_news(item: dict):
    t = f"{item.get('title','')} {item.get('snippet','')}".lower()
    pos = any(k in t for k in POSITIVE_TERMS)
    neg = any(k in t for k in DILUTION_TERMS)
    sentiment = "neutral"
    if pos and not neg: sentiment = "positive"
    elif neg and not pos: sentiment = "negative"
    elif pos and neg: sentiment = "mixed"
    portal_match = any(p in t for p in PORTAL_HINTS)
    return sentiment, portal_match

# --------------------------- Main fetch ---------------------------

def fetch_sec_prev_day():
    start_utc, end_utc, prev_date_str = prev_day_bounds_et()
    print(f"[INFO] Previous day (ET): {prev_date_str} | UTC window: {start_utc} -> {end_utc}")

    collected = []
    older_seen = 0

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
                older_seen += 1
                continue
            if upd_utc > end_utc:
                continue

            form = extract_form(en)
            if not form or form not in TRACK_FORMS:
                continue

            company = extract_company(en)
            filing_url = en.get("link", "")
            ticker = guess_ticker_from_detail(filing_url) or ""

            score, flags = score_filing(form, en.get("title", ""))

            collected.append({
                "ticker": ticker,
                "company": company,
                "form": form,
                "filed_utc": upd_utc.isoformat(),
                "filing_url": filing_url,
                "score": score,
                "flags": flags,
            })

        if older_seen >= COUNT_PER_PAGE:
            print(f"[INFO] Many entries older than window; stopping at page {page}.")
            break

        time.sleep(1.0)  # polite pacing for SEC

    filtered = [
        r for r in collected
        if r["form"] in TRACK_FORMS and within_prev_day(datetime.fromisoformat(r["filed_utc"]), start_utc, end_utc)
    ]

    # Deduplicate by filing_url
    seen = set()
    unique = []
    for r in filtered:
        k = r["filing_url"]
        if k in seen:
            continue
        seen.add(k)
        unique.append(r)

    # Sort by score desc, then filed_utc desc
    unique.sort(key=lambda x: (x["score"], x["filed_utc"]), reverse=True)

    with open(STEP6_JSON, "w", encoding="utf-8") as f:
        json.dump({"date_et": prev_date_str, "count": len(unique), "records": unique}, f, indent=2)

    print(f"[SEC] Wrote {len(unique)} records -> {STEP6_JSON}")
    return unique, prev_date_str

def run_news_overlay(step6_records, prev_date_str):
    overlay = {}
    for rec in step6_records:
        ticker = rec.get("ticker") or ""
        company = rec.get("company") or ""
        if not (ticker or company):
            continue

        queries = []
        if ticker:
            queries.append(f"{ticker} news")
            site_q = " OR ".join([f"site:{s}.com" for s in ["prnewswire","globenewswire","businesswire","benzinga","cnbc","finance.yahoo","seekingalpha"]])
            queries.append(f"{ticker} ({site_q})")
        if company:
            queries.append(f"\"{company}\" news")
            queries.append(f"\"{company}\" press release")

        hits = []
        for q in queries[:3]:
            rs = ddg_search(q, max_results=5)
            for it in rs:
                sent, portal_ok = classify_news(it)
                it["sentiment"] = sent
                it["portal_match"] = portal_ok
                hits.append(it)
            time.sleep(0.6)

        # Dedup by URL
        seen = set()
        deduped = []
        for h in hits:
            u = h.get("url")
            if not u or u in seen:
                continue
            seen.add(u)
            deduped.append(h)

        overlay[ticker or company] = {
            "ticker": ticker,
            "company": company,
            "news": deduped[:10],
            "has_positive": any(h.get("sentiment") == "positive" for h in deduped),
        }

    with open(STEP7_JSON, "w", encoding="utf-8") as f:
        json.dump({"date_et": prev_date_str, "overlay": overlay}, f, indent=2)

    print(f"[NEWS] Wrote overlay for {len(overlay)} keys -> {STEP7_JSON}")
    return overlay

def main():
    recs, prev_date_str = fetch_sec_prev_day()
    run_news_overlay(recs, prev_date_str)

if __name__ == "__main__":
    main()
