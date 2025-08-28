#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
news_overlay_only.py
Step 7 only — read data/step6_full.json, run news/PR overlay → data/step7_overlay.json
"""

import os, re, json, time, html
import requests

DATA_DIR = os.path.join(os.getcwd(), "data"); os.makedirs(DATA_DIR, exist_ok=True)
IN_JSON = os.path.join(DATA_DIR, "step6_full.json")
OUT_JSON = os.path.join(DATA_DIR, "step7_overlay.json")

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
PORTAL_HINTS = [
    "yahoo finance", "benzinga", "cnbc", "prnewswire", "globenewswire",
    "business wire", "tradingview", "investopedia", "forbes", "mitrade",
    "cointelegraph", "coincentral", "ainvest",
]

HEADERS = {
    "User-Agent": os.environ.get("SEC_USER_AGENT", "GrandMasterScript/1.2 (contact: you@example.com)"),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "close",
}

def fetch(url, **kwargs):
    try:
        resp = requests.get(url, headers=HEADERS, timeout=20, **kwargs)
        resp.raise_for_status()
        return resp
    except Exception as e:
        print(f"[WARN] GET failed: {url} -> {e}")
        return None

def ddg_search(query, max_results=5):
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
        href = re.sub(r"&amp;", "&", a.group(1))
        title = re.sub("<.*?>", "", a.group(2))
        sn = re.search(r'<a[^>]*class="result__snippet"[^>]*>(.*?)</a>', b, flags=re.DOTALL)
        snippet = re.sub("<.*?>", "", sn.group(1)) if sn else ""
        results.append({"title": title.strip(), "url": href.strip(), "snippet": snippet.strip()})
    return results

def classify_news(item):
    t = f"{item.get('title','')} {item.get('snippet','')}".lower()
    pos = any(k in t for k in POSITIVE_TERMS)
    neg = any(k in t for k in DILUTION_TERMS)
    sentiment = "neutral"
    if pos and not neg: sentiment = "positive"
    elif neg and not pos: sentiment = "negative"
    elif pos and neg: sentiment = "mixed"
    portal_match = any(p in t for p in PORTAL_HINTS)
    return sentiment, portal_match

def main():
    if not os.path.exists(IN_JSON):
        print(f"[ERROR] Missing {IN_JSON}")
        return
    with open(IN_JSON, "r", encoding="utf-8") as f:
        data = json.load(f)
    records = data.get("records", [])
    prev_date = data.get("date_et", "")

    overlay = {}
    for rec in records:
        ticker = rec.get("ticker") or ""
        company = rec.get("company") or ""
        key = ticker or company
        if not key: 
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
            for it in ddg_search(q, max_results=5):
                sent, portal = classify_news(it)
                it["sentiment"] = sent
                it["portal_match"] = portal
                hits.append(it)
            time.sleep(0.6)

        # Dedup by URL
        seen, dedup = set(), []
        for h in hits:
            u = h.get("url")
            if not u or u in seen: 
                continue
            seen.add(u); dedup.append(h)

        overlay[key] = {
            "ticker": ticker,
            "company": company,
            "news": dedup[:10],
            "has_positive": any(h.get("sentiment") == "positive" for h in dedup)
        }

    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump({"date_et": prev_date, "overlay": overlay}, f, indent=2)

    print(f"[NEWS] Wrote overlay for {len(overlay)} keys -> {OUT_JSON}")

if __name__ == "__main__":
    main()
