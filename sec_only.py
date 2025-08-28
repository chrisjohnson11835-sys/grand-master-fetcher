#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
sec_only.py — Step 6 (v17)
- Allowed forms only (8-K, 6-K, 10-Q, 10-K, 3, 4, SC 13D/G incl. amendments)
- Timestamp -> America/New_York
- Keep **yesterday ET + today ET**
- Enrich: CIK -> ticker/company (company_tickers.json), industry + SIC code (submissions API)
- **NON-NEGOTIABLE**: Banned filters aligned to SEC SIC taxonomy:
  * Exact/regex matches on official `sicDescription` strings
  * SIC numeric range bans (e.g., 6000–6999 Finance/Insurance/Real Estate, 2082/2084/2085 alcohol, 2100–2199 tobacco, 3480–3489 weapons, 7933/7948/7999 gambling)
- Outputs: snapshot.json/csv + raw.json + debug_stats.json
"""
import os, re, json, time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from bs4 import BeautifulSoup

UA = os.getenv("FETCH_UA", "Mozilla/5.0")
NY = ZoneInfo("America/New_York")

SEC_ATOM = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&output=atom&count=100&start={}"
TICKERS_JSON = "https://www.sec.gov/files/company_tickers.json"
SUBMISSIONS_API = "https://data.sec.gov/submissions/CIK{cik_padded}.json"

FORM_PREFIXES = ("8-K","6-K","10-Q","10-K","3","4","SC 13D","SC 13G")
FORM_BASE_SCORES = {"8-K":5, "6-K":4, "10-Q":5, "10-K":5, "3":2, "4":3, "SC 13D":4, "SC 13G":4}
KEYWORDS_POS = ("merger","acquire","guidance","partnership","contract","approval","up-list","uplist","spin-off","spinoff")
KEYWORDS_NEG = ("dilution","offering","register","shelf","atm","warrant","convertible","discount")

# ---- Canonical SIC-based bans ----
# Numeric ranges (inclusive) using SEC/OSHA SIC groupings
BANNED_SIC_RANGES = [
    (2082, 2082),  # Malt Beverages (Alcohol)
    (2084, 2084),  # Wines, Brandy, & Brandy Spirits
    (2085, 2085),  # Distilled & Blended Liquors
    (5813, 5813),  # Drinking Places (Alcoholic Beverages)
    (2100, 2199),  # Tobacco Products
    (6000, 6999),  # Finance, Insurance, and Real Estate (banks, lenders, insurers, brokers)
    (3480, 3489),  # Ordnance & Accessories (weapons & ammo)
    (7933, 7933),  # Bowling Centers (often excluded? leave neutral)  # kept as neutral (example)
    (7948, 7948),  # Racing, including track operation (gambling-adjacent)
    (7999, 7999),  # Amusement & Recreation, NEC (casinos often coded here)
]
# Exact/regex description bans (case-insensitive), aligned to SEC sicDescription strings
BANNED_SIC_DESCRIPTIONS_EXACT = {
    # Alcohol
    "Malt Beverages",
    "Wines, Brandy, & Brandy Spirits",
    "Distilled & Blended Liquors",
    "Drinking Places (Alcoholic Beverages)",
    # Tobacco
    "Tobacco Products",
    "Cigarettes",
    "Cigars",
    # Weapons
    "Ordnance & Accessories, NEC",
    "Small Arms",
    "Small Arms Ammunition",
    "Ammunition, Except for Small Arms",
    # Gambling / Casinos (common sicDescription variants)
    "Amusement & Recreation Services, NEC",
    "Racing, Including Track Operation",
    "Coin-Operated Amusement Devices",
    "Miscellaneous Amusement & Recreation",
    "Casino Hotels",
    "Casinos",
    # Finance/Insurance
    "National Commercial Banks",
    "State Commercial Banks",
    "Commercial Banks, NEC",
    "Savings Institutions, Federally Chartered",
    "Savings Institutions, Not Federally Chartered",
    "Credit Unions, Federally Chartered",
    "Credit Unions, Not Federally Chartered",
    "Personal Credit Institutions",
    "Short-Term Business Credit Institutions, Except Agricultural",
    "Mortgage Bankers & Loan Correspondents",
    "Loan Brokers",
    "Security Brokers, Dealers & Flotation Companies",
    "Investment Advice",
    "Life Insurance",
    "Accident & Health Insurance",
    "Fire, Marine, & Casualty Insurance",
    "Surety Insurance",
    "Title Insurance",
    "Pension, Health, & Welfare Funds",
    "Real Estate Investment Trusts",
}
BANNED_SIC_REGEX = [
    r"\bcasino\b", r"\bgambling\b", r"\blottery|lotteries\b", r"\bpari[- ]?mutuel\b",
    r"\bcredit card\b", r"\bpayday\b", r"\bcheck cashing\b", r"\btitle loan\b",
    r"adult entertainment", r"pornographic|pornography",
    r"weapons?|ammunition|ordnance|firearms?"
]

def et_today_date():
    return datetime.now(NY).date()

def et_yesterday_date():
    return (datetime.now(NY) - timedelta(days=1)).date()

def http_get(url):
    r = requests.get(url, headers={"User-Agent": UA}, timeout=30)
    r.raise_for_status()
    return r

def is_allowed_form(form: str) -> bool:
    return any(form.startswith(p) for p in FORM_PREFIXES)

def base_score(form: str) -> int:
    for p, s in FORM_BASE_SCORES.items():
        if form.startswith(p):
            return s
    return 1

def parse_iso_to_dt(s: str) -> datetime:
    s = (s or "").strip()
    if not s: raise ValueError("no timestamp")
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s)

def fetch_sec_filings(max_pages: int = 40):
    rows = []
    for p in range(max_pages):
        url = SEC_ATOM.format(p*100)
        try:
            r = http_get(url)
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
                updated_dt = parse_iso_to_dt(updated_raw)
            except Exception:
                continue
            et_dt = updated_dt.astimezone(NY)
            title_tag = e.find("title")
            link_tag = e.find("link")
            link = (link_tag.get("href") if link_tag else "").strip()
            # Extract CIK when possible
            cik = ""
            m = re.search(r"[?&]CIK=(\d{10})", link)
            if not m:
                m2 = re.search(r"/data/(\d+)/", link)
                if m2:
                    cik = m2.group(1).zfill(10)
            else:
                cik = m.group(1)
            rows.append({
                "form": form,
                "title": (title_tag.text if title_tag else "").strip(),
                "link": link,
                "cik": cik,
                "updated_et": et_dt.isoformat(),
                "updated_date_et": et_dt.date().isoformat()
            })
    return rows

def load_company_tickers():
    try:
        r = http_get(TICKERS_JSON)
        data = r.json()
        m = {}
        for _k, rec in data.items():
            cik = str(rec.get("cik_str","")).zfill(10)
            ticker = (rec.get("ticker") or "").upper()
            title = rec.get("title","") or ""
            if cik:
                m[cik] = {"ticker": ticker, "company": title}
        return m
    except Exception:
        return {}

def fetch_industry_for_cik(cik: str):
    if not cik: return "", ""
    try:
        url = SUBMISSIONS_API.format(cik_padded=str(cik).zfill(10))
        r = http_get(url)
        j = r.json()
        sic = str(j.get("sic") or "").strip()
        sic_desc = (j.get("sicDescription") or "").strip()
        return sic, sic_desc
    except Exception:
        return "", ""

def enrich(rows):
    mapping = load_company_tickers()
    industry_cache = {}
    for r in rows:
        info = mapping.get(r.get("cik",""))
        if info:
            r["ticker"] = info.get("ticker","")
            r["company"] = info.get("company","")
        else:
            # fallback parse
            m = re.search(r"\(([A-Z]{1,5})\)", r.get("title",""))
            r["ticker"] = (m.group(1) if m else r.get("ticker","") or "").upper()
            if not r.get("company"):
                r["company"] = r.get("title","").split("(")[0].strip()
        cik = r.get("cik","")
        if cik and cik not in industry_cache:
            time.sleep(0.15)
            sic, sic_desc = fetch_industry_for_cik(cik)
            industry_cache[cik] = (sic, sic_desc)
        sic, sic_desc = industry_cache.get(cik, ("",""))
        r["sic"] = sic
        r["industry"] = sic_desc
    return rows

def sic_in_banned_ranges(sic: str) -> bool:
    if not sic: return False
    try:
        n = int(sic)
    except Exception:
        return False
    for lo, hi in BANNED_SIC_RANGES:
        if lo <= n <= hi:
            return True
    return False

def sic_desc_banned(desc: str) -> bool:
    if not desc: return False
    d = desc.lower()
    if desc in BANNED_SIC_DESCRIPTIONS_EXACT:
        return True
    for rx in BANNED_SIC_REGEX:
        if re.search(rx, d, flags=re.I):
            return True
    return False

def apply_bans(rows, extra_cfg):
    # Extra cfg from file/env (optional)
    bt = {t.upper() for t in (extra_cfg.get("tickers") or [])}
    bi = [s.lower() for s in (extra_cfg.get("industries") or [])]
    bc = [s.lower() for s in (extra_cfg.get("companies") or [])]

    kept, dropped = [], []
    for r in rows:
        tkr = (r.get("ticker","") or "").upper()
        comp = (r.get("company","") or "").lower()
        ind  = (r.get("industry","") or "").lower()
        sic  = r.get("sic","")

        drop = False
        # Canonical SIC bans
        if sic_in_banned_ranges(sic): drop = True
        if not drop and sic_desc_banned(r.get("industry","")): drop = True
        # User-config bans
        if not drop and tkr and tkr in bt: drop = True
        if not drop and ind and any(s in ind for s in bi): drop = True
        if not drop and comp and any(s in comp for s in bc): drop = True

        (dropped if drop else kept).append(r)
    return kept, dropped

def normalize_list(x):
    if not x: return []
    if isinstance(x, str):
        x = [p.strip() for p in x.split(",")]
    return [s for s in (i.strip() for i in x) if s]

def load_ban_config():
    cfg = {"tickers": [], "industries": [], "companies": []}
    path = os.path.join("config","banned_filters.json")
    if os.path.isfile(path):
        try:
            with open(path,"r") as fp: cfg = json.load(fp)
        except Exception: pass
    # env fallbacks
    cfg["tickers"] = normalize_list(os.getenv("BAN_TICKERS", cfg.get("tickers","")))
    cfg["industries"] = normalize_list(os.getenv("BAN_INDUSTRIES", cfg.get("industries","")))
    cfg["companies"] = normalize_list(os.getenv("BAN_COMPANIES", cfg.get("companies","")))
    return cfg

def score_row(row: dict) -> dict:
    s = base_score(row["form"])
    tl = row.get("title","").lower()
    if any(k in tl for k in KEYWORDS_POS): s += 2
    if any(k in tl for k in KEYWORDS_NEG): s -= 2
    row["score"] = s
    row["recommended"] = s >= 5
    return row

def main():
    target_y = (datetime.now(NY) - timedelta(days=1)).date().isoformat()
    target_t = datetime.now(NY).date().isoformat()

    raw_rows = fetch_sec_filings()
    os.makedirs("data", exist_ok=True)

    # Enrich with ticker/company and SEC SIC info
    raw_rows = enrich(raw_rows)

    # Filter by date (yesterday OR today ET)
    dated = [r for r in raw_rows if r.get("updated_date_et") in (target_y, target_t)]

    # Apply canonical SEC SIC bans + user config bans
    user_bans = load_ban_config()
    kept, dropped = apply_bans(dated, user_bans)

    # Score kept
    out = [score_row(r) for r in kept]

    # Debug + outputs
    with open("data/sec_filings_raw.json","w") as fp:
        json.dump(raw_rows, fp, indent=2)

    by_date, by_form = {}, {}
    for r in raw_rows:
        by_date[r["updated_date_et"]] = by_date.get(r["updated_date_et"], 0) + 1
        by_form[r["form"]] = by_form.get(r["form"], 0) + 1

    debug = {
        "target_prev_et": target_y,
        "target_today_et": target_t,
        "counts_by_date": by_date,
        "counts_by_form": by_form,
        "total_allowed_rows": len(raw_rows),
        "after_date_filter": len(dated),
        "after_ban_filter": len(out),
        "dropped_count": len(dropped),
        "ban_config_effective": user_bans
    }
    with open("data/sec_debug_stats.json","w") as fp:
        json.dump(debug, fp, indent=2)

    csv_rows = [{
        "ticker": r.get("ticker",""),
        "company": r.get("company",""),
        "industry": r.get("industry",""),
        "form": r.get("form",""),
        "score": r.get("score",0),
        "recommended": r.get("recommended", False)
    } for r in out]

    with open("data/sec_filings_snapshot.json","w") as fp:
        json.dump(out, fp, indent=2)
    pd.DataFrame(csv_rows, columns=["ticker","company","industry","form","score","recommended"]).to_csv("data/sec_filings_snapshot.csv", index=False)

    print(f"[Step6 v17] prev={target_y} today={target_t} raw={len(raw_rows)} dated={len(dated)} kept={len(out)} dropped={len(dropped)}")

if __name__ == "__main__":
    main()
