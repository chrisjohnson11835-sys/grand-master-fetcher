#!/usr/bin/env python3
# fetch_sec_and_news.py — full file (clean, debug-ready)

import os, re, json, time
from datetime import datetime, timedelta
import requests

# ---------- Config ----------
UA = os.getenv("UA", "GrandMasterFetcher/1.0 (contact: you@example.com)")
DATA_DIR = os.getenv("OUT_DIR", "out")
NEWS_LOOKBACK_HOURS = int(os.getenv("NEWS_LOOKBACK_HOURS", "30"))
TOPN_FOR_NEWS = int(os.getenv("TOPN_FOR_NEWS", "120"))
DEBUG = os.getenv("DEBUG", "0") == "1"

TARGET_FORMS = {
    "8-K", "6-K", "10-Q", "10-K", "3", "4",
    "SC 13D", "SC 13G", "SC 13D/A", "SC 13G/A"
}

HEADERS = {
    "User-Agent": UA,
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "close",
}

# ---------- Utils ----------
def dbg(msg): 
    if DEBUG: print(f"[DEBUG] {msg}")

def ensure_dirs():
    os.makedirs(DATA_DIR, exist_ok=True)

def fetch(url, timeout=25):
    dbg(f"GET {url}")
    r = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
    r.raise_for_status()
    return r.text

def sec_master_idx_url(dt: datetime) -> str:
    y = dt.year
    q = (dt.month - 1) // 3 + 1
    ymd = dt.strftime("%Y%m%d")
    return f"https://www.sec.gov/Archives/edgar/daily-index/{y}/QTR{q}/master.{ymd}.idx"

def try_master(days_back_start=1, tries=14):
    """
    Walk back day-by-day until we find a valid SEC master daily index.
    Default checks past ~2 weeks to avoid market-holiday gaps.
    """
    for d in range(days_back_start, days_back_start + tries):
        dt = datetime.utcnow() - timedelta(days=d)
        url = sec_master_idx_url(dt)
        try:
            txt = fetch(url)
            dbg(f"SEC master OK: {url}")
            return txt, dt.strftime("%Y-%m-%d"), url
        except Exception as e:
            dbg(f"SEC master FAIL: {url} :: {e}")
            continue
    return None, None, None

def parse_master_idx(raw):
    rows, start = [], False
    for ln in raw.splitlines():
        ln = ln.strip()
        if not start:
            if "Company Name|Form Type|CIK|Date Filed|Filename" in ln:
                start = True
            continue
        if not ln:
            continue
        parts = ln.split("|")
        if len(parts) < 5:
            continue
        rows.append({
            "company": parts[0],
            "form": parts[1],
            "cik": parts[2],
            "date": parts[3],
            "path": parts[4]
        })
    return rows

def clean_text(html):
    txt = re.sub(r"<[^>]+>", " ", html)
    txt = re.sub(r"\s+", " ", txt)
    return txt.strip()

def extract_items_8k(txt):
    return sorted(set(m.upper() for m in re.findall(r"Item\s+(\d+\.\d+)", txt, flags=re.I)))

def score_context(txt):
    bull = [
        'raises guidance','beat','beats','acquisition','definitive agreement','buyback',
        'special dividend','approval','contract awarded','being acquired','all-cash',
        'premium of','activist','strategic alternatives','merger','closing of acquisition'
    ]
    bear = [
        'offering','registered direct','atm','at-the-market','shelf','warrant','reverse split',
        'delisting','deficiency','going concern','convertible','dilution','equity line'
    ]
    s = 0; low = txt.lower()
    for w in bull:
        if w in low: s += 3
    for w in bear:
        if w in low: s -= 6
    return s

def base_weight_items(items, form):
    f = form.upper(); w = 0
    if f == "8-K":
        for it in items:
            if it == "2.02": w += 10      # earnings
            if it in ("1.01","2.01"): w += 12  # MA / acquisition or material agreement
            if it == "8.01": w += 6       # other events (often PR-aligned)
            if it == "3.01": w -= 12      # listing compliance
            if it == "3.02": w -= 15      # unregistered sales (dilution)
            if it == "5.03": w -= 10      # charter/bylaw changes (splits etc.)
    elif f == "6-K": w += 6
    elif f in ("10-Q","10-K"): w += 5
    elif f == "4": w += 2
    elif f.startswith("SC 13D"): w += 10
    elif f.startswith("SC 13G"): w += 2
    return w

def guess_ticker(html):
    m = re.search(r"Trading Symbol[s]?:\s*([A-Z\.]{1,6})", html, flags=re.I)
    return m.group(1).upper() if m else ""

def fetch_doc(url):
    txt = fetch(url)
    if "Document Format Files" in txt:
        m = re.search(r'href="([^"]+\.htm[^"]*)"', txt, flags=re.I)
        if m:
            doc_url = "https://www.sec.gov" + m.group(1)
            try:
                return fetch(doc_url)
            except Exception as e:
                dbg(f"Primary doc fetch failed, using index body. {e}")
    return txt

# ---------- SEC pass ----------
def run_sec():
    master_raw, master_date, master_url = try_master(1, 14)
    if not master_raw:
        print("SEC: wrote 0 records")
        with open(os.path.join(DATA_DIR,"step6_full.json"),"w") as f:
            f.write("[]")
        return

    rows = parse_master_idx(master_raw)
    dbg(f"Parsed {len(rows)} rows from {master_url}")

    out = []
    kept, skipped_no_target, fetched, errors = 0, 0, 0, 0

    for r in rows:
        form = r["form"].upper()
        if form not in TARGET_FORMS:
            skipped_no_target += 1
            continue

        filing_url = "https://www.sec.gov/Archives/" + r["path"].lstrip("/")
        try:
            html = fetch_doc(filing_url)
            fetched += 1
        except Exception as e:
            errors += 1
            dbg(f"FILING GET FAIL {filing_url} :: {e}")
            continue

        text = clean_text(html)
        items = extract_items_8k(text) if form == "8-K" else []
        score = base_weight_items(items, form) + score_context(text)
        ticker = guess_ticker(html)
        dilution = bool(re.search(
            r"(offering|registered direct|atm|at-the-market|shelf|warrant|reverse split|delisting|deficiency|convertible)",
            text, flags=re.I
        ))

        out.append({
            "ticker": ticker,
            "company": r["company"],
            "industry": "",
            "form": r["form"],
            "cik": r["cik"],
            "accepted_ts": r["date"] + "T00:00:00Z",
            "items": items,
            "score": score,
            "reasons": [],
            "dilution_flags": (["possible_dilution"] if dilution else [])
        })
        kept += 1

    with open(os.path.join(DATA_DIR,"step6_full.json"),"w") as f:
        json.dump(out, f, indent=2)

    print(f"SEC: wrote {kept} records")
    if DEBUG:
        print(f"[DEBUG] Stats — total_rows:{len(rows)} target_forms:{kept+errors} "
              f"skipped_non_target:{skipped_no_target} fetched_docs:{fetched} errors:{errors}")

# ---------- News/PR overlay ----------
def yahoo_rss(ticker):
    url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=US&lang=en-US"
    try:
        xml = fetch(url, timeout=15)
    except Exception as e:
        dbg(f"Yahoo RSS fail {ticker}: {e}")
        return []
    items = []
    for m in re.finditer(r"<item>.*?<link>(.*?)</link>.*?<pubDate>(.*?)</pubDate>.*?</item>", xml, flags=re.S|re.I):
        items.append({"url": m.group(1).strip(), "pub": m.group(2)})
    return items

ENABLED = {
    "prnewswire": "https://www.prnewswire.com/search/news/?query=",
    "globenewswire": "https://www.globenewswire.com/Search/NewsSearch?keyword=",
    "cnbc": "https://www.cnbc.com/search/?query=",
    "tradingview": "https://www.tradingview.com/search/?q=",
    "stockstotrade": "https://stockstotrade.com/?s=",
    "coincentral": "https://coincentral.com/?s=",
    "mitrade": "https://www.mitrade.com/insights/search?keyword=",
    "ainvest": "https://www.ainvest.com/search?q=",
    "benzinga": "https://www.benzinga.com/search?q=",
    "investopedia": "https://www.investopedia.com/search?q=",
    "forbes": "https://www.forbes.com/search/?q="
}

def search_simple(base, q):
    try:
        html = fetch(base + requests.utils.quote(q), timeout=22)
    except Exception as e:
        dbg(f"Search fail {base} :: {e}")
        return []
    return list(set(re.findall(r'href="(https?://[^"]+)"', html, flags=re.I)))

def domain(url):
    m = re.match(r"https?://([^/]+)", url)
    if not m: return ""
    h = m.group(1).lower()
    h = re.sub(r"^(www\.|m\.)", "", h)
    return h

def run_news():
    # load SEC results
    path = os.path.join(DATA_DIR, "step6_full.json")
    try:
        with open(path) as f:
            sec = json.load(f)
    except Exception:
        sec = []

    # pick best per ticker
    best = {}
    for r in sec:
        t = (r.get("ticker") or "").upper()
        if not t: 
            continue
        if t not in best or (r.get("score",0) > best[t].get("score", -999)):
            best[t] = r
    top = sorted(best.values(), key=lambda x: x.get("score",0), reverse=True)[:TOPN_FOR_NEWS]

    if DEBUG:
        print(f"[DEBUG] News overlay — unique tickers from SEC: {len(best)} / scanning top: {len(top)}")

    results = []
    for r in top:
        t = (r.get("ticker") or "").upper()
        if not t: 
            continue
        company = r.get("company","")
        hits = []

        # Yahoo RSS (time filter applied)
        for it in yahoo_rss(t):
            try:
                pub = datetime.strptime(it["pub"], "%a, %d %b %Y %H:%M:%S %z").astimezone()
                if datetime.now(pub.tzinfo) - pub <= timedelta(hours=NEWS_LOOKBACK_HOURS):
                    hits.append(it["url"])
            except Exception as e:
                dbg(f"RSS parse fail {t}: {e}")

        # Domain searches
        q = f"{t} {company}".strip()
        for key, base in ENABLED.items():
            hits += search_simple(base, q)

        # de-dupe
        uniq = set(); sources = []
        for u in hits:
            d = domain(u)
            if not d: 
                continue
            if u in uniq: 
                continue
            uniq.add(u); sources.append(u)

        # score subset
        overlay = 0; veto=set(); seen_domains=set()
        for u in sources[:8]:
            try:
                html = fetch(u, timeout=20)
            except Exception as e:
                dbg(f"Fetch news fail {u}: {e}")
                continue
            txt = clean_text(html); d = domain(u); seen_domains.add(d)
            if re.search(r"(raises guidance|beats|acquisition|buyback|special dividend|approval|contract awarded|being acquired|strategic alternatives|activist|merger)", txt, flags=re.I):
                overlay += 4
            if re.search(r"(registered direct|atm|at-the-market|shelf|warrant|reverse split|delisting|deficiency|convertible)", txt, flags=re.I):
                veto.add("financing_flag")
        overlay += max(0, len(seen_domains)-1)

        results.append({
            "ticker": t,
            "sources_hit": sources,
            "pr_confirmed": bool(overlay),
            "overlay_score": overlay,
            "veto_flags": sorted(veto),
            "catalyst_type": "Positive PR/News" if overlay>0 else "None",
            "summary": "Positive language across multiple sources" if overlay>0 else "No strong PR evidence"
        })

    with open(os.path.join(DATA_DIR,"step7_overlay.json"),"w") as f:
        json.dump(results, f, indent=2)

    print(f"NEWS: wrote {len(results)} tickers")

# ---------- Main ----------
def main():
    ensure_dirs()
    run_sec()
    run_news()

if __name__ == "__main__":
    main()
