#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, re, json, time
from datetime import datetime, timedelta, timezone
from typing import List, Dict
import requests

# ------------ Config (from workflow env) ------------
UA  = os.getenv("UA", "GrandMasterFetcher/AtomOnly/1.0 (+contact: you@example.com)")
OUT = os.getenv("OUT_DIR", "out")
NEWS_LOOKBACK_HOURS = int(os.getenv("NEWS_LOOKBACK_HOURS", "36"))  # around-yesterday window
TOPN_FOR_NEWS       = int(os.getenv("TOPN_FOR_NEWS", "200"))
DEBUG               = os.getenv("DEBUG", "0") == "1"

TARGET_FORMS = {
    "8-K","8-K/A","6-K","6-K/A","10-Q","10-Q/A","10-K","10-K/A","3","4",
    "SC 13D","SC 13G","SC 13D/A","SC 13G/A"
}

HDRS = {
    "User-Agent": UA,
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}

def log(msg: str):
    if DEBUG:
        print(f"[DEBUG] {msg}", flush=True)

def ensure_out():
    os.makedirs(OUT, exist_ok=True)

def fetch(url: str, timeout: int = 25, sleep_ms: int = 200) -> str:
    time.sleep(sleep_ms/1000.0)  # be polite
    r = requests.get(url, headers=HDRS, timeout=timeout, allow_redirects=True)
    r.raise_for_status()
    return r.text

# ------------ Time helpers (UTC) ------------
def utc_today(): return datetime.now(timezone.utc).date()
def iso_yesterday(): return (utc_today() - timedelta(days=1)).strftime("%Y-%m-%d")

# ------------ Atom feed (paginated) ------------
def atom_url(start:int, count:int=200) -> str:
    # count up to ~200 is safer/reliable; we paginate via start=0,200,400,...
    return f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&start={start}&count={count}&output=atom"

def parse_atom_entries(xml: str) -> List[Dict]:
    out=[]
    for ent in re.findall(r"<entry>(.*?)</entry>", xml, flags=re.S|re.I):
        # form
        m = re.search(r"<category\s+term=\"([^\"]+)\"", ent, flags=re.I)
        form_raw = m.group(1).strip() if m else ""
        form = form_raw.upper().replace("SC13D","SC 13D").replace("SC13G","SC 13G")
        # company
        c = re.search(r"<conformed-name>(.*?)</conformed-name>", ent, flags=re.I)
        company = c.group(1).strip() if c else ""
        # cik
        ck = re.search(r"<cik>(\d+)</cik>", ent, flags=re.I)
        cik = ck.group(1).strip() if ck else ""
        # primary link and filing-href
        fh = re.search(r"<filing-href>(.*?)</filing-href>", ent, flags=re.I)
        filing_href = fh.group(1).strip() if fh else ""
        lk = re.search(r"<link[^>]+href=\"([^\"]+)\"", ent, flags=re.I)
        link = lk.group(1).strip() if lk else ""
        href = filing_href or link
        # dates: prefer filing-date, fallback to updated
        fd = re.search(r"<filing-date>(.*?)</filing-date>", ent, flags=re.I)
        filing_date = fd.group(1).strip() if fd else ""
        up = re.search(r"<updated>(.*?)</updated>", ent, flags=re.I)
        updated = up.group(1).strip() if up else ""
        d = filing_date or (updated[:10] if updated else "")

        out.append({
            "company": company,
            "form": form,
            "cik": cik,
            "date": d,                # YYYY-MM-DD
            "index_url": href         # filing index (preferred) or browse link
        })
    return out

def fetch_doc_from_index(index_or_doc_url: str) -> str:
    html = fetch(index_or_doc_url)
    # If index page has "Document Format Files", grab first .htm doc
    if "Document Format Files" in html:
        m = re.search(r'href="([^"]+\.htm[^"]*)"', html, flags=re.I)
        if m:
            doc_url = "https://www.sec.gov" + m.group(1) if m.group(1).startswith("/") else m.group(1)
            try:
                return fetch(doc_url)
            except Exception as e:
                log(f"Doc fetch FAIL (fallback to index): {doc_url} :: {e}")
    return html

# ------------ Text helpers ------------
def clean_text(html: str) -> str:
    t = re.sub(r"<[^>]+>", " ", html)
    return re.sub(r"\s+", " ", t).strip()

def extract_items_8k(txt: str):
    return sorted(set(m.upper() for m in re.findall(r"Item\s+(\d+\.\d+)", txt, flags=re.I)))

def score_context(txt: str) -> int:
    bull = ['raises guidance','beat','beats','acquisition','definitive agreement','buyback',
            'special dividend','approval','contract awarded','being acquired','all-cash',
            'premium of','activist','strategic alternatives']
    bear = ['offering','registered direct','atm','at-the-market','shelf','warrant',
            'reverse split','delisting','deficiency','going concern','convertible']
    s=0; low=txt.lower()
    for w in bull:
        if w in low: s += 3
    for w in bear:
        if w in low: s -= 6
    return s

def base_weight(form: str, items) -> int:
    f=form.upper(); w=0
    if f in ("8-K","8-K/A"):
        for it in items:
            if it=="2.02": w += 10
            if it in ("1.01","2.01"): w += 12
            if it=="8.01": w += 6
            if it=="3.01": w -= 12
            if it=="3.02": w -= 15
            if it=="5.03": w -= 10
    elif f in ("6-K","6-K/A"): w += 6
    elif f in ("10-Q","10-Q/A","10-K","10-K/A"): w += 5
    elif f=="4": w += 2
    elif f.startswith("SC 13D"): w += 10
    elif f.startswith("SC 13G"): w += 2
    return w

def guess_ticker(html: str) -> str:
    m = re.search(r"Trading Symbol[s]?:\s*([A-Z\.]{1,6})", html, flags=re.I)
    return m.group(1).upper() if m else ""

# ------------ SEC pipeline: Atom-only, previous day (UTC) ------------
def run_sec_atom_only():
    yiso = iso_yesterday()
    log(f"Yesterday (UTC): {yiso}")

    collected: List[Dict] = []
    stats = {"pages":0, "entries_seen":0, "entries_yesterday":0, "docs_fetched":0, "errors":0}

    # paginate Atom until we pass yesterday (older)
    start = 0
    count = 200  # reliable chunk size
    while True:
        url = atom_url(start, count)
        try:
            xml = fetch(url, timeout=25)
        except Exception as e:
            log(f"Atom page FAIL start={start}: {e}")
            break

        entries = parse_atom_entries(xml)
        stats["pages"] += 1
        stats["entries_seen"] += len(entries)

        if not entries:
            break

        # entries are newest → oldest; stop when entire page older than yesterday
        any_newer_than_yesterday = False
        any_yesterday = False
        for e in entries:
            d = e.get("date") or ""
            if d > yiso:
                any_newer_than_yesterday = True
            if d == yiso:
                any_yesterday = True

        # collect yesterday entries on this page
        for e in entries:
            if e.get("date") != yiso:
                continue
            if e.get("form","") not in TARGET_FORMS:
                continue
            idx = e.get("index_url") or ""
            if not idx:
                continue
            try:
                html = fetch_doc_from_index(idx)
                stats["docs_fetched"] += 1
            except Exception as ex:
                stats["errors"] += 1
                log(f"Filing fetch FAIL: {idx} :: {ex}")
                continue
            txt = clean_text(html)
            items = extract_items_8k(txt) if e["form"].startswith("8-K") else []
            score = base_weight(e["form"], items) + score_context(txt)
            ticker = guess_ticker(html)
            dilution = bool(re.search(r"(offering|registered direct|atm|at-the-market|shelf|warrant|reverse split|delisting|deficiency|convertible)", txt, flags=re.I))
            collected.append({
                "ticker": ticker,
                "company": e["company"],
                "industry": "",
                "form": e["form"],
                "cik": e["cik"],
                "accepted_ts": yiso+"T00:00:00Z",
                "items": items,
                "score": score,
                "reasons": [],
                "dilution_flags": (["possible_dilution"] if dilution else [])
            })

        stats["entries_yesterday"] += sum(1 for e in entries if e.get("date")==yiso)

        # stop condition:
        # - if we saw entries older than yesterday AND no entry newer-than-yesterday (we've paged past),
        #   OR if this page had no yesterday entries and d < yiso across the page.
        oldest_on_page = min((e.get("date","") for e in entries))
        if oldest_on_page and oldest_on_page < yiso and not any_newer_than_yesterday:
            break

        # otherwise keep paging
        start += count

        # safety cap to avoid infinite loops (rare)
        if start > 8000:
            log("Stop paging at start>8000 for safety")
            break

    # de-dup by (ticker, form, cik)
    uniq=[]; seen=set()
    for r in collected:
        key=(r.get("ticker",""), r["form"], r.get("cik",""))
        if key in seen: continue
        seen.add(key); uniq.append(r)

    with open(os.path.join(OUT,"step6_full.json"),"w",encoding="utf-8") as f:
        json.dump(uniq, f, indent=2)
    print(f"SEC: wrote {len(uniq)} records (yesterday={yiso})")
    log(f"Stats — {stats}")

# ------------ News overlay (reachable, light) ------------
def yahoo_rss(ticker: str):
    url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=US&lang=en-US"
    try:
        xml = fetch(url, timeout=15)
    except Exception as e:
        log(f"Yahoo RSS FAIL: {url} :: {e}")
        return []
    items=[]
    for m in re.finditer(r"<item>.*?<link>(.*?)</link>.*?<pubDate>(.*?)</pubDate>.*?</item>", xml, flags=re.S|re.I):
        items.append({"url": m.group(1).strip(), "pub": m.group(2)})
    return items

ENABLED = {
    "prnewswire":   "https://www.prnewswire.com/search/news/?query=",
    "globenewswire":"https://www.globenewswire.com/Search/NewsSearch?keyword=",
    "cnbc":         "https://www.cnbc.com/search/?query=",
    "tradingview":  "https://www.tradingview.com/search/?q=",
    "stockstotrade":"https://stockstotrade.com/?s=",
    "coincentral":  "https://coincentral.com/?s=",
    "mitrade":      "https://www.mitrade.com/insights/search?keyword=",
    "ainvest":      "https://www.ainvest.com/search?q=",
    "benzinga":     "https://www.benzinga.com/search?q=",
    "investopedia": "https://www.investopedia.com/search?q=",
    "forbes":       "https://www.forbes.com/search/?q="
}

def search_simple(base: str, q: str) -> List[str]:
    try:
        html = fetch(base + requests.utils.quote(q), timeout=22)
    except Exception as e:
        log(f"Search FAIL: {base} :: {e}")
        return []
    return list(set(re.findall(r'href="(https?://[^"]+)"', html, flags=re.I)))

def domain(url: str) -> str:
    m = re.match(r"https?://([^/]+)", url)
    if not m: return ""
    h = m.group(1).lower()
    return re.sub(r"^(www\.|m\.)", "", h)

def run_news():
    p = os.path.join(OUT, "step6_full.json")
    try:
        sec = json.load(open(p,"r",encoding="utf-8"))
    except Exception:
        sec = []

    best={}
    for r in sec:
        t=(r.get("ticker") or "").upper()
        if not t: continue
        if t not in best or (r.get("score",0) > best[t].get("score",-10**9)):
            best[t]=r

    top = sorted(best.values(), key=lambda x:x.get("score",0), reverse=True)[:TOPN_FOR_NEWS]

    cutoff = datetime.now(timezone.utc) - timedelta(hours=NEWS_LOOKBACK_HOURS)
    results=[]
    for r in top:
        t=(r.get("ticker") or "").upper()
        if not t: continue
        company = r.get("company","")
        hits=[]

        # Yahoo RSS (time-filtered)
        for it in yahoo_rss(t):
            try:
                pub = datetime.strptime(it["pub"], "%a, %d %b %Y %H:%M:%S %z").astimezone(timezone.utc)
                if pub >= cutoff:
                    hits.append(it["url"])
            except Exception:
                continue

        # Domain queries
        q=f"{t} {company}".strip()
        for _, base in ENABLED.items():
            hits += search_simple(base, q)

        # dedupe + score
        uniq=[]; seen=set()
        for u in hits:
            if not u.startswith("http"): continue
            if u in seen: continue
            seen.add(u); uniq.append(u)

        overlay=0; veto=set(); doms=set()
        for u in uniq[:8]:
            try:
                html = fetch(u, timeout=20)
            except Exception:
                continue
            txt = clean_text(html); d=domain(u)
            if d: doms.add(d)
            if re.search(r"(raises guidance|beats|acquisition|buyback|special dividend|approval|contract awarded|being acquired|strategic alternatives|activist)", txt, flags=re.I):
                overlay += 4
            if re.search(r"(registered direct|atm|at-the-market|shelf|warrant|reverse split|delisting|deficiency|convertible)", txt, flags=re.I):
                veto.add("financing_flag")
        overlay += max(0, len(doms)-1)

        results.append({
            "ticker": t,
            "sources_hit": uniq,
            "pr_confirmed": bool(overlay),
            "overlay_score": overlay,
            "veto_flags": sorted(list(veto)),
            "catalyst_type": "Positive PR/News" if overlay>0 else "None",
            "summary": "Positive language across multiple sources" if overlay>0 else "No strong PR evidence"
        })

    with open(os.path.join(OUT,"step7_overlay.json"),"w",encoding="utf-8") as f:
        json.dump(results, f, indent=2)
    print(f"NEWS: wrote {len(results)} tickers")

# ------------ Main ------------
def main():
    ensure_out()
    run_sec_atom_only()
    run_news()

if __name__ == "__main__":
    main()
