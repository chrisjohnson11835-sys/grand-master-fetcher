#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Grand Master — Off-host fetcher (PREVIOUS DAY ONLY)
- Pull exactly yesterday's SEC daily master index (UTC).
- If blocked/empty, fall back to SEC Atom and keep entries with date == yesterday (UTC).
- Read filings (target forms), score catalysts, write step6_full.json.
- Overlay reachable news to write step7_overlay.json.

Env (set in GitHub Actions):
  UA, OUT_DIR, NEWS_LOOKBACK_HOURS, TOPN_FOR_NEWS, DEBUG
Outputs:
  out/step6_full.json
  out/step7_overlay.json
"""

import os, re, json, time
from datetime import datetime, timedelta, timezone
from typing import List, Dict
import requests

# --------- Config from env ----------
UA  = os.getenv("UA", "GrandMasterFetcher/2.0 (+contact: you@example.com)")
OUT = os.getenv("OUT_DIR", "out")
NEWS_LOOKBACK_HOURS = int(os.getenv("NEWS_LOOKBACK_HOURS", "36"))  # 36h window around yesterday's news
TOPN_FOR_NEWS       = int(os.getenv("TOPN_FOR_NEWS", "200"))
DEBUG               = os.getenv("DEBUG", "0") == "1"

TARGET_FORMS = {
    "8-K","6-K","10-Q","10-K","3","4","SC 13D","SC 13G","SC 13D/A","SC 13G/A"
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

def fetch(url: str, timeout: int = 25, sleep_ms: int = 300) -> str:
    time.sleep(sleep_ms/1000.0)
    r = requests.get(url, headers=HDRS, timeout=timeout, allow_redirects=True)
    r.raise_for_status()
    return r.text

# --------- SEC helpers ----------
def utc_today():
    return datetime.now(timezone.utc).date()

def utc_yesterday_str():
    return (utc_today() - timedelta(days=1)).strftime("%Y%m%d")

def utc_yesterday_iso():
    return (utc_today() - timedelta(days=1)).strftime("%Y-%m-%d")

def sec_master_idx_url(ymd: str) -> str:
    dt = datetime.strptime(ymd, "%Y%m%d")
    q = (dt.month-1)//3 + 1
    return f"https://www.sec.gov/Archives/edgar/daily-index/{dt.year}/QTR{q}/master.{ymd}.idx"

def parse_master_idx(raw: str) -> List[Dict]:
    rows=[]; started=False
    header_pat = re.compile(r"company\s*name\|form\s*type\|cik\|date\s*filed\|filename", re.I)
    for ln in raw.splitlines():
        if not started and header_pat.search(ln):
            started=True
            continue
        if not started: 
            continue
        ln=ln.strip()
        if not ln: 
            continue
        parts = ln.split("|")
        if len(parts) >= 5:
            rows.append({"company":parts[0], "form":parts[1], "cik":parts[2], "date":parts[3], "path":parts[4]})
    # fallback greedy parse if header missing
    if not rows:
        for ln in raw.splitlines():
            if ln.count("|")>=4:
                parts = ln.strip().split("|")
                if len(parts)>=5 and re.match(r"^\d{10}$", parts[2]):
                    rows.append({"company":parts[0], "form":parts[1], "cik":parts[2], "date":parts[3], "path":parts[4]})
    log(f"Master parsed rows: {len(rows)}")
    return rows

def fetch_doc_from_index(index_or_doc_url: str) -> str:
    # Accept both index and document URLs
    html = fetch(index_or_doc_url)
    if "Document Format Files" in html:
        m = re.search(r'href="([^"]+\.htm[^"]*)"', html, flags=re.I)
        if m:
            doc_url = "https://www.sec.gov" + m.group(1) if m.group(1).startswith("/") else m.group(1)
            try:
                return fetch(doc_url)
            except Exception as e:
                log(f"Doc fetch FAIL (fallback to index): {doc_url} :: {e}")
    return html

def extract_items_8k(txt: str):
    return sorted(set(m.upper() for m in re.findall(r"Item\s+(\d+\.\d+)", txt, flags=re.I)))

def clean_text(html: str) -> str:
    t = re.sub(r"<[^>]+>", " ", html)
    return re.sub(r"\s+", " ", t).strip()

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

def base_weight(form: str, items: List[str]) -> int:
    f=form.upper(); w=0
    if f=="8-K":
        for it in items:
            if it=="2.02": w += 10
            if it in ("1.01","2.01"): w += 12
            if it=="8.01": w += 6
            if it=="3.01": w -= 12
            if it=="3.02": w -= 15
            if it=="5.03": w -= 10
    elif f=="6-K": w += 6
    elif f in ("10-Q","10-K"): w += 5
    elif f=="4": w += 2
    elif f.startswith("SC 13D"): w += 10
    elif f.startswith("SC 13G"): w += 2
    return w

def guess_ticker(html: str) -> str:
    m = re.search(r"Trading Symbol[s]?:\s*([A-Z\.]{1,6})", html, flags=re.I)
    return m.group(1).upper() if m else ""

# --------- Atom fallback (filter to yesterday only) ----------
def sec_atom_url(count=1000):
    return f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&count={count}&output=atom"

def parse_atom(xml: str) -> List[Dict]:
    out=[]
    for ent in re.findall(r"<entry>(.*?)</entry>", xml, flags=re.S|re.I):
        form = re.search(r"<category\s+term=\"([^\"]+)\"", ent, flags=re.I)
        form = form.group(1).strip() if form else ""
        company = re.search(r"<conformed-name>(.*?)</conformed-name>", ent, flags=re.I)
        company = company.group(1).strip() if company else ""
        link = re.search(r"<link[^>]+href=\"([^\"]+)\"", ent, flags=re.I)
        link = link.group(1).strip() if link else ""
        updt = re.search(r"<updated>(.*?)</updated>", ent, flags=re.I)
        updt = updt.group(1).strip() if updt else ""
        cik  = re.search(r"<cik>(\d+)</cik>", ent, flags=re.I)
        cik  = cik.group(1).strip() if cik else ""
        out.append({"company":company,"form":form,"cik":cik,"date":updt[:10] if updt else "", "index_url":link})
    log(f"Atom parsed entries: {len(out)}")
    return out

# --------- SEC pipeline (YESTERDAY ONLY) ----------
def run_sec():
    ymd = utc_yesterday_str()
    yiso = utc_yesterday_iso()
    log(f"Yesterday (UTC): {yiso}")

    # 1) Try daily master for exactly yesterday
    wrote = []
    try:
        url = sec_master_idx_url(ymd)
        raw = fetch(url)
        log(f"SEC master OK: {url}")
        rows = parse_master_idx(raw)
        # keep only rows with date == yesterday ISO
        rows = [r for r in rows if r.get("date","") == yiso]
        for r in rows:
            form_u = r["form"].upper().replace("SC13D","SC 13D").replace("SC13G","SC 13G")
            if form_u not in TARGET_FORMS: 
                continue
            filing_url = "https://www.sec.gov/Archives/" + r["path"].lstrip("/")
            try:
                html = fetch_doc_from_index(filing_url)
            except Exception as e:
                log(f"Filing fetch FAIL: {filing_url} :: {e}")
                continue
            text = clean_text(html)
            items = extract_items_8k(text) if form_u=="8-K" else []
            score = base_weight(form_u, items) + score_context(text)
            ticker = guess_ticker(html)
            dilution = bool(re.search(r"(offering|registered direct|atm|at-the-market|shelf|warrant|reverse split|delisting|deficiency|convertible)", text, flags=re.I))
            wrote.append({
                "ticker": ticker, "company": r["company"], "industry":"", "form": r["form"], "cik": r["cik"],
                "accepted_ts": yiso+"T00:00:00Z", "items": items, "score": score,
                "reasons": [], "dilution_flags": (["possible_dilution"] if dilution else [])
            })
    except Exception as e:
        log(f"SEC master FAIL (yesterday): {e}")

    # 2) Fallback: Atom, filtered to yesterday
    if not wrote:
        try:
            xml = fetch(sec_atom_url(1000))
            entries = parse_atom(xml)
            entries = [e for e in entries if (e.get("date") == yiso)]
            for e in entries:
                form_u = e["form"].upper().replace("SC13D","SC 13D").replace("SC13G","SC 13G")
                if form_u not in TARGET_FORMS: 
                    continue
                idx = e.get("index_url") or ""
                if not idx: 
                    continue
                try:
                    html = fetch_doc_from_index(idx)
                except Exception as ex:
                    log(f"Atom filing fetch FAIL: {idx} :: {ex}")
                    continue
                text = clean_text(html)
                items = extract_items_8k(text) if form_u=="8-K" else []
                score = base_weight(form_u, items) + score_context(text)
                ticker = guess_ticker(html)
                dilution = bool(re.search(r"(offering|registered direct|atm|at-the-market|shelf|warrant|reverse split|delisting|deficiency|convertible)", text, flags=re.I))
                wrote.append({
                    "ticker": ticker, "company": e["company"], "industry":"", "form": form_u, "cik": e["cik"],
                    "accepted_ts": yiso+"T00:00:00Z", "items": items, "score": score,
                    "reasons": [], "dilution_flags": (["possible_dilution"] if dilution else [])
                })
        except Exception as e:
            log(f"Atom FAIL: {e}")

    # de-dup (ticker,form,cik)
    uniq=[]; seen=set()
    for r in wrote:
        key=(r["ticker"], r["form"].upper(), r["cik"])
        if key in seen: 
            continue
        seen.add(key); uniq.append(r)

    with open(os.path.join(OUT,"step6_full.json"),"w",encoding="utf-8") as f:
        json.dump(uniq, f, indent=2)
    print(f"SEC: wrote {len(uniq)} records (yesterday={yiso})")

# --------- News overlay (light) ----------
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
    # read step6
    p = os.path.join(OUT, "step6_full.json")
    try:
        sec = json.load(open(p,"r",encoding="utf-8"))
    except Exception:
        sec = []

    # best SEC record per ticker
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

        # dedupe
        uniq=[]; seen=set()
        for u in hits:
            if not u.startswith("http"): continue
            if u in seen: continue
            seen.add(u); uniq.append(u)

        # quick scoring
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

def main():
    ensure_out()
    run_sec()
    run_news()

if __name__ == "__main__":
    main()
