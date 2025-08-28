#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, re, json, time
from datetime import datetime, timedelta, timezone
from typing import List, Dict
import requests

# ------------ Config ------------
UA  = os.getenv("UA", "GrandMasterFetcher/AtomOnly/1.2 (+contact: you@example.com)")
OUT = os.getenv("OUT_DIR", "out")
NEWS_LOOKBACK_HOURS = int(os.getenv("NEWS_LOOKBACK_HOURS", "36"))  # around-yesterday window
TOPN_FOR_NEWS       = int(os.getenv("TOPN_FOR_NEWS", "200"))
DEBUG               = os.getenv("DEBUG", "0") == "1"

TARGET_FORMS = {
    "8-K","8-K/A","6-K","6-K/A","10-Q","10-Q/A","10-K","10-K/A","3","4",
    "SC 13D","SC 13D/A","SC 13G","SC 13G/A"
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

def fetch(url: str, timeout: int = 25, sleep_ms: int = 200, retries: int = 2) -> str:
    err = None
    for i in range(retries+1):
        try:
            time.sleep(sleep_ms/1000.0)  # be polite
            r = requests.get(url, headers=HDRS, timeout=timeout, allow_redirects=True)
            r.raise_for_status()
            return r.text
        except Exception as e:
            err = e
            if i < retries:
                time.sleep(0.8 + 0.8*i)
            else:
                raise err

# ------------ Time helpers (UTC) ------------
def utc_today(): return datetime.now(timezone.utc).date()
def iso_yesterday(): return (utc_today() - timedelta(days=1)).strftime("%Y-%m-%d")

# ------------ Atom feed (paginated) ------------
def atom_url(start:int, count:int=200) -> str:
    # count ~200 is stable; paginate via start=0,200,400,...
    return f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&start={start}&count={count}&output=atom"

def normalize_form(raw: str) -> str:
    """Normalize SEC Atom category term into a canonical form we can score."""
    if not raw:
        return ""
    u = raw.upper().strip()

    # unify spacing for schedules
    u = u.replace("SC13D", "SC 13D").replace("SC13G", "SC 13G")
    # strip leading "FORM "
    u = re.sub(r"^FORM\s+", "", u)

    # detect amendment
    is_amend = "/A" in u or "AMEND" in u

    # direct tokens first
    for base in ("8-K","6-K","10-Q","10-K","SC 13D","SC 13G"):
        if base in u:
            if base in ("8-K","6-K","10-Q","10-K"):
                return f"{base}/A" if is_amend or f"{base}/A" in u else base
            # schedules handle A via is_amend too
            return f"{base}/A" if is_amend else base

    # map descriptive labels → canonical forms
    desc_map = [
        (r"CURRENT\s+REPORT", "8-K"),
        (r"REPORT\s+OF\s+FOREIGN", "6-K"),
        (r"FOREIGN\s+ISSUER", "6-K"),
        (r"QUARTERLY\s+REPORT", "10-Q"),
        (r"ANNUAL\s+REPORT", "10-K"),
        (r"STATEMENT\s+OF\s+CHANGES\s+IN\s+BENEFICIAL\s+OWNERSHIP", "4"),
        (r"INITIAL\s+STATEMENT\s+OF\s+BENEFICIAL\s+OWNERSHIP", "3"),
        (r"SCHEDULE\s+13D", "SC 13D"),
        (r"SCHEDULE\s+13G", "SC 13G"),
    ]
    for pat, base in desc_map:
        if re.search(pat, u):
            if base in ("8-K","6-K","10-Q","10-K","SC 13D","SC 13G"):
                return f"{base}/A" if is_amend else base
            return base

    # sometimes first token is enough (e.g., "8-K — Current report")
    first = u.split()[0]
    if first in {"8-K","6-K","10-Q","10-K","3","4","SC","SC13D","SC13G","SC"}:
        if first in ("SC13D","SC13G"):
            first = first.replace("SC13D","SC 13D").replace("SC13G","SC 13G")
        return first

    return u  # fallback; may not match TARGET_FORMS

def parse_atom_entries(xml: str) -> List[Dict]:
    out=[]
    for ent in re.findall(r"<entry>(.*?)</entry>", xml, flags=re.S|re.I):
        # raw form term
        m = re.search(r"<category\s+term=\"([^\"]+)\"", ent, flags=re.I)
        form_raw = m.group(1).strip() if m else ""
        form = normalize_form(form_raw)

        # company, cik
        c = re.search(r"<conformed-name>(.*?)</conformed-name>", ent, flags=re.I)
        company = c.group(1).strip() if c else ""
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
            "form_raw": form_raw,     # keep for debug
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
    elif f=="3": w += 1
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
    stats = {"pages":0, "entries_seen":0, "entries_yday":0, "forms_raw":{}, "forms_norm":{}, "kept_target":0, "docs_fetched":0, "errors":0}

    start = 0
    count = 200
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

        any_newer = False
        for e in entries:
            d = e.get("date") or ""
            if d > yiso:
                any_newer = True
            if d == yiso:
                stats["entries_yday"] += 1
                raw = e.get("form_raw","") or ""
                norm = e.get("form","") or ""
                stats["forms_raw"][raw] = stats["forms_raw"].get(raw,0)+1
                stats["forms_norm"][norm] = stats["forms_norm"].get(norm,0)+1

        # collect yesterday + target forms
        for e in entries:
            if e.get("date") != yiso:
                continue
            if e.get("form","") not in TARGET_FORMS:
                continue
            stats["kept_target"] += 1
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

        # stop when we've paged past yesterday and there were no newer entries on this page
        oldest_on_page = min((e.get("date","") for e in entries))
        if oldest_on_page and oldest_on_page < yiso and not any_newer:
            break

        start += count
        if start > 8000:
            log("Stop paging at start>8000 for safety")
            break

    # de-dup
    uniq=[]; seen=set()
    for r in collected:
        key=(r.get("ticker",""), r["form"], r.get("cik",""))
        if key in seen: continue
        seen.add(key); uniq.append(r)

    with open(os.path.join(OUT,"step6_full.json"),"w",encoding="utf-8") as f:
        json.dump(uniq, f, indent=2)
    print(f"SEC: wrote {len(uniq)} records (yesterday={yiso})")

    # compact debug summaries (top 12 items)
    def topn(d, n=12):
        return dict(sorted(d.items(), key=lambda kv: kv[1], reverse=True)[:n])
    log(f"Stats — pages:{stats['pages']} seen:{stats['entries_seen']} yday:{stats['entries_yday']} kept:{stats['kept_target']} docs:{stats['docs_fetched']} err:{stats['errors']}")
    log(f"Forms raw (yday top): {topn(stats['forms_raw'])}")
    log(f"Forms norm(yday top): {topn(stats['forms_norm'])}")

# ------------ News overlay (unchanged) ------------
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
