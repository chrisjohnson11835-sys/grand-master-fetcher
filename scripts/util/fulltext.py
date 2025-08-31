import requests
from typing import List, Dict
from .rate_limiter import RateLimiter

SEARCH_URL = "https://efts.sec.gov/LATEST/search-index"
HEADERS = lambda ua: {"User-Agent": ua, "Accept-Encoding":"gzip, deflate", "Host":"efts.sec.gov"}

def fmt_dt(d): return d.strftime("%Y-%m-%d")

def fetch_fulltext_window(ua:str, start_dt_et, end_dt_et, forms:List[str], page_size:int=400, max_pages:int=30) -> List[Dict]:
    rl=RateLimiter(1.5); out=[]; forms_param=",".join(forms)
    for page in range(max_pages):
        rl.wait((0.2,0.6))
        params = {
            "keys":"",
            "category":"custom",
            "forms": forms_param,
            "startdt": fmt_dt(start_dt_et),
            "enddt": fmt_dt(end_dt_et),
            "from": page*page_size,
            "size": page_size,
            "sort":"date",
            "order":"desc"
        }
        r=requests.get(SEARCH_URL, params=params, headers=HEADERS(ua), timeout=30)
        if r.status_code==429:
            rl.wait((2,3)); r=requests.get(SEARCH_URL, params=params, headers=HEADERS(ua), timeout=30)
        r.raise_for_status()
        js=r.json()
        hits=(js.get("hits") or {}).get("hits") or js.get("hits", [])
        if not hits: break
        for h in hits:
            src = h.get("_source") or h
            filed = src.get("filedAt") or src.get("filed")
            form = src.get("formType") or src.get("form")
            cik = str(src.get("ciks",[ ""])[0]).zfill(10) if src.get("ciks") else str(src.get("cik","")).zfill(10)
            comp = (src.get("display_names") or src.get("companyName") or [""])[0] if isinstance(src.get("display_names"), list) else (src.get("companyName") or "")
            link = src.get("linkToHtml") or src.get("linkToFilingDetails") or ""
            ticker = (src.get("tickers") or [""])[0] if src.get("tickers") else ""
            out.append({"title": f"{form} - {comp}","form": form,"company": comp,"cik": cik,"updated": filed.replace("T"," ").replace("Z","") if filed else "","link": link,"summary": "","ticker_hint": ticker})
        if len(hits) < page_size: break
    return out
