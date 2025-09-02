import requests, time
from .rate_limiter import RateLimiter
SEARCH_URL = "https://efts.sec.gov/LATEST/search-index"
def fmt_dt(d): return d.strftime("%Y-%m-%d")
def _req(session, ua, params):
    headers={"User-Agent":ua,"Accept-Encoding":"gzip, deflate","Host":"efts.sec.gov"}
    return session.get(SEARCH_URL, params=params, headers=headers, timeout=30)
def fetch_fulltext_window(ua, start_dt_et, end_dt_et, forms, page_size=400, max_pages=30):
    rl=RateLimiter(1.5); out=[]; s=requests.Session(); forms_csv=",".join(forms)
    def strat_a(page): return {"q":"*","dateRange":"custom","startdt":fmt_dt(start_dt_et),"enddt":fmt_dt(end_dt_et),"forms":forms_csv,"from":page*page_size,"size":page_size,"sort":"filedAt","order":"desc"}
    def strat_b(page): return {"q":"*","from":fmt_dt(start_dt_et),"to":fmt_dt(end_dt_et),"type":forms_csv,"start":page*page_size,"count":page_size}
    def strat_c(page): return {"q":"*","forms":forms_csv,"startdt":fmt_dt(start_dt_et),"enddt":fmt_dt(end_dt_et),"from":page*page_size,"size":page_size}
    strategies=[("A",strat_a),("B",strat_b),("C",strat_c)]
    for _, strat in strategies:
        page=0; local=0
        while page<max_pages:
            rl.wait((0.2,0.6)); params=strat(page); r=_req(s,ua,params)
            if r.status_code==429: time.sleep(2); r=_req(s,ua,params)
            try: r.raise_for_status(); js=r.json()
            except Exception: break
            hits=[]
            if isinstance(js,dict):
                if "hits" in js and isinstance(js["hits"],dict) and "hits" in js["hits"]: hits=js["hits"]["hits"]
                elif "results" in js and isinstance(js["results"],list): hits=js["results"]
            if not hits: break
            for h in hits:
                src=h.get("_source") or h
                filed=src.get("filedAt") or src.get("filed") or src.get("filedAtDate")
                form=src.get("formType") or src.get("form") or src.get("type")
                if isinstance(src.get("ciks"),list) and src.get("ciks"): cik=str(src["ciks"][0]).zfill(10)
                else: cik=str(src.get("cik","")).zfill(10)
                if isinstance(src.get("display_names"),list) and src.get("display_names"): comp=src["display_names"][0]
                else: comp=src.get("companyName") or src.get("name") or ""
                link=src.get("linkToHtml") or src.get("linkToFilingDetails") or src.get("link") or ""
                if isinstance(src.get("tickers"),list) and src.get("tickers"): ticker=src["tickers"][0]
                else: ticker=src.get("ticker","")
                out.append({"title":f"{form} - {comp}","form":form or "","company":comp or "","cik":cik or "","updated":(filed or "").replace("T"," ").replace("Z","") if filed else "","link":link or "","summary":"","ticker_hint":ticker or ""})
                local+=1
            if len(hits)<page_size: break
            page+=1
        if local>0: break
    return out
