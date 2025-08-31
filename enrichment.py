import time, requests
from .rate_limiter import RateLimiter
SUBMISSIONS = "https://data.sec.gov/submissions/CIK{cik}.json"
TICKERS_MAP = "https://www.sec.gov/files/company_tickers.json"
class Enricher:
    def __init__(self, ua:str, spacing_seconds:float=1.5):
        self.ua=ua; self.rl=RateLimiter(spacing_seconds); self._map=None
        self.s=requests.Session(); self.s.headers.update({"User-Agent":ua,"Accept-Encoding":"gzip, deflate"})
    def _get(self,u):
        self.rl.wait(); r=self.s.get(u, timeout=30)
        if r.status_code in (429,503): time.sleep(2); r=self.s.get(u, timeout=30)
        r.raise_for_status(); return r
    def load_tickers_map(self):
        if self._map is None:
            data=self._get(TICKERS_MAP).json(); m={}
            if isinstance(data, dict) and "data" in data:
                for row in data["data"]:
                    try: idx,cik,ticker,name=row; m[str(cik).zfill(10)]={"ticker":ticker,"name":name}
                    except: pass
            elif isinstance(data, list):
                for row in data:
                    try: m[str(row.get("cik_str","")).zfill(10)]={"ticker":row.get("ticker",""),"name":row.get("title","")}
                    except: pass
            else:
                for k,v in data.items():
                    try: m[str(v.get("cik_str","")).zfill(10)]={"ticker":v.get("ticker",""),"name":v.get("title","")}
                    except: pass
            self._map=m
        return self._map
    def enrich(self, cik:str):
        out={"ticker":"","company":"","sic":""}
        try:
            tm=self.load_tickers_map()
            if cik in tm: out["ticker"]=tm[cik]["ticker"]; out["company"]=tm[cik]["name"]
        except: pass
        try:
            js=self._get(SUBMISSIONS.format(cik=cik)).json()
            if not out["ticker"]: out["ticker"]=(js.get("tickers") or [""])[0] if js.get("tickers") else ""
            if not out["company"]: out["company"]=js.get("name","")
            out["sic"]=js.get("sic","") or ""
        except: pass
        return out
