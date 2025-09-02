# -*- coding: utf-8 -*-
import requests, time
from .rate_limiter import RateLimiter
SUB_BASE = "https://data.sec.gov/submissions/CIK{cik_padded}.json"
def get_company_profile(cik:str, ua:str, timeout:int, rl:RateLimiter):
    cik_padded = cik.zfill(10)
    url = SUB_BASE.format(cik_padded=cik_padded)
    headers = {"User-Agent": ua, "Accept-Encoding": "gzip, deflate"}
    for attempt in range(5):
        try:
            r = requests.get(url, headers=headers, timeout=timeout)
            if r.status_code == 200:
                data = r.json()
                tickers = data.get("tickers") or []
                ticker = tickers[0] if tickers else (data.get("ticker") or "")
                sic = str(data.get("sic") or "")
                sic_desc = data.get("sicDescription") or ""
                name = data.get("name") or ""
                return {"ticker": ticker, "sic": sic, "sic_desc": sic_desc, "name": name}
            if r.status_code in (429,503):
                retry_after = int(r.headers.get("Retry-After","3")); time.sleep(max(3,retry_after)); continue
        except requests.RequestException:
            time.sleep(2*(attempt+1))
        finally:
            rl.wait()
    return {"ticker":"","sic":"","sic_desc":"","name":""}
