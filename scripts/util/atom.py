# -*- coding: utf-8 -*-
import requests, time, re
from .rate_limiter import RateLimiter
ATOM_BASE = "https://www.sec.gov/cgi-bin/browse-edgar"
def fetch_atom_page(start: int, count: int, ua: str, timeout: int, rl: RateLimiter):
    params = {"action":"getcurrent","start":str(start),"count":str(count),"output":"atom"}
    headers = {"User-Agent": ua, "Accept-Encoding": "gzip, deflate"}
    for attempt in range(5):
        try:
            r = requests.get(ATOM_BASE, params=params, headers=headers, timeout=timeout)
            if r.status_code == 200: return r.text
            if r.status_code in (429,503):
                retry_after = int(r.headers.get("Retry-After","3")); time.sleep(max(3,retry_after)); continue
        except requests.RequestException:
            time.sleep(2*(attempt+1))
        finally:
            rl.wait()
    return None
def parse_atom_entries(xml_text: str):
    if not xml_text: return []
    entries = []
    for block in re.findall(r"<entry>(.*?)</entry>", xml_text, flags=re.S|re.I):
        title = _first(re.findall(r"<title>(.*?)</title>", block, flags=re.S|re.I))
        updated = _first(re.findall(r"<updated>(.*?)</updated>", block, flags=re.S|re.I))
        form = _first(re.findall(r'<category[^>]*term="([^"]+)"', block, flags=re.I))
        link = _first(re.findall(r'<link[^>]*href="([^"]+)"', block, flags=re.I))
        cik = _first(re.findall(r"/edgar/data/(\d+)/", (link or ""), flags=re.I))
        if not cik: cik = _first(re.findall(r"\(CIK\s*0*([0-9]{3,})\)", (title or ""), flags=re.I))
        entries.append({"title":_clean(title),"updated":_clean(updated),"form":(form or "").upper(),"link":link,"cik":(cik or "").lstrip("0")})
    return entries
def _clean(s): return (s or "").strip()
def _first(lst): return lst[0] if lst else None
