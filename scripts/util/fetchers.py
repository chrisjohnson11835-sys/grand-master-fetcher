import time, requests
from typing import List, Dict, Tuple
from bs4 import BeautifulSoup
from .rate_limiter import RateLimiter

SEC_ATOM = "https://www.sec.gov/cgi-bin/browse-edgar"
HEADERS = lambda ua: {"User-Agent": ua, "Accept-Encoding":"gzip, deflate", "Host":"www.sec.gov"}

class SECClient:
    def __init__(self, ua: str, spacing_seconds: float, max_retries: int, backoff_base: float, jitter_range: Tuple[float,float]):
        self.ua = ua
        self.rl = RateLimiter(spacing_seconds)
        self.max_retries = max_retries
        self.backoff = backoff_base
        self.jitter = jitter_range

    def _req(self, url, params=None):
        attempt = 0
        while True:
            self.rl.wait(self.jitter)
            r = requests.get(url, params=params, headers=HEADERS(self.ua), timeout=30)
            if r.status_code in (429, 503):
                ra = r.headers.get("Retry-After")
                time.sleep(float(ra)) if ra else time.sleep(min(60, (self.backoff ** max(1, attempt))))
                attempt += 1
                if attempt > self.max_retries:
                    raise RuntimeError(f"SEC returned {r.status_code} repeatedly for {url}")
                continue
            r.raise_for_status()
            return r

    def fetch_atom_page(self, start: int, count: int = 100) -> str:
        return self._req(SEC_ATOM, params={"action":"getcurrent","start":start,"count":count,"output":"atom"}).text

    def fetch_html_page(self, start: int, count: int = 100) -> str:
        return self._req(SEC_ATOM, params={"action":"getcurrent","start":start,"count":count}).text


def parse_atom_entries(xml: str) -> List[Dict]:
    soup = BeautifulSoup(xml, "xml")
    out: List[Dict] = []
    import re
    for e in soup.find_all("entry"):
        title = (e.find("title").text if e.find("title") else "").strip()
        updated = (e.find("updated").text if e.find("updated") else "").strip()
        link_el = e.find("link")
        link = link_el["href"].strip() if link_el and link_el.has_attr("href") else ""
        summary = (e.find("summary").text if e.find("summary") else "").strip()
        m = re.match(r"^([^-]+)-", title)
        form = m.group(1).strip() if m else ""
        mcik = re.search(r"\((\d{10})\)", title)
        cik = mcik.group(1) if mcik else ""
        company = ""
        m2 = re.search(r"-\s*(.*?)\s*\(\d{10}\)", title)
        company = m2.group(1).strip() if m2 else ""
        out.append({"title": title, "form": form, "company": company, "cik": cik, "updated": updated, "link": link, "summary": summary})
    return out


def parse_html_entries(html: str) -> List[Dict]:
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table", {"class": "tableFile2"})
    out: List[Dict] = []
    if not table:
        return out
    rows = table.find_all("tr")[1:]
    for tr in rows:
        tds = tr.find_all("td")
        if len(tds) < 5:
            continue
        company_col = tds[1].get_text(" ", strip=True)
        form = tds[0].get_text(" ", strip=True)
        link_el = tds[1].find("a", href=True)
        link = "https://www.sec.gov" + link_el["href"] if link_el else ""
        date_time = tds[3].get_text(" ", strip=True)
        cik = ""
        if link_el and "CIK=" in link_el["href"]:
            import urllib.parse as up
            qs = up.parse_qs(up.urlparse(link_el["href"]).query)
            if "CIK" in qs and qs["CIK"]:
                cik = qs["CIK"][0].zfill(10)
        out.append({"title": f"{form} - {company_col}", "form": form, "company": company_col, "cik": cik, "updated": date_time, "link": link, "summary": ""})
    return out
