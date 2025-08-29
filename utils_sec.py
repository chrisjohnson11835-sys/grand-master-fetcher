# utils_sec.py (v18.9)
import re, time, json
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, Dict, Any, List
import requests
from dateutil import parser as dtparser
try:
    from zoneinfo import ZoneInfo
except Exception:
    from backports.zoneinfo import ZoneInfo

SEC_ATOM = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&output=atom&start={start}&count={count}"

def new_session(user_agent: str):
    s = requests.Session()
    s.headers.update({"User-Agent": user_agent, "Accept-Encoding":"gzip, deflate", "Accept":"*/*"})
    return s

def et_window_now_yday(tz: str) -> Tuple[datetime, datetime]:
    now_et = datetime.now(ZoneInfo(tz))
    yday_start = datetime(now_et.year, now_et.month, now_et.day, 0, 0, 0, tzinfo=now_et.tzinfo) - timedelta(days=1)
    return (yday_start, now_et)

def parse_entry_time(entry) -> Optional[datetime]:
    for key in ("updated","published"):
        if key in entry:
            try:
                dt = dtparser.parse(entry[key])
                return dt
            except Exception:
                pass
    up = entry.get("updated_parsed") or entry.get("published_parsed")
    if up:
        try:
            return datetime(*up[:6], tzinfo=timezone.utc)
        except Exception:
            return None
    return None

def entry_form(entry) -> str:
    # Try category.term first
    cat = entry.get("category")
    candidates = []
    if isinstance(cat, dict):
        t = (cat.get("term") or "").strip()
        if t: candidates.append(t)
    # Fallback: title text
    title = entry.get("title","")
    candidates.append(title)

    for source in candidates:
        s = source.upper()

        # Handle standard forms and their amendments
        patterns = [
            r'\b(8-K(?:/A)?|6-K(?:/A)?|10-Q(?:/A)?|10-K(?:/A)?|SC 13D(?:/A)?|SC 13G(?:/A)?)\b',
            r'\bFORM?\s*3(?:/A)?\b',
            r'\bFORM?\s*4(?:/A)?\b',
            r'\b3(?:/A)?\b',
            r'\b4(?:/A)?\b',
        ]
        for pat in patterns:
            m = re.search(pat, s, flags=re.IGNORECASE)
            if m:
                val = m.group(1).upper() if m.groups() else m.group(0).upper()
                if val in ("FORM 3","3","3/A"): return "Form 3" if val != "3/A" else "3/A"
                if val in ("FORM 4","4","4/A"): return "Form 4" if val != "4/A" else "4/A"
                return val
    return ""

def extract_cik_from_link(href: str) -> Optional[str]:
    if not href: return None
    m = re.search(r'[?&]CIK=(\d{1,10})\b', href, re.I)
    if m: return m.group(1).zfill(10)
    m = re.search(r'/data/(\d{1,10})/', href)
    if m: return m.group(1).zfill(10)
    return None

def fallback_company_from_title(title: str) -> Optional[str]:
    if not title: return None
    # Try "Company Name ("
    m = re.match(r'\s*([^(\[]+?)\s*[\(\[]', title)
    if m:
        name = m.group(1).strip()
        if name and len(name) > 1:
            return name
    # Otherwise, strip trailing " - Form X" patterns
    t = re.sub(r'\s*-\s*Form\s+.*$', '', title, flags=re.IGNORECASE).strip()
    return t or None

def fetch_submissions_for_cik(session: requests.Session, cik: str) -> Optional[Dict[str,Any]]:
    url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    try:
        r = session.get(url, timeout=20)
        if r.status_code != 200:
            return None
        return r.json()
    except Exception:
        return None

def map_company_meta(sub_json: Dict[str,Any]) -> Tuple[Optional[str], Optional[str], Optional[int], Optional[str]]:
    """
    Returns (ticker, sicDescription, sic, company)
    Tries multiple fields for company name.
    """
    if not sub_json: return (None, None, None, None)
    ticker = None
    if "tickers" in sub_json and isinstance(sub_json["tickers"], list) and sub_json["tickers"]:
        ticker = sub_json["tickers"][0]
    sic = sub_json.get("sic")
    try:
        sic = int(sic) if sic is not None else None
    except Exception:
        sic = None
    sic_desc = sub_json.get("sicDescription")

    # company name candidates
    company = sub_json.get("name") or sub_json.get("companyName") or sub_json.get("entityName")
    if isinstance(company, str):
        company = company.strip()
    else:
        company = None

    return (ticker, sic_desc, sic, company)
