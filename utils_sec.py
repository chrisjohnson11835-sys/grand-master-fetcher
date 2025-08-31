# utils_sec.py (v21.1 hotfix-fetch)
import re, json, requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, Dict, Any, List
from dateutil import parser as dtparser
try:
    from zoneinfo import ZoneInfo
except Exception:
    from backports.zoneinfo import ZoneInfo

SEC_ATOM = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&output=atom&start={start}&count={count}"

def new_session(user_agent: str):
    s = requests.Session()
    s.headers.update({
        "User-Agent": user_agent,
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
        "Cache-Control": "no-cache"
    })
    retry = Retry(
        total=12,
        read=12,
        connect=12,
        backoff_factor=1.2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET"]),
        respect_retry_after_header=True,
        raise_on_status=False
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

def _prev_business_date(d: datetime) -> datetime:
    one = timedelta(days=1)
    d2 = d - one
    while d2.weekday() >= 5:
        d2 -= one
    return d2

def et_window_prev0930_to_latest0930(tz: str, cutoff_hour: int = 9, cutoff_minute: int = 30, business_days: bool = True) -> Tuple[datetime, datetime]:
    now_et = datetime.now(ZoneInfo(tz))
    today_cut = datetime(now_et.year, now_et.month, now_et.day, cutoff_hour, cutoff_minute, tzinfo=now_et.tzinfo)
    end_et = today_cut if now_et >= today_cut else today_cut - timedelta(days=1)
    if business_days:
        while end_et.weekday() >= 5:
            end_et -= timedelta(days=1)
        prev_biz = _prev_business_date(end_et)
        start_et = datetime(prev_biz.year, prev_biz.month, prev_biz.day, cutoff_hour, cutoff_minute, tzinfo=end_et.tzinfo)
    else:
        start_et = end_et - timedelta(days=1)
    return (start_et, end_et)

def parse_entry_time(entry) -> Optional[datetime]:
    for key in ("updated","published"):
        if key in entry:
            try:
                return dtparser.parse(entry[key])
            except Exception:
                pass
    up = entry.get("updated_parsed") or entry.get("published_parsed")
    if up:
        try:
            return datetime(*up[:6], tzinfo=timezone.utc)
        except Exception:
            return None
    return None

_PATTERNS = [
    r'\b8-K(?:/A)?\b', r'\b6-K(?:/A)?\b', r'\b10-Q(?:/A)?\b', r'\b10-K(?:/A)?\b',
    r'\bSC 13D(?:/A)?\b', r'\bSC 13G(?:/A)?\b',
    r'\bFORM?\s*3(?:/A)?\b', r'\bFORM?\s*4(?:/A)?\b', r'\b3(?:/A)?\b', r'\b4(?:/A)?\b'
]

def _match_form(s: str) -> Optional[str]:
    if not s: return None
    s_up = s.upper()
    for pat in _PATTERNS:
        m = re.search(pat, s_up, flags=re.IGNORECASE)
        if m:
            val = m.group(0).upper()
            if val in ("FORM 3","3"): return "Form 3"
            if val in ("FORM 4","4"): return "Form  4".replace("  ", " ")
            return val
    return None

def entry_form(entry) -> str:
    tags = entry.get("tags") or entry.get("categories")
    if isinstance(tags, list):
        for t in tags:
            term = (t.get("term") or t.get("label") or "").strip()
            f = _match_form(term)
            if f: return f
    cat = entry.get("category")
    if isinstance(cat, dict):
        f = _match_form((cat.get("term") or "").strip())
        if f: return f
    for k in ("title","summary"):
        f = _match_form(entry.get(k,""))
        if f: return f
    content = entry.get("content") or []
    if isinstance(content, list) and content:
        f = _match_form(content[0].get("value",""))
        if f: return f
    return _match_form(entry.get("link","")) or ""

def extract_cik_from_link(href: str) -> Optional[str]:
    if not href: return None
    m = re.search(r'[?&]CIK=(\d{1,10})\b', href, re.I)
    if m: return m.group(1).zfill(10)
    m = re.search(r'/data/(\d{1,10})/', href)
    if m: return m.group(1).zfill(10)
    return None

def fallback_company_from_title(title: str) -> Optional[str]:
    if not title: return None
    m = re.match(r'\s*([^(\[]+?)\s*[\(\[]', title)
    if m:
        name = m.group(1).strip()
        if name and len(name) > 1:
            return name
    t = re.sub(r'\s*-\s*Form\s+.*$', '', title, flags=re.IGNORECASE).strip()
    return t or None

def fetch_submissions_for_cik(session: requests.Session, cik: str) -> Optional[Dict[str,Any]]:
    url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    try:
        r = session.get(url, timeout=25)
        if r.status_code != 200:
            return None
        return r.json()
    except Exception:
        return None

def map_company_meta(sub_json: Dict[str,Any]):
    if not sub_json: return (None, None, None, None)
    ticker = None
    if isinstance(sub_json.get("tickers"), list) and sub_json["tickers"]:
        ticker = sub_json["tickers"][0]
    sic = sub_json.get("sic")
    try: sic = int(sic) if sic is not None else None
    except Exception: sic = None
    sic_desc = sub_json.get("sicDescription")
    company = sub_json.get("name") or sub_json.get("companyName") or sub_json.get("entityName")
    if isinstance(company, str): company = company.strip()
    else: company = None
    return (ticker, sic_desc, sic, company)

def load_json(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def within_window(dt: datetime, start_et: datetime, end_et: datetime, local_tz: str) -> bool:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt_et = dt.astimezone(ZoneInfo(local_tz))
    return start_et <= dt_et <= end_et

def banned_by_sic(sic, prefixes: List[str], exact: List[int]) -> bool:
    if sic is None: return False
    if sic in exact: return True
    s = str(sic)
    return any(s.startswith(p) for p in prefixes)

def banned_by_keywords(text: str, kw: Dict[str, List[str]]) -> bool:
    text_l = (text or "").lower()
    for group in kw.values():
        for term in group:
            if term and term.lower() in text_l:
                return True
    return False

def item_codes_from_text(text: str) -> List[str]:
    return re.findall(r'\b([1-9]\.\d{2})\b', (text or ""))

def score_record(rec: Dict[str,Any], scoring: Dict[str,Any]) -> int:
    score = 0
    form = rec.get("form","").upper()
    score += scoring["form_weights"].get(form, scoring["form_weights"].get(form.replace("/A",""), 0))
    text = f"{rec.get('title','')} {rec.get('summary','')}".lower()
    if form.startswith("8-K"):
        for item in item_codes_from_text(text):
            score += scoring["item_boosts_8k"].get(item, 0)
    if any(pk in text for pk in scoring.get("positive_keywords", [])):
        score += scoring.get("pos_keyword_boost", 0)
    if any(nk in text for nk in scoring.get("negative_keywords", [])):
        score -= scoring.get("neg_keyword_penalty", 0)
    if any(df in text for df in scoring.get("dilution_flags", [])):
        score -= scoring.get("dilution_penalty", 0)
    if form in ("FORM 4","4","4/A","Form 4"):
        if any(t in text for t in scoring.get("form4_pos_boost_terms", [])):
            score += scoring.get("form4_pos_boost", 0)
    return max(score, 0)
