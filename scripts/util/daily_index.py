from datetime import datetime
from dateutil import tz
from typing import List, Dict, Optional
NY_TZ = tz.gettz("America/New_York")
def quarter_for_month(m:int)->int: return 1 if m<=3 else 2 if m<=6 else 3 if m<=9 else 4
def index_url_for_date(d: datetime) -> str:
    return f"https://www.sec.gov/Archives/edgar/daily-index/{d.year}/QTR{quarter_for_month(d.month)}/master.{d.strftime('%Y%m%d')}.idx"
def parse_master_idx(text: str) -> List[Dict]:
    out=[]; lines=text.splitlines(); start=0
    for i,ln in enumerate(lines):
        if ln.strip().startswith("-----"): start=i+1; break
    for ln in lines[start:]:
        parts = ln.split("|")
        if len(parts)<5: continue
        cik, comp, form, datefiled, filename = parts[:5]
        out.append({"cik":cik.strip().zfill(10),"company":comp.strip(),"form":form.strip(),"date_filed":datefiled.strip(),"filename":filename.strip()})
    return out
def acceptance_from_header_txt(txt: str) -> Optional[datetime]:
    for ln in txt.splitlines():
        if "ACCEPTANCE-DATETIME:" in ln:
            val="".join(ch for ch in ln.split("ACCEPTANCE-DATETIME:")[-1].strip() if ch.isdigit())
            if len(val)>=14:
                dt=datetime.strptime(val[:14], "%Y%m%d%H%M%S"); return dt.replace(tzinfo=NY_TZ)
    return None
