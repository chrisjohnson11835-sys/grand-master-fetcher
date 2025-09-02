from datetime import datetime, timedelta
from dateutil import tz, parser
NY_TZ = tz.gettz("America/New_York")
def _is_weekend(dt_et): return dt_et.weekday() >= 5
def _last_business_day(dt_et):
    d = dt_et
    while d.weekday() >= 5: d -= timedelta(days=1)
    return d
def _prev_business_day(dt_et):
    d = dt_et - timedelta(days=1)
    while d.weekday() >= 5: d -= timedelta(days=1)
    return d
def compute_windows(now_utc=None, weekend_guard=True):
    now_et = datetime.now(tz=NY_TZ) if now_utc is None else now_utc.astimezone(NY_TZ)
    if weekend_guard and _is_weekend(now_et):
        bday = _last_business_day(now_et)
        primary_start = bday.replace(hour=9, minute=30, second=0, microsecond=0)
        primary_end = (bday + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        tail_start = primary_end
        tail_end = bday.replace(hour=9, minute=30, second=0, microsecond=0) + timedelta(days=1)
        return primary_start, primary_end, tail_start, tail_end, True
    boundary = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
    return boundary - timedelta(days=1), boundary, None, None, False
def compute_prev_bday_windows(now_utc=None):
    now_et = datetime.now(tz=NY_TZ) if now_utc is None else now_utc.astimezone(NY_TZ)
    bday = _prev_business_day(now_et)
    if bday.weekday()==4:
        start = bday.replace(hour=9, minute=30, second=0, microsecond=0)
        end_mid = (bday + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        return start, end_mid, end_mid, start + timedelta(days=1)
    else:
        start = bday.replace(hour=9, minute=30, second=0, microsecond=0)
        return start, start + timedelta(days=1), None, None
def parse_edgar_datetime_et(s: str):
    if not s: raise ValueError("Empty datetime string")
    s2 = s.strip()
    try:
        dt = parser.isoparse(s2); 
        return dt.replace(tzinfo=NY_TZ) if dt.tzinfo is None else dt.astimezone(NY_TZ)
    except Exception: pass
    from datetime import datetime as _dt
    for fmt in ("%Y-%m-%d %H:%M:%S","%Y-%m-%d %H:%M"):
        try: return _dt.strptime(s2, fmt).replace(tzinfo=NY_TZ)
        except ValueError: continue
    s3 = s2.replace("T"," ").replace("Z","")
    for fmt in ("%Y-%m-%d %H:%M:%S.%f","%Y-%m-%d %H:%M:%S"):
        try: return _dt.strptime(s3, fmt).replace(tzinfo=NY_TZ)
        except ValueError: continue
    raise ValueError(f"Unrecognized EDGAR datetime: {s}")
