from datetime import datetime, timedelta
from dateutil import tz, parser

NY_TZ = tz.gettz("America/New_York")

def _is_weekend(dt_et):
    return dt_et.weekday() >= 5  # Sat=5, Sun=6

def _last_business_day(dt_et):
    # Return date (at same time) positioned to the most recent Mon-Fri day
    d = dt_et
    while d.weekday() >= 5:
        d = d - timedelta(days=1)
    return d

def compute_windows(now_utc=None, weekend_guard=True):
    """
    Returns: (primary_start_et, primary_end_et, weekend_tail_start_et, weekend_tail_end_et, weekend_guard_applied)

    - Weekday: primary = [yesterday 09:30, today 09:30]; tail = None
    - Weekend (Sat/Sun) with guard:
        primary = [Friday 09:30, Saturday 00:00]
        tail    = [Saturday 00:00, Saturday 09:30]  (captures rare Saturday filings)
    All tz in America/New_York.
    """
    now_et = datetime.now(tz=NY_TZ) if now_utc is None else now_utc.astimezone(NY_TZ)

    if weekend_guard and _is_weekend(now_et):
        bday = _last_business_day(now_et)  # Friday if running on Sat/Sun
        primary_start = bday.replace(hour=9, minute=30, second=0, microsecond=0)
        primary_end = (bday + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)  # midnight into Sat
        tail_start = primary_end
        tail_end = bday.replace(hour=9, minute=30, second=0, microsecond=0) + timedelta(days=1)  # Sat 09:30
        return primary_start, primary_end, tail_start, tail_end, True

    # Weekday strict 09:30 -> 09:30
    boundary = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
    primary_end = boundary
    primary_start = primary_end - timedelta(days=1)
    return primary_start, primary_end, None, None, False

def parse_edgar_datetime_et(dt_str: str):
    if not dt_str:
        raise ValueError("Empty datetime string")
    s = dt_str.strip()
    # ISO first
    try:
        dt = parser.isoparse(s)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=NY_TZ)
        return dt.astimezone(NY_TZ)
    except Exception:
        pass
    # Common SEC formats
    from datetime import datetime as _dt
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            dt_naive = _dt.strptime(s, fmt)
            return dt_naive.replace(tzinfo=NY_TZ)
        except ValueError:
            continue
    s2 = s.replace("T"," ").replace("Z","")
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            dt_naive = _dt.strptime(s2, fmt)
            return dt_naive.replace(tzinfo=NY_TZ)
        except ValueError:
            continue
    raise ValueError(f"Unrecognized EDGAR datetime: {dt_str}")
