from datetime import datetime, timedelta
from dateutil import tz, parser

NY_TZ = tz.gettz("America/New_York")

def _is_business_day(dt_et):
    # Monday=0 .. Sunday=6
    return dt_et.weekday() < 5

def _prev_business_day(dt_et):
    d = dt_et
    while d.weekday() >= 5:
        d = d - timedelta(days=1)
    if d.weekday() == 5:  # Saturday -> Friday
        d = d - timedelta(days=1)
    if d.weekday() == 6:  # Sunday -> Friday
        d = d - timedelta(days=2)
    return d

def compute_et_window(now_utc=None, weekend_guard=True):
    """Return (start_et, end_et, weekend_guard_applied).
    Normal: yesterday 09:30 ET -> today 09:30 ET.
    If weekend_guard and today is Sat/Sun, shift to latest business day 09:30 -> +1 day 09:30 (i.e., Friday -> Saturday).
    """
    now_et = datetime.now(tz=NY_TZ) if now_utc is None else now_utc.astimezone(NY_TZ)

    if weekend_guard and now_et.weekday() >= 5:
        # Use the most recent business day's 09:30 as the anchor
        bday = _prev_business_day(now_et)
        start_et = bday.replace(hour=9, minute=30, second=0, microsecond=0)
        end_et = (start_et + timedelta(days=1))
        return start_et, end_et, True

    # Weekday path: strict boundary 09:30 → 09:30
    boundary = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
    end_et = boundary
    start_et = end_et - timedelta(days=1)
    return start_et, end_et, False

def parse_edgar_datetime_et(dt_str: str):
    """Parse SEC timestamps safely and convert to America/New_York.
    Handles:
      - ISO8601 with Z or timezone offset (UTC or other tz) => convert to ET
      - Plain 'YYYY-MM-DD HH:MM[:SS]' (assume ET as provided on HTML pages)
      - Milliseconds, microseconds, etc.
    """
    if not dt_str:
        raise ValueError("Empty datetime string")
    s = dt_str.strip()
    # Try best-effort ISO parsing
    try:
        dt = parser.isoparse(s)
        if dt.tzinfo is None:
            # Assume ET for naive ISO
            return dt.replace(tzinfo=NY_TZ)
        return dt.astimezone(NY_TZ)
    except Exception:
        pass
    # Fallbacks for common SEC formats
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            dt_naive = datetime.strptime(s, fmt)
            return dt_naive.replace(tzinfo=NY_TZ)
        except ValueError:
            continue
    # Last attempt: strip 'T'/'Z' and try again
    s2 = s.replace("T"," ").replace("Z","")
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            dt_naive = datetime.strptime(s2, fmt)
            return dt_naive.replace(tzinfo=NY_TZ)
        except ValueError:
            continue
    raise ValueError(f"Unrecognized EDGAR datetime: {dt_str}")
