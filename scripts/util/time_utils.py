from datetime import datetime, timedelta, timezone
from dateutil import tz
NY_TZ = tz.gettz("America/New_York")
def compute_et_window(now_utc=None):
    now_et = datetime.now(tz=NY_TZ) if now_utc is None else now_utc.astimezone(NY_TZ)
    boundary = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
    end_et = boundary if now_et >= boundary else boundary
    start_et = end_et - timedelta(days=1)
    return start_et, end_et
def parse_edgar_datetime_et(dt_str):
    s=dt_str.replace("T"," ").replace("Z","")
    for fmt in ("%Y-%m-%d %H:%M:%S","%Y-%m-%d %H:%M"):
        try:
            from datetime import datetime
            dt_naive = datetime.strptime(s.strip(), fmt)
            return dt_naive.replace(tzinfo=NY_TZ)
        except ValueError: continue
    raise ValueError("Unrecognized EDGAR datetime: "+dt_str)
