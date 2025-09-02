# -*- coding: utf-8 -*-
from datetime import datetime, timedelta, time
import pytz
ET = pytz.timezone("America/New_York")
UTC = pytz.utc
def now_et():
    return datetime.now(ET)
def to_et(dt):
    if dt.tzinfo is None: return ET.localize(dt)
    return dt.astimezone(ET)
def prev_working_day(d):
    d = d.date(); wd = d.weekday()
    if wd == 0: delta = 3
    elif wd == 6: delta = 2
    else: delta = 1
    from datetime import date
    base = datetime.combine(d, time(0,0))
    return (base - timedelta(days=delta)).date()
def window_prev_day_0930_to_next_0900(now_et_dt):
    prev_day = prev_working_day(now_et_dt)
    start = ET.localize(datetime(prev_day.year, prev_day.month, prev_day.day, 9, 30, 0))
    end = (start + timedelta(days=1)).replace(hour=9, minute=0, second=0)
    return start, end
def parse_acceptance_datetime(value: str):
    if not value or len(value) < 14: return None
    y=int(value[0:4]); m=int(value[4:6]); d=int(value[6:8]); hh=int(value[8:10]); mm=int(value[10:12]); ss=int(value[12:14])
    return ET.localize(datetime(y,m,d,hh,mm,ss))
def iso_et(dt): return dt.astimezone(ET).isoformat()
