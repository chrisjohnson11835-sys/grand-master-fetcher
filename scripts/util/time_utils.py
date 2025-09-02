# -*- coding: utf-8 -*-
from datetime import datetime, timedelta, time
import pytz

ET = pytz.timezone("America/New_York")
UTC = pytz.utc

def now_et():
    return datetime.now(ET)

def to_et(dt: datetime):
    if dt.tzinfo is None:
        return ET.localize(dt)
    return dt.astimezone(ET)

def prev_working_day(d: datetime):
    # Move back to previous weekday (Mon-Fri)
    d = d.date()
    weekday = d.weekday()  # Mon=0..Sun=6
    if weekday == 0:  # Monday -> Friday
        delta = 3
    elif weekday == 6:  # Sunday -> Friday
        delta = 2
    else:
        delta = 1
    from datetime import date
    base = datetime.combine(d, time(0,0))
    return (base - timedelta(days=delta)).date()

def window_prev_day_0930_to_next_0900(now_et_dt: datetime):
    prev_day = prev_working_day(now_et_dt)
    start = ET.localize(datetime(prev_day.year, prev_day.month, prev_day.day, 9, 30, 0))
    end = start + timedelta(days=1)
    end = end.replace(hour=9, minute=0, second=0)
    return start, end

def parse_acceptance_datetime(value: str):
    # e.g., 20250901183055 (ET)
    if not value or len(value) < 14:
        return None
    year = int(value[0:4])
    mon = int(value[4:6])
    day = int(value[6:8])
    hh = int(value[8:10])
    mm = int(value[10:12])
    ss = int(value[12:14])
    dt = datetime(year, mon, day, hh, mm, ss)
    return ET.localize(dt)

def iso_et(dt):
    return dt.astimezone(ET).isoformat()
