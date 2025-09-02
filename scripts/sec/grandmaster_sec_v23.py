# -*- coding: utf-8 -*-
import os, json, csv, time, re
from datetime import datetime, timedelta
import requests
from scripts.util.time_utils import ET, now_et, window_prev_day_0930_to_next_0900, parse_acceptance_datetime, iso_et
from scripts.util.daily_index import fetch_master_idx, parse_master_idx
from scripts.util.rate_limiter import RateLimiter
from scripts.util.enrichment import get_company_profile
from scripts.util.bans import is_banned
from scripts.util.uploader import maybe_upload
from scripts.util.atom import fetch_atom_page, parse_atom_entries

SUPPORTED_FORMS = {"8-K","8-K/A","6-K","10-Q","10-Q/A","10-K","10-K/A","3","3/A","4","4/A","SC 13D","SC 13D/A","SC 13G","SC 13G/A"}

def load_config():
    with open("config/config.json","r",encoding="utf-8") as f: return json.load(f)

def qtr_of_month(m): return (m-1)//3+1

def derive_txt_url(filename: str):
    path = filename.strip()
    if path.endswith("-index.htm"): return f"https://www.sec.gov/Archives/{path.replace('-index.htm','.txt')}"
    if path.endswith(".txt"): return f"https://www.sec.gov/Archives/{path}"
    if path.endswith("/"): path = path[:-1]
    if "/" in path:
        base = path.rsplit("/",1)[0]; acc = path.rsplit("/",1)[1]
        if acc.endswith(".htm"): acc = acc.replace(".htm",".txt")
        return f"https://www.sec.gov/Archives/{base}/{acc}"
    return f"https://www.sec.gov/Archives/{path}"

def get_acceptance_dt_et(txt_url: str, ua: str, timeout: int, rl: RateLimiter):
    headers = {"User-Agent": ua, "Accept-Encoding": "gzip, deflate"}
    for attempt in range(5):
        try:
            r = requests.get(txt_url, headers=headers, timeout=timeout)
            if r.status_code == 200:
                m = re.search(r"ACCEPTANCE-DATETIME:\s*([0-9]{14})", r.text)
                if m: return parse_acceptance_datetime(m.group(1))
                m2 = re.search(r"ACCEPTANCE-DATE\s*:\s*([0-9]{8})\s*ACCEPTANCE-TIME\s*:\s*([0-9]{6})", r.text, flags=re.I)
                if m2: return parse_acceptance_datetime(m2.group(1)+m2.group(2))
                return None
            if r.status_code in (429,503):
                retry_after = int(r.headers.get("Retry-After","3")); time.sleep(max(3,retry_after)); continue
        except requests.RequestException:
            time.sleep(2*(attempt+1))
        finally:
            rl.wait()
    return None

def in_window(dt_et, start_et, end_et): return (dt_et is not None) and (start_et <= dt_et < end_et)

def fetch_daily_index_entries(yyyymmdd, ua, timeout, rl):
    year=int(yyyymmdd[:4]); mon=int(yyyymmdd[4:6]); qtr=(mon-1)//3+1
    txt = fetch_master_idx(year, qtr, yyyymmdd, ua, timeout, rl)
    return parse_master_idx(txt) if txt else []

def auto_shift_prev_bday_until_index(nowET, ua, timeout, rl, max_back=7):
    base_start, base_end = window_prev_day_0930_to_next_0900(nowET)
    prev_day = base_start.date()
    for i in range(max_back):
        ymd = prev_day.strftime("%Y%m%d")
        entries = fetch_daily_index_entries(ymd, ua, timeout, rl)
        if entries is not None:  # found index (may be empty on weekend/holiday, but exists)
            # Use this prev_day; construct new start/end for that day
            start = base_start.replace(year=prev_day.year, month=prev_day.month, day=prev_day.day)
            end = (start + timedelta(days=1)).replace(hour=9, minute=0, second=0)
            return prev_day, start, end, (i>0)
        prev_day = prev_day - timedelta(days=1)
    return base_start.date(), base_start, base_end, False

def main():
    os.makedirs("data", exist_ok=True)
    started_utc = datetime.utcnow().isoformat()+"Z"
    cfg = load_config()
    ua = cfg.get("user_agent","GrandMasterSEC/23.2M (+contact)")
    timeout = int(cfg.get("timeout_sec",20))
    rl = RateLimiter(reqs_per_sec=float(cfg.get("reqs_per_sec",0.7)))

    nowET = now_et()
    base_start_et, base_end_et = window_prev_day_0930_to_next_0900(nowET)
    prev_day, start_et, end_et, shifted = auto_shift_prev_bday_until_index(nowET, ua, timeout, rl, max_back=7)

    ymd_prev = prev_day.strftime("%Y%m%d")
    next_day = prev_day + timedelta(days=1)
    ymd_next = next_day.strftime("%Y%m%d")

    entries_prev = fetch_daily_index_entries(ymd_prev, ua, timeout, rl)
    entries_next = fetch_daily_index_entries(ymd_next, ua, timeout, rl)

    tail_from_atom = []
    now_date = nowET.date()
    tail_used = "none"
    if next_day == now_date:
        tail_used = "atom"
        start, count = 0, 100
        for _ in range(12):
            xml = fetch_atom_page(start=start, count=count, ua=ua, timeout=timeout, rl=rl)
            page = parse_atom_entries(xml)
            if not page: break
            tail_from_atom.extend(page)
            start += count

    candidates = []
    def add_from_dailyidx(ent):
        form = ent["form"].upper()
        if form not in SUPPORTED_FORMS: return
        candidates.append({"src":"daily-index","company":ent["company"],"form":form,"cik":ent["cik"],"filename":ent["filename"]})

    for e in entries_prev: add_from_dailyidx(e)
    for e in entries_next: add_from_dailyidx(e)

    for a in tail_from_atom:
        form = (a.get("form") or "").upper()
        if form not in SUPPORTED_FORMS: continue
        link = a.get("link") or ""
        if not link: continue
        txt_url = link.replace("-index.htm",".txt") if link.endswith("-index.htm") else link
        candidates.append({"src":"atom","company":a.get("title") or "","form":form,"cik":(a.get("cik") or ""), "filename": txt_url.replace("https://www.sec.gov/Archives/","")})

    seen = set(); uniq = []
    for e in candidates:
        key = (e["cik"], e["filename"], e["form"])
        if key in seen: continue
        seen.add(key); uniq.append(e)

    profile_cache = {}
    raw_records = []; min_scanned_et = None; entries_seen = 0

    for e in uniq:
        entries_seen += 1
        fn = e["filename"]
        txt_url = fn if fn.startswith("http") else f"https://www.sec.gov/Archives/{fn}"
        acc_dt_et = get_acceptance_dt_et(txt_url, ua, timeout, rl)
        if acc_dt_et is not None and (min_scanned_et is None or acc_dt_et < min_scanned_et):
            min_scanned_et = acc_dt_et
        if not in_window(acc_dt_et, start_et, end_et): continue

        cik = (e.get("cik") or "").lstrip("0")
        if cik and cik not in profile_cache:
            profile_cache[cik] = get_company_profile(cik, ua, timeout, rl)
        prof = profile_cache.get(cik, {"ticker":"","sic":"","sic_desc":"","name":""})

        company = e["company"] or prof.get("name") or ""
        ticker = prof.get("ticker") or ""
        sic = prof.get("sic") or ""
        sic_desc = prof.get("sic_desc") or ""

        if is_banned(company, sic, sic_desc): continue

        raw_records.append({"cik":cik,"company":company,"form":e["form"],"ticker":ticker,"sic":sic,"industry":sic_desc,"accepted_et":acc_dt_et.astimezone(ET).isoformat(),"txt_url":txt_url,"source":e.get("src","")})

    snapshot = [{"company":r["company"],"ticker":r["ticker"],"industry":r["industry"],"form":r["form"],"accepted_et":r["accepted_et"],"cik":r["cik"]} for r in raw_records]

    with open("data/sec_filings_raw.json","w",encoding="utf-8") as f: json.dump(raw_records,f,indent=2)
    with open("data/sec_filings_snapshot.json","w",encoding="utf-8") as f: json.dump(snapshot,f,indent=2)
    with open("data/sec_filings_snapshot.csv","w",newline="",encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(["company","ticker","industry","form","accepted_et","cik"])
        for r in snapshot: w.writerow([r["company"],r["ticker"],r["industry"],r["form"],r["accepted_et"],r["cik"]])

    finished_utc = datetime.utcnow().isoformat()+"Z"
    debug_stats = {"version":"v23.2M","started_utc":started_utc,"hit_boundary":bool(len(snapshot)>0),"auto_shifted_prev_bday":bool(shifted),"weekend_tail_scanned":False,"source_primary":"daily-index","source_tail":tail_used,"entries_seen":entries_seen,"entries_kept":len(snapshot),"last_oldest_et_scanned": iso_et(min_scanned_et) if min_scanned_et else None,"window_start_et": iso_et(start_et),"window_end_et": iso_et(end_et),"finished_utc":finished_utc}
    with open("data/sec_debug_stats.json","w",encoding="utf-8") as f: json.dump(debug_stats,f,indent=2)

    _ = maybe_upload(["data/sec_filings_raw.json","data/sec_filings_snapshot.json","data/sec_filings_snapshot.csv","data/sec_debug_stats.json"], cfg)

if __name__ == "__main__":
    main()
