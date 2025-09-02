# -*- coding: utf-8 -*-
"""
ONE-ATTEMPT FIX:
- Deterministic coverage via Daily Index for prev working day 09:30 → next day 09:00 ET
- Supported forms: 8-K, 6-K, 10-Q, 10-K, 3, 4, SC 13D/G (+/A)
- Enrichment: ticker + SIC/industry via submissions API
- Bans: finance/insurance/RE, alcohol/tobacco/gambling/weapons/adult/payday/credit/bank/lending
- Outputs: snapshot/json/csv/raw + debug stats
"""
import os, json, csv, time, re
from datetime import datetime, timedelta
import requests
from dateutil.relativedelta import relativedelta

from scripts.util.time_utils import ET, UTC, now_et, window_prev_day_0930_to_next_0900, parse_acceptance_datetime, iso_et
from scripts.util.daily_index import fetch_master_idx, parse_master_idx
from scripts.util.rate_limiter import RateLimiter
from scripts.util.enrichment import get_company_profile
from scripts.util.bans import is_banned
from scripts.util.uploader import maybe_upload

SUPPORTED_FORMS = {
    "8-K", "8-K/A",
    "6-K",
    "10-Q", "10-Q/A",
    "10-K", "10-K/A",
    "3", "3/A",
    "4", "4/A",
    "SC 13D", "SC 13D/A",
    "SC 13G", "SC 13G/A",
}

def load_config():
    with open("config/config.json", "r", encoding="utf-8") as f:
        return json.load(f)

def qtr_of_month(m):
    return (m - 1) // 3 + 1

def derive_txt_url(filename: str):
    # master idx "filename" often like: edgar/data/0000000000/0000000000-25-000123/primary-document
    # or "...-index.htm" -> replace with ".txt"
    path = filename.strip()
    if path.endswith("-index.htm"):
        return f"https://www.sec.gov/Archives/{path.replace('-index.htm', '.txt')}"
    if path.endswith(".txt"):
        return f"https://www.sec.gov/Archives/{path}"
    # fallback: try to append .txt at the folder level
    if path.endswith("/"):
        path = path[:-1]
    if "/" in path:
        base = path.rsplit("/", 1)[0]
        acc = path.rsplit("/", 1)[1]
        if acc.endswith(".htm"):
            acc = acc.replace(".htm", ".txt")
        return f"https://www.sec.gov/Archives/{base}/{acc}"
    return f"https://www.sec.gov/Archives/{path}"

def get_acceptance_dt_et(txt_url: str, ua: str, timeout: int, rl: RateLimiter):
    headers = {"User-Agent": ua, "Accept-Encoding": "gzip, deflate"}
    for attempt in range(5):
        try:
            r = requests.get(txt_url, headers=headers, timeout=timeout)
            if r.status_code == 200:
                # Find ACCEPTANCE-DATETIME: YYYYMMDDHHMMSS
                m = re.search(r"ACCEPTANCE-DATETIME:\s*([0-9]{14})", r.text)
                if m:
                    dt = parse_acceptance_datetime(m.group(1))
                    return dt
                # Sometimes header block slightly different casing
                m2 = re.search(r"ACCEPTANCE-DATE\s*:\s*([0-9]{8})\s*ACCEPTANCE-TIME\s*:\s*([0-9]{6})", r.text, flags=re.IGNORECASE)
                if m2:
                    dt = parse_acceptance_datetime(m2.group(1) + m2.group(2))
                    return dt
                return None
            if r.status_code in (429, 503):
                retry_after = int(r.headers.get("Retry-After", "3"))
                time.sleep(max(3, retry_after))
                continue
        except requests.RequestException:
            time.sleep(2 * (attempt + 1))
        finally:
            rl.wait()
    return None

def in_window(dt_et: datetime, start_et: datetime, end_et: datetime) -> bool:
    return (dt_et is not None) and (start_et <= dt_et < end_et)

def main():
    os.makedirs("data", exist_ok=True)
    started_utc = datetime.utcnow().isoformat() + "Z"
    cfg = load_config()
    ua = cfg.get("user_agent", "GrandMasterSEC/23.2M (+contact)")
    timeout = int(cfg.get("timeout_sec", 20))
    rl = RateLimiter(reqs_per_sec=float(cfg.get("reqs_per_sec", 0.7)))

    nowET = now_et()
    start_et, end_et = window_prev_day_0930_to_next_0900(nowET)

    # Determine which daily indexes to fetch
    prev_day = start_et.date()
    next_day = (start_et + timedelta(days=1)).date()
    year1, qtr1, ymd1 = prev_day.year, qtr_of_month(prev_day.month), prev_day.strftime("%Y%m%d")
    year2, qtr2, ymd2 = next_day.year, qtr_of_month(next_day.month), next_day.strftime("%Y%m%d")

    source_primary = "daily-index"
    entries_seen = 0
    kept = []

    # Fetch and parse prev_day index
    txt1 = fetch_master_idx(year1, qtr1, ymd1, ua, timeout, rl)
    entries1 = parse_master_idx(txt1) if txt1 else []
    # Fetch and parse next_day index (for 00:00-09:00)
    txt2 = fetch_master_idx(year2, qtr2, ymd2, ua, timeout, rl)
    entries2 = parse_master_idx(txt2) if txt2 else []

    candidates = []
    for ent in entries1 + entries2:
        entries_seen += 1
        form = ent["form"].upper()
        if form not in SUPPORTED_FORMS:
            continue
        candidates.append(ent)

    # Dedup by (cik, filename, form)
    seen = set()
    filtered = []
    for e in candidates:
        key = (e["cik"], e["filename"], e["form"])
        if key in seen: 
            continue
        seen.add(key)
        filtered.append(e)

    # Enrichment cache by CIK
    profile_cache = {}

    raw_records = []
    min_scanned_et = None

    for e in filtered:
        txt_url = derive_txt_url(e["filename"])
        acc_dt_et = get_acceptance_dt_et(txt_url, ua, timeout, rl)
        if acc_dt_et is not None:
            if (min_scanned_et is None) or (acc_dt_et < min_scanned_et):
                min_scanned_et = acc_dt_et
        if not in_window(acc_dt_et, start_et, end_et):
            continue

        cik = e["cik"]
        if cik not in profile_cache:
            profile_cache[cik] = get_company_profile(cik, ua, timeout, rl)
        prof = profile_cache[cik]

        company = e["company"]
        ticker = prof.get("ticker") or ""
        sic = prof.get("sic") or ""
        sic_desc = prof.get("sic_desc") or ""

        if is_banned(company, sic, sic_desc):
            continue

        rec = {
            "cik": cik,
            "company": company,
            "form": e["form"],
            "ticker": ticker,
            "sic": sic,
            "industry": sic_desc,
            "accepted_et": iso_et(acc_dt_et) if acc_dt_et else "",
            "txt_url": txt_url,
        }
        raw_records.append(rec)

    # Snapshot = only required columns
    snapshot = [
        {
            "company": r["company"],
            "ticker": r["ticker"],
            "industry": r["industry"],
            "form": r["form"],
            "accepted_et": r["accepted_et"],
            "cik": r["cik"],
        }
        for r in raw_records
    ]

    # Write outputs
    with open("data/sec_filings_raw.json", "w", encoding="utf-8") as f:
        json.dump(raw_records, f, indent=2)

    with open("data/sec_filings_snapshot.json", "w", encoding="utf-8") as f:
        json.dump(snapshot, f, indent=2)

    with open("data/sec_filings_snapshot.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["company", "ticker", "industry", "form", "accepted_et", "cik"])
        for r in snapshot:
            w.writerow([r["company"], r["ticker"], r["industry"], r["form"], r["accepted_et"], r["cik"]])

    finished_utc = datetime.utcnow().isoformat() + "Z"

    debug_stats = {
        "version": cfg.get("version", "v23.2M"),
        "started_utc": started_utc,
        "hit_boundary": bool(len(snapshot) > 0),
        "auto_shifted_prev_bday": False,  # separate logic could be added for holidays if needed
        "weekend_tail_scanned": False,
        "source_primary": source_primary,
        "source_tail": "none",
        "entries_seen": entries_seen,
        "entries_kept": len(snapshot),
        "last_oldest_et_scanned": iso_et(min_scanned_et) if min_scanned_et else None,
        "window_start_et": iso_et(start_et),
        "window_end_et": iso_et(end_et),
        "finished_utc": finished_utc,
    }
    with open("data/sec_debug_stats.json", "w", encoding="utf-8") as f:
        json.dump(debug_stats, f, indent=2)

    # Optional upload
    _ = maybe_upload([
        "data/sec_filings_raw.json",
        "data/sec_filings_snapshot.json",
        "data/sec_filings_snapshot.csv",
        "data/sec_debug_stats.json",
    ], cfg)

if __name__ == "__main__":
    main()
