#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Grand Master — SEC ONLY (Step 6) v19.6
- Output window: prev business day 09:30 ET -> latest 09:30 ET
- Expanded scraping depth via scan_extend_days (default 2)
- 100-per-page cap + advance by actual items (no skips)
- Tags-based form detection; enrichment; safe CSV; webhook deploy; pages_debug
"""
import os, json, time
from typing import Any, Dict, List
import feedparser
import pandas as pd
from utils_sec import (
    SEC_ATOM, new_session, et_window_prev0930_to_latest0930, parse_entry_time, entry_form,
    extract_cik_from_link, load_json, within_window, fetch_submissions_for_cik,
    map_company_meta, banned_by_sic, banned_by_keywords, score_record, fallback_company_from_title
)

def ensure_dir(p): os.makedirs(p, exist_ok=True)

def cfg_val(cfg, key, default):
    return cfg.get(key, default)

def main():
    root = os.path.dirname(os.path.abspath(__file__))
    cfg = load_json(os.path.join(root, "config", "settings.json"))
    scoring = load_json(os.path.join(root, "config", "scoring.json"))
    ban_pref = load_json(os.path.join(root, "config", "banned_sic_prefixes.json"))
    ban_exact = load_json(os.path.join(root, "config", "banned_sic_exact.json"))
    ban_kw = load_json(os.path.join(root, "config", "banned_keywords.json"))
    tz = cfg.get("timezone","America/New_York")
    ua = cfg.get("user_agent","GrandMasterSEC/1.0 (contact@example.com)")

    outdir = os.path.join(root, "outputs"); ensure_dir(outdir)

    start_et, end_et = et_window_prev0930_to_latest0930(tz, cutoff_hour=9, cutoff_minute=30, business_days=True)
    session = new_session(ua)

    # Depth extension
    from datetime import timedelta
    scan_extend_days = int(cfg_val(cfg, "scan_extend_days", 2))
    extended_stop_et = start_et - timedelta(days=scan_extend_days)

    max_pages = int(cfg_val(cfg, "max_pages", 400))
    requested_count = int(cfg_val(cfg, "count_per_page", 100))
    count = min(max(requested_count, 1), 100)
    page_pause = float(cfg_val(cfg, "page_pause_sec", 1.2))
    max_empty_pages = int(cfg_val(cfg, "max_empty_pages", 8))
    retry_503 = int(cfg_val(cfg, "retry_503", 5))
    retry_sleep = float(cfg_val(cfg, "retry_sleep_sec", 1.5))

    allowed_forms = {
        "8-K","8-K/A","6-K","6-K/A","10-Q","10-Q/A","10-K","10-K/A",
        "SC 13D","SC 13D/A","SC 13G","SC 13G/A","Form 3","3/A","Form 4","4/A"
    }

    raw_rows: List[Dict[str,Any]] = []
    kept_rows: List[Dict[str,Any]] = []
    stats = {
        "window_mode": "prev_0930_to_latest_0930",
        "window_start_et": start_et.isoformat(),
        "window_end_et": end_et.isoformat(),
        "cutoff_local_time": "09:30",
        "pages_fetched": 0,
        "entries_seen": 0,
        "entries_kept": 0,
        "banned_sic": 0,
        "banned_kw": 0,
        "errors": 0,
        "hit_boundary": False,
        "hit_extended_boundary": False,
        "hit_page_limit": False,
        "atom_fetch_errors": 0,
        "atom_http_codes": [],
        "pages_debug": [],
        "last_oldest_et_scanned": None,
        "effective_count_used": count,
        "scan_extend_days": scan_extend_days,
        "extended_stop_et": extended_stop_et.isoformat(),
    }

    empty_streak = 0
    start_idx = 0

    for p in range(max_pages):
        url = SEC_ATOM.format(start=start_idx, count=count)
        stats["pages_fetched"] += 1

        # Retry fetch with gentle backoff
        page_text = None
        http_codes_local = []
        for attempt in range(1, retry_503+1):
            try:
                r = session.get(url, timeout=30)
                http_codes_local.append(r.status_code)
                if r.status_code == 200 and r.text.strip():
                    page_text = r.text
                    break
            except Exception:
                http_codes_local.append("EXC")
            time.sleep(retry_sleep * attempt)
        stats["atom_http_codes"].extend(http_codes_local)

        if page_text is None:
            stats["atom_fetch_errors"] += 1
            empty_streak += 1
            if empty_streak >= max_empty_pages:
                break
            time.sleep(page_pause)
            continue

        feed = feedparser.parse(page_text)
        entries = feed.get("entries", []) or []
        n = len(entries)
        if n == 0:
            empty_streak += 1
            if empty_streak >= max_empty_pages:
                break
            time.sleep(page_pause)
            continue
        else:
            empty_streak = 0

        oldest_et_on_page = None
        newest_et_on_page = None

        for e in entries:
            ftime = parse_entry_time(e)
            if not ftime:
                continue
            if oldest_et_on_page is None or ftime < oldest_et_on_page:
                oldest_et_on_page = ftime
            if newest_et_on_page is None or ftime > newest_et_on_page:
                newest_et_on_page = ftime

            if not within_window(ftime, start_et, end_et, tz):
                continue

            form = entry_form(e)
            if form not in allowed_forms:
                continue

            title = e.get("title","")
            summary = e.get("summary","") or e.get("content",[{"value":""}])[0].get("value","")
            link = e.get("link","")

            cik = extract_cik_from_link(link)
            ticker = None; sic = None; industry = None; company = None
            if cik:
                try:
                    sub_json = fetch_submissions_for_cik(session, cik)
                    ticker, industry, sic, company = map_company_meta(sub_json)
                except Exception:
                    stats["errors"] += 1
            if not company:
                company = fallback_company_from_title(title)

            banned = False
            if banned_by_sic(sic, ban_pref, ban_exact):
                banned = True; stats["banned_sic"] += 1
            else:
                blob = " ".join([title or "", summary or "", industry or "", company or ""])
                if banned_by_keywords(blob, ban_kw):
                    banned = True; stats["banned_kw"] += 1

            rec = {
                "filing_datetime": ftime.isoformat(),
                "form": form,
                "company": company,
                "ticker": ticker,
                "cik": cik,
                "industry": industry,
                "sic": sic,
                "title": title,
                "summary": summary,
                "link": link
            }
            raw_rows.append(rec)
            if banned:
                continue

            rec["score"] = score_record(rec, scoring)
            kept_rows.append(rec)

        # Debug coverage
        def to_et_str(dt):
            try:
                from zoneinfo import ZoneInfo
            except Exception:
                from backports.zoneinfo import ZoneInfo
            return dt.astimezone(ZoneInfo(tz)).isoformat() if dt else None

        stats["pages_debug"].append({
            "page": p,
            "start_idx": start_idx,
            "returned_entries": n,
            "newest_et": to_et_str(newest_et_on_page),
            "oldest_et": to_et_str(oldest_et_on_page),
        })
        stats["last_oldest_et_scanned"] = stats["pages_debug"][-1]["oldest_et"]

        if oldest_et_on_page:
            try:
                from zoneinfo import ZoneInfo
            except Exception:
                from backports.zoneinfo import ZoneInfo
            oldest_et = oldest_et_on_page.astimezone(ZoneInfo(tz))
            if oldest_et < start_et:
                stats["hit_boundary"] = True
            if oldest_et < extended_stop_et:
                stats["hit_extended_boundary"] = True
                break

        start_idx += n
        time.sleep(page_pause)

    kept_rows.sort(key=lambda r: (r.get("score",0), r.get("filing_datetime","")), reverse=True)
    stats["entries_kept"] = len(kept_rows)

    outdir = os.path.join(root, "outputs"); ensure_dir(outdir)
    raw_path = os.path.join(outdir, "sec_filings_raw.json")
    with open(raw_path,"w",encoding="utf-8") as f: f.write(json.dumps(raw_rows, indent=2))
    stats_path = os.path.join(outdir, "sec_debug_stats.json")
    with open(stats_path,"w",encoding="utf-8") as f: f.write(json.dumps(stats, indent=2))
    snap_path = os.path.join(outdir, "sec_filings_snapshot.json")
    with open(snap_path,"w",encoding="utf-8") as f: f.write(json.dumps(kept_rows, indent=2))

    cols = ["filing_datetime","form","company","ticker","cik","industry","sic","title","score","link"]
    if kept_rows:
        df = pd.DataFrame(kept_rows)
    else:
        df = pd.DataFrame(columns=cols)
    df = df.reindex(columns=cols)
    csv_path = os.path.join(outdir, "sec_filings_snapshot.csv")
    df.to_csv(csv_path, index=False)

    print("Outputs written to outputs/.")

    if cfg.get("enable_webhook_deploy"):
        try:
            from deploy.webhook_deploy import deploy_files
            deploy_files(cfg, [snap_path, csv_path, raw_path, stats_path])
        except Exception as e:
            print(f"Deploy skipped/error: {e}")

if __name__ == "__main__":
    main()
