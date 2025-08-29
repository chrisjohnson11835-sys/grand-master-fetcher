#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Grand Master — SEC ONLY (Step 6) v18.3
Hotfix: write CSV safely even when no rows are kept.
"""
import os, json
from typing import Any, Dict, List
import feedparser
import pandas as pd
from utils_sec import (
    SEC_ATOM, new_session, et_window_now_yday, parse_entry_time, entry_form,
    extract_cik_from_link, load_json, within_window, fetch_submissions_for_cik,
    map_ticker_industry, banned_by_sic, banned_by_keywords, score_record
)

def ensure_dir(p): os.makedirs(p, exist_ok=True)

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
    start_et, end_et = et_window_now_yday(tz)
    session = new_session(ua)

    max_pages = int(cfg.get("max_pages", 60))
    count = int(cfg.get("count_per_page", 200))
    allowed_forms = {
        "8-K","8-K/A","6-K","6-K/A","10-Q","10-Q/A","10-K","10-K/A",
        "SC 13D","SC 13D/A","SC 13G","SC 13G/A","Form 3","3/A","Form 4","4/A"
    }

    raw_rows: List[Dict[str,Any]] = []
    kept_rows: List[Dict[str,Any]] = []
    stats = {
        "window_start_et": start_et.isoformat(),
        "window_end_et": end_et.isoformat(),
        "pages_fetched": 0,
        "entries_seen": 0,
        "entries_kept": 0,
        "banned_sic": 0,
        "banned_kw": 0,
        "errors": 0,
        "hit_boundary": False,
        "hit_page_limit": False
    }

    crossed_boundary = False

    for p in range(max_pages):
        url = SEC_ATOM.format(start=p*count, count=count)
        stats["pages_fetched"] += 1
        feed = feedparser.parse(url)
        entries = feed.get("entries", [])
        if not entries:
            break

        oldest_et_on_page = None

        for e in entries:
            stats["entries_seen"] += 1
            ftime = parse_entry_time(e)
            if not ftime:
                continue

            if oldest_et_on_page is None or ftime < oldest_et_on_page:
                oldest_et_on_page = ftime

            if not within_window(ftime, start_et, end_et, tz):
                continue

            form = entry_form(e)
            if form not in allowed_forms:
                continue

            title = e.get("title","")
            summary = e.get("summary","") or e.get("content",[{"value":""}])[0].get("value","")
            link = e.get("link","")

            cik = extract_cik_from_link(link)
            ticker = None; sic = None; industry = None
            if cik:
                try:
                    sub_json = fetch_submissions_for_cik(session, cik)
                    ticker, industry, sic = map_ticker_industry(sub_json)
                except Exception:
                    stats["errors"] += 1

            banned = False
            if banned_by_sic(sic, ban_pref, ban_exact):
                banned = True; stats["banned_sic"] += 1
            else:
                blob = " ".join([title or "", summary or "", industry or ""])
                if banned_by_keywords(blob, ban_kw):
                    banned = True; stats["banned_kw"] += 1

            rec = {
                "filing_datetime": ftime.isoformat(),
                "form": form,
                "title": title,
                "summary": summary,
                "link": link,
                "cik": cik,
                "ticker": ticker,
                "industry": industry,
                "sic": sic,
            }
            raw_rows.append(rec)
            if banned: continue

            rec["score"] = score_record(rec, scoring)
            kept_rows.append(rec)

        if oldest_et_on_page:
            try:
                from zoneinfo import ZoneInfo
            except Exception:
                from backports.zoneinfo import ZoneInfo
            oldest_et = oldest_et_on_page.astimezone(ZoneInfo(tz))
            if oldest_et < start_et:
                crossed_boundary = True
                stats["hit_boundary"] = True
                break

    if not crossed_boundary and stats["pages_fetched"] >= max_pages:
        stats["hit_page_limit"] = True

    kept_rows.sort(key=lambda r: (r.get("score",0), r.get("filing_datetime","")), reverse=True)
    stats["entries_kept"] = len(kept_rows)

    # Write JSON outputs
    raw_path = os.path.join(outdir, "sec_filings_raw.json")
    with open(raw_path,"w",encoding="utf-8") as f: f.write(json.dumps(raw_rows, indent=2))
    stats_path = os.path.join(outdir, "sec_debug_stats.json")
    with open(stats_path,"w",encoding="utf-8") as f: f.write(json.dumps(stats, indent=2))
    snap_path = os.path.join(outdir, "sec_filings_snapshot.json")
    with open(snap_path,"w",encoding="utf-8") as f: f.write(json.dumps(kept_rows, indent=2))

    # Safe CSV (write headers even if zero rows)
    cols = ["filing_datetime","form","ticker","cik","industry","sic","title","score","link"]
    import pandas as pd
    if kept_rows:
        df = pd.DataFrame(kept_rows)
    else:
        df = pd.DataFrame(columns=cols)
    df = df.reindex(columns=cols)
    df.to_csv(os.path.join(outdir, "sec_filings_snapshot.csv"), index=False)

    print("Done. See outputs/.")

if __name__ == "__main__":
    main()
