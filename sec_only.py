#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Grand Master — SEC ONLY (Final v21)
Reliable 09:30→09:30 scraper with:
- Seek mode: jump deep until we cross "today 09:30 ET", then fine-scan.
- Resume mode: checkpoint after every page; next run resumes where it stopped.
- Seen-keys dedupe across runs.
- Page-budget per attempt + heartbeat logs every 10 pages.
- Enrichment via SEC submissions (ticker, industry, sic, company).
- Ban filters (SIC + keywords) + scoring.
- Outputs (raw/snapshot/csv/stats) + webhook deploy with response logging.
"""
import os, json, time, hashlib, feedparser, pandas as pd
from typing import Any, Dict, List, Tuple
from utils_sec import (
    SEC_ATOM, new_session, et_window_prev0930_to_latest0930, parse_entry_time, entry_form,
    extract_cik_from_link, load_json, within_window, fetch_submissions_for_cik,
    map_company_meta, banned_by_sic, banned_by_keywords, score_record, fallback_company_from_title
)

def ensure_dir(p): os.makedirs(p, exist_ok=True)
def cfg(cfg, k, d): return cfg.get(k, d)

def safe_load(path, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def safe_write(path, data):
    try:
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp, path)
    except Exception:
        pass

def make_key(rec: Dict[str, Any]) -> str:
    base = f"{rec.get('link','')}|{rec.get('cik','')}|{rec.get('form','')}|{rec.get('filing_datetime','')}"
    if not base.strip("|"):
        base = f"{rec.get('title','')}|{rec.get('summary','')}"
    return hashlib.sha256(base.encode("utf-8","ignore")).hexdigest()

def main():
    root = os.path.dirname(os.path.abspath(__file__))
    cfgj = load_json(os.path.join(root, "config", "settings.json"))
    scoring = load_json(os.path.join(root, "config", "scoring.json"))
    ban_pref = load_json(os.path.join(root, "config", "banned_sic_prefixes.json"))
    ban_exact = load_json(os.path.join(root, "config", "banned_sic_exact.json"))
    ban_kw = load_json(os.path.join(root, "config", "banned_keywords.json"))
    tz = cfgj.get("timezone","America/New_York")
    ua = cfgj.get("user_agent","GrandMasterSEC/1.0 (contact@example.com)")
    outdir = os.path.join(root, "outputs"); ensure_dir(outdir)
    ckpt_path = os.path.join(outdir, "sec_checkpoint.json")
    seen_path = os.path.join(outdir, "sec_seen_keys.json")

    start_et, end_et = et_window_prev0930_to_latest0930(tz, 9, 30, True)
    session = new_session(ua)

    from datetime import timedelta
    scan_extend_days = int(cfg(cfgj, "scan_extend_days", 3))
    extended_stop_et = start_et - timedelta(days=scan_extend_days)

    max_pages = int(cfg(cfgj, "max_pages", 2000))
    count = min(max(int(cfg(cfgj, "count_per_page", 100)),1), 100)
    page_pause = float(cfg(cfgj, "page_pause_sec", 2.0))
    max_empty_pages = int(cfg(cfgj, "max_empty_pages", 40))
    retry_503 = int(cfg(cfgj, "retry_503", 10))
    retry_sleep = float(cfg(cfgj, "retry_sleep_sec", 2.0))
    use_seek = bool(cfg(cfgj, "seek_mode", True))
    page_budget = int(cfg(cfgj, "attempt_page_budget", 250))

    allowed_forms = {
        "8-K","8-K/A","6-K","6-K/A","10-Q","10-Q/A","10-K","10-K/A",
        "SC 13D","SC 13D/A","SC 13G","SC 13G/A","Form 3","3/A","Form 4","4/A"
    }

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
        "seek_mode": use_seek,
        "resume": {}
    }

    start_idx = 0
    ckpt = safe_load(ckpt_path, {})
    if ckpt and ckpt.get("window_start_et")==stats["window_start_et"] and ckpt.get("window_end_et")==stats["window_end_et"] and ckpt.get("status")=="incomplete":
        start_idx = int(ckpt.get("next_start_idx", 0))
        stats["resume"] = {"resumed": True, "prev_start_idx": start_idx, "prev_last_oldest_et_scanned": ckpt.get("last_oldest_et_scanned")}
        print(f"[worker] Resuming at start_idx={start_idx}")
    else:
        stats["resume"] = {"resumed": False}

    seen = set(safe_load(seen_path, []))

    def fetch(url):
        codes = []
        txt = None
        for a in range(1, retry_503+1):
            try:
                r = session.get(url, timeout=30)
                codes.append(r.status_code)
                if r.status_code == 200 and r.text.strip():
                    txt = r.text; break
            except Exception:
                codes.append("EXC")
            time.sleep(retry_sleep * a)
        stats["atom_http_codes"].extend(codes)
        return txt

    def to_et(dt):
        if dt is None: return None
        try:
            from zoneinfo import ZoneInfo
        except Exception:
            from backports.zoneinfo import ZoneInfo
        return dt.astimezone(ZoneInfo(tz)).isoformat()

    def bounds(entries):
        oldest = None; newest = None
        for e in entries:
            t = parse_entry_time(e)
            if not t: continue
            if oldest is None or t < oldest: oldest = t
            if newest is None or t > newest: newest = t
        return newest, oldest

    raw_rows: List[Dict[str,Any]] = []
    kept_rows: List[Dict[str,Any]] = []

    crossed_end = False
    empty_streak = 0
    this_pages = 0

    for p in range(max_pages):
        if this_pages >= page_budget:
            print(f"[worker] Page budget hit ({page_budget}). Checkpoint + exit attempt.")
            break
        url = SEC_ATOM.format(start=start_idx, count=count)
        stats["pages_fetched"] += 1
        this_pages += 1

        text = fetch(url)
        if text is None:
            stats["atom_fetch_errors"] += 1
            empty_streak += 1
            ck = {"status":"incomplete","window_start_et":stats["window_start_et"],"window_end_et":stats["window_end_et"],
                  "next_start_idx": start_idx, "last_oldest_et_scanned": stats["last_oldest_et_scanned"]}
            safe_write(ckpt_path, ck)
            if empty_streak >= max_empty_pages: break
            time.sleep(page_pause); continue
        empty_streak = 0

        feed = feedparser.parse(text)
        entries = feed.get("entries", []) or []
        n = len(entries)
        if n == 0:
            empty_streak += 1
            ck = {"status":"incomplete","window_start_et":stats["window_start_et"],"window_end_et":stats["window_end_et"],
                  "next_start_idx": start_idx, "last_oldest_et_scanned": stats["last_oldest_et_scanned"]}
            safe_write(ckpt_path, ck)
            if empty_streak >= max_empty_pages: break
            time.sleep(page_pause); continue

        newest, oldest = bounds(entries)
        stats["pages_debug"].append({"page": p, "start_idx": start_idx, "returned_entries": n,
                                     "newest_et": to_et(newest), "oldest_et": to_et(oldest)})
        stats["last_oldest_et_scanned"] = stats["pages_debug"][-1]["oldest_et"]
        if p % 10 == 0:
            print(f"[worker] p={p} start_idx={start_idx} newest={stats['pages_debug'][-1]['newest_et']} oldest={stats['pages_debug'][-1]['oldest_et']}")

        # Seek until crossing today's 09:30 ET
        if not crossed_end and use_seek and oldest is not None:
            from dateutil import tz as _tz
            import datetime as _dt
            try:
                from zoneinfo import ZoneInfo
            except Exception:
                from backports.zoneinfo import ZoneInfo
            oldest_et = oldest.astimezone(ZoneInfo(tz))
            if oldest_et > end_et:
                gap_hours = (oldest_et - end_et).total_seconds()/3600.0
                jump = 2000 if gap_hours>4 else 1000 if gap_hours>2 else 500 if gap_hours>1 else 200
                start_idx += jump
                ck = {"status":"incomplete","window_start_et":stats["window_start_et"],"window_end_et":stats["window_end_et"],
                      "next_start_idx": start_idx, "last_oldest_et_scanned": stats["last_oldest_et_scanned"]}
                safe_write(ckpt_path, ck)
                time.sleep(page_pause); continue
            else:
                crossed_end = True
                stats["hit_boundary"] = True

        # Process within strict window
        for e in entries:
            t = parse_entry_time(e)
            if not t or not within_window(t, start_et, end_et, tz): continue
            form = entry_form(e)
            if form not in allowed_forms: continue
            title = e.get("title","")
            summary = e.get("summary","") or e.get("content",[{"value":""}])[0].get("value","")
            link = e.get("link","")
            cik = extract_cik_from_link(link)
            ticker = None; sic=None; industry=None; company=None
            if cik:
                try:
                    sub = fetch_submissions_for_cik(session, cik)
                    ticker, industry, sic, company = map_company_meta(sub)
                except Exception:
                    stats["errors"] += 1
            if not company:
                company = fallback_company_from_title(title)
            rec = {"filing_datetime": t.isoformat(),"form": form,"company": company,"ticker": ticker,"cik": cik,
                   "industry": industry,"sic": sic,"title": title,"summary": summary,"link": link}
            key = make_key(rec)
            if key in seen: continue
            seen.add(key)
            raw_rows.append(rec)
            blob = " ".join([title or "", summary or "", str(industry or ""), str(company or "")])
            if banned_by_sic(sic, ban_pref, ban_exact) or banned_by_keywords(blob, ban_kw):
                if banned_by_sic(sic, ban_pref, ban_exact): stats["banned_sic"] += 1
                else: stats["banned_kw"] += 1
                continue
            rec["score"] = score_record(rec, scoring)
            kept_rows.append(rec)

        # Stop after extended stop
        if oldest is not None:
            try:
                from zoneinfo import ZoneInfo
            except Exception:
                from backports.zoneinfo import ZoneInfo
            if oldest.astimezone(ZoneInfo(tz)) < extended_stop_et:
                stats["hit_extended_boundary"] = True
                break

        start_idx += n
        ck = {"status":"incomplete","window_start_et":stats["window_start_et"],"window_end_et":stats["window_end_et"],
              "next_start_idx": start_idx, "last_oldest_et_scanned": stats["last_oldest_et_scanned"]}
        safe_write(ckpt_path, ck)
        time.sleep(page_pause)

    kept_rows.sort(key=lambda r: (r.get("score",0), r.get("filing_datetime","")), reverse=True)
    stats["entries_seen"] = len(raw_rows)
    stats["entries_kept"] = len(kept_rows)

    raw_path = os.path.join(outdir,"sec_filings_raw.json")
    with open(raw_path,"w",encoding="utf-8") as f: json.dump(raw_rows,f,indent=2)
    stats_path = os.path.join(outdir,"sec_debug_stats.json")
    with open(stats_path,"w",encoding="utf-8") as f: json.dump(stats,f,indent=2)
    snap_path = os.path.join(outdir,"sec_filings_snapshot.json")
    with open(snap_path,"w",encoding="utf-8") as f: json.dump(kept_rows,f,indent=2)
    import pandas as pd
    cols = ["filing_datetime","form","company","ticker","cik","industry","sic","title","score","link"]
    df = pd.DataFrame(kept_rows) if kept_rows else pd.DataFrame(columns=cols)
    df = df.reindex(columns=cols)
    csv_path = os.path.join(outdir, "sec_filings_snapshot.csv")
    df.to_csv(csv_path, index=False)

    print("Outputs written to outputs/.")

    # Save seen registry & finalize checkpoint
    try:
        with open(seen_path,"w",encoding="utf-8") as f: json.dump(sorted(list(seen)), f)
    except Exception:
        pass
    ck = {"status":"complete","window_start_et":stats["window_start_et"],"window_end_et":stats["window_end_et"],
          "next_start_idx": 0, "last_oldest_et_scanned": stats["last_oldest_et_scanned"]}
    safe_write(ckpt_path, ck)

    if cfgj.get("enable_webhook_deploy"):
        try:
            from deploy.webhook_deploy import deploy_files
            deploy_files(cfgj, [snap_path, csv_path, raw_path, stats_path])
        except Exception as e:
            print(f"Deploy skipped/error: {e}")

if __name__ == "__main__":
    main()
