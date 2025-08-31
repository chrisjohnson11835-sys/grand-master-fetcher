#!/usr/bin/env python3
# sec_only.py (v22 dual-source fallback)
import os, json, time, random, feedparser, pandas as pd, hashlib, requests
from typing import Any, Dict, List
from utils_sec import (
    SEC_ATOM, new_session, et_window_prev0930_to_latest0930, parse_entry_time, entry_form,
    extract_cik_from_link, load_json, within_window, fetch_submissions_for_cik,
    map_company_meta, banned_by_sic, banned_by_keywords, score_record, fallback_company_from_title
)
from sec_sources import fetch_atom_page, fetch_html_page

def ensure_dir(p): os.makedirs(p, exist_ok=True)
def cfg(c,k,d): return c.get(k,d)

def safe_write(path, data):
    try:
        tmp = path + ".tmp"
        with open(tmp,"w",encoding="utf-8") as f: json.dump(data,f,indent=2)
        os.replace(tmp, path)
    except Exception:
        pass

def main():
    root = os.path.dirname(os.path.abspath(__file__))
    cfgj = load_json(os.path.join(root,"config","settings.json"))
    scoring = load_json(os.path.join(root,"config","scoring.json"))
    ban_pref = load_json(os.path.join(root,"config","banned_sic_prefixes.json"))
    ban_exact = load_json(os.path.join(root,"config","banned_sic_exact.json"))
    ban_kw = load_json(os.path.join(root,"config","banned_keywords.json"))

    tz = cfgj.get("timezone","America/New_York")
    ua = cfgj.get("user_agent","GrandMasterSEC/1.0 (contact@example.com)")
    outdir = os.path.join(root,"outputs"); ensure_dir(outdir)
    ckpt_path = os.path.join(outdir,"sec_checkpoint.json")
    seen_path = os.path.join(outdir,"sec_seen_keys.json")

    start_et, end_et = et_window_prev0930_to_latest0930(tz, 9, 30, True)

    session = new_session(ua)

    from datetime import timedelta
    scan_extend_days = int(cfg(cfgj,"scan_extend_days",3))
    extended_stop_et = start_et - timedelta(days=scan_extend_days)

    max_pages = int(cfg(cfgj,"max_pages",2000))
    count = min(max(int(cfg(cfgj,"count_per_page",100)),1),100)
    pause = float(cfg(cfgj,"page_pause_sec",1.6))
    max_empty = int(cfg(cfgj,"max_empty_pages",40))
    use_seek = bool(cfg(cfgj,"seek_mode",True))
    page_budget = int(cfg(cfgj,"attempt_page_budget",300))
    retry_503 = int(cfg(cfgj,"retry_503",12))
    retry_sleep = float(cfg(cfgj,"retry_sleep_sec",2.5))

    allowed = {"8-K","8-K/A","6-K","6-K/A","10-Q","10-Q/A","10-K","10-K/A","SC 13D","SC 13D/A","SC 13G","SC 13G/A","Form 3","3/A","Form 4","4/A"}

    stats = {
        "window_mode":"prev_0930_to_latest_0930",
        "window_start_et": start_et.isoformat(),
        "window_end_et": end_et.isoformat(),
        "cutoff_local_time":"09:30",
        "pages_fetched":0,"entries_seen":0,"entries_kept":0,
        "banned_sic":0,"banned_kw":0,"errors":0,
        "hit_boundary":False,"hit_extended_boundary":False,"hit_page_limit":False,
        "atom_fetch_errors":0,"atom_http_codes":[],
        "pages_debug":[],"last_oldest_et_scanned":None,"effective_count_used": count,
        "scan_extend_days":scan_extend_days,"extended_stop_et":extended_stop_et.isoformat(),
        "seek_mode":use_seek, "fallback_used": False
    }

    # resume
    start_idx = 0
    try:
        ckpt = json.load(open(ckpt_path,"r",encoding="utf-8"))
        if ckpt.get("window_start_et")==stats["window_start_et"] and ckpt.get("window_end_et")==stats["window_end_et"] and ckpt.get("status")=="incomplete":
            start_idx = int(ckpt.get("next_start_idx",0))
            print(f"[worker] Resuming at start_idx={start_idx}")
    except Exception:
        pass

    try:
        seen = set(json.load(open(seen_path,"r",encoding="utf-8")))
    except Exception:
        seen = set()

    raw_rows=[]; kept_rows=[]
    empty_streak=0; pages_this_attempt=0; crossed_end=False
    consecutive_fail=0
    FALLBACK_THRESHOLD = 8

    def fetch_text(url):
        nonlocal consecutive_fail, pause
        try:
            r = session.get(url, timeout=30)
            stats["atom_http_codes"].append(r.status_code)
            if r.status_code == 200 and r.text.strip():
                consecutive_fail = 0
                return r.text
        except Exception:
            stats["atom_http_codes"].append("EXC")
        stats["atom_fetch_errors"] += 1
        consecutive_fail += 1
        pause = min(3.2, pause * 1.2)
        return None

    for p in range(max_pages):
        if pages_this_attempt >= page_budget:
            print(f"[worker] Page budget hit ({page_budget}). Checkpoint + exit attempt.")
            break

        url = SEC_ATOM.format(start=start_idx, count=count)
        stats["pages_fetched"] += 1; pages_this_attempt += 1

        text = fetch_text(url)
        entries = []
        mode = "atom"
        if text is None and consecutive_fail >= FALLBACK_THRESHOLD:
            html_text = fetch_text(url.replace("output=atom",""))
            if html_text:
                entries = fetch_html_page(html_text)
                stats["fallback_used"] = True
                mode = "html"
                consecutive_fail = 0
        elif text:
            entries = fetch_atom_page(feedparser, text)

        if not entries:
            empty_streak += 1
            ck={"status":"incomplete","window_start_et":stats["window_start_et"],"window_end_et":stats["window_end_et"],
                "next_start_idx": start_idx,"last_oldest_et_scanned": stats["last_oldest_et_scanned"]}
            tmp = os.path.join(outdir,"sec_checkpoint.json")
            safe_write(tmp, ck)
            if empty_streak >= max_empty: break
            time.sleep(pause + random.uniform(0.1,0.3)); continue
        empty_streak = 0

        newest=None; oldest=None
        for e in entries:
            t = e.get("updated")
            try:
                from dateutil import parser as dtp
                if t: dt = dtp.parse(t)
                else: dt = None
            except Exception:
                dt = None
            if dt:
                if oldest is None or dt < oldest: oldest=dt
                if newest is None or dt > newest: newest=dt

        def to_iso(dt):
            if not dt: return None
            try:
                from zoneinfo import ZoneInfo
            except Exception:
                from backports.zoneinfo import ZoneInfo
            return dt.astimezone(ZoneInfo(tz)).isoformat()

        stats["pages_debug"].append({"page":p,"start_idx":start_idx,"returned_entries":len(entries),
                                     "newest_et": to_iso(newest), "oldest_et": to_iso(oldest), "mode": mode})
        stats["last_oldest_et_scanned"] = stats["pages_debug"][-1]["oldest_et"]
        if p % 10 == 0:
            print(f"[worker] p={p} start_idx={start_idx} newest={stats['pages_debug'][-1]['newest_et']} oldest={stats['pages_debug'][-1]['oldest_et']} mode={mode}")

        if not crossed_end and oldest is not None:
            try:
                from zoneinfo import ZoneInfo
            except Exception:
                from backports.zoneinfo import ZoneInfo
            oldest_et = oldest.astimezone(ZoneInfo(tz))
            if oldest_et > end_et:
                gap_hours = (oldest_et - end_et).total_seconds()/3600.0
                jump = 2000 if gap_hours>4 else 1000 if gap_hours>2 else 500 if gap_hours>1 else 200
                start_idx += jump
                ck={"status":"incomplete","window_start_et":stats["window_start_et"],"window_end_et":stats["window_end_et"],
                    "next_start_idx": start_idx,"last_oldest_et_scanned": stats["last_oldest_et_scanned"]}
                safe_write(os.path.join(outdir,"sec_checkpoint.json"), ck)
                time.sleep(pause); continue
            else:
                crossed_end = True
                stats["hit_boundary"] = True

        for e in entries:
            faux = {
                "title": e.get("title",""),
                "summary": e.get("summary",""),
                "link": e.get("link",""),
                "updated": e.get("updated"),
                "tags": e.get("tags"),
                "category": e.get("category"),
                "updated_parsed": None
            }
            from dateutil import parser as dtp
            try:
                dt = dtp.parse(faux["updated"]) if faux["updated"] else None
            except Exception:
                dt = None
            if not dt or not within_window(dt, start_et, end_et, tz): 
                continue

            form = entry_form(faux)
            if form not in allowed: 
                continue
            title = faux["title"]; summary = faux["summary"]; link = faux["link"]
            cik = extract_cik_from_link(link)
            ticker=None; sic=None; industry=None; company=None
            if cik:
                try:
                    sub = fetch_submissions_for_cik(session, cik)
                    ticker, industry, sic, company = map_company_meta(sub)
                except Exception:
                    pass
            if not company:
                company = fallback_company_from_title(title)
            rec={"filing_datetime": dt.isoformat(), "form": form, "company": company, "ticker": ticker, "cik": cik,
                 "industry": industry, "sic": sic, "title": title, "summary": summary, "link": link}
            key = hashlib.sha256((link or title).encode("utf-8","ignore")).hexdigest()
            if key in seen: continue
            seen.add(key)
            raw_rows.append(rec)
            blob = " ".join([title or "", summary or "", str(industry or ""), str(company or "")])
            if banned_by_sic(sic, ban_pref, ban_exact) or banned_by_keywords(blob, ban_kw): continue
            rec["score"] = score_record(rec, scoring)
            kept_rows.append(rec)

        if oldest is not None:
            try:
                from zoneinfo import ZoneInfo
            except Exception:
                from backports.zoneinfo import ZoneInfo
            if oldest.astimezone(ZoneInfo(tz)) < extended_stop_et:
                stats["hit_extended_boundary"] = True; break

        start_idx += len(entries)
        safe_write(os.path.join(outdir,"sec_checkpoint.json"),
                   {"status":"incomplete","window_start_et":stats["window_start_et"],"window_end_et":stats["window_end_et"],
                    "next_start_idx": start_idx,"last_oldest_et_scanned": stats["last_oldest_et_scanned"]})
        time.sleep(pause + random.uniform(0.1,0.3))

    stats["entries_seen"] = len(raw_rows); stats["entries_kept"] = len(kept_rows)
    ensure_dir(outdir)
    with open(os.path.join(outdir,"sec_filings_raw.json"),"w",encoding="utf-8") as f: json.dump(raw_rows,f,indent=2)
    with open(os.path.join(outdir,"sec_debug_stats.json"),"w",encoding="utf-8") as f: json.dump(stats,f,indent=2)
    with open(os.path.join(outdir,"sec_filings_snapshot.json"),"w",encoding="utf-8") as f: json.dump(kept_rows,f,indent=2)
    cols=["filing_datetime","form","company","ticker","cik","industry","sic","title","score","link"]
    df = pd.DataFrame(kept_rows) if kept_rows else pd.DataFrame(columns=cols)
    df = df.reindex(columns=cols)
    df.to_csv(os.path.join(outdir,"sec_filings_snapshot.csv"), index=False)
    print("Outputs written to outputs/.")
    try:
        json.dump(sorted(list(seen)), open(seen_path,"w",encoding="utf-8"))
    except Exception:
        pass
    safe_write(os.path.join(outdir,"sec_checkpoint.json"),
               {"status":"complete","window_start_et":stats["window_start_et"],"window_end_et":stats["window_end_et"],
                "next_start_idx": 0,"last_oldest_et_scanned": stats["last_oldest_et_scanned"]})
    if cfgj.get("enable_webhook_deploy"):
        try:
            from deploy.webhook_deploy import deploy_files
            deploy_files(cfgj, [os.path.join(outdir,"sec_filings_snapshot.json"),
                                os.path.join(outdir,"sec_filings_snapshot.csv"),
                                os.path.join(outdir,"sec_filings_raw.json"),
                                os.path.join(outdir,"sec_debug_stats.json")])
        except Exception as e:
            print(f"Deploy skipped/error: {e}")

if __name__ == "__main__":
    main()
