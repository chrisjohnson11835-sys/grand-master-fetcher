#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, sys, json, csv, time
from collections import defaultdict
from datetime import datetime
from scripts.util.time_utils import compute_windows, compute_prev_bday_windows, parse_edgar_datetime_et
from scripts.util.fetchers import SECClient, parse_html_entries, parse_atom_entries
from scripts.util.fulltext import fetch_fulltext_window
from scripts.util.enrichment import Enricher
from scripts.util.bans import is_banned
from scripts.util.scoring import score_entry, extract_eightk_items, extract_form4_codes
from scripts.util.uploader import post_file

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "config", "config.json")

def ensure_dir(p): os.makedirs(p, exist_ok=True)

def _within(dt, a, b): return (dt>=a) and (dt<b)

def _time_window_collect(client, kind, start_et, end_et, cfg, ua):
    """Collect entries *bounded* to [start_et, end_et) with early-stop by datetime.
    kind: 'fulltext'|'html'|'atom'
    Returns (entries_in_window, pages_fetched, source_used)
    """
    count_per_page=int(cfg.get('count_per_page',100))
    max_pages=int(cfg.get('max_pages',30))
    entries=[]; pages=0
    if kind=='fulltext':
        ft = fetch_fulltext_window(ua, start_et, end_et, cfg.get('forms_supported',[]),
                                   page_size=int(cfg.get('fulltext_page_size',400)),
                                   max_pages=max_pages)
        return ft, 0, 'fulltext'
    elif kind=='html':
        start_idx=0
        while pages < max_pages:
            html=client.fetch_html_page(start=start_idx, count=count_per_page)
            page_entries = parse_html_entries(html)
            if not page_entries: break
            dts=[parse_edgar_datetime_et(e.get('updated','')) for e in page_entries if e.get('updated')]
            if not dts: break
            page_max=max(dts); page_min=min(dts)
            for e,dt in zip(page_entries,dts):
                if _within(dt,start_et,end_et): entries.append(e)
            pages+=1; start_idx+=count_per_page
            if page_max < start_et: break
    elif kind=='atom':
        start_idx=0
        while pages < max_pages:
            xml=client.fetch_atom_page(start=start_idx, count=count_per_page)
            page_entries = parse_atom_entries(xml)
            if not page_entries: break
            dts=[parse_edgar_datetime_et(e.get('updated','')) for e in page_entries if e.get('updated')]
            if not dts: break
            page_max=max(dts); page_min=min(dts)
            for e,dt in zip(page_entries,dts):
                if _within(dt,start_et,end_et): entries.append(e)
            pages+=1; start_idx+=count_per_page
            if page_max < start_et: break
    return entries, pages, kind

def gather_for_window(client, cfg, ua, start_et, end_et):
    ft, _, _ = _time_window_collect(client, 'fulltext', start_et, end_et, cfg, ua)
    if ft: return ft, 'fulltext'
    html, _, _ = _time_window_collect(client, 'html', start_et, end_et, cfg, ua)
    if html: return html, 'html'
    atom, _, _ = _time_window_collect(client, 'atom', start_et, end_et, cfg, ua)
    return atom, 'atom' if atom else 'none'

def dedupe_entries(entries):
    seen=set(); out=[]
    for e in entries:
        k=(e.get('link',''), e.get('form',''), e.get('updated',''))
        if k in seen: continue
        seen.add(k); out.append(e)
    return out

def main():
    ensure_dir(DATA_DIR)
    stats={
        "version":"v23.1h",
        "started_utc":datetime.utcnow().isoformat()+"Z",
        "hit_boundary":False,
        "hit_extended_boundary":False,
        "auto_shifted_prev_bday":False,
        "weekend_tail_scanned":False,
        "source_primary":"none",
        "source_tail":"none",
        "entries_seen":0,
        "entries_kept":0,
        "last_oldest_et_scanned":None,
        "last_error":None
    }
    raw_path=os.path.join(DATA_DIR,"sec_filings_raw.json")
    snap_json_path=os.path.join(DATA_DIR,"sec_filings_snapshot.json")
    snap_csv_path=os.path.join(DATA_DIR,"sec_filings_snapshot.csv")
    stats_path=os.path.join(DATA_DIR,"sec_debug_stats.json")

    try:
        cfg=json.load(open(CONFIG_PATH,"r",encoding="utf-8"))
        ua=f"{cfg.get('user_agent_org','GrandMasterSEC-v23.1h')} | {cfg.get('contact_email','changeme@example.com')}"
        client=SECClient(ua, cfg.get('request_spacing_seconds',1.2), cfg.get('max_retries',5), cfg.get('retry_backoff_base',2.0), tuple(cfg.get('retry_jitter_range',[0.2,0.6])))
        enr=Enricher(ua, cfg.get('request_spacing_seconds',1.2))

        # Limits / budgets
        max_after_form=int(cfg.get('max_entries_after_form',400))
        top_doc_parse=int(cfg.get('top_doc_parse',120))
        per_cik_cap=int(cfg.get('per_cik_cap',3))
        soft_budget_sec=int(cfg.get('soft_budget_seconds',1800))  # 30 minutes
        t0=time.monotonic()

        p_start, p_end, t_start, t_end, guard = compute_windows()
        stats["hit_extended_boundary"] = bool(guard)
        print(f"[INFO] Primary window ET: {p_start.isoformat()} -> {p_end.isoformat()}  (weekend_guard={bool(guard)})")
        if t_start and t_end:
            print(f"[INFO] Weekend tail ET: {t_start.isoformat()} -> {t_end.isoformat()}")

        # Collect primary/tail with time-aware early stop
        primary_entries, source_primary = gather_for_window(client, cfg, ua, p_start, p_end)
        stats["source_primary"] = source_primary
        print(f"[INFO] Primary collected (in-window): {len(primary_entries)} from {source_primary}")

        tail_entries=[]
        if t_start and t_end:
            stats["weekend_tail_scanned"]=True
            tail_entries, source_tail = gather_for_window(client, cfg, ua, t_start, t_end)
            stats["source_tail"]=source_tail
            print(f"[INFO] Tail collected (in-window): {len(tail_entries)} from {source_tail}")

        entries = dedupe_entries(primary_entries + tail_entries)
        print(f"[INFO] Combined after dedupe: {len(entries)}")

        # If weekday window empty, auto-shift to prev bday windows
        if not entries and not guard:
            print("[WARN] No entries in weekday window — auto-shifting to previous business day window(s)…")
            stats["auto_shifted_prev_bday"]=True
            pb_start, pb_end, pb_tail_start, pb_tail_end = compute_prev_bday_windows()
            print(f"[INFO] Prev bday primary ET: {pb_start.isoformat()} -> {pb_end.isoformat()}")
            if pb_tail_start and pb_tail_end:
                print(f"[INFO] Prev bday tail ET: {pb_tail_start.isoformat()} -> {pb_tail_end.isoformat()}")
            primary_entries, source_primary = gather_for_window(client, cfg, ua, pb_start, pb_end)
            stats["source_primary"]=source_primary
            print(f"[INFO] Primary collected (prev bday, in-window): {len(primary_entries)} from {source_primary}")
            tail_entries=[]
            if pb_tail_start and pb_tail_end:
                tail_entries, source_tail = gather_for_window(client, cfg, ua, pb_tail_start, pb_tail_end)
                stats["source_tail"]=source_tail
                print(f"[INFO] Tail collected (prev bday, in-window): {len(tail_entries)} from {source_tail}")
            entries = dedupe_entries(primary_entries + tail_entries)
            print(f"[INFO] Combined after dedupe (shifted): {len(entries)}")

        # Quick form filter + per-CIK cap
        forms_ok=set(cfg.get("forms_supported",[]))
        by_cik=defaultdict(int); strict=[]
        for e in entries:
            if e.get("form","") not in forms_ok: continue
            cik=e.get("cik","")
            if per_cik_cap>0 and cik:
                if by_cik[cik] >= per_cik_cap: continue
                by_cik[cik]+=1
            strict.append(e)
        print(f"[INFO] After form filter + per-CIK cap: {len(strict)}")

        # Hard cap to keep under CI time budget
        if len(strict) > max_after_form:
            strict = strict[:max_after_form]
            print(f"[INFO] Truncated to top {max_after_form} for processing budget.")

        if strict:
            stats["hit_boundary"]=True
            # compute earliest updated among strict
            dts=[]
            for e in strict:
                try: dts.append(parse_edgar_datetime_et(e.get("updated","")))
                except: pass
            if dts:
                stats["last_oldest_et_scanned"]=min(d.isoformat() for d in dts)

        # Enrichment cache (by CIK)
        enr_cache={}
        def enr_lookup(cik):
            if not cik: return {"ticker":"", "company":"", "sic":""}
            if cik in enr_cache: return enr_cache[cik]
            info=enr.enrich(cik)
            enr_cache[cik]=info
            return info

        # First-pass scoring (no doc fetch)
        for e in strict:
            cik = e.get("cik","").zfill(10) if e.get("cik") else ""
            info = enr_lookup(cik)
            e["ticker"]=info.get("ticker","") or e.get("ticker_hint","")
            e["company"]=e.get("company") or info.get("company","")
            e["sic"]=info.get("sic","")
            e["score"]=score_entry(e, cfg)

        # Sort and second-pass doc parsing for top N (8-K / 4 only)
        strict.sort(key=lambda x:(x.get("score",0), x.get("updated","")), reverse=True)
        def minimal_doc(entry):
            link=entry.get("link",""); 
            if not link: return ""
            try:
                html=client._req(link).text
            except Exception: return ""
            from bs4 import BeautifulSoup
            soup=BeautifulSoup(html,"lxml")
            doc_url=""
            for a in soup.select("table.tableFile a[href]"):
                href=a.get("href","")
                if href.lower().endswith((".htm",".html",".txt",".xml")):
                    doc_url="https://www.sec.gov"+href if href.startswith("/") else href; break
            if not doc_url: return ""
            try:
                return client._req(doc_url).text[:5000]
            except Exception: return ""

        refined=0
        for e in strict[:top_doc_parse]:
            if time.monotonic()-t0 > soft_budget_sec: 
                print("[WARN] Soft time budget reached; skipping further doc parsing."); break
            if e.get("form") in ("8-K","4"):
                excerpt = minimal_doc(e)
                if excerpt:
                    if e["form"]=="8-K": e["eightk_items"]=extract_eightk_items(excerpt)
                    if e["form"]=="4": e["form4_codes"]=extract_form4_codes(excerpt)
                    e["doc_text_excerpt"]=excerpt
                    e["score"]=score_entry(e, cfg)
                    refined+=1
        print(f"[INFO] Doc-parsed refined entries: {refined}")

        # Final sort
        strict.sort(key=lambda x:(x.get("score",0), x.get("updated","")), reverse=True)

        # Output files
        json.dump(strict, open(raw_path,"w",encoding="utf-8"), indent=2, ensure_ascii=False)
        snap_rows=[{
            "filing_datetime": e.get("updated") or "",
            "form": e.get("form",""),
            "company": e.get("company",""),
            "ticker": e.get("ticker",""),
            "cik": e.get("cik",""),
            "industry": "",
            "sic": e.get("sic",""),
            "title": e.get("title",""),
            "score": e.get("score",0),
            "link": e.get("link","")
        } for e in strict]
        json.dump(snap_rows, open(snap_json_path,"w",encoding="utf-8"), indent=2, ensure_ascii=False)
        with open(snap_csv_path,"w",newline="",encoding="utf-8") as f:
            cols=["filing_datetime","form","company","ticker","cik","industry","sic","title","score","link"]
            w=csv.DictWriter(f, fieldnames=cols); w.writeheader()
            for r in snap_rows: w.writerow(r)

        stats["entries_seen"]=len(strict); stats["entries_kept"]=len(strict); stats["finished_utc"]=datetime.utcnow().isoformat()+"Z"
        json.dump(stats, open(stats_path,"w",encoding="utf-8"), indent=2)

        # Upload
        h_url=cfg.get("hostinger_upload_url","").strip(); secret=cfg.get("hostinger_secret","").strip()
        if h_url and secret and h_url.startswith("http"):
            for p in (raw_path, snap_json_path, snap_csv_path, stats_path):
                try: post_file(h_url, secret, p, "/public_html/data")
                except Exception as ue: print("[UPLOAD WARN]", ue)

        passed = bool(stats.get("hit_boundary")) and stats.get("entries_kept",0) > 0
        print("DIAGNOSTIC:", "PASS" if passed else "FAIL", json.dumps({"hit_boundary":stats.get("hit_boundary"),"entries_kept":stats.get("entries_kept",0)}))
        if os.environ.get("CI","")=="true" and not passed: sys.exit(1)

    except Exception as ex:
        stats["last_error"]=str(ex); stats["finished_utc"]=datetime.utcnow().isoformat()+"Z"
        json.dump(stats, open(stats_path,"w",encoding="utf-8"), indent=2)
        raise

if __name__=="__main__":
    main()
