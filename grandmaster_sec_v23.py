#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, sys, json, csv
from datetime import datetime
from scripts.util.time_utils import compute_et_window, parse_edgar_datetime_et
from scripts.util.fetchers import SECClient
from scripts.util.fulltext import fetch_fulltext_window
from scripts.util.enrichment import Enricher
from scripts.util.bans import is_banned
from scripts.util.scoring import score_entry, extract_eightk_items, extract_form4_codes
from scripts.util.uploader import post_file

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "config", "config.json")

def ensure_dir(p): os.makedirs(p, exist_ok=True)

def main():
    ensure_dir(DATA_DIR)
    stats={"version":"v23.1b","started_utc":datetime.utcnow().isoformat()+"Z","hit_boundary":False,"fallback_used":False,"fulltext_used":False,"entries_seen":0,"entries_kept":0,"last_oldest_et_scanned":None,"last_error":None}
    raw_path=os.path.join(DATA_DIR,"sec_filings_raw.json")
    snap_json_path=os.path.join(DATA_DIR,"sec_filings_snapshot.json")
    snap_csv_path=os.path.join(DATA_DIR,"sec_filings_snapshot.csv")
    stats_path=os.path.join(DATA_DIR,"sec_debug_stats.json")

    try:
        cfg=json.load(open(CONFIG_PATH,"r",encoding="utf-8"))
        ua=f"{cfg.get('user_agent_org','GrandMasterSEC-v23.1b')} | {cfg.get('contact_email','changeme@example.com')}"
        client=SECClient(ua, cfg.get('request_spacing_seconds',1.5), cfg.get('max_retries',5), cfg.get('retry_backoff_base',2.0), tuple(cfg.get('retry_jitter_range',[0.2,0.6])))
        enr=Enricher(ua, cfg.get('request_spacing_seconds',1.5))

        start_et, end_et = compute_et_window()
        ft_entries = fetch_fulltext_window(client.ua, start_et, end_et, cfg.get('forms_supported',[]), page_size=int(cfg.get('fulltext_page_size',400)), max_pages=30)
        stats["fulltext_used"]=True

        def within_window(et_dt, start, end): return (et_dt>=start) and (et_dt<end)
        strict=[]
        for e in ft_entries:
            try: dt_et=parse_edgar_datetime_et(e.get("updated",""))
            except: continue
            if within_window(dt_et, start_et, end_et):
                e["updated_et"]=dt_et.isoformat(); strict.append(e)
        if strict: stats["hit_boundary"]=True; stats["last_oldest_et_scanned"]=min([x["updated_et"] for x in strict])

        forms_ok=set(cfg.get("forms_supported",[]))
        strict=[e for e in strict if e.get("form","") in forms_ok]

        def minimal_doc(entry):
            link=entry.get("link","")
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
                doc=client._req(doc_url).text
            except Exception: return ""
            if entry.get("form")=="8-K": entry["eightk_items"]=extract_eightk_items(doc)
            if entry.get("form")=="4": entry["form4_codes"]=extract_form4_codes(doc)
            return doc[:5000]

        enriched=[]
        for e in strict:
            cik = e.get("cik","").zfill(10) if e.get("cik") else ""
            info = enr.enrich(cik) if cik else {"ticker":"", "company":"", "sic":""}
            e["ticker"]=info.get("ticker","") or e.get("ticker_hint","")
            e["company"]=e.get("company") or info.get("company","")
            e["sic"]=info.get("sic","")
            excerpt = minimal_doc(e) if e.get("form") in ("8-K","4") else ""
            e["doc_text_excerpt"]=excerpt
            enriched.append(e)

        kept=[]
        for e in enriched:
            if is_banned(e, cfg): e["banned"]=True; continue
            e["banned"]=False; e["score"]=score_entry(e, cfg); kept.append(e)
        kept.sort(key=lambda x:(x.get("score",0), x.get("updated_et","")), reverse=True)

        json.dump(kept, open(raw_path,"w",encoding="utf-8"), indent=2, ensure_ascii=False)

        snap_rows=[{
            "filing_datetime": e.get("updated_et") or e.get("updated",""),
            "form": e.get("form",""),
            "company": e.get("company",""),
            "ticker": e.get("ticker",""),
            "cik": e.get("cik",""),
            "industry": "",
            "sic": e.get("sic",""),
            "title": e.get("title",""),
            "score": e.get("score",0),
            "link": e.get("link","")
        } for e in kept]
        json.dump(snap_rows, open(snap_json_path,"w",encoding="utf-8"), indent=2, ensure_ascii=False)
        import csv
        with open(snap_csv_path,"w",newline="",encoding="utf-8") as f:
            cols=["filing_datetime","form","company","ticker","cik","industry","sic","title","score","link"]
            w=csv.DictWriter(f, fieldnames=cols); w.writeheader()
            for r in snap_rows: w.writerow(r)

        stats["entries_seen"]=len(strict); stats["entries_kept"]=len(kept); stats["finished_utc"]=datetime.utcnow().isoformat()+"Z"
        json.dump(stats, open(stats_path,"w",encoding="utf-8"), indent=2)

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
