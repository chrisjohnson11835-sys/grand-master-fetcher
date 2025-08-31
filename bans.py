def is_banned(entry, cfg):
    sic=str(entry.get("sic","") or "")
    if any(sic.startswith(p) for p in cfg.get("banned_sic_prefixes",[])): return True
    blob = " ".join([entry.get("company",""), entry.get("summary",""), entry.get("title","")]).lower()
    for kw in cfg.get("banned_keywords",[]): 
        if kw.lower() in blob: return True
    return False
