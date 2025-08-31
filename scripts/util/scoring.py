import re
EIGHTK_ITEM_RE = re.compile(r"Item\s+(\d+\.\d+)", re.I)
def extract_eightk_items(text): return list(sorted(set(EIGHTK_ITEM_RE.findall(text or ""))))
def extract_form4_codes(text):
    codes=set()
    for m in re.finditer(r"Transaction\s+Code\s*[:\-]?\s*([A-Z])", text or "", re.I): codes.add(m.group(1).upper())
    for m in re.finditer(r"\bCode[:\-\s]+([A-Z])\b", text or "", re.I): codes.add(m.group(1).upper())
    return list(sorted(codes))
def score_entry(e,cfg):
    w=cfg.get("weights",{}); base=w.get("base",{}); s=base.get(e.get("form",""),0)
    if e.get("form","")=="8-K":
        for it in e.get("eightk_items",[]): s+=w.get("eightk_items",{}).get(it,0)
    if e.get("form","")=="4":
        for c in e.get("form4_codes",[]): s+=w.get("form4",{}).get(c,0)
    lb=(" ".join([e.get("title",""), e.get("summary",""), e.get("doc_text_excerpt","")])).lower()
    for kw in cfg.get("positive_keywords",[]): 
        if kw.lower() in lb: s+=w.get("keywords",{}).get("positive",0)
    for kw in cfg.get("negative_keywords",[]): 
        if kw.lower() in lb: s+=w.get("keywords",{}).get("negative",0)
    return s
