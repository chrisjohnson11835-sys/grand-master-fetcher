# sec_sources.py (v22 dual-source)
import re, html, datetime
from html.parser import HTMLParser

def _normalize_whitespace(s):
    return re.sub(r'\s+', ' ', (s or '').strip())

class _GetCurrentHTMLParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.in_row = False
        self.col = 0
        self.current = {}
        self.rows = []
        self.in_a = False
        self.a_href = None
        self.buffer = ""

    def handle_starttag(self, tag, attrs):
        if tag == "tr":
            self.in_row = True
            self.col = 0
            self.current = {}
        elif self.in_row and tag == "td":
            self.col += 1
            self.buffer = ""
        elif self.in_row and tag == "a":
            self.in_a = True
            for k,v in attrs:
                if k.lower()=="href":
                    self.a_href = v

    def handle_endtag(self, tag):
        if tag == "a":
            self.in_a = False
        elif tag == "td" and self.in_row:
            text = _normalize_whitespace(html.unescape(self.buffer))
            if self.col == 1:
                self.current["title"] = text
            elif self.col == 2:
                self.current["form_text"] = text
            elif self.col == 3:
                self.current["cik_text"] = text
            elif self.col == 4:
                self.current["filed_text"] = text
            elif self.col >= 5 and self.a_href and ("Archives/edgar/data" in self.a_href or "cgi-bin/browse-edgar" in self.a_href):
                self.current["link"] = self.a_href
            self.buffer = ""
        elif tag == "tr" and self.in_row:
            self.in_row = False
            if self.current:
                self.rows.append(self.current)
            self.current = {}

    def handle_data(self, data):
        if self.in_row:
            self.buffer += data

def fetch_atom_page(feedparser, text):
    feed = feedparser.parse(text)
    entries = feed.get("entries",[]) or []
    norm = []
    for e in entries:
        norm.append({
            "title": e.get("title",""),
            "summary": e.get("summary","") or (e.get("content",[{"value":""}])[0].get("value","") if e.get("content") else ""),
            "link": e.get("link",""),
            "updated": e.get("updated") or e.get("published"),
            "tags": e.get("tags") or e.get("categories"),
            "category": e.get("category"),
            "updated_parsed": e.get("updated_parsed") or e.get("published_parsed")
        })
    return norm

def fetch_html_page(text):
    p = _GetCurrentHTMLParser()
    p.feed(text)
    out = []
    for r in p.rows:
        title = r.get("title","")
        link = r.get("link","")
        form_text = r.get("form_text","")
        filed_text = r.get("filed_text","")
        dt_iso = None
        try:
            m = re.search(r'(\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2})', filed_text)
            if m:
                dt_iso = m.group(1)
        except Exception:
            pass
        out.append({
            "title": title,
            "summary": "",
            "link": link,
            "updated": dt_iso,
            "tags": [{"term": form_text}] if form_text else [],
            "category": {"term": form_text} if form_text else None,
            "updated_parsed": None
        })
    return out
