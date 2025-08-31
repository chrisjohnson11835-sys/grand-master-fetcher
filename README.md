# Grand Master SEC — v23.1 (Full-Text Forced + Aggressive Crawl + Diagnostics)

**What’s new vs v23**
- `force_fulltext` **ON by default** — skips Atom & HTML and goes straight to EDGAR Full-Text.
- Larger crawl window: `fulltext_page_size: 400`, `max_pages: 80` (handles heavy filing days).
- Keeps v23 diagnostics, bans, scoring, Hostinger deployment, and self-healing workflow.

**Run locally**
```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python scripts/sec/grandmaster_sec_v23.py
```
