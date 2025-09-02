# GrandMaster SEC – ONE-ATTEMPT FIX (Deterministic Daily Index)

**Purpose (as per your final ask):**
Scan the **previous working day's** filings for **8-K, 6-K, 10-Q, 10-K, Form 3, Form 4, SC 13D/G (+/A)** strictly between:
- **Prev working day 09:30 ET → Next calendar day 09:00 ET**

**Outputs (to `data/`):**
- `sec_filings_snapshot.json` — compact list (company, ticker, industry, form, accepted_et, cik)
- `sec_filings_snapshot.csv` — CSV of the above
- `sec_filings_raw.json` — raw records (per filing)
- `sec_debug_stats.json` — run stats (hit_boundary, entries_seen, last_oldest_et_scanned, source_primary, ...)

**Sources & Guarantees:**
- Deterministic via **Daily Index** (`master.YYYYMMDD.idx`) for prev working day and next day (00:00-09:00).
- Exact acceptance times via each filing’s **header .txt** (`ACCEPTANCE-DATETIME`), converted to **America/New_York**.
- Enrichment via **Submissions API** for **ticker** and **SIC/Industry**.
- **Bans** applied by SIC range + keywords (alcohol, tobacco, gambling, weapons, adult entertainment, financial services/banks/insurers/payday).

**Fair-access & Compliance:**
- UA with contact email (edit in `config/config.json`).
- Throttling ~0.7 req/s (configurable).
- Retry with backoff on 429/503.
- Respect `Retry-After`.

---

## Install & Run (module mode)
```bash
pip install -r requirements.txt
python -u -m scripts.sec.grandmaster_sec_v23
```

## GitHub Actions (optional)
Workflow included: `.github/workflows/grandmaster_sec_0930.yml`
- Name: `GrandMaster SEC | 09:30→09:00 Previous-Day Scan (Deterministic)`
- Triggers: cron near **09:35 ET** and `workflow_dispatch`.

## Hostinger Upload (optional)
Fill in `hostinger_upload_url` and `hostinger_secret` in `config/config.json`. Uploader will POST files in `data/`.

---

## Notes
- This fix prioritizes **correct window coverage** and **form set**. It scans **prev business day 09:30 → next day 09:00 ET**.
- If you want to re-enable the multi-source chain (EFTS → HTML → Atom), this skeleton allows extension, but **Daily Index is sufficient** to satisfy the acceptance now.
