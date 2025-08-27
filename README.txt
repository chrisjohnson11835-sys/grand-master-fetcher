# Off-host Fetcher (GitHub Actions)
This fetcher runs on GitHub Actions (not Hostinger), builds `step6_full.json` and `step7_overlay.json`, then SFTPs them to your Hostinger `public_html/data/`.

## Setup
1. Create a **private** repo, add these files.
2. In repo Settings → Secrets and variables → Actions, add:
   - `FETCH_UA` = `GrandMasterFetcher/1.0 (contact: you@example.com)`
   - `SFTP_HOST` = your Hostinger SFTP host
   - `SFTP_PORT` = `22`
   - `SFTP_USER` = your SFTP username
   - `SFTP_PASS` = your SFTP password
   - `SFTP_DIR`  = `/home/USER/domains/YOURDOMAIN/public_html/data/`
3. Commit. The workflow runs every 30 minutes and on-demand.
4. Your Hostinger page `grand_master_script.php` will update from these JSONs.
