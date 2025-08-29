# deploy/webhook_deploy.py
import os, pathlib, requests
from typing import List

def deploy_files(cfg, file_paths: List[str]):
    """
    Send each output file to your Hostinger site via POST.
    Env vars override settings.json:
      - WEBHOOK_URL
      - WEBHOOK_SECRET
    """
    url = os.environ.get("WEBHOOK_URL") or cfg.get("webhook_url")
    secret = os.environ.get("WEBHOOK_SECRET") or cfg.get("webhook_secret")
    allowed = set(cfg.get("hostinger_allowed_filenames", []))

    if not url or not secret:
        print("Skip deploy: missing URL or secret")
        return

    for fp in file_paths:
        name = pathlib.Path(fp).name
        if name not in allowed:
            print(f"Skip {name}: not in allowed list")
            continue

        with open(fp, "rb") as f:
            files = {"file": (name, f, "application/octet-stream")}
            data = {"secret": secret, "filename": name}
            print(f"POST -> {url} [{name}]")
            try:
                r = requests.post(url, data=data, files=files, timeout=30)
                print(f"{name}: {r.status_code} {r.text[:200]}")
            except Exception as e:
                print(f"{name}: deploy error {e}")
