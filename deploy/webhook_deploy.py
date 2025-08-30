# deploy/webhook_deploy.py (v21)
import os, json, requests

def deploy_files(cfg, paths):
    url = cfg.get("webhook_url")
    secret = cfg.get("webhook_secret")
    if not url or not secret:
        print("[deploy] Missing webhook_url or webhook_secret; skipping.")
        return
    for p in paths:
        fname = os.path.basename(p)
        files = {"file": (fname, open(p, "rb"), "application/json")}
        data = {"secret": secret, "filename": fname}
        try:
            r = requests.post(url, data=data, files=files, timeout=30)
            print(f"[deploy] POST {fname} -> {r.status_code} {r.text[:200]}")
        except Exception as e:
            print(f"[deploy] Error sending {fname}: {e}")
