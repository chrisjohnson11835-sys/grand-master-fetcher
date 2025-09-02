# -*- coding: utf-8 -*-
import requests, os
def maybe_upload(files, config):
    url = (config or {}).get("hostinger_upload_url","")
    secret = (config or {}).get("hostinger_secret","")
    if not url or not secret: return {"uploaded": False, "reason":"hostinger not configured"}
    data = {"secret": secret}; fs = {}
    for path in files:
        if os.path.exists(path): fs[os.path.basename(path)] = open(path, "rb")
    try:
        r = requests.post(url, data=data, files=fs, timeout=30)
        return {"uploaded": r.status_code==200, "status": r.status_code, "text": r.text[:400]}
    except Exception as e:
        return {"uploaded": False, "error": str(e)}
    finally:
        for f in fs.values(): f.close()
