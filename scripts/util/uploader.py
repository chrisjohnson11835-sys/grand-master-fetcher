import requests, os
def post_file(url, secret, local_path, remote_dir="/public_html/data"):
    with open(local_path,"rb") as f:
        files={"file":(os.path.basename(local_path), f, "application/octet-stream")}
        data={"secret":secret,"target_dir":remote_dir}
        r=requests.post(url, data=data, files=files, timeout=60); r.raise_for_status(); return r.text
