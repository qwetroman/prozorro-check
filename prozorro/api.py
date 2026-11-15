"""Клієнт API Worker (Cloudflare). Ключ виводиться з TG_BOT_TOKEN так само, як у Worker."""
from __future__ import annotations

import hashlib
import hmac
import os
import time

import requests

from .common import ENV, HERE, log

URL_FILE = os.path.join(HERE, "worker", "url.txt")   # записує workflow worker-deploy


def base_url():
    url = ENV("WORKER_URL")
    if not url and os.path.exists(URL_FILE):
        url = open(URL_FILE, encoding="utf-8").read().strip()
    if not url:
        raise RuntimeError("невідома адреса Worker: немає WORKER_URL і worker/url.txt (спершу запустіть worker-deploy)")
    return url.rstrip("/")


def api_key():
    token = ENV("TG_BOT_TOKEN")
    if not token:
        raise RuntimeError("немає TG_BOT_TOKEN")
    return hmac.new(token.encode(), b"prozorro-api", hashlib.sha256).hexdigest()


def call(path, body=None, timeout=120):
    """GET (body=None) або POST до /api/<path>; повторює при збоях мережі та 5xx."""
    url = f"{base_url()}/api/{path}"
    headers = {"Authorization": f"Bearer {api_key()}"}
    for attempt in range(4):
        try:
            r = (requests.post(url, json=body, headers=headers, timeout=timeout) if body is not None
                 else requests.get(url, headers=headers, timeout=timeout))
            if r.status_code < 500:
                j = r.json()
                if not j.get("ok"):
                    raise RuntimeError(f"Worker /api/{path}: {r.status_code} {j.get('error')}")
                return j
            err = f"{r.status_code} {r.text[:200]}"
        except requests.RequestException as e:
            err = str(e)
        log(f"Worker /api/{path}: {err}, повтор {attempt + 1}")
        time.sleep(5 * (attempt + 1))
    raise RuntimeError(f"Worker /api/{path} недоступний: {err}")


def chunks(items, n=40):
    for i in range(0, len(items), n):
        yield items[i:i + n]
