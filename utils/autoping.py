import threading
import requests
import time
import os

def autoping():
    url = os.getenv("PING_URL", "")
    if not url:
        return
    while True:
        try:
            requests.get(url + "/ping", timeout=10)
        except Exception:
            pass
        time.sleep(600)  # Каждые 10 минут

def start_ping():
    threading.Thread(target=autoping, daemon=True).start()

