# src/etl_api.py
import requests
import time
import json
from pathlib import Path
from datetime import datetime

OPENWEATHER_URL = "https://api.openweathermap.org/data/2.5/weather"

def fetch_weather(city, api_key, retries=3, backoff=1):
    params = {"q": city, "appid": api_key, "units": "metric"}
    for i in range(retries):
        r = requests.get(OPENWEATHER_URL, params=params, timeout=10)
        if r.status_code == 200:
            return r.json()
        time.sleep(backoff * (i + 1))
    r.raise_for_status()

def save_raw_jsonl(record, out_path="data/raw/weather.jsonl"):
    p = Path(out_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")

if __name__ == "__main__":
    import os
    api_key = os.getenv("OPENWEATHER_API_KEY", "")
    if not api_key:
        raise SystemExit("Set OPENWEATHER_API_KEY environment variable")
    city = "Ho Chi Minh"
    rec = fetch_weather(city, api_key)
    rec["ingest_ts"] = datetime.utcnow().isoformat()
    out = f"data/raw/weather_{datetime.utcnow().strftime('%Y%m%d')}.jsonl"
    save_raw_jsonl(rec, out)
    print("Saved", out)
