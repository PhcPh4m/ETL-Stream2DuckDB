# src/etl_api.py
import os
import time
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- Logging đơn giản ---
logging.basicConfig(
    level=os.getenv("ETL_LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s",
)

OPENWEATHER_URL = "https://api.openweathermap.org/data/2.5/weather"


def _requests_session_with_retries(
    total_retries: int = 5,
    backoff_factor: float = 0.5,
    status_forcelist: tuple = (429, 500, 502, 503, 504),
) -> requests.Session:
    """Tạo session requests có retry/backoff cho các lỗi tạm thời và 429."""
    session = requests.Session()
    retries = Retry(
        total=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=frozenset(["GET", "POST"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def call_api_with_retry(
    params: Dict[str, Any],
    api_key: Optional[str] = None,
    max_retries: int = 5,
    backoff_factor: float = 1.0,
    timeout: int = 10,
) -> Dict[str, Any]:
    """Gọi OpenWeather API với retry/backoff và xử lý throttle (429)."""
    if api_key:
        params = dict(params)
        params.setdefault("appid", api_key)

    session = _requests_session_with_retries(total_retries=max_retries, backoff_factor=backoff_factor)

    attempt = 0
    while True:
        attempt += 1
        try:
            logging.info("API request attempt %d params=%s", attempt, {k: v for k, v in params.items() if k != "appid"})
            resp = session.get(OPENWEATHER_URL, params=params, timeout=timeout)
            status = resp.status_code

            # Throttle handling: nếu 429, tôn trọng Retry-After nếu có
            if status == 429:
                retry_after = resp.headers.get("Retry-After")
                wait = float(retry_after) if retry_after and retry_after.isdigit() else backoff_factor * attempt
                logging.warning("Received 429 Too Many Requests. Waiting %.1fs before retry.", wait)
                time.sleep(wait)
            elif 200 <= status < 300:
                logging.info("API call successful (status %d).", status)
                return resp.json()
            else:
                # lỗi tạm thời khác: log và để session Retry xử lý
                logging.warning("API returned status %d: %s", status, resp.text[:200])
        except requests.RequestException as e:
            logging.warning("RequestException on attempt %d: %s", attempt, e)

        if attempt >= max_retries:
            msg = f"API call failed after {attempt} attempts (last status {locals().get('status', 'N/A')})."
            logging.error(msg)
            raise RuntimeError(msg)

        # exponential backoff jittered
        sleep_for = backoff_factor * (2 ** (attempt - 1))
        jitter = sleep_for * 0.1
        sleep_time = sleep_for + (jitter * (0.5 - os.urandom(1)[0] / 255.0))
        logging.info("Sleeping %.2fs before next attempt.", sleep_time)
        time.sleep(max(0.1, sleep_time))


def save_raw_jsonl(
    data: Dict[str, Any],
    out_dir: str = "data/raw",
    filename_date: Optional[datetime] = None,
) -> str:
    """Append một record JSONL vào file data/raw/YYYYMMDD.jsonl.

    Record có cấu trúc:
    {
      "ingest_ts": "<ISO UTC>",
      "source": "openweather",
      "payload": { ... original API json ... }
    }
    """
    filename_date = filename_date or datetime.now(timezone.utc)
    out_dir_path = Path(out_dir)
    out_dir_path.mkdir(parents=True, exist_ok=True)

    fname = filename_date.strftime("%Y%m%d") + ".jsonl"
    out_path = out_dir_path / fname

    record = {
        "ingest_ts": datetime.now(timezone.utc).isoformat(),
        "source": "openweather",
        "payload": data,
    }

    with out_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")

    logging.info("Saved raw JSONL to %s", str(out_path))
    return str(out_path)


def ingest_weather(
    city: str,
    api_key: Optional[str] = None,
    out_dir: str = "data/raw",
    max_retries: int = 5,
    backoff_factor: float = 1.0,
) -> str:
    """High-level: gọi API và lưu raw JSONL. Trả về đường dẫn file đã ghi."""
    api_key = api_key or os.getenv("OPENWEATHER_API_KEY")
    if not api_key:
        raise RuntimeError("OPENWEATHER_API_KEY not set (pass api_key or set env var)")

    params = {"q": city, "units": "metric"}
    payload = call_api_with_retry(params=params, api_key=api_key, max_retries=max_retries, backoff_factor=backoff_factor)
    return save_raw_jsonl(payload, out_dir=out_dir)


# CLI support khi chạy trực tiếp
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Ingest weather data and save raw JSONL")
    parser.add_argument("--city", default="Ho Chi Minh", help="City name for OpenWeather")
    parser.add_argument("--out-dir", default="data/raw", help="Output directory for JSONL")
    parser.add_argument("--retries", type=int, default=5, help="Max retries for API calls")
    parser.add_argument("--backoff", type=float, default=1.0, help="Backoff factor (seconds base)")
    parser.add_argument("--api-key", default=None, help="OpenWeather API key (or set OPENWEATHER_API_KEY)")
    args = parser.parse_args()

    path = ingest_weather(city=args.city, api_key=args.api_key, out_dir=args.out_dir, max_retries=args.retries, backoff_factor=args.backoff)
    print("Saved:", path)
