# src/transform.py
import json
from pathlib import Path
from datetime import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def read_jsonl(path):
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"{path} not found")
    records = []
    with p.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            records.append(json.loads(line))
    return records

def flatten_weather_record(rec):
    # safe getters with defaults
    out = {}
    out["city_id"] = rec.get("id")
    out["city_name"] = rec.get("name")
    weather = rec.get("weather") or []
    if weather and isinstance(weather, list):
        out["weather_main"] = weather[0].get("main") if weather[0] else None
        out["weather_description"] = weather[0].get("description") if weather[0] else None
    else:
        out["weather_main"] = None
        out["weather_description"] = None
    main = rec.get("main") or {}
    out["temp_c"] = main.get("temp")
    out["feels_like_c"] = main.get("feels_like")
    out["humidity"] = main.get("humidity")
    wind = rec.get("wind") or {}
    out["wind_speed_m_s"] = wind.get("speed")
    out["dt_api"] = rec.get("dt")  # epoch seconds from API
    out["ingest_ts"] = rec.get("ingest_ts")  # should be ISO string added at ingest
    # derive date (YYYY-MM-DD) from ingest_ts if present, else from dt_api
    date_val = None
    if out["ingest_ts"]:
        try:
            date_val = datetime.fromisoformat(out["ingest_ts"]).date().isoformat()
        except Exception:
            date_val = None
    if not date_val and out["dt_api"]:
        try:
            date_val = datetime.utcfromtimestamp(int(out["dt_api"])).date().isoformat()
        except Exception:
            date_val = None
    out["date"] = date_val
    return out

def normalize_jsonl_to_df(jsonl_path):
    records = read_jsonl(jsonl_path)
    flat = [flatten_weather_record(r) for r in records]
    df = pd.DataFrame(flat)
    # ensure ingest_ts is datetime
    if "ingest_ts" in df.columns:
        df["ingest_ts"] = pd.to_datetime(df["ingest_ts"], errors="coerce")
    return df

def write_parquet_partitioned(df, out_dir="data/processed", partition_col="date", compression="snappy"):
    p = Path(out_dir)
    p.mkdir(parents=True, exist_ok=True)
    # drop rows without partition_col
    if partition_col not in df.columns:
        raise KeyError(f"Partition column {partition_col} not in dataframe")
    df = df.copy()
    df = df[df[partition_col].notna()]
    # write one parquet file per partition (simple approach)
    for date_val, group in df.groupby(partition_col):
        part_dir = p / f"{partition_col}={date_val}"
        part_dir.mkdir(parents=True, exist_ok=True)
        out_file = part_dir / f"part-{date_val}.parquet"
        # use pyarrow via pandas to_parquet
        group.to_parquet(out_file, index=False, compression=compression)
    return str(p)

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python src/transform.py <path-to-jsonl>")
        sys.exit(1)
    jsonl = sys.argv[1]
    df = normalize_jsonl_to_df(jsonl)
    out = write_parquet_partitioned(df)
    print("Wrote partitioned parquet to", out)
