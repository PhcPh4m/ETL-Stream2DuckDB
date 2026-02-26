# src/transform.py
import json
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

RAW_DIR = Path("data/raw")
PROCESSED_DIR = Path("data/processed")
SOURCE = "openweather"


def _read_jsonl_file(path: Path) -> List[Dict[str, Any]]:
    records = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                # skip malformed lines but continue
                continue
    return records


def collect_raw_files(raw_dir: Path = RAW_DIR, date: Optional[str] = None) -> List[Path]:
    """Return list of raw jsonl files. If date provided (YYYYMMDD) only that file."""
    if date:
        candidate = raw_dir / f"{date}.jsonl"
        return [candidate] if candidate.exists() else []
    return sorted(raw_dir.glob("*.jsonl"))


def normalize_records(records: List[Dict[str, Any]]) -> pd.DataFrame:
    """Flatten raw records into a normalized DataFrame using pd.json_normalize.

    Each raw record is expected to have top-level keys:
      - ingest_ts (ISO string)
      - source
      - payload (the API JSON)
    We flatten payload and attach ingest metadata.
    """
    rows = []
    for rec in records:
        ingest_ts = rec.get("ingest_ts")
        source = rec.get("source", SOURCE)
        payload = rec.get("payload", {}) if isinstance(rec.get("payload", {}), dict) else {}

        # flatten payload; use '_' as separator for nested keys
        if payload:
            flat = pd.json_normalize(payload, sep="_")
            # if json_normalize returns multiple rows (arrays), keep first row as fallback
            row = flat.iloc[0].to_dict() if len(flat) > 0 else {}
        else:
            row = {}

        # attach metadata
        row["ingest_ts"] = ingest_ts
        row["source"] = source
        # keep original top-level fields if present (e.g., name/id)
        for k in ("id", "name"):
            if k in rec and k not in row:
                row[k] = rec.get(k)
        rows.append(row)

    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    return df


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Basic cleaning:
    - drop exact duplicates
    - parse timestamps (ingest_ts, dt -> obs_ts)
    - derive date column (YYYY-MM-DD)
    - cast common numeric fields
    - fill basic missing values
    """
    if df.empty:
        return df

    # drop exact duplicates
    df = df.drop_duplicates().reset_index(drop=True)

    # parse ingest_ts (ISO) to timezone-aware datetime
    if "ingest_ts" in df.columns:
        df["ingest_ts"] = pd.to_datetime(df["ingest_ts"], utc=True, errors="coerce")

    # parse observation timestamp from API 'dt' (epoch seconds) if present
    if "dt" in df.columns:
        df["obs_ts"] = pd.to_datetime(df["dt"], unit="s", utc=True, errors="coerce")
        # remove raw dt to avoid confusion
        df = df.drop(columns=["dt"], errors="ignore")

    # derive date: prefer ingest_ts, fallback to obs_ts, else today's UTC date
    if "ingest_ts" in df.columns and df["ingest_ts"].notna().any():
        df["date"] = df["ingest_ts"].dt.date.astype("string")
    elif "obs_ts" in df.columns and df["obs_ts"].notna().any():
        df["date"] = df["obs_ts"].dt.date.astype("string")
    else:
        df["date"] = datetime.now(timezone.utc).date().isoformat()

    # common numeric casts (normalized names from OpenWeather)
    numeric_map = {
        "main_temp": ["main_temp", "main_temp_c", "temp", "main_temp"],  # try common variants
        "main_humidity": ["main_humidity", "humidity"],
        "wind_speed": ["wind_speed", "wind_speed_m_s", "wind_speed_ms", "wind_speed"],
        "clouds_all": ["clouds_all", "clouds_all"],
        "main_pressure": ["main_pressure", "pressure"],
    }
    for target, candidates in numeric_map.items():
        for c in candidates:
            if c in df.columns:
                df[target] = pd.to_numeric(df[c], errors="coerce")
                break

    # ensure city name exists
    if "name" in df.columns:
        df["name"] = df["name"].fillna("unknown")
    else:
        df["name"] = "unknown"

    # reorder columns to put key fields first if present
    cols = list(df.columns)
    preferred = ["name", "id", "ingest_ts", "obs_ts", "date", "main_temp", "main_humidity", "wind_speed"]
    new_order = [c for c in preferred if c in cols] + [c for c in cols if c not in preferred]
    df = df.loc[:, new_order]

    return df


def save_partitioned_parquet(df: pd.DataFrame, out_base: Path = PROCESSED_DIR, compression: str = "snappy") -> List[str]:
    """Write DataFrame partitioned by 'date' into out_base/date=YYYY-MM-DD/part-*.parquet.
    Returns list of written file paths.
    """
    written = []
    if df.empty:
        return written

    if "date" not in df.columns:
        df["date"] = datetime.now(timezone.utc).date().isoformat()

    # write one parquet file per partition (simple, deterministic)
    for date_val, group in df.groupby("date"):
        part_dir = out_base / f"date={date_val}"
        part_dir.mkdir(parents=True, exist_ok=True)
        # create pyarrow table for stable typing
        table = pa.Table.from_pandas(group.reset_index(drop=True))
        out_path = part_dir / f"part-{datetime.now(timezone.utc).strftime('%H%M%S')}.parquet"
        pq.write_table(table, out_path, compression=compression)
        written.append(str(out_path))
    return written


def transform_raw(date: Optional[str] = None, raw_dir: Path = RAW_DIR, out_dir: Path = PROCESSED_DIR) -> List[str]:
    """High-level: read raw JSONL(s), normalize, clean, and write partitioned Parquet.
    - date: optional YYYYMMDD to process a single raw file
    Returns list of written parquet file paths.
    """
    files = collect_raw_files(raw_dir=raw_dir, date=date)
    if not files:
        print(f"No raw files found in {raw_dir} for date={date}")
        return []

    all_records = []
    for f in files:
        all_records.extend(_read_jsonl_file(f))

    df = normalize_records(all_records)
    df = clean_dataframe(df)
    written = save_partitioned_parquet(df, out_base=out_dir)
    print(f"Transformed {len(all_records)} raw records -> {len(df)} rows; wrote {len(written)} parquet files.")
    return written


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Transform raw JSONL to partitioned Parquet")
    parser.add_argument("--date", default=None, help="YYYYMMDD to process (optional)")
    parser.add_argument("--raw-dir", default=str(RAW_DIR), help="Raw directory")
    parser.add_argument("--out-dir", default=str(PROCESSED_DIR), help="Processed output base dir")
    args = parser.parse_args()

    transform_raw(date=args.date, raw_dir=Path(args.raw_dir), out_dir=Path(args.out_dir))
