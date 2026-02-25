# ETL-Stream2DuckDB

**End‑to‑end ETL pipeline:** batch API ingest + simulated streaming → Parquet → DuckDB. Includes simple data quality checks and a lightweight CI setup. Designed to be easy to run on Colab and clear to read for hiring managers.

---

## Overview

**ETL-Stream2DuckDB** is a friendly demo project that shows how to build a complete data pipeline:

- **Batch ingest** from a public API (with retry/backoff).  
- **Simulated streaming** (producer → consumer) for event data.  
- **Transform** JSON into partitioned Parquet files (snappy).  
- **Query** data directly with **DuckDB**.  
- **Data quality checks** and **unit tests** with a simple CI workflow.

This repo is meant for learning and for a portfolio: clear code, runnable notebooks, and short docs.

---

## Quick start (run on Colab)

1. Open a new Colab notebook and run:

```bash
# 1) Clone the repo and enter folder
!git clone https://github.com/PhcPh4m/ETL-.git
%cd api2duckdb-pipeline

# 2) Install dependencies
!pip install -r requirements.txt
```
## Open in notebook

```
notebooks/02_batch_etl_api_to_duckdb.ipynb
```
## Run cell in order:

Setup & install

- Ingest (fetch API → save raw JSONL)

- Transform (normalize → write Parquet partitioned by date)

- Load & query (DuckDB reads Parquet)

- Data quality checks (run DQ rules and view report)

# Transform normalize raw JSONL to partitioned Parquet
## Purpose  
src/transform.py converts newline-delimited OpenWeather JSON records into a flat pandas DataFrame and writes Parquet files partitioned by date. This prepares data for DuckDB queries and downstream DQ checks.

## Input

- data/raw/weather_YYYYMMDD.jsonl (newline-delimited JSON)

## Output

data/processed/date=YYYY-MM-DD/part-YYYY-MM-DD.parquet

## Output schema (main columns)

- city_id, city_name

- weather_main, weather_description

- temp_c, feels_like_c, humidity

- wind_speed_m_s

- dt_api (epoch seconds from API)

- ingest_ts (ISO timestamp added at ingest)

- date (derived YYYY-MM-DD used for partitioning)

## Run locally

``` bash
python src/transform.py data/raw/weather_20240101.jsonl
```

# Run in Colab

```python
from src.transform import normalize_jsonl_to_df, write_parquet_partitioned

df = normalize_jsonl_to_df("data/raw/weather_20240101.jsonl")
write_parquet_partitioned(df, out_dir="data/processed", partition_col="date", compression="snappy")
```

## DuckDB quick example

```python
import duckdb
con = duckdb.connect(database=':memory:')
con.execute("CREATE VIEW weather AS SELECT * FROM read_parquet('data/processed/date=*/part-*.parquet')")
print(con.execute("SELECT date, COUNT(*) FROM weather GROUP BY date ORDER BY date").fetchdf())
```

## Notes

- ingest_ts is added at ingest; if missing, date is derived from dt (API epoch).

- Parquet uses snappy compression by default. Change via write_parquet_partitioned(..., compression='gzip') if needed.

- Do not commit API keys; use environment variables or getpass in Colab.

# Repo structure

```
project-root/
├─ notebooks/
│  └─ 02_batch_etl_api_to_duckdb.ipynb
├─ src/
│  ├─ etl_api.py
│  ├─ transform.py
│  ├─ dq_checks.py
│  └─ utils.py
├─ data/                 # gitignored (raw/ and processed/)
├─ tests/
│  └─ test_etl.py
├─ .github/
│  └─ workflows/ci.yml
├─ requirements.txt
├─ .gitignore
└─ README.md
```

# How it works

``` bash
# Run a quick ingest script (local)
python src/etl_api.py
# Run transform (local)
python -c "from src.transform import normalize_and_write_parquet; normalize_and_write_parquet('data/raw/events_YYYYMMDD.jsonl')"
# Run tests
pytest -q
```

# Data quality rules (examples)

- Null rate: fail if null rate for a critical column > 10%.

- Unique key: ensure event_id (or id) is unique per file.

- Schema check: required columns exist and types are reasonable.

DQ checks return simple JSON-like results so they are easy to extend and log.

# Limitations & scaling

This demo is not production. It runs on Colab or a single machine and is meant for learning and portfolio use.

To scale to production:

- Store files in S3 (or other cloud storage).

- Use Kafka (or managed streaming) for real streaming ingestion.

- Use Delta Lake or Iceberg for table management and schema evolution.

- Use Airflow or Prefect for orchestration and run on Kubernetes for reliability.

- Replace DuckDB with a managed data warehouse (BigQuery, Snowflake) for concurrency and scale.

# Tests & CI

- Unit tests live in tests/ and run with pytest.

- A lightweight GitHub Actions workflow runs tests on push/PR to main. CI is intentionally minimal to avoid long runs.

# Troubleshooting (quick)

- Network / API errors: retry the cell after a short wait; check rate limits.

- Colab runtime resets: save dw.duckdb or processed Parquet to Google Drive before closing.

- Missing packages: re-run !pip install -r requirements.txt.

- 401 Invalid API key: confirm your OpenWeather account email and use the correct API key; do not commit keys.

# Contributing

- Keep commits small and focused.

- Use branch names like feature/etl-api or fix/dq-check.

- Add tests for new functionality and update the notebook if behavior changes.

# License

- MIT License — feel free to reuse this demo for learning and portfolio purposes.


# Contact
- Author: Phúc (PhcPh4m)
- Repo: api2duckdb-pipeline on GitHub
