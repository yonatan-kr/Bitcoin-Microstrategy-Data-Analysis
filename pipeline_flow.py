"""
pipeline_flow.py — Prefect flow for incremental BTC/MSTR pipeline refresh.

Runs:
  1. Incremental BTC candle load from Binance → BigQuery
  2. MSTR daily + hourly reload from yfinance → BigQuery
  3. dbt run to rebuild all models

GCP auth: reads GOOGLE_APPLICATION_CREDENTIALS_JSON env var (set as a
Prefect Secret in the work pool or deployment env variables).
"""

import json
import os
import subprocess
import tempfile
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import requests
import yfinance as yf
from google.cloud import bigquery
from google.oauth2 import service_account
from prefect import flow, task, get_run_logger

EST         = ZoneInfo("America/New_York")
PROJECT_ID  = "mstr-btc-491122"
DATASET_ID  = "btc_data"
ROOT        = Path(__file__).parent
DBT_DIR     = ROOT / "transform" / "btc_dbt"


# ── Helpers ───────────────────────────────────────────────────────────────────

def get_bq_client() -> bigquery.Client:
    """Build a BigQuery client from env var credentials or ADC."""
    creds_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    if creds_json:
        info  = json.loads(creds_json)
        creds = service_account.Credentials.from_service_account_info(
            info,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        return bigquery.Client(project=PROJECT_ID, credentials=creds)
    return bigquery.Client(project=PROJECT_ID)  # falls back to ADC locally


# ── BTC task ─────────────────────────────────────────────────────────────────

BTC_TABLE  = f"{PROJECT_ID}.{DATASET_ID}.btc_hourly_ohlcv"
SYMBOL     = "BTCUSDT"
CHUNK_SIZE = 1000
START_MS   = int(datetime(2017, 1, 1, tzinfo=EST).timestamp() * 1000)
COLUMNS    = ["open_time","open","high","low","close","volume",
              "close_time","quote_volume","trades","taker_buy_base","taker_buy_quote","ignore"]

BTC_SCHEMA = [
    bigquery.SchemaField("open_time_est",     "DATETIME"),
    bigquery.SchemaField("open",              "FLOAT64"),
    bigquery.SchemaField("high",              "FLOAT64"),
    bigquery.SchemaField("low",               "FLOAT64"),
    bigquery.SchemaField("close",             "FLOAT64"),
    bigquery.SchemaField("volume",            "FLOAT64"),
    bigquery.SchemaField("close_time_est",    "DATETIME"),
    bigquery.SchemaField("quote_volume",      "FLOAT64"),
    bigquery.SchemaField("trades",            "INT64"),
    bigquery.SchemaField("taker_buy_base",    "FLOAT64"),
    bigquery.SchemaField("taker_buy_quote",   "FLOAT64"),
    bigquery.SchemaField("load_datetime_est", "DATETIME"),
]


def _fetch_btc_chunk(start_ms: int) -> pd.DataFrame:
    resp = requests.get(
        "https://api.binance.com/api/v3/klines",
        params={"symbol": SYMBOL, "interval": "1h", "startTime": start_ms, "limit": CHUNK_SIZE},
        timeout=15,
    )
    resp.raise_for_status()
    raw = resp.json()
    if not raw:
        return pd.DataFrame()
    df = pd.DataFrame(raw, columns=COLUMNS).drop(columns=["ignore"])
    df["open_time_est"]  = (pd.to_datetime(df["open_time"],  unit="ms", utc=True)
                             .dt.tz_convert(EST).dt.tz_localize(None))
    df["close_time_est"] = (pd.to_datetime(df["close_time"], unit="ms", utc=True)
                             .dt.tz_convert(EST).dt.tz_localize(None))
    df.drop(columns=["open_time", "close_time"], inplace=True)
    float_cols = ["open","high","low","close","volume","quote_volume","taker_buy_base","taker_buy_quote"]
    df[float_cols]  = df[float_cols].astype(float)
    df["trades"]    = df["trades"].astype(int)
    return df


@task(name="load-btc", retries=2, retry_delay_seconds=30)
def load_btc():
    logger = get_run_logger()
    client = get_bq_client()

    # Get last loaded timestamp
    try:
        row     = list(client.query(f"SELECT MAX(close_time_est) as ts FROM `{BTC_TABLE}`").result())[0]
        max_ts  = row.ts
        start_ms = int(pd.Timestamp(max_ts).tz_localize(EST).timestamp() * 1000) + 1 if max_ts else START_MS
    except Exception:
        start_ms = START_MS

    mode = "incremental" if start_ms != START_MS else "full"
    logger.info(f"BTC load mode: {mode} from {datetime.fromtimestamp(start_ms/1000, tz=EST)}")

    now_ms, chunks, total = int(datetime.now(EST).timestamp() * 1000), [], 0
    cursor_ms = start_ms
    while cursor_ms < now_ms:
        df = _fetch_btc_chunk(cursor_ms)
        if df.empty:
            break
        chunks.append(df)
        total     += len(df)
        cursor_ms  = int(pd.Timestamp(df["close_time_est"].iloc[-1]).tz_localize(EST).timestamp() * 1000) + 1

    if not chunks:
        logger.info("BTC already up to date.")
        return

    df = pd.concat(chunks, ignore_index=True)
    df["load_datetime_est"] = datetime.now(EST).replace(tzinfo=None)
    df = df[["open_time_est","open","high","low","close","volume",
             "close_time_est","quote_volume","trades","taker_buy_base","taker_buy_quote","load_datetime_est"]]

    write_disp = bigquery.WriteDisposition.WRITE_TRUNCATE if mode == "full" else bigquery.WriteDisposition.WRITE_APPEND
    job = client.load_table_from_dataframe(df, BTC_TABLE,
          job_config=bigquery.LoadJobConfig(schema=BTC_SCHEMA, write_disposition=write_disp))
    job.result()
    logger.info(f"BTC: loaded {total:,} new candles. Table now has {client.get_table(BTC_TABLE).num_rows:,} rows.")


# ── MSTR task ─────────────────────────────────────────────────────────────────

MSTR_DAILY_SCHEMA = [
    bigquery.SchemaField("date_est",          "DATE"),
    bigquery.SchemaField("open",              "FLOAT64"),
    bigquery.SchemaField("high",              "FLOAT64"),
    bigquery.SchemaField("low",               "FLOAT64"),
    bigquery.SchemaField("close",             "FLOAT64"),
    bigquery.SchemaField("volume",            "INT64"),
    bigquery.SchemaField("load_datetime_est", "DATETIME"),
]

MSTR_HOURLY_SCHEMA = [
    bigquery.SchemaField("open_time_est",     "DATETIME"),
    bigquery.SchemaField("open",              "FLOAT64"),
    bigquery.SchemaField("high",              "FLOAT64"),
    bigquery.SchemaField("low",               "FLOAT64"),
    bigquery.SchemaField("close",             "FLOAT64"),
    bigquery.SchemaField("volume",            "INT64"),
    bigquery.SchemaField("load_datetime_est", "DATETIME"),
]


@task(name="load-mstr", retries=2, retry_delay_seconds=30)
def load_mstr():
    logger = get_run_logger()
    client = get_bq_client()
    load_dt = datetime.now(EST).replace(tzinfo=None)

    # Daily
    daily = yf.download("MSTR", start="2017-01-01", interval="1d", auto_adjust=True, progress=False)
    daily = daily.reset_index()
    daily.columns = [c[0].lower() if isinstance(c, tuple) else c.lower() for c in daily.columns]
    daily.rename(columns={"date": "date_est"}, inplace=True)
    daily["date_est"] = pd.to_datetime(daily["date_est"]).dt.date
    daily["volume"]   = daily["volume"].astype("int64")
    daily["load_datetime_est"] = load_dt
    daily = daily[["date_est","open","high","low","close","volume","load_datetime_est"]]
    j = client.load_table_from_dataframe(daily, f"{PROJECT_ID}.{DATASET_ID}.mstr_daily_ohlcv",
        job_config=bigquery.LoadJobConfig(schema=MSTR_DAILY_SCHEMA,
                   write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE))
    j.result()
    logger.info(f"MSTR daily: {len(daily):,} rows loaded.")

    # Hourly
    from datetime import timedelta
    hourly = yf.download("MSTR", start=(datetime.now(EST) - timedelta(days=729)).strftime("%Y-%m-%d"),
                         interval="1h", auto_adjust=True, progress=False)
    hourly = hourly.reset_index()
    hourly.columns = [c[0].lower() if isinstance(c, tuple) else c.lower() for c in hourly.columns]
    hourly.rename(columns={"datetime": "open_time_est"}, inplace=True)
    hourly["open_time_est"] = (pd.to_datetime(hourly["open_time_est"], utc=True)
                                .dt.tz_convert(EST).dt.tz_localize(None))
    hourly["volume"] = hourly["volume"].astype("int64")
    hourly["load_datetime_est"] = load_dt
    hourly = hourly[["open_time_est","open","high","low","close","volume","load_datetime_est"]]
    j = client.load_table_from_dataframe(hourly, f"{PROJECT_ID}.{DATASET_ID}.mstr_hourly_ohlcv",
        job_config=bigquery.LoadJobConfig(schema=MSTR_HOURLY_SCHEMA,
                   write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE))
    j.result()
    logger.info(f"MSTR hourly: {len(hourly):,} rows loaded.")


# ── dbt task ──────────────────────────────────────────────────────────────────

@task(name="dbt-run", retries=1)
def run_dbt():
    logger = get_run_logger()
    result = subprocess.run("dbt run", shell=True, cwd=DBT_DIR,
                            capture_output=True, text=True)
    logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError("dbt run failed")
    logger.info("dbt run completed successfully.")


# ── Flow ──────────────────────────────────────────────────────────────────────

@flow(name="btc-mstr-pipeline", log_prints=True)
def btc_mstr_pipeline():
    load_btc()
    load_mstr()
    run_dbt()


if __name__ == "__main__":
    btc_mstr_pipeline()
