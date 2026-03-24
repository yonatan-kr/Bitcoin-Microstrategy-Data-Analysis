import requests
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
from google.cloud import bigquery

EST = ZoneInfo("America/New_York")

# ── Config ────────────────────────────────────────────────────────────────────
PROJECT_ID  = "mstr-btc-491122"
DATASET_ID  = "btc_data"
TABLE_ID    = "btc_hourly_ohlcv"
SYMBOL      = "BTCUSDT"
INTERVAL    = "1h"
START_MS    = int(datetime(2018, 1, 1, tzinfo=EST).timestamp() * 1000)
CHUNK_SIZE  = 1000          # max candles per Binance request
BQ_TABLE    = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
# ──────────────────────────────────────────────────────────────────────────────

COLUMNS = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_volume", "trades",
    "taker_buy_base", "taker_buy_quote", "ignore",
]

BQ_SCHEMA = [
    bigquery.SchemaField("open_time_est",       "DATETIME"),
    bigquery.SchemaField("open",                "FLOAT64"),
    bigquery.SchemaField("high",                "FLOAT64"),
    bigquery.SchemaField("low",                 "FLOAT64"),
    bigquery.SchemaField("close",               "FLOAT64"),
    bigquery.SchemaField("volume",              "FLOAT64"),
    bigquery.SchemaField("close_time_est",      "DATETIME"),
    bigquery.SchemaField("quote_volume",        "FLOAT64"),
    bigquery.SchemaField("trades",              "INT64"),
    bigquery.SchemaField("taker_buy_base",      "FLOAT64"),
    bigquery.SchemaField("taker_buy_quote",     "FLOAT64"),
    bigquery.SchemaField("load_datetime_est",   "DATETIME"),
]


def fetch_chunk(start_ms: int) -> pd.DataFrame:
    url = "https://api.binance.com/api/v3/klines"
    params = {
        "symbol":    SYMBOL,
        "interval":  INTERVAL,
        "startTime": start_ms,
        "limit":     CHUNK_SIZE,
    }
    resp = requests.get(url, params=params, timeout=15)
    resp.raise_for_status()
    raw = resp.json()
    if not raw:
        return pd.DataFrame()

    df = pd.DataFrame(raw, columns=COLUMNS).drop(columns=["ignore"])
    # Convert to EST then strip tz so BigQuery stores as DATETIME
    df["open_time_est"]  = (pd.to_datetime(df["open_time"],  unit="ms", utc=True)
                              .dt.tz_convert(EST).dt.tz_localize(None))
    df["close_time_est"] = (pd.to_datetime(df["close_time"], unit="ms", utc=True)
                              .dt.tz_convert(EST).dt.tz_localize(None))
    df.drop(columns=["open_time", "close_time"], inplace=True)
    float_cols = ["open", "high", "low", "close", "volume",
                  "quote_volume", "taker_buy_base", "taker_buy_quote"]
    df[float_cols] = df[float_cols].astype(float)
    df["trades"]    = df["trades"].astype(int)
    return df


def fetch_all_history() -> pd.DataFrame:
    now_ms     = int(datetime.now(EST).timestamp() * 1000)
    cursor_ms  = START_MS
    chunks     = []
    total      = 0

    print(f"Fetching all hourly {SYMBOL} candles from Binance...")
    while cursor_ms < now_ms:
        df = fetch_chunk(cursor_ms)
        if df.empty:
            break
        chunks.append(df)
        total     += len(df)
        cursor_ms  = int(pd.Timestamp(df["close_time_est"].iloc[-1])
                         .tz_localize(EST).timestamp() * 1000) + 1
        pct        = min(100, (cursor_ms - START_MS) / (now_ms - START_MS) * 100)
        print(f"  {total:>7,} candles fetched  ({pct:.1f}%)  "
              f"up to {df['open_time_est'].iloc[-1].strftime('%Y-%m-%d %H:%M')} EST",
              end="\r")

    print()
    return pd.concat(chunks, ignore_index=True)


def load_to_bigquery(df: pd.DataFrame) -> None:
    client = bigquery.Client(project=PROJECT_ID)

    # Create dataset if it doesn't exist
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    dataset_ref.location = "US"
    client.create_dataset(dataset_ref, exists_ok=True)

    # Add load timestamp in EST (tz-naive for DATETIME)
    df["load_datetime_est"] = datetime.now(EST).replace(tzinfo=None)

    # Enforce column order
    df = df[["open_time_est", "open", "high", "low", "close", "volume",
             "close_time_est", "quote_volume", "trades",
             "taker_buy_base", "taker_buy_quote", "load_datetime_est"]]

    job_config = bigquery.LoadJobConfig(
        schema=BQ_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    print(f"Loading {len(df):,} rows into {BQ_TABLE} ...")
    job = client.load_table_from_dataframe(df, BQ_TABLE, job_config=job_config)
    job.result()  # wait for completion

    table = client.get_table(BQ_TABLE)
    print(f"Done. Table now has {table.num_rows:,} rows.")


if __name__ == "__main__":
    df = fetch_all_history()
    print(f"\nTotal candles: {len(df):,}")
    print(f"Date range:    {df['open_time_est'].iloc[0]}  to  {df['open_time_est'].iloc[-1]}")
    print()
    load_to_bigquery(df)
