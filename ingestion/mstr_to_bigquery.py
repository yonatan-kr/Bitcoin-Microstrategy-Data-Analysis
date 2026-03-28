import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from google.cloud import bigquery

EST = ZoneInfo("America/New_York")

# ── Config ────────────────────────────────────────────────────────────────────
PROJECT_ID     = "mstr-btc-491122"
DATASET_ID     = "btc_data"
DAILY_TABLE    = "mstr_daily_ohlcv"
HOURLY_TABLE   = "mstr_hourly_ohlcv"
TICKER         = "MSTR"
DAILY_START    = "2017-01-01"
# yfinance max lookback for hourly data
HOURLY_START   = datetime.now(EST) - timedelta(days=729)
# ──────────────────────────────────────────────────────────────────────────────

BQ_DAILY_SCHEMA = [
    bigquery.SchemaField("date_est",          "DATE"),
    bigquery.SchemaField("open",              "FLOAT64"),
    bigquery.SchemaField("high",              "FLOAT64"),
    bigquery.SchemaField("low",               "FLOAT64"),
    bigquery.SchemaField("close",             "FLOAT64"),
    bigquery.SchemaField("volume",            "INT64"),
    bigquery.SchemaField("load_datetime_est", "DATETIME"),
]

BQ_HOURLY_SCHEMA = [
    bigquery.SchemaField("open_time_est",     "DATETIME"),
    bigquery.SchemaField("open",              "FLOAT64"),
    bigquery.SchemaField("high",              "FLOAT64"),
    bigquery.SchemaField("low",               "FLOAT64"),
    bigquery.SchemaField("close",             "FLOAT64"),
    bigquery.SchemaField("volume",            "INT64"),
    bigquery.SchemaField("load_datetime_est", "DATETIME"),
]


def fetch_daily() -> pd.DataFrame:
    print(f"Fetching daily MSTR data from {DAILY_START} ...")
    df = yf.download(TICKER, start=DAILY_START, interval="1d", auto_adjust=True, progress=False)
    df = df.reset_index()

    # Flatten multi-level columns if present
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [col[0].lower() for col in df.columns]
    else:
        df.columns = [col.lower() for col in df.columns]

    df.rename(columns={"date": "date_est"}, inplace=True)
    df["date_est"] = pd.to_datetime(df["date_est"]).dt.date
    df["volume"]   = df["volume"].astype("int64")
    df = df[["date_est", "open", "high", "low", "close", "volume"]]

    print(f"  {len(df):,} daily rows  ({df['date_est'].iloc[0]}  to  {df['date_est'].iloc[-1]})")
    return df


def fetch_hourly() -> pd.DataFrame:
    start_str = HOURLY_START.strftime("%Y-%m-%d")
    print(f"Fetching hourly MSTR data from {start_str} ...")
    df = yf.download(TICKER, start=start_str, interval="1h", auto_adjust=True, progress=False)
    df = df.reset_index()

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [col[0].lower() for col in df.columns]
    else:
        df.columns = [col.lower() for col in df.columns]

    df.rename(columns={"datetime": "open_time_est"}, inplace=True)
    df["open_time_est"] = (
        pd.to_datetime(df["open_time_est"], utc=True)
        .dt.tz_convert(EST)
        .dt.tz_localize(None)
    )
    df["volume"] = df["volume"].astype("int64")
    df = df[["open_time_est", "open", "high", "low", "close", "volume"]]

    print(f"  {len(df):,} hourly rows  ({df['open_time_est'].iloc[0]}  to  {df['open_time_est'].iloc[-1]})")
    return df


def load_to_bigquery(df: pd.DataFrame, table_id: str, schema: list) -> None:
    client     = bigquery.Client(project=PROJECT_ID)
    bq_table   = f"{PROJECT_ID}.{DATASET_ID}.{table_id}"

    dataset_ref          = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    dataset_ref.location = "US"
    client.create_dataset(dataset_ref, exists_ok=True)

    df["load_datetime_est"] = datetime.now(EST).replace(tzinfo=None)

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    print(f"Loading {len(df):,} rows into {bq_table} ...")
    job = client.load_table_from_dataframe(df, bq_table, job_config=job_config)
    job.result()
    print(f"Done. Table now has {client.get_table(bq_table).num_rows:,} rows.")


if __name__ == "__main__":
    daily_df  = fetch_daily()
    hourly_df = fetch_hourly()

    print()
    load_to_bigquery(daily_df,  DAILY_TABLE,  BQ_DAILY_SCHEMA)
    print()
    load_to_bigquery(hourly_df, HOURLY_TABLE, BQ_HOURLY_SCHEMA)
