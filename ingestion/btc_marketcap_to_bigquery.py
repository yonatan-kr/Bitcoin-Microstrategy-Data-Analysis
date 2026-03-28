import time
import requests
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
from google.cloud import bigquery

EST = ZoneInfo("America/New_York")

# ── Config ────────────────────────────────────────────────────────────────────
PROJECT_ID = "mstr-btc-491122"
DATASET_ID = "btc_data"
TABLE_ID   = "btc_daily_marketcap"
BQ_TABLE   = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
# ──────────────────────────────────────────────────────────────────────────────

BQ_SCHEMA = [
    bigquery.SchemaField("date_est",          "DATE"),
    bigquery.SchemaField("price_usd",         "FLOAT64"),
    bigquery.SchemaField("market_cap_usd",    "FLOAT64"),
    bigquery.SchemaField("volume_usd",        "FLOAT64"),
    bigquery.SchemaField("load_datetime_est", "DATETIME"),
]


def fetch_marketcap() -> pd.DataFrame:
    """Fetch full daily BTC market cap history from CoinGecko (free, no key)."""
    url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"
    params = {
        "vs_currency": "usd",
        "days":        "max",
        "interval":    "daily",
    }

    print("Fetching BTC market cap history from CoinGecko...")
    for attempt in range(3):
        resp = requests.get(url, params=params, timeout=30)
        if resp.status_code == 429:
            print("  Rate limited, waiting 60s...")
            time.sleep(60)
            continue
        resp.raise_for_status()
        break

    data = resp.json()

    prices     = pd.DataFrame(data["prices"],      columns=["ts_ms", "price_usd"])
    market_cap = pd.DataFrame(data["market_caps"], columns=["ts_ms", "market_cap_usd"])
    volume     = pd.DataFrame(data["total_volumes"], columns=["ts_ms", "volume_usd"])

    df = prices.merge(market_cap, on="ts_ms").merge(volume, on="ts_ms")
    df["date_est"] = (
        pd.to_datetime(df["ts_ms"], unit="ms", utc=True)
        .dt.tz_convert(EST)
        .dt.date
    )
    df.drop(columns=["ts_ms"], inplace=True)
    df = df[["date_est", "price_usd", "market_cap_usd", "volume_usd"]]

    print(f"  {len(df):,} daily records fetched")
    print(f"  Range: {df['date_est'].iloc[0]}  to  {df['date_est'].iloc[-1]}")
    return df


def load_to_bigquery(df: pd.DataFrame) -> None:
    client = bigquery.Client(project=PROJECT_ID)

    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    dataset_ref.location = "US"
    client.create_dataset(dataset_ref, exists_ok=True)

    df["load_datetime_est"] = datetime.now(EST).replace(tzinfo=None)

    job_config = bigquery.LoadJobConfig(
        schema=BQ_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    print(f"\nLoading {len(df):,} rows into {BQ_TABLE} ...")
    job = client.load_table_from_dataframe(df, BQ_TABLE, job_config=job_config)
    job.result()

    table = client.get_table(BQ_TABLE)
    print(f"Done. Table now has {table.num_rows:,} rows.")


if __name__ == "__main__":
    df = fetch_marketcap()
    load_to_bigquery(df)
