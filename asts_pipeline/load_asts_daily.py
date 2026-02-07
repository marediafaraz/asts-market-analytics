import os
import requests
import pandas as pd
from datetime import datetime, timezone
from google.cloud import bigquery

TICKER = os.getenv("TICKER", "ASTS")
API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")  # set in env, don't hardcode
BQ_PROJECT = os.getenv("BQ_PROJECT")
BQ_DATASET = os.getenv("BQ_DATASET", "asts_live")
BQ_TABLE = os.getenv("BQ_TABLE", "asts_stock_daily_raw")

def fetch_daily_adjusted(ticker: str, api_key: str) -> dict:
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_DAILY_ADJUSTED",
        "symbol": ticker,
        "outputsize": "compact",
        "apikey": api_key,
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    if "Time Series (Daily)" not in data:
        raise RuntimeError(f"Unexpected API response: {data}")
    return data

def to_dataframe(payload: dict, ticker: str) -> pd.DataFrame:
    ts = payload["Time Series (Daily)"]
    rows = []
    for d, v in ts.items():
        rows.append({
            "date": d,
            "ticker": ticker,
            "open": float(v["1. open"]),
            "high": float(v["2. high"]),
            "low": float(v["3. low"]),
            "close": float(v["4. close"]),
            "volume": int(float(v["6. volume"])),
            "loaded_at": datetime.now(timezone.utc).isoformat()
        })
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    return df

def load_bigquery(df: pd.DataFrame, project: str, dataset: str, table: str):
    client = bigquery.Client(project=project)
    table_id = f"{project}.{dataset}.{table}"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema=[
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("ticker", "STRING"),
            bigquery.SchemaField("open", "FLOAT"),
            bigquery.SchemaField("high", "FLOAT"),
            bigquery.SchemaField("low", "FLOAT"),
            bigquery.SchemaField("close", "FLOAT"),
            bigquery.SchemaField("volume", "INTEGER"),
            bigquery.SchemaField("loaded_at", "TIMESTAMP"),
        ],
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

def main():
    if not API_KEY:
        raise SystemExit("Missing env var ALPHAVANTAGE_API_KEY")
    if not BQ_PROJECT:
        raise SystemExit("Missing env var BQ_PROJECT")

    payload = fetch_daily_adjusted(TICKER, API_KEY)
    df = to_dataframe(payload, TICKER)

    # Optional: keep only latest N rows from API (AlphaVantage compact ~100)
    load_bigquery(df, BQ_PROJECT, BQ_DATASET, BQ_TABLE)
    print(f"Loaded {len(df)} rows for {TICKER} into {BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}")

if __name__ == "__main__":
    main()
