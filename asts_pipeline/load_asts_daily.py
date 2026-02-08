****
cat > load_asts_daily.py << 'PY'
import os
import requests
from datetime import datetime, timezone
from google.cloud import bigquery

PROJECT_ID = "project-bd8dfc8e-d597-4b1c-903"
DATASET = "asts_live"
TABLE = "asts_stock_daily"
SYMBOL = "ASTS"

# API

import os
ALPHAVANTAGE_API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")
if not ALPHAVANTAGE_API_KEY:
    raise RuntimeError("Missing ALPHAVANTAGE_API_KEY")


def get_max_date(client: bigquery.Client) -> str | None:
    query = f"""
    SELECT MAX(date) AS max_date
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    """
    rows = list(client.query(query).result())
    max_date = rows[0].max_date
    return max_date.isoformat() if max_date else None

def fetch_time_series_daily():
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": SYMBOL,
        "apikey": ALPHAVANTAGE_API_KEY,
        "outputsize": "compact"
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    # Handle API throttling / errors clearly
    if "Time Series (Daily)" not in data:
        raise RuntimeError(f"AlphaVantage error/limit hit: {data}")

    return data["Time Series (Daily)"]

def insert_rows(client: bigquery.Client, time_series: dict, max_date: str | None):
    now_ts = datetime.now(timezone.utc).isoformat()

    rows = []
    for date_str, v in time_series.items():
        # date_str is 'YYYY-MM-DD'
        if max_date and date_str <= max_date:
            continue

        rows.append({
            "date": date_str,
            "ticker": SYMBOL,
            "open": float(v["1. open"]),
            "high": float(v["2. high"]),
            "low": float(v["3. low"]),
            "close": float(v["4. close"]),
            "volume": int(v["5. volume"]),
            "loaded_at": now_ts
        })

    if not rows:
        print("No new rows to insert (already up to date).")
        return

    table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"
    errors = client.insert_rows_json(table_id, rows)
    if errors:
        raise RuntimeError(f"BigQuery insert errors: {errors}")

    print(f"Inserted {len(rows)} new rows into {table_id}")

def main():
    client = bigquery.Client(project=PROJECT_ID)

    max_date = get_max_date(client)
    print("Current MAX(date) in BigQuery =", max_date)

    ts = fetch_time_series_daily()
    insert_rows(client, ts, max_date)

if __name__ == "__main__":
    main()
