-- StandardSQL (BigQuery)
-- Model: asts_stock_daily_enriched
-- Purpose:
--   Enrich daily ASTS market data with lag features, day-over-day changes,
--   and rolling moving averages for Looker Studio charts.

CREATE OR REPLACE TABLE `project-bd8dfc8e-d597-4b1c-903.asts_live.asts_stock_daily_enriched` AS
SELECT
  date,
  ticker,
  open,
  high,
  low,
  close,
  volume,

  -- Previous trading day values
  LAG(close)  OVER (PARTITION BY ticker ORDER BY date) AS prev_close,
  LAG(volume) OVER (PARTITION BY ticker ORDER BY date) AS prev_volume,

  -- Day-over-day changes
  close - LAG(close) OVER (PARTITION BY ticker ORDER BY date) AS close_change,
  SAFE_DIVIDE(
    close - LAG(close) OVER (PARTITION BY ticker ORDER BY date),
    LAG(close) OVER (PARTITION BY ticker ORDER BY date)
  ) AS close_pct_change,

  -- Rolling moving averages (trading days)
  AVG(close) OVER (
    PARTITION BY ticker
    ORDER BY date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS ma_7d,

  AVG(close) OVER (
    PARTITION BY ticker
    ORDER BY date
    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
  ) AS ma_20d

FROM `project-bd8dfc8e-d597-4b1c-903.asts_live.asts_stock_daily`;

