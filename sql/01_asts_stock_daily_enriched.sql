-- Tested query used to build asts_stock_daily_enriched
-- Source: asts_live.asts_stock_daily

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

FROM asts_live.asts_stock_daily;

