-- Purpose:
--   Latest per-ticker snapshot used by KPI tiles in Looker Studio
--   Returns exactly one row per ticker

SELECT
  date,
  ticker,
  open,
  high,
  low,
  close,
  volume,
  prev_close,
  prev_volume,
  close_change,
  close_pct_change,
  ma_7d,
  ma_20d
FROM `project-bd8dfc8e-d597-4b1c-903.asts_live.asts_stock_daily_enriched`
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY ticker
  ORDER BY date DESC
) = 1;
