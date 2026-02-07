# ASTS Market Analytics

End-to-end market data pipeline + warehouse + dashboard.

## Tech stack
- Python (API ingestion)
- BigQuery (data warehouse + SQL transformations)
- Looker Studio (visualization)

## Data flow
API → Python → BigQuery (raw + enriched tables/views) → Looker Studio dashboard

## Repo structure
- asts_pipeline/ : ingestion scripts
- sql/ : SQL models/views
- dashboard/ : dashboard notes/screenshots

## How it works

1. A Python script pulls daily market data from an external API  
2. Raw data is written to BigQuery  
3. SQL models calculate daily change, percent change, and rolling metrics  
4. A latest snapshot table feeds KPI tiles  
5. Looker Studio connects directly to BigQuery for visualization
