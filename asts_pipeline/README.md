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
