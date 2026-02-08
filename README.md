# ASTS Market Analytics

End-to-end market data pipeline and analytics dashboard for AST SpaceMobile (ASTS).

This project demonstrates the full analytics workflow:
API ingestion → BigQuery warehouse → SQL modeling → Looker Studio visualization.

## Tech Stack
- Python (API ingestion)
- BigQuery (data warehouse)
- SQL (analytics modeling)
- Looker Studio (dashboard)

## Dashboard
View-only Looker Studio dashboard:
https://lookerstudio.google.com/reporting/7359a79f-acdd-40cd-839b-19bbca1c7da8


## Data Flow
API → Python → BigQuery (raw + enriched tables/views) → Looker Studio

## Repository Structure
- `asts_pipeline/` — Python ingestion pipeline
- `sql/` — BigQuery SQL models and views

## How It Works
1. Python script pulls daily market data from an external API  
2. Raw data is written to BigQuery  
3. SQL models calculate daily change, percent change, and rolling metrics  
4. A latest snapshot table feeds KPI tiles  
5. Looker Studio connects directly to BigQuery for visualization
