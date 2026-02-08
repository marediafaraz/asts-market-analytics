# ASTS Data Ingestion Pipeline

This folder contains the Python pipeline used to pull daily AST SpaceMobile (ASTS)
market data from an external API and load it into BigQuery.

## What this pipeline does
- Fetches daily OHLCV market data for ASTS
- Normalizes and validates the raw API response
- Appends new rows to a BigQuery raw table
- Designed to run once per day

## Files
- `load_asts_daily.py` — main ingestion script
- `requirements.txt` — Python dependencies required to run the pipeline

## Setup
```bash
pip install -r requirements.txt
python load_asts_daily.py

