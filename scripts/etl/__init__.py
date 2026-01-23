"""
ETL Scripts Module
==================

Scripts for running ETL transforms using the new framework.

Scripts:
- run_orderbook_features.py: Transform raw orderbook → features

Example Usage:

python scripts/etl/run_orderbook_features.py --trades data/raw/ready/ccxt/trades --exchange coinbaseadvanced --symbol BTC-USD --year 2025 --month 12 --day 18 --hour 0 --state-path orderbook_state.json

"""
