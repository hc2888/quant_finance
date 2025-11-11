"""All static / constant variables used throughout SQL infrastructure."""

from typing import List

# ----------------------------------------------------------------------------------------------------------------------
# NOTE: Use `adjusted` prices for calculating non-intraday prices; otherwise use `non-adjusted` prices for real-time
# All the schemas created in the `quant_finance` RDS
SCHEMA_LIST: List[str] = [
    "alpha_vantage",
    "backtesting",
    "fred",
    "global",
    "model_prep_api",
    "sharadar",
]
SCHEMA_OTHER_LIST: List[str] = [
    "alpha_vantage_metadata",
    "backtesting_metadata",
    "fred_metadata",
    "model_prep_api_metadata",
    "sharadar_metadata",
    "temp_tables",
]
SCHEMA_DBT_LIST: List[str] = ["dbt_objects"]
SCHEMA_TOTAL_LIST: List[str] = SCHEMA_LIST + SCHEMA_OTHER_LIST + SCHEMA_DBT_LIST
# ----------------------------------------------------------------------------------------------------------------------
"""DEFAULT COLUMNS EVERY TABLE SHOULD HAVE."""
DEFAULT_COLUMNS_LIST: List[str] = [
    "unique_id UUID DEFAULT gen_random_uuid()",
    "exec_timestamp TIMESTAMP NOT NULL",
    "market_date DATE NOT NULL",
    "market_year INTEGER NOT NULL",
    "market_month INTEGER NOT NULL",
]
INTRADAY_COLUMNS_LIST: List[str] = [
    "market_timestamp TIMESTAMP NOT NULL",
    "market_clock TEXT NOT NULL",
    "market_hour INTEGER NOT NULL",
    "market_minute INTEGER NOT NULL",
    "symbol TEXT NOT NULL",
    "open NUMERIC(10,2) NOT NULL",
    "high NUMERIC(10,2) NOT NULL",
    "low NUMERIC(10,2) NOT NULL",
    "close NUMERIC(10,2) NOT NULL",
    "volume BIGINT NOT NULL",
]
INTRADAY_COLUMNS_LIST: List[str] = DEFAULT_COLUMNS_LIST + INTRADAY_COLUMNS_LIST
