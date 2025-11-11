"""All the static variables used for the DAG."""

from typing import Dict, List

prev_day_column_suffix: str = "_prev"
prev_day_table_name: str = "prev_day_added_data"
value_multiplier: int = 100

algebra_values_table_name: str = "algebra"

algebra_columns_dict: Dict[str, List[str]] = {"sp500": ["sp500_price_close"], "cboe": ["cboe_price_close"]}
