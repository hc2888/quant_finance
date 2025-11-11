"""Utility functions used throughout this DAG."""

from datetime import datetime, timedelta
from typing import Dict, List, Tuple


# ----------------------------------------------------------------------------------------------------------------------
def generate_combinations(max_market_date_dict: Dict[str, str]) -> List[Tuple[str, str, str]]:
    """Generate a list of tuples with all possible combinations of key string and date string;

    from the beginning date string within the provided dictionary until today, month by month.

    :param max_market_date_dict: Dict[str, str]: A dictionary with keys as ETF names and current max market dates.

    :return: List[Tuple[str, str, str]]: A list of tuples where the first element is the dict key,the second element
    is the date string in "YYYY-MM" format, and the third element is the MAX `market_date` already in the SQL table.
    """
    import pytz

    etf_combinations: List[Tuple[str, str, str]] = []
    # Latest date can only be 2 weeks ago at the LATEST because NEWER data is not available with this API call-type
    two_weeks_ago: datetime = datetime.now().astimezone(pytz.timezone(zone="US/Eastern")) - timedelta(days=14)

    etf_symbol: str
    start_date_str: str
    for etf_symbol, start_date_str in max_market_date_dict.items():
        target_date: datetime = datetime.strptime(start_date_str, "%Y-%m-%d")

        while target_date.strftime("%Y-%m-%d") <= two_weeks_ago.strftime("%Y-%m-%d"):
            etf_combinations.append((etf_symbol, target_date.strftime("%Y-%m"), start_date_str))
            # Move to the next month
            if target_date.month == 12:
                target_date: datetime = datetime(year=target_date.year + 1, month=1, day=1)
            else:
                target_date: datetime = datetime(year=target_date.year, month=target_date.month + 1, day=1)

    return etf_combinations
