"""Set of functions to get meta-information from Schwab Brokerage Account(s)."""

from typing import Any, Dict, List

from schwab_api.const_vars import ACCOUNT_URL
from schwab_api.util_funcs import get_new_access_token, get_schwab_acct_hashes


# ----------------------------------------------------------------------------------------------------------------------
def acct_market_orders(target_account: str = "Market", num_days_range: int = 365) -> List[Dict[str, Any]]:
    """Get all the market orders on the Schwab account for the past X days.

    :param target_account: str: The target Schwab account.
    :param num_days_range: int: The past number of days to look back on.
    :return: List[Dict[str, Any]]: The set of market orders for the target Schwab account in the past X days.
    """
    import datetime

    import pytz
    import requests

    # Define U.S. Eastern timezone
    eastern_timezone: datetime.tzinfo = pytz.timezone(zone="US/Eastern")
    # Current time in Eastern Time (automatically handles DST)
    now_eastern: datetime.datetime = datetime.datetime.now(tz=eastern_timezone).replace(microsecond=0)
    # Time 365 days ago in Eastern Time
    num_days_ago_eastern: datetime.datetime = now_eastern - datetime.timedelta(days=num_days_range)
    # Format as ISO 8601 with timezone offset (e.g., -04:00 or -05:00)
    from_entered_time: str = num_days_ago_eastern.isoformat()
    to_entered_time: str = now_eastern.isoformat()

    account_hash: str = get_schwab_acct_hashes()[target_account]
    new_access_token: str = get_new_access_token()

    market_orders: List[Dict[str, Any]] = requests.get(
        url=rf"{ACCOUNT_URL}/accounts/{account_hash}/orders",
        headers={"Authorization": rf"Bearer {new_access_token}"},
        params={
            "fromEnteredTime": from_entered_time,
            "toEnteredTime": to_entered_time,
        },
    ).json()
    return market_orders
