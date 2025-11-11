"""Set of functions to retrieve market data from Schwab API."""

from typing import Any, Dict, List, Union

import requests
from global_utils.general_utils import json_read
from requests import Response
from schwab_api.const_vars import MARKET_URL, TOKEN_JSON_PATH


# ----------------------------------------------------------------------------------------------------------------------
def get_multi_symbols_quote(symbols_list: List[str]) -> Dict[str, Any]:
    """Get real-time price quotes from a list of equity symbols.

    :param symbols_list: List[str]: List of all the symbols to retrieve quotes on.
    :return: Dict[str, Any]: Dictionary with the data results for all the symbols.
    """
    from datetime import datetime

    access_token: str = json_read(file_name_path=TOKEN_JSON_PATH)["access_token"]
    symbols_list_str: str = ",".join(symbols_list)

    order_params: Dict[str, Any] = {
        "symbols": symbols_list_str,  # must be a comma-separated string
    }
    order_response: Response = requests.get(
        url=rf"{MARKET_URL}/quotes",
        headers={"Authorization": rf"Bearer {access_token}"},
        params=order_params,  # correct for GET requests
    )

    extended_vars_list: List[str] = ["quoteTime", "tradeTime"]
    quote_vars_list: List[str] = extended_vars_list + ["askTime", "bidTime"]
    regular_vars_list: List[str] = ["regularMarketTradeTime"]

    unix_data_dict: Dict[str, List[str]] = {
        "extended": extended_vars_list,
        "quote": quote_vars_list,
        "regular": regular_vars_list,
    }
    # NOTE: The MAIN dictionary with all the data
    main_data_dict: Dict[str, Any] = order_response.json()

    symbol: str
    for symbol in symbols_list:
        # The data in the `main_data_dict` is actually being changed
        symbol_quote_data: Dict[str, Any] = main_data_dict[symbol]

        unix_var: str
        for unix_var in unix_data_dict.keys():
            temp_vars_list: List[str] = unix_data_dict[unix_var]
            temp_data_dict: Dict[str, Union[int, float]] = symbol_quote_data[unix_var]

            temp_var: str
            for temp_var in temp_vars_list:
                unix_time: int = temp_data_dict[temp_var]
                datetime_ts: datetime = datetime.fromtimestamp(timestamp=(unix_time / 1000))
                symbol_quote_data[unix_var][temp_var]: str = datetime_ts.strftime("%Y-%m-%d %H:%M:%S")

    # NOTE: Turn all this information into a Pandas Dataframe.
    # NOTE: OR ... take note of how to use the Data Dictionary results to do real-time executions.

    order_status: int = order_response.status_code
    if order_status == 201:
        return main_data_dict
    else:
        raise ValueError(order_response._content)
