"""Set of functions to execute various orders into Schwab account(s)."""

from typing import Any, Dict, Literal

import requests
from global_utils.general_utils import json_read
from requests import Response
from schwab_api.const_vars import ACCOUNT_URL, TOKEN_JSON_PATH
from schwab_api.util_funcs import get_schwab_acct_hashes


# ----------------------------------------------------------------------------------------------------------------------
def buy_stock_order(
    symbol: str,
    order_type: Literal["MARKET", "LIMIT"],
    stock_quantity: int,
    max_buy_price: float,
    target_account: str = "Market",
) -> int:
    """Place a BUY order for an equity stock.

    :param symbol: str: Target equity stock to BUY.
    :param order_type: Literal["MARKET", "LIMIT"]: The type of BUY order.
    :param stock_quantity: int: How many units of the stock to BUY.
    :param max_buy_price: float: The MAX price at which to buy the stock if this is a LIMIT order.
    :param target_account: str: The target Schwab account to execute the BUY order on.
    :return: int: Whether the order placed was a success; `201` == SUCCESS.
    """
    order_params: Dict[str, Any] = {
        "orderType": order_type,
        "session": "NORMAL",
        "duration": "DAY",
        "orderStrategyType": "SINGLE",
        "orderLegCollection": [
            {
                "instruction": "BUY",
                "quantity": stock_quantity,
                "instrument": {"symbol": symbol, "assetType": "EQUITY"},
            }
        ],
    }
    if order_type == "LIMIT":
        order_params["price"]: float = max_buy_price

    account_hash: str = get_schwab_acct_hashes()[target_account]
    access_token: str = json_read(file_name_path=TOKEN_JSON_PATH)["access_token"]
    order_response: Response = requests.post(
        url=rf"{ACCOUNT_URL}/accounts/{account_hash}/orders",
        headers={"Authorization": rf"Bearer {access_token}"},
        json=order_params,  # NOTE: use `json` parameter, NOT the `param` parameter for this particular POST
    )
    order_status: int = order_response.status_code
    if order_status != 201:
        raise ValueError(order_response._content)
    return order_status


# ----------------------------------------------------------------------------------------------------------------------
def sell_stock_order(
    symbol: str,
    stock_quantity: int,
    target_account: str = "Market",
    min_sell_perc: float = 0.0050,  # 50 Basis Points
    add_stop_limit: bool = False,
) -> int:
    """Place a SELL Trail Stop or Trail Stop Limit order.

    :param symbol: str: Target equity stock to SELL.
    :param stock_quantity: int: How many units of the stock to SELL.
    :param target_account: str: The target Schwab account to execute the SELL order on.
    :param min_sell_perc: float: The minimum percentage below the point-in-time BID price of the stock.
    :param add_stop_limit: bool: Whether this SELL order should have a limit on it.
    :return: int: Whether the order placed was a success; `201` == SUCCESS.
    """
    if add_stop_limit:
        # You will ONLY accept a selling price that is `min_sell_perc` or BETTER.
        # BUT... it means that you are NOT guaranteed to sell the stock if the price dips TOO below too fast.
        order_type: str = "TRAILING_STOP_LIMIT"
    else:
        # Once the `min_sell_perc` is hit, the stock will be sold at MARKET price.
        # Which means you MIGHT end up automatically selling your stock at something WAY BELOW the `min_sell_perc`
        order_type: str = "TRAILING_STOP"

    order_params: Dict[str, Any] = {
        "complexOrderStrategyType": "NONE",
        "orderType": order_type,
        "session": "NORMAL",
        "stopPriceLinkBasis": "BID",
        "stopPriceLinkType": "PERCENT",
        # The expected value does NOT want the RAW decimal.
        # It wants the `regular looking number` while pretending to have the `%` sign at the end.
        "stopPriceOffset": min_sell_perc * 100,
        "duration": "DAY",
        "orderStrategyType": "SINGLE",
        "orderLegCollection": [
            {
                "instruction": "SELL",
                "quantity": stock_quantity,
                "instrument": {"symbol": symbol, "assetType": "EQUITY"},
            },
        ],
    }

    if add_stop_limit:
        order_params["priceLinkType"]: str = "PERCENT"
        order_params["priceLinkBasis"]: str = "BID"
        # The expected value does NOT want the RAW decimal.
        # It wants the `regular looking number` while pretending to have the `%` sign at the end.
        order_params["priceOffset"]: float = min_sell_perc * 100

    account_hash: str = get_schwab_acct_hashes()[target_account]
    access_token: str = json_read(file_name_path=TOKEN_JSON_PATH)["access_token"]
    order_response: Response = requests.post(
        url=rf"{ACCOUNT_URL}/accounts/{account_hash}/orders",
        headers={"Authorization": rf"Bearer {access_token}"},
        json=order_params,  # NOTE: use `json` parameter, NOT the `param` parameter for this particular POST
    )
    order_status: int = order_response.status_code
    if order_status != 201:
        raise ValueError(order_response._content)
    return order_status
