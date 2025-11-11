"""Set of Schwab-related utility functions."""

import base64
from typing import Dict, Union

import pandas as pd
import requests
from global_utils.develop_secrets import (
    SCHWAB_APP_KEY,
    SCHWAB_CALLBACK_URL,
    SCHWAB_SECRET_KEY,
)
from global_utils.develop_vars import TEMP_DATA_PATH
from global_utils.sql_utils import SQL_ENGINE
from schwab_api.const_vars import AUTH_URL


# ----------------------------------------------------------------------------------------------------------------------
def get_schwab_acct_hashes() -> Dict[str, str]:
    """Retrieve your Schwab Account Hashes.

    :return: Dict[str, str]: Dictionary with all account hashes.
    """
    sql_query: str = r"SELECT account_name, account_hash FROM global.schwab_accounts"
    df_schwab_accounts: pd.DataFrame = pd.read_sql(sql=sql_query, con=SQL_ENGINE)
    # NOTE: Dictionary with all the Schwab Account Hash nomenclatures
    schwab_acct_hashes: Dict[str, str] = dict(
        zip(df_schwab_accounts["account_name"], df_schwab_accounts["account_hash"])
    )
    return schwab_acct_hashes


# ----------------------------------------------------------------------------------------------------------------------
def get_new_access_token(refresh_token: str) -> str:
    """Get new access token for the given timeframe / execution.

    :param refresh_token: str: The Schwab Refresh token that needs to be used.
    :return: str: The NEW Schwab access token.
    """

    # GET A NEW ACCESS TOKEN USING THE STORED REFRESH TOKEN WHICH LASTS FOR 7 DAYS
    token_data: Dict[str, str] = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "redirect_uri": SCHWAB_CALLBACK_URL,
    }
    # noinspection PyTypeChecker
    encoded_authorization: bytes = base64.b64encode(s=bytes(rf"{SCHWAB_APP_KEY}:{SCHWAB_SECRET_KEY}", "utf-8"))
    encoded_authorization: str = encoded_authorization.decode(encoding="utf-8")
    api_header: Dict[str, str] = {
        "Authorization": rf"Basic {encoded_authorization}",  # type: ignore[str-bytes-safe]
        "Content-Type": r"application/x-www-form-urlencoded",
    }
    token_response: Dict[str, Union[str, int]] = requests.post(
        url=rf"{AUTH_URL}/token",
        headers=api_header,
        data=token_data,
    ).json()
    new_access_token: str = token_response["access_token"]

    return new_access_token


# ----------------------------------------------------------------------------------------------------------------------
def store_new_access_token(refresh_token: str, file_path: str = TEMP_DATA_PATH) -> None:
    """Store a new Schwab Access Token as a JSON file.

    :param file_path: str: Path to file.
    :param refresh_token: str: The Schwab Refresh token that needs to be used.
    """
    from global_utils.general_utils import create_exec_timestamp_str, json_write

    new_access_token: str = get_new_access_token(refresh_token=refresh_token)
    python_dict: Dict[str, str] = {
        "access_token": new_access_token,
        "created_timestamp": create_exec_timestamp_str(),
    }
    json_write(python_dict=python_dict, file_path=file_path, file_name="schwab_token")
