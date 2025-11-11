"""Retrieve Schwab Token to either update or retrieve a token for the first time."""

import base64
from typing import Dict, List, Union

import pandas as pd
import requests
from global_utils.develop_secrets import (
    QUANT_PROJ_PATH,
    SCHWAB_ACCOUNTS_INFO,
    SCHWAB_APP_KEY,
    SCHWAB_CALLBACK_URL,
    SCHWAB_SECRET_KEY,
)
from global_utils.general_utils import create_exec_timestamp_str
from global_utils.sql_utils import SQL_ENGINE
from schwab_api.const_vars import ACCOUNT_URL, AUTH_URL
from schwab_api.util_funcs import store_new_access_token

# ----------------------------------------------------------------------------------------------------------------------
authorize_string_list: List[str] = [
    rf"{AUTH_URL}/authorize",
    rf"?client_id={SCHWAB_APP_KEY}",
    rf"&redirect_uri={SCHWAB_CALLBACK_URL}",
]

authorize_url: str = "".join(authorize_string_list)
print(authorize_url)

return_link: str = input("Paste The Authorization URL Here:")
link_code: str = return_link[return_link.index(r"code=") + 5 : return_link.index(r"%40")]
link_code: str = rf"{link_code}@"

# noinspection PyTypeChecker
encoded_authorization: bytes = base64.b64encode(s=bytes(rf"{SCHWAB_APP_KEY}:{SCHWAB_SECRET_KEY}", "utf-8"))
encoded_authorization: str = encoded_authorization.decode(encoding="utf-8")
api_header: Dict[str, str] = {
    "Authorization": rf"Basic {encoded_authorization}",  # type: ignore[str-bytes-safe]
    "Content-Type": r"application/x-www-form-urlencoded",
}

api_data: Dict[str, str] = {
    "grant_type": "authorization_code",
    "code": link_code,
    "redirect_uri": SCHWAB_CALLBACK_URL,
}

api_response: Dict[str, Union[str, int]] = requests.post(
    url=rf"{AUTH_URL}/token",
    headers=api_header,
    data=api_data,
).json()

refresh_token: str = api_response["refresh_token"]  # VALID FOR 7 DAYS

data_dict: Dict[str, List[str]] = {
    "refresh_token": [refresh_token],
    "stored_timestamp": [create_exec_timestamp_str()],
    "expired_timestamp": [create_exec_timestamp_str(future_days=7)],
}
df_data: pd.DataFrame = pd.DataFrame(data=data_dict)
df_data.to_sql(
    schema="global",
    name="schwab_token",
    con=SQL_ENGINE,
    if_exists="replace",
    index=False,
    chunksize=10_000,
)
print(r"NEW REFRESH TOKEN INSERTED INTO `schwab_token` SQL TABLE")

store_new_access_token(refresh_token=refresh_token, file_path=rf"{QUANT_PROJ_PATH}/temp_data")
print(r"NEW ACCESS TOKEN STORED AS JSON")

# ----------------------------------------------------------------------------------------------------------------------
access_token: str = api_response["access_token"]  # VALID FOR 30 MINUTES

account_info_list: List[Dict[str, str]] = requests.get(
    url=rf"{ACCOUNT_URL}/accounts/accountNumbers",
    headers={"Authorization": rf"Bearer {access_token}"},
).json()

pandas_data_list: List[Dict[str, str]] = []
num: int
for num in range(len(account_info_list)):
    account_num: str = account_info_list[num]["accountNumber"]
    account_name: str = SCHWAB_ACCOUNTS_INFO[account_num]
    account_hash: str = account_info_list[num]["hashValue"]
    temp_data_dict: Dict[str, str] = {
        "account_num": account_num,
        "account_name": account_name,
        "account_hash": account_hash,
    }
    pandas_data_list.append(temp_data_dict)

df_accounts: pd.DataFrame = pd.DataFrame(data=pandas_data_list)
df_accounts.to_sql(
    schema="global",
    name="schwab_accounts",
    con=SQL_ENGINE,
    if_exists="replace",
    index=False,
    chunksize=10_000,
)
print(r"UPDATED ACTIVE ACCOUNTS INSERTED INTO `schwab_accounts` SQL TABLE")
