"""All static / constant variables used throughout Schwab API module."""

from global_utils.develop_vars import TEMP_DATA_PATH

# ----------------------------------------------------------------------------------------------------------------------
AUTH_URL: str = r"https://api.schwabapi.com/v1/oauth"
ACCOUNT_URL: str = r"https://api.schwabapi.com/trader/v1"
MARKET_URL: str = r"https://api.schwabapi.com/marketdata/v1"
# This has the `access_token` for real-time Schwab API executions
TOKEN_JSON_PATH: str = rf"{TEMP_DATA_PATH}/schwab_token.json"
