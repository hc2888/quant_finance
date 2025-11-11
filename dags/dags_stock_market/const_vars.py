"""All static / constant variables used throughout DAGs."""

from typing import Dict, List, Union

from global_utils.develop_secrets import (
    AAA10Y_KEY,
    DAAA_KEY,
    DGS1_KEY,
    T10Y2Y_KEY,
    T10YFF_KEY,
    USEPUINDXD_KEY,
    WLEMUINDXD_KEY,
)
from sql_infra.universal_vars import DEFAULT_COLUMNS_LIST, INTRADAY_COLUMNS_LIST

# ----------------------------------------------------------------------------------------------------------------------
# NOTE: NO PRICE IN `sharadar.daily_prices` CAN BE GREATER OR LOWER THAN THESE VALUES; PERIODICALLY UPDATE THIS.
# NOTE: RESEARCH ONLINE AND GAUGE WHAT THE BEST GREATER AND LOWER VALUES SHOULD BE.
SHARADAR_PRICE_THRESHOLD: int = 10_000
SHARADAR_PRICE_MINIMUM: Union[int, float] = 0.01
# ----------------------------------------------------------------------------------------------------------------------

# noinspection PyTypeChecker
MATH_INDEX_LIST: List[str] = ["fred"]
# ----------------------------------------------------------------------------------------------------------------------
ALPHA_VANTAGE_REAL_TIME_COLUMNS_LIST: List[str] = INTRADAY_COLUMNS_LIST
# ----------------------------------------------------------------------------------------------------------------------
MODEL_PREP_INTRA_COLUMNS_LIST: List[str] = INTRADAY_COLUMNS_LIST
# ----------------------------------------------------------------------------------------------------------------------
SHARADAR_DAILY_PRICES_LIST_ORIG: List[str] = [
    "symbol TEXT NOT NULL",
    "open NUMERIC(7,2) NOT NULL",
    "high NUMERIC(7,2) NOT NULL",
    "low NUMERIC(7,2) NOT NULL",
    "close NUMERIC(7,2) NOT NULL",
    "close_adj NUMERIC(7,2) NOT NULL",
    "close_un_adj NUMERIC(7,2) NOT NULL",
    "volume BIGINT NOT NULL",
]
SHARADAR_DAILY_PRICES_COLUMNS_LIST: List[str] = DEFAULT_COLUMNS_LIST + SHARADAR_DAILY_PRICES_LIST_ORIG

SHARADAR_EXCLUDE_SYMBOLS_LIST_ORIG: List[str] = [
    "symbol TEXT NOT NULL",
    "lastpricedate TEXT NOT NULL",
    "name TEXT",
    "exchange TEXT",
    "isdelisted TEXT",
    "category TEXT",
    "cusips TEXT",
    "siccode INTEGER",
    "sicsector TEXT",
    "sicindustry TEXT",
    "famasector TEXT",
    "famaindustry TEXT",
    "sector TEXT",
    "industry TEXT",
    "scalemarketcap TEXT",
    "scalerevenue TEXT",
    "relatedtickers TEXT",
    "currency TEXT",
    "location TEXT",
    "secfilings TEXT",
]
SHARADAR_EXCLUDE_SYMBOLS_COLUMNS_LIST: List[str] = DEFAULT_COLUMNS_LIST + SHARADAR_EXCLUDE_SYMBOLS_LIST_ORIG
# ----------------------------------------------------------------------------------------------------------------------
"""
All the symbol nomenclatures from the FRED website that are currently being used.

Also has the name to identify the FRED metric for Python scripts & Airflow.
"""

EQUITY_MARKET_UNCERTAINTY_DICT: Dict[str, str] = {
    "symbol": "WLEMUINDXD",
    "api_key": WLEMUINDXD_KEY,
    "name": "equity_market_uncertainty",
}
ECON_POLICY_UNCERTAINTY_DICT: Dict[str, str] = {
    "symbol": "USEPUINDXD",
    "api_key": USEPUINDXD_KEY,
    "name": "econ_policy_uncertainty",
}
TREASURY_1_YR_DICT: Dict[str, str] = {
    "symbol": "DGS1",
    "api_key": DGS1_KEY,
    "name": "treasury_1_yr",
}
TREASURY_10_YR_MINUS_TREASURY_2_YR_DICT: Dict[str, str] = {  # NOTE: USE THIS TO CALCULATE INVERTED YIELD CURVE
    "symbol": "T10Y2Y",
    "api_key": T10Y2Y_KEY,
    "name": "treasury_10_yr_minus_treasury_2_yr",
}
TREASURY_10_YR_MINUS_FED_RATE_DICT: Dict[str, str] = {
    "symbol": "T10YFF",
    "api_key": T10YFF_KEY,
    "name": "treasury_10_yr_minus_fed_rate",
}
AAA_CORP_BOND_DICT: Dict[str, str] = {
    "symbol": "DAAA",
    "api_key": DAAA_KEY,
    "name": "aaa_corp_bond",
}
AAA_CORP_BOND_TO_TREASURY_10_YR_DICT: Dict[str, str] = {
    "symbol": "AAA10Y",
    "api_key": AAA10Y_KEY,
    "name": "aaa_corp_bond_to_treasury_10_yr",
}
# ----------------------------------------------------------------------------------------------------------------------
FRED_SYMBOLS_LIST: List[str] = ["WLEMUINDXD", "USEPUINDXD", "DGS1", "T10Y2Y", "T10YFF", "DAAA", "AAA10Y"]

FRED_DICT_LIST: List[Dict[str, str]] = [
    EQUITY_MARKET_UNCERTAINTY_DICT,
    ECON_POLICY_UNCERTAINTY_DICT,
    TREASURY_1_YR_DICT,
    TREASURY_10_YR_MINUS_TREASURY_2_YR_DICT,
    TREASURY_10_YR_MINUS_FED_RATE_DICT,
    AAA_CORP_BOND_DICT,
    AAA_CORP_BOND_TO_TREASURY_10_YR_DICT,
]

FRED_COLUMNS_LIST: List[str] = DEFAULT_COLUMNS_LIST

fred_symbol: str
for fred_symbol in FRED_SYMBOLS_LIST:
    FRED_COLUMNS_LIST.append(f"""{fred_symbol} NUMERIC(7,2) NOT NULL""")
# ----------------------------------------------------------------------------------------------------------------------
ALPHA_VANTAGE_ETF_DICT: Dict[str, str] = {  # TOTAL ETF's: 24
    "ACWI": "2008-03-26",  # Shares MSCI ACWI ETF; Global Economy INCLUDING U.S.
    "ACWX": "2008-04-01",  # iShares MSCI ACWI ex U.S. ETF; Global Economy EXCLUDING U.S.
    "AGG": "2003-09-23",  # iShares Core US Aggregate Bond ETF
    "ANGL": "2012-04-10",  # VanEck Vectors Fallen Angel High Yield Bond ETF
    "BIL": "2007-05-25",  # SPDR Bloomberg 1-3 Month T-Bill ETF
    "BND": "2007-04-03",  # Vanguard Total Bond Market Index Fund ETF
    "CWI": "2007-01-10",  # SPDR MSCI ACWI ex-US ETF
    "EDV": "2008-01-29",  # Vanguard Extended Duration Treasury ETF
    "GLD": "2004-11-08",  # SPDR Gold Shares
    "GOVT": "2012-02-14",  # iShares U.S. Treasury Bond ETF
    "HYG": "2007-04-04",  # iShares iBoxx $ High Yield Corporate Bond ETF
    "HYHG": "2013-05-21",  # ProShares High Yield - Interest Rate Hedged ETF
    "IEF": "2002-07-22",  # iShares 7-10 Year Treasury Bond ETF
    "IYR": "2005-11-30",  # iShares U.S. Real Estate ETF
    "JNK": "2007-12-01",  # SPDR Bloomberg High Yield Bond ETF
    "PHB": "2007-11-15",  # Invesco Fundamental High Yield Corporate Bond ETF
    "QTUM": "2018-09-04",  # Defiance Quantum
    "SDS": "2006-07-11",  # ProShares UltraShort S&P 500 ETF
    "SH": "2006-06-19",  # ProShares Short S&P 500 ETF
    "SHV": "2007-01-05",  # iShares Short Treasury Bond ETF
    "SPAB": "2007-06-05",  # SPDR Portfolio Aggregate Bond ETF
    "SPLG": "2010-01-19",  # SPDR Portfolio S&P 500 ETF
    "SPY": "2000-01-03",  # NOTE: SPDR S&P 500; The fund is the largest and oldest ETF in the USA.
    "TLT": "2002-07-22",  # iShares 20+ Year Treasury Bond ETF
    "UUP": "2007-03-01",  # Invesco DB US Dollar Index Bullish Fund
    "UVIX": "2022-03-28",  # 2x Long VIX Futures
    "UVXY": "2011-10-04",  # ProShares Ultra VIX Short-Term Futures ETF
    "VGLT": "2010-01-01",  # Vanguard Long-Term Treasury Index Fund ETF
    "VGSH": "2009-11-23",  # Vanguard Short-Term Treasury Index Fund ETF
    "VT": "2008-06-24",  # Vanguard Total World Stock ETF
    "VXX": "2009-03-02",  # iPath Series B S&P 500 VIX Short-Term Futures ETN
    "XLI": "2000-12-01",  # Industrial Select Sector SPDR Fund
}
# ----------------------------------------------------------------------------------------------------------------------
MODEL_PREP_API_SYMBOLS_LIST: List[str] = [
    # NOTE: US Indices
    "^COR3M",  # CBOE 3-MONTH IMPLIED CORRELATION INDEX
    "^DJI",  # DOW JONES INDUSTRIAL AVERAGE
    "^GSPC",  # S&P 500
    "^GVZ",  # CBOE GOLD VOLATILITY INDEX
    "^IXIC",  # NASDAQ COMPOSITE
    "^NYA",  # NYSE COMPOSITE
    "^OEX",  # S&P 100
    "^OVX",  # CBOE CRUDE OIL VOLATILITY INDEX
    "^RUA",  # RUSSELL 3000
    "^RUI",  # RUSSELL 1000
    "^RUT",  # RUSSELL 2000
    "^RVX",  # RUSSELL VOLATILITY INDEX
    "^VIF",  # CBOE FAR TERM VIX INDEX
    "^VIN",  # CBOE NEAR-TERM VIX INDEX
    "^VIX",  # CBOE VOLATILITY INDEX
    "^VIX3M",  # CBOE S&P 500 3 MONTH VOLATILITY
    "^VIX6M",  # CBOE S&P 500 6 MONTH VOLATILITY
    "^VWA",  # CBOE VIX INDICATIVE ASK INDEX
    "^VWB",  # CBOE VIX INDICATIVE BID INDEX
    "^VXN",  # CBOE NASDAQ VOLATILITY INDEX
    "^VXTLT",  # 20+ YEAR TREASURY BOND VOLATILITY INDEX
    "^XAX",  # NYSE AMERICAN COMPOSITE INDEX
    # NOTE: Exchange Traded Funds (ETFs)
    "ACWI",  # Shares MSCI ACWI ETF; Global Economy INCLUDING U.S.
    "ACWX",  # iShares MSCI ACWI ex U.S. ETF; Global Economy EXCLUDING U.S.
    "AGG",  # iShares Core US Aggregate Bond ETF
    "ANGL",  # VanEck Vectors Fallen Angel High Yield Bond ETF
    "BIL",  # SPDR Bloomberg 1-3 Month T-Bill ETF
    "BND",  # Vanguard Total Bond Market Index Fund ETF
    "CWI",  # SPDR MSCI ACWI ex-US ETF
    "EDV",  # Vanguard Extended Duration Treasury ETF
    "GLD",  # SPDR Gold Shares
    "GOVT",  # iShares U.S. Treasury Bond ETF
    "HYG",  # iShares iBoxx $ High Yield Corporate Bond ETF
    "HYHG",  # ProShares High Yield - Interest Rate Hedged ETF
    "IEF",  # iShares 7-10 Year Treasury Bond ETF
    "IYR",  # iShares U.S. Real Estate ETF
    "JNK",  # SPDR Bloomberg High Yield Bond ETF
    "PHB",  # Invesco Fundamental High Yield Corporate Bond ETF
    "QTUM",  # Defiance Quantum
    "SDS",  # ProShares UltraShort S&P 500 ETF
    "SH",  # ProShares Short S&P 500 ETF
    "SHV",  # iShares Short Treasury Bond ETF
    "SPAB",  # SPDR Portfolio Aggregate Bond ETF
    "SPLG",  # SPDR Portfolio S&P 500 ETF
    "SPY",  # NOTE: SPDR S&P 500; The fund is the largest and oldest ETF in the USA.
    "TLT",  # iShares 20+ Year Treasury Bond ETF
    "UUP",  # Invesco DB US Dollar Index Bullish Fund
    "UVIX",  # 2x Long VIX Futures
    "UVXY",  # ProShares Ultra VIX Short-Term Futures ETF
    "VGLT",  # Vanguard Long-Term Treasury Index Fund ETF
    "VGSH",  # Vanguard Short-Term Treasury Index Fund ETF
    "VT",  # Vanguard Total World Stock ETF
    "VXX",  # iPath Series B S&P 500 VIX Short-Term Futures ETN
    "XLI",  # Industrial Select Sector SPDR Fund
]
# ----------------------------------------------------------------------------------------------------------------------
SHARADAR_INDEX_LIST: List[str] = [
    "^DJI",  # DOW JONES INDUSTRIAL AVERAGE
    "^GSPC",  # S&P 500
    "^IXIC",  # NASDAQ COMPOSITE
    "^RUT",  # RUSSELL 2000
    "^VIX",  # CBOE VOLATILITY INDEX
]
