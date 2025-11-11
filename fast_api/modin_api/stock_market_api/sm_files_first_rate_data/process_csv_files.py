"""Process CSV Files from First Rate Data."""

import warnings
from typing import Dict, List

from modin_api.modin_utils import activate_ray, clean_ray_memory, shutdown_ray, verify_no_null_rows

warnings.filterwarnings(action="ignore")
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

INDEX_VARIANTS: List[str] = [
    "full_1day",
    "full_1min",
    "full_5min",
    "full_30min",
    "full_1hour",
]

ETF_VARIANTS: List[str] = [
    "full_1day_UNADJUSTED",
    "full_1day_adjsplit",
    "full_1day_adjsplitdiv",
    "full_1min_UNADJUSTED",
    "full_1min_adjsplit",
    "full_1min_adjsplitdiv",
    "full_5min_adjsplit",
    "full_5min_adjsplitdiv",
    "full_30min_adjsplit",
    "full_30min_adjsplitdiv",
    "full_1hour_adjsplit",
    "full_1hour_adjsplitdiv",
]


# ----------------------------------------------------------------------------------------------------------------------
def main() -> None:
    """Main execution."""
    import os

    import modin.pandas as mpd
    from global_utils.develop_secrets import QUANT_PROJ_PATH
    from global_utils.general_utils import create_exec_timestamp_str, json_read
    from global_utils.sql_utils import SQL_CHUNKSIZE_MODIN, SQL_CONN_STRING

    os.chdir(path=rf"{QUANT_PROJ_PATH}/data_files/first_rate_data")

    symbols_dict: Dict[str, List[str]] = json_read(file_name_path=rf"{QUANT_PROJ_PATH}/temp_data/gather_symbols.json")
    target_symbols: List[str] = symbols_dict["symbols"]

    exec_timestamp: str = create_exec_timestamp_str()

    activate_ray()

    symbol: str
    for symbol in MODEL_PREP_API_SYMBOLS_LIST:
        target_file_list: List[str] = INDEX_VARIANTS if "^" in symbol else ETF_VARIANTS
        asset_type: str = "index" if "^" in symbol else "etf"

        file_name: str
        for file_name in target_file_list:
            if "1min" in file_name:
                time_frame: str = "1min"
            elif "5min" in file_name:
                time_frame: str = "5min"
            elif "30min" in file_name:
                time_frame: str = "30min"
            elif "1hour" in file_name:
                time_frame: str = "60min"
            else:
                time_frame: str = "daily"

            if "adjsplit" in file_name:
                price_type: str = "split_only"
            elif "adjsplitdiv" in file_name:
                price_type: str = "split_div"
            else:
                price_type: str = "non_adj"

            if "^" in symbol:
                target_table: str = rf"{asset_type}_{time_frame}"
            else:
                target_table: str = rf"{asset_type}_{price_type}_{time_frame}"

            if symbol in target_symbols:
                print(rf"TARGET SYMBOL & FILE: {symbol}_{file_name}")
                temp_df: mpd.DataFrame = mpd.read_csv(
                    filepath_or_buffer=f"{symbol}_{file_name}.csv",
                    header=None,
                    names=["timestamp", "open", "high", "low", "close", "volume"],
                )
                temp_df["exec_timestamp"]: mpd.Series = exec_timestamp
                temp_df["market_date"]: mpd.Series = mpd.to_datetime(arg=temp_df["timestamp"]).dt.strftime("%Y-%m-%d")
                temp_df["market_year"]: mpd.Series = mpd.to_datetime(arg=temp_df["timestamp"]).dt.strftime("%Y")
                temp_df["market_month"]: mpd.Series = mpd.to_datetime(arg=temp_df["timestamp"]).dt.month

                if time_frame == "daily":
                    temp_df: mpd.DataFrame = temp_df.drop(columns=["timestamp"])
                else:
                    rename_columns: Dict[str, str] = {"timestamp": "market_timestamp"}
                    temp_df.rename(columns=rename_columns, inplace=True)
                    market_timestamp_dt: mpd.Series = mpd.to_datetime(arg=temp_df["market_timestamp"])
                    temp_df["market_clock"]: mpd.Series = market_timestamp_dt.dt.strftime(date_format="%H:%M")
                    temp_df["market_hour"]: mpd.Series = mpd.to_datetime(arg=temp_df["market_timestamp"]).dt.hour
                    temp_df["market_minute"]: mpd.Series = mpd.to_datetime(arg=temp_df["market_timestamp"]).dt.minute

                temp_df["symbol"]: mpd.Series = symbol
                # For any equity / index that do not have `volume` data.
                temp_df["volume"]: mpd.Series = temp_df["volume"].fillna(value=0)

                verify_no_null_rows(target_df=temp_df)
                temp_df.to_sql(
                    schema="model_prep_api",
                    name=target_table,
                    con=SQL_CONN_STRING,
                    if_exists="append",
                    index=False,
                    chunksize=int(SQL_CHUNKSIZE_MODIN / len(temp_df.columns)),
                )
                # NOTE: Clean out memory to NOT let Ray memory usage constantly creep up
                del temp_df, market_timestamp_dt
                clean_ray_memory()

    shutdown_ray()


# ----------------------------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    main()
