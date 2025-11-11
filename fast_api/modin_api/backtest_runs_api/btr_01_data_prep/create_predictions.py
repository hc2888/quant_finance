"""Create price predictions for various future minute intervals."""

import warnings
from typing import List, Union

import modin.pandas as mpd
import pandas as pd
from global_utils.general_utils import verify_no_null_values
from modin_api.modin_utils import activate_ray, clean_ray_memory, shutdown_ray

# noinspection PyProtectedMember
from pandas._libs.missing import NAType

warnings.filterwarnings(action="ignore")

# ----------------------------------------------------------------------------------------------------------------------
# Define trading start and end in minutes since midnight
START_MINUTES: int = 9 * 60 + 30  # 9:30 = 570 minutes
END_MINUTES: int = 16 * 60  # 16:00 = 960 minutes
SESSION_LENGTH: int = END_MINUTES - START_MINUTES  # 390 minutes total (6.5 hours)


# ----------------------------------------------------------------------------------------------------------------------
def _calc_price_prediction(
    current_close: float,
    target_high: float,
    target_low: float,
    target_close: float,
) -> Union[int, NAType]:
    """Create BUY SELL signals comparing the current price to future prices.

    :param current_close: float: Current closing price.
    :param target_high: float: Future high price.
    :param target_low: float: Future low price.
    :param target_close: float: Future close price.

    :return: Union[int, NAType]: The BUY SELL signal.
    """
    # NOTE: Within a Modin Dataframe, using `pd.NA` seems to work best for `dropna()` purposes
    output_signal: Union[int, NAType] = pd.NA

    if verify_no_null_values(target_func=_calc_price_prediction):
        if all(
            [
                target_high > current_close,
                target_low > current_close,
                target_close > current_close,
            ]
        ):
            output_signal: int = 1
        elif all(
            [
                target_high < current_close,
                target_low < current_close,
                target_close < current_close,
            ]
        ):
            output_signal: int = -1
        else:
            output_signal: int = 0

    return output_signal


# ----------------------------------------------------------------------------------------------------------------------
def main() -> None:
    """Main execution."""
    from datetime import datetime, timedelta

    import numpy as np
    from global_utils.general_utils import BLUE_START, COLOR_END, create_exec_timestamp_str
    from global_utils.sql_utils import SQL_CHUNKSIZE_MODIN, SQL_CONN_STRING, SQL_ENGINE, cx_read_sql
    from sqlalchemy import text
    from sqlalchemy.engine import Row

    with SQL_ENGINE.connect().execution_options(isolation_level="AUTOCOMMIT") as sql_connection:
        sql_query: str = "SELECT DISTINCT symbol FROM temp_tables.btr_predictive_prices"
        # noinspection PyTypeChecker
        sql_result: List[Row] = sql_connection.execute(text(sql_query)).fetchall()
        symbol_list: List[str] = [row[0] for row in sql_result]

    chunk_size: int = 10  # NOTE: Need to only query X `symbol`s worth of data rows at a time to prevent OOM Error
    symbol_chunks: List[str] = [
        str(symbol_list[num : num + chunk_size])[1:-1] for num in range(0, len(symbol_list), chunk_size)
    ]

    exec_timestamp: str = create_exec_timestamp_str()

    activate_ray()

    symbol_set: str
    for symbol_set in symbol_chunks:
        print(f"SYMBOL SET: {symbol_set}")
        start_time: datetime = datetime.now()

        sql_query: str = f"SELECT * FROM temp_tables.btr_predictive_prices WHERE symbol IN ({symbol_set})"
        main_df: mpd.DataFrame = cx_read_sql(sql_query=sql_query, create_partition=False, df_type="modin")

        end_time: datetime = datetime.now()
        runtime: timedelta = end_time - start_time

        hours: int
        remainder: int
        minutes: int
        seconds: int
        hours, remainder = divmod(runtime.seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        print(f"READ SQL RUNTIME --> HOURS: {hours}, MINUTES: {minutes}, SECONDS: {seconds}")

        print(f"EXECUTING FUTURE PREDICTION PRICE OUTCOMES")
        main_df["predict_5"]: mpd.Series = main_df.apply(
            func=lambda row: _calc_price_prediction(
                current_close=row["close"],
                target_high=row["high_5"],
                target_low=row["low_5"],
                target_close=row["close_5"],
            ),
            axis=1,
        )

        main_df["predict_15"]: mpd.Series = main_df.apply(
            func=lambda row: _calc_price_prediction(
                current_close=row["close"],
                target_high=row["high_15"],
                target_low=row["low_15"],
                target_close=row["close_15"],
            ),
            axis=1,
        )

        main_df["predict_30"]: mpd.Series = main_df.apply(
            func=lambda row: _calc_price_prediction(
                current_close=row["close"],
                target_high=row["high_30"],
                target_low=row["low_30"],
                target_close=row["close_30"],
            ),
            axis=1,
        )

        main_df["predict_60"]: mpd.Series = main_df.apply(
            func=lambda row: _calc_price_prediction(
                current_close=row["close"],
                target_high=row["high_60"],
                target_low=row["low_60"],
                target_close=row["close_60"],
            ),
            axis=1,
        )

        print(f"EXECUTING PREVIOUS PRICE OUTCOMES")
        main_df["prev_5"]: mpd.Series = main_df.apply(
            func=lambda row: _calc_price_prediction(
                current_close=row["close"],
                target_high=row["prev_high_5"],
                target_low=row["prev_low_5"],
                target_close=row["prev_close_5"],
            ),
            axis=1,
        )

        main_df["prev_15"]: mpd.Series = main_df.apply(
            func=lambda row: _calc_price_prediction(
                current_close=row["close"],
                target_high=row["prev_high_15"],
                target_low=row["prev_low_15"],
                target_close=row["prev_close_15"],
            ),
            axis=1,
        )

        main_df["prev_30"]: mpd.Series = main_df.apply(
            func=lambda row: _calc_price_prediction(
                current_close=row["close"],
                target_high=row["prev_high_30"],
                target_low=row["prev_low_30"],
                target_close=row["prev_close_30"],
            ),
            axis=1,
        )

        main_df["prev_60"]: mpd.Series = main_df.apply(
            func=lambda row: _calc_price_prediction(
                current_close=row["close"],
                target_high=row["prev_high_60"],
                target_low=row["prev_low_60"],
                target_close=row["prev_close_60"],
            ),
            axis=1,
        )

        # NOTE: Drop rows where there is no predicted outcomes across ALL time horizons
        main_df: mpd.DataFrame = main_df.dropna(
            axis=0,
            how="all",
            subset=[
                "predict_5",
                "predict_15",
                "predict_30",
                "predict_60",
                "prev_5",
                "prev_15",
                "prev_30",
                "prev_60",
            ],
        )

        print(f"EXECUTING FORWARD FILL OF PREVIOUS PRICE OUTCOMES")
        # NOTE: Forward fill missing relative price positions
        main_df: mpd.DataFrame = main_df.sort_values(
            by=["symbol", "market_timestamp"], ascending=True, ignore_index=True
        )
        columns_to_fill: List[str] = ["prev_5", "prev_15", "prev_30", "prev_60"]
        # Group the columns to forward-fill, keeping the original index
        grouped_columns: mpd.core.groupby.DataFrameGroupBy = main_df.groupby(by="symbol")[columns_to_fill]
        # Apply forward-fill transform safely (preserves index alignment)
        main_df[columns_to_fill]: mpd.DataFrame = grouped_columns.transform(func=lambda group: group.ffill())
        # Reset index safely afterward
        main_df: mpd.DataFrame = main_df.reset_index(drop=True)

        print(f"EXECUTING `market_date` CUTOFFS")
        # NOTE: Get rid of the `market_date`s that are already in the SQL table
        main_df: mpd.DataFrame = main_df[main_df["market_date"] > main_df["cutoff_date"]].reset_index(drop=True)
        main_df: mpd.DataFrame = main_df.drop(columns=["cutoff_date"])

        print(f"FILLING METADATA COLUMNS")
        main_df["exec_timestamp"]: mpd.Series = exec_timestamp
        main_df["market_year"]: mpd.Series = mpd.to_datetime(arg=main_df["market_date"]).dt.strftime("%Y")
        main_df["market_month"]: mpd.Series = mpd.to_datetime(arg=main_df["market_date"]).dt.month

        market_timestamp_dt: mpd.Series = mpd.to_datetime(arg=main_df["market_timestamp"])
        main_df["market_clock"]: mpd.Series = market_timestamp_dt.dt.strftime(date_format="%H:%M")

        main_df["market_hour"]: mpd.Series = mpd.to_datetime(arg=main_df["market_timestamp"]).dt.hour
        main_df["market_minute"]: mpd.Series = mpd.to_datetime(arg=main_df["market_timestamp"]).dt.minute

        print(f"FILLING TIME OF DAY SINE & COSINE VALUES")
        # Split 'HH:MM' strings into hours and minutes
        time_parts: mpd.DataFrame = main_df["market_clock"].str.split(":", expand=True)
        hour: mpd.Series = time_parts.iloc[:, 0].astype(int)
        minute: mpd.Series = time_parts.iloc[:, 1].astype(int)
        # Convert to total minutes since midnight
        total_minutes: mpd.Series = hour * 60 + minute
        # Normalize between 0 and 1 relative to trading session
        time_of_day: mpd.Series = (total_minutes - START_MINUTES) / SESSION_LENGTH
        # Clip for safety (ensure no out-of-range values)
        time_of_day: mpd.Series = time_of_day.clip(lower=0.0, upper=1.0)
        # Add cyclical encodings for intraday periodicity
        main_df["time_sin"]: mpd.Series = (time_of_day * 2 * np.pi).map(arg=lambda row: np.sin(row)).round(decimals=4)
        main_df["time_cos"]: mpd.Series = (time_of_day * 2 * np.pi).map(arg=lambda row: np.cos(row)).round(decimals=4)

        data_rows: str = "{:,}".format(len(main_df))
        print(BLUE_START + rf"INSERTING SQL DATA: NUMBER OF ROWS: {data_rows}" + COLOR_END)

        main_df.to_sql(
            schema="backtesting",
            name="target_vars",
            con=SQL_CONN_STRING,
            if_exists="append",
            index=False,
            chunksize=int(SQL_CHUNKSIZE_MODIN / len(main_df.columns)),
        )
        # NOTE: Clean out memory to NOT let Ray memory usage constantly creep up
        del main_df, grouped_columns, market_timestamp_dt, time_parts, hour, minute, total_minutes, time_of_day
        clean_ray_memory()

    shutdown_ray()


# ----------------------------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    main()
