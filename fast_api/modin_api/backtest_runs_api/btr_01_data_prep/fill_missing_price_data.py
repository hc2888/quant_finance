"""Fill in missing price data for every `symbol`s `market_timestamp`."""

import warnings
from typing import List

import modin.pandas as mpd
from modin_api.modin_utils import activate_ray, clean_ray_memory, shutdown_ray, verify_no_null_rows

warnings.filterwarnings(action="ignore")


# ----------------------------------------------------------------------------------------------------------------------
def main() -> None:
    """Main execution."""
    from datetime import datetime, timedelta

    from global_utils.general_utils import create_exec_timestamp_str
    from global_utils.sql_utils import SQL_CHUNKSIZE_MODIN, SQL_CONN_STRING, SQL_ENGINE, cx_read_sql
    from sqlalchemy import text
    from sqlalchemy.engine import Row

    with SQL_ENGINE.connect().execution_options(isolation_level="AUTOCOMMIT") as sql_connection:
        sql_query: str = "SELECT DISTINCT symbol FROM temp_tables.btr_missing_prices"
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

        sql_query: str = f"SELECT * FROM temp_tables.btr_missing_prices WHERE symbol IN ({symbol_set})"
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

        main_df["volume"]: mpd.Series = main_df["volume"].fillna(value=0)
        main_df: mpd.DataFrame = main_df.sort_values(
            by=["symbol", "market_timestamp"], ascending=True, ignore_index=True
        )

        print(f"FILLING MISSING PRICES")
        main_df: mpd.DataFrame = main_df.groupby(by=["symbol"]).apply(func=lambda group: group.ffill().bfill())
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

        verify_no_null_rows(target_df=main_df)
        main_df.to_sql(
            schema="backtesting",
            name="intraday",
            con=SQL_CONN_STRING,
            if_exists="append",
            index=False,
            chunksize=int(SQL_CHUNKSIZE_MODIN / len(main_df.columns)),
        )
        # NOTE: Clean out memory to NOT let Ray memory usage constantly creep up
        del main_df
        clean_ray_memory()

    shutdown_ray()


# ----------------------------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    main()
