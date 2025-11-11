"""Gets data from the FRED website, processes data, and creates a temp SQL table."""

from typing import Dict, Union

from airflow.models import TaskInstance
from airflow.sdk import Context


# ----------------------------------------------------------------------------------------------------------------------
def create_fred_temp_table(
    symbol: str,
    api_key: str,
    context: Dict[str, Union[Context, TaskInstance]],
) -> None:
    """Gets data from the FRED website, processes data, and creates a temp SQL table.

    :param symbol: str: The FRED nomenclature from the website.
    :param api_key: str: The registered API Key from the FRED website.
    :param context: Union[Context, TaskInstance]: Set of variables that are inherent in
        every Airflow Task function that can be utilized.
    """
    import pandas as pd
    from fredapi import Fred
    from global_utils.sql_utils import SQL_CHUNKSIZE_PANDAS, SQL_ENGINE, TEMP_SCHEMA
    from sqlalchemy import text

    api_engine: Fred = Fred(api_key=api_key)

    begin_market_date: str = context["ti"].xcom_pull(
        task_ids="process_raw_data.get_market_date_ranges",
        key="begin_market_date",
    )
    today_date: str = context["ti"].xcom_pull(task_ids="process_raw_data.get_market_date_ranges", key="today_date")

    print(f"""begin_market_date: {begin_market_date}""")
    # NOTE: THE MAIN API INVOCATION
    # NOTE: EXECUTION OF THE `request`
    fred_data_df: pd.DataFrame = pd.DataFrame(
        data=api_engine.get_series(series_id=symbol, observation_start=begin_market_date, observation_end=today_date)
    )
    print(rf"FRED DF COLUMNS BEFORE: {list(fred_data_df.columns)}")
    symbol_lowercase: str = symbol.lower()
    # Reset the index into a column; the index has all the `market_date`s
    fred_data_df.reset_index(inplace=True)
    fred_data_df.rename(columns={"index": "market_date", 0: symbol_lowercase}, inplace=True)

    with SQL_ENGINE.connect().execution_options(isolation_level="AUTOCOMMIT") as sql_connection:
        sql_query: str = rf"DROP TABLE IF EXISTS {TEMP_SCHEMA}.sm_etl_01_fred_{symbol_lowercase} CASCADE"
        sql_connection.execute(text(sql_query))

        if not fred_data_df.empty:
            print(rf"FRED DF COLUMNS AFTER: {list(fred_data_df.columns)}")
            fred_data_df["market_date"]: pd.Series = fred_data_df["market_date"].dt.strftime("%Y-%m-%d")
            fred_data_df: pd.DataFrame = fred_data_df[fred_data_df["market_date"] >= "1985-01-01"]
            fred_data_df.dropna(inplace=True, ignore_index=True)
            # NOTE: NO NEED to get rid of the `market_date`s that are already in the SQL table
            # NOTE: `parsed_date: datetime = parse(last_market_date) + relativedelta(days=1)`
            fred_data_df.to_sql(
                schema=TEMP_SCHEMA,
                name=rf"sm_etl_01_fred_{symbol_lowercase}",
                con=SQL_ENGINE,
                if_exists="replace",
                index=False,
                chunksize=int(SQL_CHUNKSIZE_PANDAS / len(fred_data_df.columns)),
            )
            sql_query: str = rf"CREATE INDEX ON {TEMP_SCHEMA}.sm_etl_01_fred_{symbol_lowercase} (market_date)"
            sql_connection.execute(text(sql_query))
        else:
            # If the Dataframe is empty, then the `market_date` column is initially a `BIGINT` column data-type
            fred_data_df["market_date"]: pd.Series = fred_data_df["market_date"].astype(str)
            fred_data_df.to_sql(
                schema=TEMP_SCHEMA,
                name=rf"sm_etl_01_fred_{symbol_lowercase}",
                con=SQL_ENGINE,
                if_exists="replace",
                index=False,
                chunksize=int(SQL_CHUNKSIZE_PANDAS / len(fred_data_df.columns)),
            )
