"""Task Group for the Main ETL."""

from typing import Dict, List, Optional, Tuple, Type, Union

from airflow.models import TaskInstance
from airflow.sdk import Context, TaskGroup, task, task_group
from global_airflow.airflow_utils import DEFAULT_TRIGGER

# TODO: Add another key value that is an integer for the dates / splitting thing to maximize API usage
API_DICT: Dict[str, Dict[str, str]] = {
    "etf_non_adj_1min": {"url": "historical-chart/1min", "nonadjusted": "true"},
    "etf_non_adj_daily": {"url": "historical-price-eod/non-split-adjusted", "nonadjusted": "true"},
    "etf_split_only_1min": {"url": "historical-chart/1min", "nonadjusted": "false"},
    "etf_split_only_5min": {"url": "historical-chart/5min", "nonadjusted": "false"},
    "etf_split_only_30min": {"url": "historical-chart/30min", "nonadjusted": "false"},
    "etf_split_only_60min": {"url": "historical-chart/1hour", "nonadjusted": "false"},
    "etf_split_div_daily": {"url": "historical-price-eod/dividend-adjusted", "nonadjusted": "false"},
    "index_1min": {"url": "historical-chart/1min", "nonadjusted": "true"},
    "index_5min": {"url": "historical-chart/5min", "nonadjusted": "true"},
    "index_30min": {"url": "historical-chart/30min", "nonadjusted": "true"},
    "index_60min": {"url": "historical-chart/1hour", "nonadjusted": "true"},
    "index_daily": {"url": "historical-price-eod/non-split-adjusted", "nonadjusted": "true"},
}


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="get_market_dates", trigger_rule=DEFAULT_TRIGGER)
def get_market_dates(target_table: str, **context: Union[Context, TaskInstance]) -> None:
    """Retrieve the set of market dates that need to be processed.

    :param target_table: str: The target SQL data table.
    :param context: Union[Context, Dict[str, str], TaskInstance]: Set of variables that are
        inherent in every Airflow Task function that can be pulled and used.
    """
    import numpy as np
    import pandas as pd
    from global_utils.sql_utils import SQL_CHUNKSIZE_PANDAS, SQL_ENGINE

    # This query always works because you are guaranteed to already have data for the `symbol`s in target SQL table.
    # TODO: Need to get market dates across ETFs and Index
    sql_query: str = f"""
    SELECT symbol, MAX(market_date)::TEXT AS market_date
    FROM model_prep_api.{target_table}
    GROUP BY 1
    """
    main_df: pd.DataFrame = pd.read_sql(sql=sql_query, con=SQL_ENGINE)

    sql_query: str = """SELECT market_date FROM temp_tables.sm_all_market_days ORDER BY market_date ASC"""
    market_dates: pd.DataFrame = pd.read_sql(sql=sql_query, con=SQL_ENGINE)

    min_market_date: Union[str, np.nan] = main_df["market_date"].min()
    min_market_date: str = "1900-01-01" if pd.isna(obj=min_market_date) else min_market_date

    context["ti"].xcom_push(key="min_market_date", value=min_market_date)
    market_dates: pd.DataFrame = market_dates[market_dates["market_date"] >= min_market_date].reset_index(drop=True)

    market_dates.to_sql(
        schema="temp_tables",
        name=target_table,
        con=SQL_ENGINE,
        if_exists="replace",
        index=False,
        chunksize=int(SQL_CHUNKSIZE_PANDAS / len(market_dates.columns)),
    )


# ----------------------------------------------------------------------------------------------------------------------
@task.branch(task_id="data_distro", trigger_rule=DEFAULT_TRIGGER)
def data_distro(target_table: str, cpu_cores_per_task: int = 0, **context: Union[Context, TaskInstance]) -> str:
    """Create distributed set of SQL temp tables to ETL real-time data.

    :param target_table: str: The target SQL data table.
    :param cpu_cores_per_task: int: max distributed CPU cores allowed for the DAG.
    :param context: Union[Context, TaskInstance]: Set of variables that are inherent in
        every Airflow Task function that can be utilized.

    :return: str: Airflow Task to trigger.
    """
    import pandas as pd
    from global_utils.sql_utils import SQL_CHUNKSIZE_PANDAS, SQL_ENGINE
    from sqlalchemy import text

    # This query always works because you are guaranteed to already have data for the `symbol`s in target SQL table.
    # TODO: Need to get market dates across ETFs and Index
    sql_query: str = f"""
    SELECT symbol, MAX(market_date)::TEXT AS market_date
    FROM model_prep_api.{target_table}
    GROUP BY 1
    """
    main_df: pd.DataFrame = pd.read_sql(sql=sql_query, con=SQL_ENGINE)

    with SQL_ENGINE.connect().execution_options(isolation_level="AUTOCOMMIT") as sql_connection:
        sql_query: str = f"SELECT MAX(market_date) FROM temp_tables.{target_table}"
        max_market_date: str = sql_connection.execute(text(sql_query)).scalar()

    context["ti"].xcom_push(key="max_market_date", value=max_market_date)
    print(rf"MAX_MARKET_DATE: {max_market_date}")

    main_df["end_market_date"]: str = max_market_date
    main_df: pd.DataFrame = main_df[main_df["market_date"] < main_df["end_market_date"]].reset_index(drop=True)

    if not main_df.empty:
        # Ensure datetime types
        main_df["market_date"]: pd.Timestamp = pd.to_datetime(arg=main_df["market_date"])
        main_df["end_market_date"]: pd.Timestamp = pd.to_datetime(arg=main_df["end_market_date"])

        main_df: pd.DataFrame = main_df.sort_values(by=["symbol", "market_date"], ascending=True)
        # Expand each row with daily date strings
        main_df: pd.DataFrame = main_df.groupby(by="symbol", as_index=False).apply(
            func=lambda group: pd.DataFrame(
                data={
                    "market_date": pd.date_range(
                        start=group["market_date"].iloc[0],
                        end=group["end_market_date"].iloc[0],
                        freq="D",
                        # NOTE: Get rid of the `market_date`s that are already in the SQL table
                    )[1:].strftime("%Y-%m-%d"),
                    "symbol": group["symbol"].iloc[0],
                }
            )
        )
        main_df: pd.DataFrame = main_df.reset_index(drop=True)

        min_market_date: str = main_df["market_date"].min()
        context["ti"].xcom_push(key="min_market_date", value=min_market_date)
        print(rf"MIN_MARKET_DATE: {min_market_date}")

        sql_query: str = f"""
        SELECT DISTINCT market_date
        FROM temp_tables.{target_table}
        WHERE market_date >= '{min_market_date}'
        AND market_date <= '{max_market_date}'
        ORDER BY market_date ASC
        """
        dates_df: pd.DataFrame = pd.read_sql(sql=sql_query, con=SQL_ENGINE)

        main_df: pd.DataFrame = main_df.merge(right=dates_df, on=["market_date"], how="inner").reset_index(drop=True)

        split_num: int = min(len(main_df), cpu_cores_per_task)
        context["ti"].xcom_push(key="split_num", value=split_num)
        print(rf"TOTAL SPLIT NUM COUNT: {split_num}")

        group_num: int
        for group_num in range(split_num):
            temp_df: pd.DataFrame = main_df.iloc[group_num::cpu_cores_per_task].copy()
            temp_df.to_sql(
                schema="temp_tables",
                name=f"sm_model_prep_{target_table}_{group_num}",
                con=SQL_ENGINE,
                if_exists="replace",
                index=False,
                chunksize=int(SQL_CHUNKSIZE_PANDAS / len(temp_df.columns)),
            )
        task_var: str = r"main_etl.launch_process"
    else:
        task_var: str = r"main_etl.no_data_to_process"
    return task_var


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="process_data", trigger_rule=DEFAULT_TRIGGER)
def process_data(target_table: str, group_num: int = 0, **context: Union[Context, TaskInstance]) -> None:
    """Process Model Prep API data.

    :param target_table: str: The target SQL data table.
    :param group_num: int: Designated number from distributed set of temp SP500 symbol SQL tables.
    :param context: Union[Context, TaskInstance]: Set of variables that are inherent in
        every Airflow Task function that can be utilized.
    """
    import time
    import urllib.error
    from http.client import IncompleteRead, RemoteDisconnected

    import pandas as pd
    import requests
    from global_utils.develop_secrets import MODEL_PREP_API_KEY
    from global_utils.develop_vars import API_MAX_ATTEMPTS
    from global_utils.sql_utils import SQL_CHUNKSIZE_PANDAS, SQL_ENGINE
    from requests.exceptions import RequestException
    from sqlalchemy import text

    exec_timestamp: str = context["ti"].xcom_pull(
        task_ids=r"store_dag_info",
        key="exec_timestamp",
        map_indexes=[None],
    )[0]

    # Use this value to determine if this task will be activated or not
    split_num: int = context["ti"].xcom_pull(
        task_ids=rf"main_etl.{target_table}.data_distro",
        key="split_num",
        map_indexes=[None],
    )[0]
    # Only activate this task if there is a distro table to pull from
    if group_num < split_num:
        sql_query: str = f"""SELECT * FROM temp_tables.sm_model_prep_{target_table}_{group_num}"""
        with SQL_ENGINE.connect().execution_options(isolation_level="AUTOCOMMIT") as sql_connection:
            data_df: pd.DataFrame = pd.read_sql(sql=sql_query, con=sql_connection)
            # noinspection PyTypeChecker
            data_tuple_list: List[Tuple[str, str]] = list(data_df.itertuples(index=False, name=None))

        url_exceptions: Tuple[Type[Exception], ...] = (
            RequestException,
            IncompleteRead,
            urllib.error.URLError,
            RemoteDisconnected,
        )
        # TODO: Add logic that accounts for the count market_date split thing
        market_date: str
        symbol: str
        for market_date, symbol in data_tuple_list:
            # NOTE: THE MAIN API INVOCATION
            # Base URL (without query string)
            base_url: str = r"https://financialmodelingprep.com/stable/historical-chart/1min"
            # Query parameters
            query_params: Dict[str, str] = {
                "symbol": symbol,
                "from": market_date,
                "to": market_date,
                "nonadjusted": "false",
                "apikey": MODEL_PREP_API_KEY,
            }
            url_request_attempts: int = 0
            url_max_attempts: int = API_MAX_ATTEMPTS
            while url_request_attempts < url_max_attempts:
                try:
                    # NOTE: EXECUTION OF THE `request`
                    response: requests.Response = requests.get(url=base_url, params=query_params)
                    api_data: Union[List[str], Dict[str, str], None] = response.json()
                    temp_df: pd.DataFrame = pd.json_normalize(data=api_data)
                    if not temp_df.empty:
                        temp_df: pd.DataFrame = temp_df.sort_values(by="date", ascending=True, ignore_index=True)
                        temp_df["exec_timestamp"]: pd.Series = exec_timestamp

                        timestamp_series: pd.Series = pd.to_datetime(arg=temp_df["date"])
                        temp_df["market_date"]: pd.Series = timestamp_series.dt.strftime("%Y-%m-%d")
                        temp_df["market_year"]: pd.Series = timestamp_series.dt.strftime("%Y")
                        temp_df["market_month"]: pd.Series = timestamp_series.dt.month

                        rename_columns: Dict[str, str] = {"date": "market_timestamp"}
                        temp_df.rename(columns=rename_columns, inplace=True)

                        market_timestamp_dt: pd.Series = pd.to_datetime(arg=temp_df["market_timestamp"])
                        temp_df["market_clock"]: pd.Series = market_timestamp_dt.dt.strftime(date_format="%H:%M")

                        temp_df["market_hour"]: pd.Series = pd.to_datetime(arg=temp_df["market_timestamp"]).dt.hour
                        temp_df["market_minute"]: pd.Series = pd.to_datetime(arg=temp_df["market_timestamp"]).dt.minute
                        temp_df["symbol"]: pd.Series = symbol
                        # For any equity / index that do not have `volume` data.
                        temp_df["volume"]: pd.Series = temp_df["volume"].fillna(value=0)
                        temp_df.to_sql(
                            schema="model_prep_api",
                            name=target_table,
                            con=SQL_ENGINE,
                            if_exists="append",
                            index=False,
                            chunksize=int(SQL_CHUNKSIZE_PANDAS / len(temp_df.columns)),
                        )
                        df_length: int = len(temp_df)
                        insert_msg: str = rf"SYMBOL: {symbol}, MARKET_DATE: {market_date}; {df_length} ROWS PROCESSED"
                        print(insert_msg)
                        break
                    else:
                        no_data_msg: str = rf"SYMBOL: {symbol}, MARKET_DATE: {market_date}; NO DATA TO PROCESS"
                        print(no_data_msg)
                        break
                except url_exceptions as error_msg:
                    url_request_attempts += 1
                    attempt_msg: str = rf"URL_REQUEST_ATTEMPT: {url_request_attempts} OF {url_max_attempts}"
                    print(attempt_msg)
                    error_msg: str = rf"URL LIB ERROR MSG: {error_msg}"
                    print(error_msg)
                    time.sleep(30)
                    continue
            if url_request_attempts >= url_max_attempts:
                max_attempts_msg: str = r"MAX `url_request_attempts` EXECUTED; MOVING ON TO NEXT SYMBOL"
                print(max_attempts_msg)
                failed_vars_msg: str = rf"FAILED VARIABLES: SYMBOL: {symbol}; MARKET_DATE: {market_date}"
                print(failed_vars_msg)
                break
        drop_table_query: str = rf"DROP TABLE IF EXISTS temp_tables.sm_model_prep_{target_table}_{group_num} CASCADE"
        with SQL_ENGINE.connect().execution_options(isolation_level="AUTOCOMMIT") as sql_connection:
            sql_connection.execute(text(drop_table_query))


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="dedupe_sql_table", trigger_rule=DEFAULT_TRIGGER)
def dedupe_sql_table(target_table: str, **context: Union[Context, TaskInstance]) -> None:
    """De-duplicate the target SQL table.

    :param target_table: str: The target SQL data table.
    :param context: Union[Context, TaskInstance]: Set of variables that are inherent in every Airflow Task function
        that can be utilized.
    """
    from global_utils.sql_utils import dedupe_sql_table

    min_market_date: str = context["ti"].xcom_pull(
        task_ids=f"main_etl.{target_table}.data_distro",
        key="min_market_date",
    )
    max_market_date: str = context["ti"].xcom_pull(
        task_ids=f"main_etl.{target_table}.data_distro",
        key="max_market_date",
    )
    # Deduplicate the SQL Table
    partition_by_cols: List[str] = ["market_date", "symbol", "market_timestamp"]
    dedupe_sql_table(
        sql_schema="model_prep_api",
        sql_table=target_table,
        partition_by_cols=partition_by_cols,
        begin_market_date=min_market_date,
        end_market_date=max_market_date,
    )


# ----------------------------------------------------------------------------------------------------------------------
def create_main_etl(target_table: str, cpu_cores_per_task: int = 0) -> TaskGroup:
    """Creates Task Group for the target combination `price_type` and `time_interval`.

    :param target_table: str: The target SQL data table.
    :param cpu_cores_per_task: int: CPU count for set of dynamic Airflow Tasks.

    :return: TaskGroup: The Airflow Task Group.
    """

    @task_group(group_id=target_table)
    def main_etl() -> None:
        """Task Group for the main ETL process."""
        from airflow.providers.standard.operators.empty import EmptyOperator
        from airflow.providers.standard.operators.python import PythonOperator

        tg_finish: EmptyOperator = EmptyOperator(task_id="tg_finish", trigger_rule=DEFAULT_TRIGGER)

        branch_data_distro: str = data_distro(target_table=target_table, cpu_cores_per_task=cpu_cores_per_task)
        launch_process: EmptyOperator = EmptyOperator(task_id="launch_process", trigger_rule=DEFAULT_TRIGGER)

        no_data_to_process: EmptyOperator = EmptyOperator(task_id="no_data_to_process", trigger_rule=DEFAULT_TRIGGER)

        # What is the maximum number of PARALLEL-IZED Dynamic Airflow Tasks that occur at any point in the DAG?
        distro_range_list: List[int] = [num for num in range(cpu_cores_per_task)]

        process_data_tasks_partial: PythonOperator = process_data.partial(target_table=target_table)
        # noinspection PyUnresolvedReferences
        process_data_tasks: PythonOperator = process_data_tasks_partial.expand(group_num=distro_range_list)

        """BEGIN FLOW."""
        # noinspection PyTypeChecker,PyUnresolvedReferences
        (get_market_dates(target_table=target_table) >> branch_data_distro)
        """MAIN FLOW."""
        # noinspection PyTypeChecker,PyUnresolvedReferences
        (
            branch_data_distro
            >> launch_process
            >> process_data_tasks
            >> dedupe_sql_table(target_table=target_table)
            >> tg_finish
        )
        """NO DATA FLOW."""
        # noinspection PyTypeChecker,PyUnresolvedReferences
        (branch_data_distro >> no_data_to_process >> tg_finish)

    # noinspection PyTypeChecker
    return main_etl()


# ----------------------------------------------------------------------------------------------------------------------
@task_group(group_id="main_etl")
def tg_01_main_etl() -> None:
    """Task Group for the main ETL process."""
    from global_utils.general_utils import calc_cpu_cores_per_task

    # NOTE: All available intraday time intervals
    tables_list: List[str] = [
        "etf_non_adj_1min",
        "etf_non_adj_daily",
        "etf_split_only_1min",
        "etf_split_only_5min",
        "etf_split_only_30min",
        "etf_split_only_60min",
        "etf_split_div_daily",
        "index_1min",
        "index_5min",
        "index_30min",
        "index_60min",
        "index_daily",
    ]
    # What is the maximum number of PARALLEL-IZED Dynamic Airflow Tasks that occur at any point in the DAG?
    num_max_parallel_dynamic_tasks: int = len(tables_list)
    cpu_cores_per_task: int = calc_cpu_cores_per_task(num_max_parallel_dynamic_tasks=num_max_parallel_dynamic_tasks)

    main_etl_group_list: List[Optional[TaskGroup]] = []
    table_name: str
    for table_name in tables_list:
        main_etl_group_list.append(create_main_etl(target_table=table_name, cpu_cores_per_task=cpu_cores_per_task))
