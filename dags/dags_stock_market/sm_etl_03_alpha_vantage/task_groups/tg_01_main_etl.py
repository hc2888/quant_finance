"""Task group for the main ETL."""

from typing import Dict, Iterator, List, Literal, Optional, Tuple, Type, Union

from airflow.models import TaskInstance
from airflow.sdk import Context, TaskGroup, task, task_group
from global_airflow.airflow_utils import DEFAULT_TRIGGER

# ----------------------------------------------------------------------------------------------------------------------
# DAG-SPECIFIC VARIABLES
TARGET_SCHEMA: str = "alpha_vantage"


# ----------------------------------------------------------------------------------------------------------------------
@task.branch(task_id="etf_distro", trigger_rule=DEFAULT_TRIGGER)
def etf_distro(
    cpu_cores_per_task: int = 0,
    price_type: Literal["adj", "non_adj"] = "non_adj",
    time_interval: Literal["1min", "5min", "15min", "30min", "60min"] = "1min",
    **context: Union[Context, TaskInstance],
) -> str:
    """Takes `ALPHA_VANTAGE_ETF_DICT` and creates temp distro tables.

    :param cpu_cores_per_task: int: max distributed CPU cores allowed for the DAG.
    :param price_type: Literal["adj", "non_adj"]: Whether the price data is `adjusted` or `non-adjusted`.
    :param time_interval: Literal["1min", "5min", "15min", "30min", "60min"]: The price intraday interval.
    :param context: Union[Context, TaskInstance]: Set of variables that are inherent in
        every Airflow Task function that can be utilized.

    :return: str: Airflow Task to trigger.
    """
    from datetime import datetime, timedelta

    import pandas as pd
    import pytz
    from dags_stock_market.const_vars import ALPHA_VANTAGE_ETF_DICT
    from dags_stock_market.sm_etl_03_alpha_vantage.util_funcs import generate_combinations
    from dateutil.parser import parse
    from dateutil.relativedelta import relativedelta
    from global_utils.sql_utils import SQL_CHUNKSIZE_PANDAS, SQL_ENGINE
    from sqlalchemy import text

    execute_backfill: bool = context["params"]["execute_backfill"]
    if execute_backfill:
        with SQL_ENGINE.connect().execution_options(isolation_level="AUTOCOMMIT") as sql_connection:
            # Delete all the data from the SQL table beforehand
            delete_intraday: str = f"""
            DELETE FROM backtesting.intraday
            WHERE symbol IN (SELECT DISTINCT symbol FROM {TARGET_SCHEMA}.{price_type}_{time_interval})"""
            sql_connection.execute(text(delete_intraday))

            truncate_query: str = rf"TRUNCATE TABLE {TARGET_SCHEMA}.{price_type}_{time_interval} CASCADE"
            sql_connection.execute(text(truncate_query))

    symbols_list: List[str] = list(ALPHA_VANTAGE_ETF_DICT.keys())
    symbols_str: str = str(symbols_list)[1:-1]
    sql_query: str = f"""
    SELECT symbol, MAX(market_date) AS market_date
    FROM {TARGET_SCHEMA}.{price_type}_{time_interval}
    WHERE symbol IN ({symbols_str})
    GROUP BY 1
    """
    main_df: pd.DataFrame = pd.read_sql(sql=sql_query, con=SQL_ENGINE)
    main_df: pd.DataFrame = main_df.set_index(keys="symbol")
    # NOTE: Latest date can only be 2 weeks ago at the LATEST because NEWER data is SOMETIMES not available.
    # NOTE: Sometimes you CAN end up with more recent data because the API downloads data for the entire month.
    two_weeks_ago: datetime = datetime.now().astimezone(pytz.timezone(zone="US/Eastern")) - timedelta(days=14)
    # If the day is Saturday (5), subtract 1 day to go back to Friday
    # If the day is Sunday (6), subtract 2 days to go back to Friday
    if two_weeks_ago.weekday() == 5:  # Saturday
        two_weeks_ago: datetime = two_weeks_ago - timedelta(days=1)
    elif two_weeks_ago.weekday() == 6:  # Sunday
        two_weeks_ago: datetime = two_weeks_ago - timedelta(days=2)
    two_weeks_ago: str = two_weeks_ago.strftime(r"%Y-%m-%d")

    max_market_date_dict: Dict[str, str] = {}
    symbol: str
    for symbol in symbols_list:
        # NOTE: You have to subtract ONE day because of the `cutoff_date` mechanism within the `process_data()` task
        earliest_date_str: str = ALPHA_VANTAGE_ETF_DICT[symbol]
        earliest_date_dt: datetime = parse(earliest_date_str) - relativedelta(days=1)
        earliest_date: str = earliest_date_dt.strftime("%Y-%m-%d")  # type: ignore[call-arg]

        max_market_date: str = main_df.at[symbol, "market_date"] if symbol in main_df.index else earliest_date
        print(rf"SYMBOL: {symbol}; MAX MARKET DATE: {max_market_date}; TWO WEEKS AGO DATE: {two_weeks_ago}")
        if max_market_date < two_weeks_ago:
            max_market_date_dict[symbol]: str = max_market_date

    if max_market_date_dict:
        print(rf"MAX MARKET DATE DICTIONARY: {max_market_date_dict}")
        etf_list: List[Tuple[str, str, str]] = generate_combinations(max_market_date_dict=max_market_date_dict)
        begin_market_date: str = min(list(max_market_date_dict.values()))

        # Variables used in the `dedupe_sql_table()` Task
        context["ti"].xcom_push(key="begin_market_date", value=begin_market_date)
        print(rf"BEGIN_MARKET_DATE: {begin_market_date}")
        context["ti"].xcom_push(key="two_weeks_ago", value=two_weeks_ago)
        print(rf"TWO_WEEKS_AGO: {two_weeks_ago}")

        split_num: int = min(len(etf_list), cpu_cores_per_task)
        context["ti"].xcom_push(key="split_num", value=split_num)
        print(rf"TOTAL SPLIT NUM COUNT: {split_num}")

        # Splits out the list of all `symbol` / `market month` pairings to evenly distributed lists.
        # Turns them into Pandas Dataframes and creates temp SQL tables for each Dataframe.
        group_num: int
        for group_num in range(split_num):
            temp_list: List[Tuple[str, str, str]] = etf_list[group_num::cpu_cores_per_task]
            temp_df: pd.DataFrame = pd.DataFrame(data=temp_list, columns=["symbol", "market_month", "cutoff_date"])
            temp_df.to_sql(
                schema="temp_tables",
                name=f"sm_{price_type}_{time_interval}_etf_distro_{group_num}",
                con=SQL_ENGINE,
                if_exists="replace",
                index=False,
                chunksize=int(SQL_CHUNKSIZE_PANDAS / len(temp_df.columns)),
            )
        # Since `process_data` is a MAPPED Task, Airflow does not to skip these MAPPED tasks
        # when a MAPPED Airflow Task comes IMMEDIATELY after an Airflow BRANCH task
        task_var: str = rf"main_etl.{price_type}_{time_interval}_etl.launch_etl"
    else:
        task_var: str = rf"main_etl.{price_type}_{time_interval}_etl.no_data_to_process"
    return task_var


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="process_data", trigger_rule=DEFAULT_TRIGGER)
def process_data(
    group_num: int = 0,
    price_type: Literal["adj", "non_adj"] = "non_adj",
    time_interval: Literal["1min", "5min", "15min", "30min", "60min"] = "1min",
    **context: Union[Context, TaskInstance],
) -> None:
    """Process the Alpha Vantage data.

    :param group_num: int: Designated number from distributed set of temp SP500 symbol SQL tables.
    :param price_type: Literal["adj", "non_adj"]: Whether the price data is `adjusted` or `non-adjusted`.
    :param time_interval: Literal["1min", "5min", "15min", "30min", "60min"]: The price intraday interval.
    :param context: Union[Context, TaskInstance]: Set of variables that are inherent in
        every Airflow Task function that can be utilized.
    """
    import time
    import urllib.error
    from http.client import IncompleteRead, RemoteDisconnected

    import pandas as pd
    from global_utils.develop_secrets import ALPHA_VANTAGE_API_KEY
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
        task_ids=rf"main_etl.{price_type}_{time_interval}_etl.etf_distro",
        key="split_num",
        map_indexes=[None],
    )[0]

    # Only activate this task if there is a distro table to pull from
    if group_num < split_num:
        sql_query: str = f"""SELECT * FROM temp_tables.sm_{price_type}_{time_interval}_etf_distro_{group_num}"""
        with SQL_ENGINE.connect().execution_options(isolation_level="AUTOCOMMIT") as sql_connection:
            etl_df: pd.DataFrame = pd.read_sql(sql=sql_query, con=sql_connection)
            # noinspection PyTypeChecker
            etl_tuple_list: List[Tuple[str, str, str]] = list(etl_df.itertuples(index=False, name=None))

        url_exceptions: Tuple[Type[Exception], ...] = (
            RequestException,
            IncompleteRead,
            urllib.error.URLError,
            RemoteDisconnected,
        )
        adjusted_tag: str = "true" if price_type == "adj" else "false"
        symbol: str
        market_month: str
        cutoff_date: str
        for symbol, market_month, cutoff_date in etl_tuple_list:
            # NOTE: THE MAIN API INVOCATION
            url_link_list: List[str] = [
                r"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY",
                rf"symbol={symbol}",
                rf"interval={time_interval}",
                rf"apikey={ALPHA_VANTAGE_API_KEY}",
                rf"adjusted={adjusted_tag}",
                rf"month={market_month}",
                r"outputsize=full",
                r"extended_hours=false",
                r"datatype=csv",
            ]
            url_link_str: str = r"&".join(url_link_list)
            url_request_attempts: int = 0
            url_max_attempts: int = API_MAX_ATTEMPTS
            while True:
                while url_request_attempts < url_max_attempts:
                    try:
                        # NOTE: EXECUTION OF THE `request`
                        chunk_iterator: Iterator[pd.DataFrame] = pd.read_csv(
                            filepath_or_buffer=url_link_str,
                            storage_options={"ssl": True},
                            chunksize=1_000,
                        )
                        chunk_list: List[pd.DataFrame] = [chunk for chunk in chunk_iterator]
                        temp_df: pd.DataFrame = pd.concat(objs=chunk_list, axis=0, ignore_index=True)
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
                    failed_vars_msg: str = rf"FAILED VARIABLES: SYMBOL: {symbol}; MARKET_MONTH: {market_month}"
                    print(failed_vars_msg)
                    failed_info: List[Dict[str, str]] = [
                        {
                            "symbol": symbol,
                            "market_month": market_month,
                            "time_interval": time_interval,
                        }
                    ]
                    failed_df: pd.DataFrame = pd.DataFrame(data=failed_info)
                    failed_df.to_sql(
                        schema="temp_tables",
                        name="alpha_vantage_failed",
                        con=SQL_ENGINE,
                        if_exists="append",
                        index=False,
                        chunksize=int(SQL_CHUNKSIZE_PANDAS / len(failed_df.columns)),
                    )
                    break

                # noinspection PyUnboundLocalVariable
                if r"{" in list(temp_df.columns):
                    response_msg: str = temp_df.iat[0, 0]
                    if (r"invalid api call" in response_msg.lower()) and (url_request_attempts < url_max_attempts):
                        url_request_attempts += 1
                        attempt_msg: str = rf"URL_REQUEST_ATTEMPT: {url_request_attempts} OF {url_max_attempts}"
                        print(attempt_msg)
                        invalid_api_msg: str = rf"INVALID API CALL: {response_msg}"
                        print(invalid_api_msg)
                        invalid_vars_msg: str = rf"INVALID VARIABLES: SYMBOL: {symbol}; MARKET_MONTH: {market_month}"
                        print(invalid_vars_msg)
                        time.sleep(30)
                        continue
                    elif url_request_attempts >= url_max_attempts:
                        max_attempts_msg: str = r"MAX `url_request_attempts` EXECUTED; MOVING ON TO NEXT SYMBOL"
                        print(max_attempts_msg)
                        failed_vars_msg: str = rf"FAILED VARIABLES: SYMBOL: {symbol}; MARKET_MONTH: {market_month}"
                        print(failed_vars_msg)
                        failed_info: List[Dict[str, str]] = [
                            {
                                "symbol": symbol,
                                "market_month": market_month,
                                "time_interval": time_interval,
                            }
                        ]
                        failed_df: pd.DataFrame = pd.DataFrame(data=failed_info)
                        failed_df.to_sql(
                            schema="temp_tables",
                            name="alpha_vantage_failed",
                            con=SQL_ENGINE,
                            if_exists="append",
                            index=False,
                            chunksize=int(SQL_CHUNKSIZE_PANDAS / len(failed_df.columns)),
                        )
                        error_msg: str = rf"ERROR MESSAGE: {response_msg}"
                        print(error_msg)
                        break
                    else:
                        url_request_attempts += 1
                        attempt_msg: str = rf"URL_REQUEST_ATTEMPT: {url_request_attempts} OF {url_max_attempts}"
                        print(attempt_msg)
                        api_limit_response: str = rf"API LIMIT RESPONSE MSG: {response_msg}"
                        print(api_limit_response)
                        time.sleep(30)
                        continue
                elif not temp_df.empty:
                    temp_df["exec_timestamp"]: pd.Series = exec_timestamp
                    temp_df["market_date"]: pd.Series = pd.to_datetime(arg=temp_df["timestamp"]).dt.strftime("%Y-%m-%d")
                    temp_df["market_year"]: pd.Series = pd.to_datetime(arg=temp_df["timestamp"]).dt.strftime("%Y")
                    temp_df["market_month"]: pd.Series = pd.to_datetime(arg=temp_df["timestamp"]).dt.month
                    rename_columns: Dict[str, str] = {
                        "timestamp": "market_timestamp",
                    }
                    temp_df.rename(columns=rename_columns, inplace=True)

                    market_timestamp_dt: pd.Series = pd.to_datetime(arg=temp_df["market_timestamp"])
                    temp_df["market_clock"]: pd.Series = market_timestamp_dt.dt.strftime(date_format="%H:%M")

                    temp_df["market_hour"]: pd.Series = pd.to_datetime(arg=temp_df["market_timestamp"]).dt.hour
                    temp_df["market_minute"]: pd.Series = pd.to_datetime(arg=temp_df["market_timestamp"]).dt.minute
                    temp_df["symbol"]: pd.Series = symbol
                    # Filter rows where all `NUMERIC(10,2)` columns are within the valid range
                    target_columns: List[str] = ["open", "high", "low", "close"]
                    max_value: float = (10 ** (10 - 2)) - (10**-2)  # NUMERIC(10,2)
                    valid_positive_mask: pd.Series = (temp_df[target_columns] > 0).all(axis=1)
                    valid_range_mask: pd.Series = (temp_df[target_columns] < max_value).all(axis=1)
                    valid_mask: pd.Series = valid_positive_mask & valid_range_mask
                    # Filter rows based on `valid mask`
                    temp_df: pd.DataFrame = temp_df.loc[valid_mask].reset_index(drop=True)
                    # Filter out rows where the volume is ZERO
                    # Rows with ZERO volume seem to have very erroneous price data
                    temp_df: pd.DataFrame = temp_df[temp_df["volume"] > 0]
                    # NOTE: Get rid of the `market_date`s that are already in the SQL table
                    temp_df: pd.DataFrame = temp_df[temp_df["market_date"] > cutoff_date].reset_index(drop=True)
                    # NOTE: Do not be surprised when the latest market date is greater than 2 weeks ago
                    # The `generate_combinations()` functions produces `YYYY-MM` format.
                    temp_df.to_sql(
                        schema=TARGET_SCHEMA,
                        name=rf"{price_type}_{time_interval}",
                        con=SQL_ENGINE,
                        if_exists="append",
                        index=False,
                        chunksize=int(SQL_CHUNKSIZE_PANDAS / len(temp_df.columns)),
                    )
                    insert_msg: str = rf"SYMBOL: {symbol}, MARKET_MONTH: {market_month}; {len(temp_df)} ROWS PROCESSED"
                    print(insert_msg)
                    break
                else:
                    no_data_msg: str = rf"SYMBOL: {symbol}, MARKET_MONTH: {market_month}; NO DATA TO PROCESS"
                    print(no_data_msg)
                    break
        drop_table_query: str = rf"DROP TABLE IF EXISTS temp_tables.sm_{price_type}_etf_distro_{group_num} CASCADE"
        with SQL_ENGINE.connect().execution_options(isolation_level="AUTOCOMMIT") as sql_connection:
            sql_connection.execute(text(drop_table_query))


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="dedupe_sql_table", trigger_rule=DEFAULT_TRIGGER)
def dedupe_sql_table(
    price_type: Literal["adj", "non_adj"],
    time_interval: Literal["1min", "5min", "15min", "30min", "60min"],
    **context: Union[Context, TaskInstance],
) -> None:
    """De-duplicate the target SQL table.

    :param price_type: Literal["adj", "non_adj"]: Whether the price data is `adjusted` or `non-adjusted`.
    :param time_interval: Literal["1min", "5min", "15min", "30min", "60min"]: The price intraday interval.
    :param context: Union[Context, TaskInstance]: Set of variables that are inherent in every Airflow Task function
        that can be utilized.
    """
    from datetime import datetime

    from global_utils.sql_utils import dedupe_sql_table

    begin_market_date: str = context["ti"].xcom_pull(
        task_ids=rf"main_etl.{price_type}_{time_interval}_etl.etf_distro",
        key="begin_market_date",
    )
    # Convert to first of the month for PROPER deduping
    # b/c Alpha Vantage API downloads data per `symbol`s ENTIRE month
    begin_market_date: str = datetime.strptime(begin_market_date, "%Y-%m-%d").replace(day=1).strftime("%Y-%m-%d")

    two_weeks_ago: str = context["ti"].xcom_pull(
        task_ids=rf"main_etl.{price_type}_{time_interval}_etl.etf_distro",
        key="two_weeks_ago",
    )
    # Deduplicate the SQL Table
    partition_by_cols: List[str] = ["market_date", "symbol", "market_timestamp"]
    dedupe_sql_table(
        sql_schema=TARGET_SCHEMA,
        sql_table=rf"{price_type}_{time_interval}",
        partition_by_cols=partition_by_cols,
        begin_market_date=begin_market_date,
        end_market_date=two_weeks_ago,
    )


# ----------------------------------------------------------------------------------------------------------------------
def create_price_etl(
    price_type: Literal["adj", "non_adj"],
    time_interval: Literal["1min", "5min", "15min", "30min", "60min"],
    cpu_cores_per_task: int = 0,
) -> TaskGroup:
    """Creates Task Group for the target combination `price_type` and `time_interval`.

    :param price_type: Literal["adj", "non_adj"]: Whether the price data is `adjusted` or `non-adjusted`.
    :param time_interval: Literal["1min", "5min", "15min", "30min", "60min"]: The price intraday interval.
    :param cpu_cores_per_task: int: CPU count for set of dynamic Airflow Tasks.

    :return: TaskGroup: The Airflow Task Group.
    """

    @task_group(group_id=rf"{price_type}_{time_interval}_etl")
    def price_etl() -> None:
        """Task Group for the ETL process."""

        from airflow.providers.standard.operators.empty import EmptyOperator
        from airflow.providers.standard.operators.python import PythonOperator

        branch_etf_distro: str = etf_distro(
            cpu_cores_per_task=cpu_cores_per_task,
            price_type=price_type,
            time_interval=time_interval,
        )
        # Since `process_data` is a MAPPED Task, Airflow does not to skip these MAPPED tasks
        # when a MAPPED Airflow Task comes IMMEDIATELY after an Airflow BRANCH task
        launch_etl: EmptyOperator = EmptyOperator(task_id="launch_etl", trigger_rule=DEFAULT_TRIGGER)
        no_data_to_process: EmptyOperator = EmptyOperator(task_id="no_data_to_process", trigger_rule=DEFAULT_TRIGGER)
        tg_finish: EmptyOperator = EmptyOperator(task_id="tg_finish", trigger_rule=DEFAULT_TRIGGER)

        distro_range_list: List[int] = [num for num in range(cpu_cores_per_task)]
        process_data_tasks_partial: PythonOperator = process_data.partial(
            price_type=price_type,
            time_interval=time_interval,
        )
        # noinspection PyUnresolvedReferences
        process_data_tasks: PythonOperator = process_data_tasks_partial.expand(group_num=distro_range_list)
        """MAIN FLOW."""
        # noinspection PyTypeChecker,PyUnresolvedReferences
        (
            branch_etf_distro
            >> launch_etl
            >> process_data_tasks
            >> dedupe_sql_table(price_type=price_type, time_interval=time_interval)
            >> tg_finish
        )
        """NO DATA FLOW."""
        # noinspection PyTypeChecker,PyUnresolvedReferences
        (branch_etf_distro >> no_data_to_process >> tg_finish)

    # noinspection PyTypeChecker
    return price_etl()


# ----------------------------------------------------------------------------------------------------------------------
@task_group(group_id="main_etl")
def tg_01_main_etl() -> None:
    """Task Group for the main ETL process."""
    from global_utils.general_utils import calc_cpu_cores_per_task

    # NOTE: In case you want to collect `adjusted` real time price data; but not reliable
    # price_type_list: List[str] = ["adj", "non_adj"]
    price_type_list: List[str] = ["non_adj"]
    # NOTE: All available intraday time intervals
    time_intervals_list: List[str] = ["1min", "5min", "15min", "30min", "60min"]
    # time_intervals_list: List[str] = ["1min"]
    # What is the maximum number of PARALLEL-IZED Dynamic Airflow Tasks that occur at any point in the DAG?
    num_max_parallel_dynamic_tasks: int = len(price_type_list) * len(time_intervals_list)
    cpu_cores_per_task: int = calc_cpu_cores_per_task(num_max_parallel_dynamic_tasks=num_max_parallel_dynamic_tasks)

    price_task_group_list: List[Optional[TaskGroup]] = []
    price_type: Literal["adj", "non_adj"]
    for price_type in price_type_list:
        time_interval: Literal["1min", "5min", "15min", "30min", "60min"]
        for time_interval in time_intervals_list:
            price_task_group_list.append(
                create_price_etl(
                    price_type=price_type,
                    time_interval=time_interval,
                    cpu_cores_per_task=cpu_cores_per_task,
                )
            )
