"""Processes raw FRED data."""

from typing import Dict, List, Optional, Union

from airflow.models import TaskInstance
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Context, task, task_group
from global_airflow.airflow_utils import DEFAULT_TRIGGER

# ----------------------------------------------------------------------------------------------------------------------
TARGET_SCHEMA: str = "fred"
TARGET_TABLE: str = "raw_data"


# ----------------------------------------------------------------------------------------------------------------------
@task.branch(task_id="get_market_date_ranges", trigger_rule=DEFAULT_TRIGGER)
def get_market_date_ranges(**context: Union[Context, TaskInstance]) -> Union[List[str], str]:
    """Get the established market_date range for this DAG run.

    :param context: Union[Context, TaskInstance]: Set of variables that are inherent in every Airflow Task function
        that can be utilized.

    :return: Union[List[str], str]: The Airflow Task(s) to trigger.
    """
    from datetime import datetime
    from typing import List

    import pytz
    from dags_stock_market.const_vars import FRED_DICT_LIST
    from dateutil.relativedelta import relativedelta
    from global_utils.sql_utils import SQL_ENGINE
    from sqlalchemy import text

    execute_backfill: bool = context["params"]["execute_backfill"]

    with SQL_ENGINE.connect().execution_options(isolation_level="AUTOCOMMIT") as sql_connection:
        if execute_backfill:
            # Delete all the data from the SQL table beforehand
            truncate_query: str = rf"TRUNCATE TABLE {TARGET_SCHEMA}.{TARGET_TABLE} CASCADE"
            sql_connection.execute(text(truncate_query))

        fred_count_query: str = rf"""SELECT COUNT(*) FROM {TARGET_SCHEMA}.{TARGET_TABLE}"""
        fred_table_count: int = sql_connection.execute(text(fred_count_query)).scalar()

        # determines if the beginning market_date is going to be the MAX market_date FROM fred.raw_data
        # or if the beginning market_date is going to be a default date
        if (fred_table_count > 0) and (not execute_backfill):
            # the MAX market_date FROM `fred.raw_data` as the begin_date
            fred_date_query: str = rf"""SELECT MAX(market_date) FROM {TARGET_SCHEMA}.{TARGET_TABLE}"""
            last_market_date: datetime = sql_connection.execute(text(fred_date_query)).scalar()
            # NOTE: This prevents `market_date`s that are already in the SQL Table from being processed
            parsed_date: datetime = last_market_date + relativedelta(days=1)
            begin_market_date: str = parsed_date.strftime("%Y-%m-%d")  # type: ignore[call-arg]
        else:
            # Do NOT make this string `0000-00-00`
            # ERROR MSG: `pandas._libs.tslibs.parsing.DateParseError: year 0 is out of range: 0000-00-00, at position 0`
            begin_market_date: str = "1900-01-01"

    today_datetime: datetime = datetime.now().astimezone(pytz.timezone(zone="US/Eastern"))
    today_date: str = today_datetime.strftime("%Y-%m-%d")  # type: ignore[call-arg]

    print(f"""BEGIN_MARKET_DATE: {begin_market_date}; TODAY_DATE {today_date}""")
    # sanity check to determine if new data needs to be processed
    # ex. if the MAX date in fred.raw_data is equal to sp500.raw_data, it means no new data was ingested
    if begin_market_date < today_date:
        # noinspection PyArgumentList
        context["ti"].xcom_push(key="market_date_range", value="VALID")
        # noinspection PyArgumentList
        context["ti"].xcom_push(key="begin_market_date", value=begin_market_date)
        # noinspection PyArgumentList
        context["ti"].xcom_push(key="today_date", value=today_date)

        task_var: List[str] = []
        fred_metric: Dict[str, str]
        for fred_metric in FRED_DICT_LIST:
            target_metric: str = fred_metric["name"]
            task_var.append(f"""process_raw_data.{target_metric}""")
        return task_var
    else:
        # noinspection PyArgumentList
        context["ti"].xcom_push(key="market_date_range", value="NOT VALID")
        return "process_raw_data.no_data_to_process"


# ----------------------------------------------------------------------------------------------------------------------
@task.branch(task_id="check_data_count", trigger_rule=DEFAULT_TRIGGER)
def check_data_count() -> str:
    """Get the established market_date range for this DAG run.

    :return: str: The Airflow Task to trigger.
    """
    from dags_stock_market.sm_etl_01_fred.sql_queries import (
        DATE_SCHEMA_TABLE,
        data_count_query,
    )
    from global_utils.sql_utils import SQL_ENGINE
    from sqlalchemy.sql import text

    with SQL_ENGINE.connect().execution_options(isolation_level="AUTOCOMMIT") as sql_connection:
        sql_query: str = rf"DROP TABLE IF EXISTS {DATE_SCHEMA_TABLE} CASCADE"
        sql_connection.execute(text(sql_query))
        row_count: int = sql_connection.execute(text(data_count_query)).scalar()

    if row_count > 0:
        return "process_raw_data.sql_distinct_market_dates"
    else:
        return "process_raw_data.no_data_to_process"


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="sql_distinct_market_dates", trigger_rule=DEFAULT_TRIGGER)
def sql_distinct_market_dates() -> None:
    """Create temp SQL table for all unique timestamps."""
    from dags_stock_market.sm_etl_01_fred.sql_queries import (
        DATE_SCHEMA_TABLE,
        combine_market_dates_query,
    )
    from global_utils.sql_utils import SQL_ENGINE
    from sqlalchemy.sql import text

    with SQL_ENGINE.connect().execution_options(isolation_level="AUTOCOMMIT") as sql_connection:
        sql_connection.execute(text(rf"DROP TABLE IF EXISTS {DATE_SCHEMA_TABLE} CASCADE"))
        sql_connection.execute(text(combine_market_dates_query))
        sql_query: str = rf"CREATE INDEX ON {DATE_SCHEMA_TABLE} (market_date)"
        sql_connection.execute(text(sql_query))


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="sql_combine_data", trigger_rule=DEFAULT_TRIGGER)
def sql_combine_data() -> None:
    """Combine all the temp data."""
    from dags_stock_market.sm_etl_01_fred.sql_queries import (
        COMBINED_DATA_TABLE,
        combine_all_data_query,
    )
    from global_utils.sql_utils import SQL_ENGINE
    from sqlalchemy import text

    with SQL_ENGINE.connect().execution_options(isolation_level="AUTOCOMMIT") as sql_connection:
        sql_connection.execute(text(rf"DROP TABLE IF EXISTS {COMBINED_DATA_TABLE} CASCADE"))
        sql_connection.execute(text(combine_all_data_query))

        idx_name: str = COMBINED_DATA_TABLE.replace(".", "_")
        sql_query: str = rf"CREATE INDEX {idx_name}_idx ON {COMBINED_DATA_TABLE} (cx_partition)"
        sql_connection.execute(text(sql_query))


# ----------------------------------------------------------------------------------------------------------------------
@task.branch(task_id="process_combined_data", trigger_rule=DEFAULT_TRIGGER)
def process_combined_data() -> str:
    """Takes combined temp FRED SQL table, date-sorts the values, and forward fills missing data.

    :return: str: The Airflow Task to trigger.
    """
    import requests
    from global_utils.develop_vars import DOCKER_IP, MODIN_FASTAPI_PORT, TEMP_DATA_PATH
    from global_utils.general_utils import json_read
    from requests.models import Response

    sub_api_name: str = "sm_etl_01_fred"
    sub_api_target_func: str = "process_combined_data"

    target_api_route: str = rf"{sub_api_name}/{sub_api_target_func}/"
    http_response: Response = requests.post(url=rf"http://{DOCKER_IP}:{MODIN_FASTAPI_PORT}/{target_api_route}")
    http_response.raise_for_status()

    file_name_path: str = rf"{TEMP_DATA_PATH}/sm_etl_01_fred/process_combined_data.json"
    data_dict: Dict[str, str] = json_read(file_name_path=file_name_path)
    task_var: str = data_dict["task_var"]

    return task_var


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="dedupe_sql_table", trigger_rule=DEFAULT_TRIGGER)
def dedupe_sql_table(**context: Union[Context, TaskInstance]) -> None:
    """De-duplicate the `alpha_vantage.real_time` SQL table.

    :param context: Union[Context, TaskInstance]: Set of variables that are inherent in every Airflow Task function
        that can be utilized.
    """
    from global_utils.sql_utils import dedupe_sql_table

    begin_market_date: str = context["ti"].xcom_pull(
        task_ids="process_raw_data.get_market_date_ranges",
        key="begin_market_date",
    )
    today_date: str = context["ti"].xcom_pull(task_ids="process_raw_data.get_market_date_ranges", key="today_date")

    # Deduplicate the SQL Table
    partition_by_cols: List[str] = ["market_date"]
    dedupe_sql_table(
        sql_schema="fred",
        sql_table="raw_data",
        partition_by_cols=partition_by_cols,
        begin_market_date=begin_market_date,
        end_market_date=today_date,
    )


# ----------------------------------------------------------------------------------------------------------------------
def create_temp_sql_task(dict_object: Dict[str, Union[str, int, List[str]]]) -> PythonOperator:
    """Creates temporary SQL table for given FRED metric symbol.

    :param dict_object: Dict[str, Union[str, int, List[str]]]: The FRED data that comes with the FRED symbol,
        api key, and name from the FRED website.

    :return: PythonOperator: A Task function that creates a temp SQL table for the given FRED metric symbol.
    """
    from dags_stock_market.sm_etl_01_fred.util_funcs.func_create_fred_temp_table import (
        create_fred_temp_table,
    )

    @task(task_id=dict_object["name"], trigger_rule=DEFAULT_TRIGGER)
    def temp_sql(**context: Union[Context, TaskInstance]) -> None:
        """Creates temporary SQL table for the given FRED metric.

        :param context: Union[Context, TaskInstance]: Set of variables that are inherent in every Airflow Task
            function that can be utilized.
        """
        create_fred_temp_table(
            symbol=dict_object["symbol"],
            api_key=dict_object["api_key"],
            context=context,
        )

    # noinspection PyTypeChecker
    return temp_sql()


# ----------------------------------------------------------------------------------------------------------------------
@task_group(group_id="process_raw_data")
def tg_01_process_raw_data() -> None:
    """Task Group for the entire FRED data-related process."""
    from typing import List, Union

    from airflow.providers.standard.operators.empty import EmptyOperator
    from dags_stock_market.const_vars import FRED_DICT_LIST

    tg_finish: EmptyOperator = EmptyOperator(task_id="tg_finish", trigger_rule=DEFAULT_TRIGGER)

    branch_get_market_date_ranges: Union[List[str], str] = get_market_date_ranges()
    branch_check_data_count: str = check_data_count()
    branch_process_combined_data: str = process_combined_data()
    no_data_to_process: EmptyOperator = EmptyOperator(task_id="no_data_to_process", trigger_rule=DEFAULT_TRIGGER)

    temp_sql_task_list: List[Optional[PythonOperator]] = []
    fred_metric: Dict[str, str]
    for fred_metric in FRED_DICT_LIST:
        temp_sql_task_list.append(create_temp_sql_task(dict_object=fred_metric))

    """MAIN FLOW"""
    # noinspection PyUnresolvedReferences,PyStatementEffect
    (branch_get_market_date_ranges >> temp_sql_task_list >> branch_check_data_count)
    # noinspection PyTypeChecker,PyUnresolvedReferences
    (branch_check_data_count >> sql_distinct_market_dates() >> sql_combine_data() >> branch_process_combined_data)
    # noinspection PyTypeChecker,PyUnresolvedReferences
    (branch_process_combined_data >> dedupe_sql_table() >> tg_finish)

    """NO DATA FLOW"""
    # noinspection PyTypeChecker
    (branch_get_market_date_ranges >> no_data_to_process >> tg_finish)
    # noinspection PyTypeChecker
    (branch_check_data_count >> no_data_to_process >> tg_finish)
    # noinspection PyTypeChecker
    (branch_process_combined_data >> no_data_to_process >> tg_finish)
