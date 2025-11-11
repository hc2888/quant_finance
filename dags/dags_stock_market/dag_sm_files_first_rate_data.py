"""Process data from First Rate Data."""

from typing import Dict, List, Union

from airflow.models import TaskInstance
from airflow.sdk import Context, dag, task
from global_airflow.airflow_utils import DEFAULT_ARGS, DEFAULT_TRIGGER, drop_temp_tables, store_dag_info

# ----------------------------------------------------------------------------------------------------------------------
DAG_NAME: str = "sm_files_first_rate_data"

DROP_TABLES: bool = False

CONFIG_PARAMS: Dict[str, Union[bool, List[str]]] = {
    "execute_backfill": False,
}


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="check_backfill_status", trigger_rule=DEFAULT_TRIGGER)
def check_backfill_status(**context: Union[Context, TaskInstance]) -> None:
    """Check whether this is a backfill run; TRUNCATE SQL Table if this is a backfill execution.

    :param context: Union[Context, TaskInstance]: Set of variables that are inherent in
        every Airflow Task function that can be utilized.
    """
    from dags_stock_market.const_vars import MODEL_PREP_API_SYMBOLS_LIST
    from global_utils.sql_utils import SQL_ENGINE
    from sqlalchemy import text

    execute_backfill: bool = context["params"]["execute_backfill"]

    if execute_backfill:
        with SQL_ENGINE.connect().execution_options(isolation_level="AUTOCOMMIT") as sql_connection:
            all_symbols: str = str(MODEL_PREP_API_SYMBOLS_LIST)[1:-1]
            delete_intraday: str = f"DELETE FROM backtesting.intraday WHERE symbol IN ({all_symbols})"
            sql_connection.execute(text(delete_intraday))

            delete_target_vars: str = f"DELETE FROM backtesting.target_vars WHERE symbol IN ({all_symbols})"
            sql_connection.execute(text(delete_target_vars))

            time_intervals_list: List[str] = ["1min", "5min", "30min", "60min", "daily"]
            time_interval: str
            for time_interval in time_intervals_list:
                truncate_query: str = rf"TRUNCATE TABLE model_prep_api.etf_split_only_{time_interval} CASCADE"
                sql_connection.execute(text(truncate_query))

                truncate_query: str = rf"TRUNCATE TABLE model_prep_api.etf_split_div_{time_interval} CASCADE"
                sql_connection.execute(text(truncate_query))

            target_table_list: List[str] = [
                "etf_non_adj_1min",
                "etf_non_adj_daily",
                "index_1min",
                "index_5min",
                "index_30min",
                "index_60min",
                "index_daily",
            ]
            table_name: str
            for table_name in target_table_list:
                truncate_query: str = rf"TRUNCATE TABLE model_prep_api.{table_name} CASCADE"
                sql_connection.execute(text(truncate_query))


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="dedupe_etf_split_only", trigger_rule=DEFAULT_TRIGGER)
def dedupe_etf_split_only() -> None:
    """De-duplicate the target SQL table."""
    from global_utils.sql_utils import dedupe_sql_table

    time_intervals_list: List[str] = ["1min", "5min", "30min", "60min", "daily"]
    partition_by_cols: List[str] = ["market_date", "symbol", "market_timestamp"]

    time_interval: str
    for time_interval in time_intervals_list:
        index_columns: List[str] = partition_by_cols.copy()
        if time_interval == "daily":
            index_columns.remove("market_timestamp")

        dedupe_sql_table(
            sql_schema="model_prep_api",
            sql_table=f"etf_split_only_{time_interval}",
            partition_by_cols=index_columns,
        )


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="dedupe_etf_split_div", trigger_rule=DEFAULT_TRIGGER)
def dedupe_etf_split_div() -> None:
    """De-duplicate the target SQL table."""
    from global_utils.sql_utils import dedupe_sql_table

    time_intervals_list: List[str] = ["1min", "5min", "30min", "60min", "daily"]
    partition_by_cols: List[str] = ["market_date", "symbol", "market_timestamp"]

    time_interval: str
    for time_interval in time_intervals_list:
        index_columns: List[str] = partition_by_cols.copy()
        if time_interval == "daily":
            index_columns.remove("market_timestamp")

        dedupe_sql_table(
            sql_schema="model_prep_api",
            sql_table=f"etf_split_div_{time_interval}",
            partition_by_cols=index_columns,
        )


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="dedupe_non_adj_index", trigger_rule=DEFAULT_TRIGGER)
def dedupe_non_adj_index() -> None:
    """De-duplicate the target SQL table."""
    from global_utils.sql_utils import dedupe_sql_table

    target_table_list: List[str] = [
        "etf_non_adj_1min",
        "etf_non_adj_daily",
        "index_1min",
        "index_5min",
        "index_30min",
        "index_60min",
        "index_daily",
    ]
    partition_by_cols: List[str] = ["market_date", "symbol", "market_timestamp"]

    table_name: str
    for table_name in target_table_list:
        index_columns: List[str] = partition_by_cols.copy()
        if "daily" in table_name:
            index_columns.remove("market_timestamp")

        dedupe_sql_table(
            sql_schema="model_prep_api",
            sql_table=table_name,
            partition_by_cols=index_columns,
        )


# ----------------------------------------------------------------------------------------------------------------------
@dag(
    dag_id=DAG_NAME,
    default_args=DEFAULT_ARGS,
    # `max_active_runs` defines how many running concurrent instances of a DAG there are allowed to be.
    max_active_runs=1,
    catchup=False,
    params=CONFIG_PARAMS,
    on_failure_callback=drop_temp_tables(temp_table_prefix="sm", drop_tables=DROP_TABLES),
    tags=["files", "stock_market"],
)
def main() -> None:
    """The main DAG flow."""
    from airflow.providers.standard.operators.empty import EmptyOperator
    from airflow.sdk import TaskGroup
    from dags_stock_market.sm_files_first_rate_data.task_groups.tg_01_process_csv_files import tg_01_process_csv_files

    # noinspection PyNoneFunctionAssignment,PyTypeChecker
    tg_01_process_csv_files: TaskGroup = tg_01_process_csv_files()

    dag_finish: EmptyOperator = EmptyOperator(task_id="dag_finish", trigger_rule=DEFAULT_TRIGGER)

    # noinspection PyTypeChecker,PyUnresolvedReferences
    (
        store_dag_info()
        >> check_backfill_status()
        >> tg_01_process_csv_files
        >> [dedupe_etf_split_only(), dedupe_etf_split_div(), dedupe_non_adj_index()]
        >> dag_finish
    )


# ----------------------------------------------------------------------------------------------------------------------
main()
