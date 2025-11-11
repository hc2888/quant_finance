"""Blank Template that is the basis for future DAGs."""

from typing import Dict, Union

from airflow.models import TaskInstance
from airflow.sdk import Context, dag, task
from global_airflow.airflow_utils import DEFAULT_ARGS, DEFAULT_TRIGGER, drop_temp_tables, store_dag_info
from global_utils.develop_vars import FIRST_MARKET_DATE, LAST_MARKET_DATE

# ----------------------------------------------------------------------------------------------------------------------
DAG_NAME: str = "air_env_blank_template"

DROP_TABLES: bool = False

CONFIG_PARAMS: Dict[str, str] = {
    "begin_date": FIRST_MARKET_DATE,
    "end_date": LAST_MARKET_DATE,
}


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="dummy_task", trigger_rule=DEFAULT_TRIGGER)
def dummy_task(**context: Union[Context, TaskInstance]) -> None:
    """Dummy Placeholder task.

    :param context: Union[Context, TaskInstance]: Set of variables that are inherent in
        every Airflow Task function that can be utilized.
    """
    import pandas as pd
    from global_utils.sql_utils import cx_read_sql

    begin_date: str = context["params"]["begin_date"]
    print(begin_date)

    num_rows: int = 1_000_000
    sql_query: str = f"SELECT * FROM backtesting.intraday LIMIT {num_rows}"
    main_df: pd.DataFrame = cx_read_sql(sql_query=sql_query)
    print(len(main_df))


# ----------------------------------------------------------------------------------------------------------------------
@dag(
    dag_id=DAG_NAME,
    default_args=DEFAULT_ARGS,
    # `max_active_runs` defines how many running concurrent instances of a DAG there are allowed to be.
    max_active_runs=1,
    catchup=False,
    params=CONFIG_PARAMS,
    on_failure_callback=drop_temp_tables(temp_table_prefix="sm", drop_tables=DROP_TABLES),
    tags=["air_env"],
)
def main() -> None:
    """The main DAG flow."""
    from airflow.providers.standard.operators.empty import EmptyOperator

    dag_finish: EmptyOperator = EmptyOperator(task_id="dag_finish", trigger_rule=DEFAULT_TRIGGER)

    # noinspection PyTypeChecker,PyUnresolvedReferences
    (store_dag_info() >> dummy_task() >> dag_finish)


# ----------------------------------------------------------------------------------------------------------------------
main()
