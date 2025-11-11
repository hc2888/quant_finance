"""DOCUMENTATION LINK: https://www.alphavantage.co/documentation/."""

from typing import Any, Dict, Union

from airflow.models import TaskInstance
from airflow.sdk import Context, dag, task
from global_airflow.airflow_utils import DEFAULT_ARGS, DEFAULT_TRIGGER, drop_temp_tables, store_dag_info

# ----------------------------------------------------------------------------------------------------------------------
DAG_NAME: str = "sm_etl_03_alpha_vantage"

DROP_TABLES: bool = True

CONFIG_PARAMS: Dict[str, Any] = {
    "execute_backfill": False,
    "trigger_next_dag": True,
}


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="create_failed_table", trigger_rule=DEFAULT_TRIGGER)
def create_failed_table() -> None:
    """Create temp SQL table that logs all the failed stock symbol API hits."""
    from global_utils.sql_utils import SQL_ENGINE
    from sqlalchemy import text

    with SQL_ENGINE.connect().execution_options(isolation_level="AUTOCOMMIT") as sql_connection:
        schema_table: str = "temp_tables.alpha_vantage_failed"
        drop_query: str = rf"DROP TABLE IF EXISTS {schema_table} CASCADE"
        sql_connection.execute(text(drop_query))

        create_query: str = f"""
            CREATE TABLE {schema_table} (
            symbol TEXT NOT NULL
            , market_month TEXT NOT NULL
            , time_interval TEXT NOT NULL
            )
            """
        sql_connection.execute(text(create_query))


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="invoke_next_dag", trigger_rule=DEFAULT_TRIGGER)
def invoke_next_dag(**context: Union[Context, TaskInstance]) -> None:
    """Invoke the next DAG.

    :param context: Union[Context, Dict[str, str], TaskInstance]: Set of variables that are
        inherent in every Airflow Task function that can be pulled and used.
    """
    from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

    trigger_next_dag: bool = context["params"]["trigger_next_dag"]

    if trigger_next_dag:
        params_dict: Dict[str, Any] = {}

        # noinspection PyTypeChecker
        TriggerDagRunOperator(
            task_id="trigger_dag",
            trigger_dag_id="btr_01_data_prep",
            trigger_rule=DEFAULT_TRIGGER,
            conf=params_dict,
        ).execute(context=context)


# ----------------------------------------------------------------------------------------------------------------------
@dag(
    dag_id=DAG_NAME,
    default_args=DEFAULT_ARGS,
    # `max_active_runs` defines how many running concurrent instances of a DAG there are allowed to be.
    max_active_runs=1,
    catchup=False,
    on_failure_callback=drop_temp_tables(temp_table_prefix=r"sm_etf_distro", drop_tables=DROP_TABLES),
    params=CONFIG_PARAMS,
    tags=["stock_market", "etl"],
)
def main() -> None:
    """The main DAG flow."""
    from airflow.providers.standard.operators.empty import EmptyOperator
    from airflow.sdk import TaskGroup
    from dags_stock_market.sm_etl_03_alpha_vantage.task_groups.tg_01_main_etl import tg_01_main_etl

    dag_finish: EmptyOperator = EmptyOperator(task_id="dag_finish", trigger_rule=DEFAULT_TRIGGER)

    # noinspection PyNoneFunctionAssignment,PyTypeChecker
    tg_01_main_etl: TaskGroup = tg_01_main_etl()

    """MAIN FLOW"""
    # noinspection PyUnresolvedReferences,PyTypeChecker
    (store_dag_info() >> create_failed_table() >> tg_01_main_etl >> invoke_next_dag() >> dag_finish)


# ----------------------------------------------------------------------------------------------------------------------
main()
