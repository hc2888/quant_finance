"""Creates SQL tables."""

from typing import Dict

from airflow.sdk import dag, task
from global_airflow.airflow_utils import DEFAULT_ARGS, DEFAULT_TRIGGER, store_dag_info

# ----------------------------------------------------------------------------------------------------------------------
DAG_NAME: str = "air_env_sql_infra"

CONFIG_PARAMS: Dict[str, bool] = {
    "reset_exec_timestamp": False,
}


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="create_schemas", trigger_rule=DEFAULT_TRIGGER)
def create_schemas() -> None:
    """Creates all the RDS Schemas for stock market data / processes."""
    from global_utils.sql_utils import SQL_ENGINE
    from sql_infra.universal_vars import SCHEMA_TOTAL_LIST
    from sqlalchemy import text

    with SQL_ENGINE.connect().execution_options(isolation_level="AUTOCOMMIT") as sql_connection:
        schema_name: str
        for schema_name in SCHEMA_TOTAL_LIST:
            sql_connection.execute(text(rf"""CREATE SCHEMA IF NOT EXISTS {schema_name}"""))

        sql_connection.execute(text(r"""DROP TABLE IF EXISTS global.tables_roster"""))
        sql_connection.execute(text(r"""CREATE TABLE global.tables_roster (schema_name TEXT, table_name TEXT)"""))


# ----------------------------------------------------------------------------------------------------------------------
@dag(
    dag_id=DAG_NAME,
    default_args=DEFAULT_ARGS,
    catchup=False,
    params=CONFIG_PARAMS,
    tags=["air_env", "infra"],
)
def main() -> None:
    """The main DAG flow."""
    from airflow.providers.standard.operators.empty import EmptyOperator
    from airflow.sdk import TaskGroup
    from sql_infra.btm_tg_from_signals import btm_tg_from_signals
    from sql_infra.btr_tg_market_data import btr_tg_market_data
    from sql_infra.sm_tg_daily_data import sm_tg_daily_data
    from sql_infra.sm_tg_intraday import sm_tg_intraday

    dag_finish: EmptyOperator = EmptyOperator(task_id="dag_finish", trigger_rule=DEFAULT_TRIGGER)

    # noinspection PyNoneFunctionAssignment,PyTypeChecker
    btm_tg_from_signals: TaskGroup = btm_tg_from_signals()
    # noinspection PyNoneFunctionAssignment,PyTypeChecker
    btr_tg_market_data: TaskGroup = btr_tg_market_data()
    # noinspection PyNoneFunctionAssignment,PyTypeChecker
    sm_tg_daily_data: TaskGroup = sm_tg_daily_data()
    # noinspection PyNoneFunctionAssignment,PyTypeChecker
    sm_tg_intraday: TaskGroup = sm_tg_intraday()

    # noinspection PyTypeChecker,PyUnresolvedReferences
    (
        store_dag_info()
        >> create_schemas()
        >> [
            btm_tg_from_signals,
            btr_tg_market_data,
            sm_tg_daily_data,
            sm_tg_intraday,
        ]
        >> dag_finish
    )


# ----------------------------------------------------------------------------------------------------------------------
main()
