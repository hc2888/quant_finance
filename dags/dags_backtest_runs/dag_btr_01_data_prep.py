"""Prepare market data for backtest runs; making sure targeted stocks have proper amount of intraday data."""

from typing import Any, Dict, List, Union

from airflow.models import TaskInstance
from airflow.sdk import Context, dag, task
from global_airflow.airflow_utils import DEFAULT_ARGS, DEFAULT_TRIGGER, store_dag_info
from global_utils.develop_vars import BACKTEST_BEGIN_DATE

# ----------------------------------------------------------------------------------------------------------------------
DAG_NAME: str = "btr_01_data_prep"

DROP_TABLES: bool = False

CONFIG_PARAMS: Dict[str, Union[bool, str, List[str]]] = {
    "execute_backfill": False,
    "trigger_next_dag": True,
    "backtest_begin_date": BACKTEST_BEGIN_DATE,  # The earliest `market_date` to store in `backtesting.intraday`
}


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="check_backfill", trigger_rule=DEFAULT_TRIGGER)
def check_backfill(**context: Union[Context, TaskInstance]) -> None:
    """Check if this is a backfill run.

    :param context: Union[Context, TaskInstance]: Set of variables that are inherent in
        every Airflow Task function that can be utilized.

    :return: str: Airflow Task to trigger.
    """
    from global_utils.sql_utils import SQL_ENGINE
    from sqlalchemy import text

    execute_backfill: bool = context["params"]["execute_backfill"]

    with SQL_ENGINE.connect().execution_options(isolation_level="AUTOCOMMIT") as sql_connection:
        if execute_backfill:
            truncate_intraday: str = rf"TRUNCATE TABLE backtesting.intraday CASCADE"
            sql_connection.execute(text(truncate_intraday))

            truncate_target_vars: str = rf"TRUNCATE TABLE backtesting.target_vars CASCADE"
            sql_connection.execute(text(truncate_target_vars))


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
            trigger_dag_id="btr_02_features_prep",
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
    params=CONFIG_PARAMS,
    tags=["btr"],
)
def main() -> None:
    """The main DAG flow."""
    from airflow.providers.standard.operators.empty import EmptyOperator
    from airflow.sdk import TaskGroup
    from dags_backtest_runs.btr_01_data_prep.task_groups.tg_01_missing_prices import tg_01_missing_prices
    from dags_backtest_runs.btr_01_data_prep.task_groups.tg_02_target_vars import tg_02_target_vars

    dag_finish: EmptyOperator = EmptyOperator(task_id="dag_finish", trigger_rule=DEFAULT_TRIGGER)

    # noinspection PyNoneFunctionAssignment,PyTypeChecker
    tg_01_missing_prices: TaskGroup = tg_01_missing_prices()
    # noinspection PyNoneFunctionAssignment,PyTypeChecker
    tg_02_target_vars: TaskGroup = tg_02_target_vars()

    """MAIN FLOW"""
    # noinspection PyTypeChecker,PyUnresolvedReferences
    (
        store_dag_info()
        >> check_backfill()
        >> tg_01_missing_prices
        >> tg_02_target_vars
        >> invoke_next_dag()
        >> dag_finish
    )


# ----------------------------------------------------------------------------------------------------------------------
main()
