"""Executes metadata statistical analysis on stock_market data."""

from typing import Dict

from airflow.sdk import dag
from global_airflow.airflow_utils import DEFAULT_ARGS, DEFAULT_TRIGGER, store_dag_info
from global_utils.develop_vars import FIRST_MARKET_DATE, LAST_MARKET_DATE

# ----------------------------------------------------------------------------------------------------------------------
DAG_NAME: str = "sm_math_metadata"

CONFIG_PARAMS: Dict[str, str] = {
    "begin_date": FIRST_MARKET_DATE,
    "end_date": LAST_MARKET_DATE,
}


# ----------------------------------------------------------------------------------------------------------------------
@dag(
    dag_id=DAG_NAME,
    default_args=DEFAULT_ARGS,
    catchup=False,
    params=CONFIG_PARAMS,
    tags=["stock_market", "math"],
)
def main() -> None:
    """The main DAG flow."""
    from airflow.providers.standard.operators.empty import EmptyOperator
    from airflow.sdk import TaskGroup
    from dags_stock_market.sm_math_metadata.task_groups.tg_01_correlation_analysis import tg_01_correlation_analysis
    from dags_stock_market.sm_math_metadata.task_groups.tg_01_numerical_analysis import tg_01_numerical_analysis

    # noinspection PyNoneFunctionAssignment,PyTypeChecker
    tg_01_correlation_analysis: TaskGroup = tg_01_correlation_analysis()
    # noinspection PyNoneFunctionAssignment,PyTypeChecker
    tg_01_numerical_analysis: TaskGroup = tg_01_numerical_analysis()

    dag_finish: EmptyOperator = EmptyOperator(task_id="dag_finish", trigger_rule=DEFAULT_TRIGGER)

    # noinspection PyTypeChecker,PyUnresolvedReferences
    (store_dag_info() >> [tg_01_correlation_analysis, tg_01_numerical_analysis] >> dag_finish)


# ----------------------------------------------------------------------------------------------------------------------
main()
