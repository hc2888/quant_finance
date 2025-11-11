"""Math calculations applied to stock_market data."""

from typing import Dict

from airflow.sdk import dag
from global_airflow.airflow_utils import DEFAULT_ARGS, DEFAULT_TRIGGER, drop_temp_tables, store_dag_info
from global_utils.develop_vars import FIRST_MARKET_DATE, LAST_MARKET_DATE

# ----------------------------------------------------------------------------------------------------------------------
DAG_NAME: str = "sm_math_calcs"

DROP_TABLES: bool = False

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
    on_failure_callback=drop_temp_tables(temp_table_prefix="sm", drop_tables=DROP_TABLES),
    tags=["stock_market", "math"],
)
def main() -> None:
    """The main DAG flow."""
    from airflow.providers.standard.operators.empty import EmptyOperator
    from airflow.sdk import TaskGroup
    from dags_stock_market.sm_math_calcs.task_groups.tg_01_previous_day_columns import tg_01_previous_day_columns
    from dags_stock_market.sm_math_calcs.task_groups.tg_02_algebra_values import tg_02_algebra_values

    # noinspection PyNoneFunctionAssignment,PyTypeChecker
    tg_01_previous_day_columns: TaskGroup = tg_01_previous_day_columns()
    # noinspection PyNoneFunctionAssignment,PyTypeChecker
    tg_02_algebra_values: TaskGroup = tg_02_algebra_values()

    dag_finish: EmptyOperator = EmptyOperator(task_id="dag_finish", trigger_rule=DEFAULT_TRIGGER)

    # noinspection PyTypeChecker,PyUnresolvedReferences
    (
        store_dag_info()
        >> tg_01_previous_day_columns
        >> tg_02_algebra_values
        >> drop_temp_tables(temp_table_prefix="sm", drop_tables=DROP_TABLES)
        >> dag_finish
    )


# ----------------------------------------------------------------------------------------------------------------------
main()
