"""Executes FRED data processing and analysis."""

from typing import Any, Dict, Union

from airflow.models import TaskInstance
from airflow.sdk import Context, dag, task
from global_airflow.airflow_utils import DEFAULT_ARGS, DEFAULT_TRIGGER, store_dag_info

# ----------------------------------------------------------------------------------------------------------------------
DAG_NAME: str = "sm_etl_01_fred"

CONFIG_PARAMS: Dict[str, Any] = {
    "execute_backfill": False,
    "trigger_next_dag": True,
}

# Update parameter value(s)
DEFAULT_ARGS["retries"]: int = 0


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
            trigger_dag_id="sm_etl_02_model_prep_api",
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
    tags=["stock_market", "etl"],
)
def main() -> None:
    """The main DAG flow."""
    from airflow.providers.standard.operators.empty import EmptyOperator
    from airflow.sdk import TaskGroup
    from dags_stock_market.sm_etl_01_fred.task_groups.tg_01_process_raw_data import tg_01_process_raw_data
    from global_airflow.airflow_utils import drop_temp_tables

    # noinspection PyNoneFunctionAssignment,PyTypeChecker
    tg_01_process_raw_data: TaskGroup = tg_01_process_raw_data()

    dag_finish: EmptyOperator = EmptyOperator(task_id="dag_finish", trigger_rule=DEFAULT_TRIGGER)

    # noinspection PyTypeChecker,PyUnresolvedReferences
    (
        store_dag_info()
        >> tg_01_process_raw_data
        >> drop_temp_tables(temp_table_prefix=DAG_NAME)
        >> invoke_next_dag()
        >> dag_finish
    )


# ----------------------------------------------------------------------------------------------------------------------
main()
