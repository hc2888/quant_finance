"""DOCUMENTATION LINK: https://site.financialmodelingprep.com/developer/docs/stable."""

from typing import Any, Dict, List, Union

from airflow.models import TaskInstance
from airflow.sdk import Context, dag, task
from global_airflow.airflow_utils import DEFAULT_ARGS, DEFAULT_TRIGGER, drop_temp_tables, store_dag_info

# ----------------------------------------------------------------------------------------------------------------------
DAG_NAME: str = "sm_etl_02_model_prep_api"

DROP_TABLES: bool = False

CONFIG_PARAMS: Dict[str, Any] = {
    "trigger_next_dag": True,
}


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="all_market_days", trigger_rule=DEFAULT_TRIGGER)
def all_market_days() -> None:
    """Retrieve the entire range of market dates."""
    import pandas as pd
    from global_utils.develop_secrets import ALPHA_VANTAGE_API_KEY
    from global_utils.sql_utils import SQL_CHUNKSIZE_PANDAS, SQL_ENGINE

    url_link_list: List[str] = [
        r"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY",
        rf"symbol=SPY",
        rf"apikey={ALPHA_VANTAGE_API_KEY}",
        r"outputsize=full",
        r"datatype=csv",
    ]
    url_link_str: str = r"&".join(url_link_list)
    market_days: pd.DataFrame = pd.read_csv(filepath_or_buffer=url_link_str, storage_options={"ssl": True})

    market_days: pd.DataFrame = market_days.rename(columns={"timestamp": "market_date"})

    drop_columns: List[str] = ["open", "high", "low", "close", "volume"]
    market_days: pd.DataFrame = market_days.rename(columns={"timestamp": "market_date"}).drop(columns=drop_columns)

    market_days.to_sql(
        schema="temp_tables",
        name=f"sm_all_market_days",
        con=SQL_ENGINE,
        if_exists="replace",
        index=False,
        chunksize=int(SQL_CHUNKSIZE_PANDAS / len(market_days.columns)),
    )


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
            trigger_dag_id="sm_etl_03_alpha_vantage",
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
    on_failure_callback=drop_temp_tables(temp_table_prefix="sm", drop_tables=DROP_TABLES),
    tags=["stock_market", "etl"],
)
def main() -> None:
    """The main DAG flow."""
    from airflow.providers.standard.operators.empty import EmptyOperator
    from airflow.sdk import TaskGroup
    from dags_stock_market.sm_etl_02_model_prep_api.task_groups.tg_01_main_etl import tg_01_main_etl

    # noinspection PyNoneFunctionAssignment,PyTypeChecker
    tg_01_main_etl: TaskGroup = tg_01_main_etl()

    dag_finish: EmptyOperator = EmptyOperator(task_id="dag_finish", trigger_rule=DEFAULT_TRIGGER)

    """MAIN FLOW."""
    # noinspection PyTypeChecker,PyUnresolvedReferences
    (store_dag_info() >> all_market_days() >> tg_01_main_etl >> invoke_next_dag() >> dag_finish)


# ----------------------------------------------------------------------------------------------------------------------
main()
