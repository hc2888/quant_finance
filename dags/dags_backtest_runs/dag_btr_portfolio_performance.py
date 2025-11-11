"""Execute a simulated historical market price run that measures the performance of a Portfolio."""

from typing import Dict, List, Union

from airflow.models import TaskInstance
from airflow.sdk import Context, dag, task
from global_airflow.airflow_utils import DEFAULT_ARGS, DEFAULT_TRIGGER, drop_temp_tables, store_dag_info

# ----------------------------------------------------------------------------------------------------------------------
DAG_NAME: str = "btr_portfolio_performance"

DROP_TABLES: bool = False

CONFIG_PARAMS: Dict[str, Union[str, List[str]]] = {
    "begin_market_date": "2025-01-13",
    "end_market_date": "2025-01-17",
    "symbol_list": ["SPY", "GOVT", "QTUM"],
}


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="execute_run", trigger_rule=DEFAULT_TRIGGER)
def execute_run(**context: Union[Context, TaskInstance]) -> None:
    """Execute a VectorBT Pro Portfolio run that outputs performance statistics etc.

    :param context: Union[Context, TaskInstance]: Set of variables that are inherent in
        every Airflow Task function that can be utilized.
    """
    import pandas as pd
    from backtest_metrics.from_signals.portfolio_runs import portfolio_performance
    from global_utils.sql_utils import SQL_ENGINE

    symbol_list: List[str] = context["params"]["symbol_list"]
    begin_market_date: str = context["params"]["begin_market_date"]
    end_market_date: str = context["params"]["end_market_date"]

    exec_timestamp: str = context["ti"].xcom_pull(task_ids=r"store_dag_info", key="exec_timestamp")

    symbol_list_str = str(symbol_list)[1:-1]
    sql_query: str = f"""
    SELECT
    symbol
    , market_date
    , market_year
    , market_month
    , market_timestamp
    , market_hour
    , market_minute
    , open
    , high
    , low
    , close
    , volume

    FROM alpha_vantage.non_adj_1min

    WHERE symbol IN ({symbol_list_str})
    AND market_date BETWEEN '{begin_market_date}' AND '{end_market_date}'
    """
    main_df: pd.DataFrame = pd.read_sql(sql=sql_query, con=SQL_ENGINE)

    portfolio_results: Dict[str, pd.DataFrame] = portfolio_performance(
        main_df=main_df,
        symbol_list=symbol_list,
        begin_market_date=begin_market_date,
        exec_timestamp=exec_timestamp,
    )
    stat_results: pd.DataFrame = portfolio_results["stat_results"]
    stat_results.to_sql(
        schema="backtesting",
        name="stat_results",
        con=SQL_ENGINE,
        if_exists="append",
        index=False,
    )

    order_results: pd.DataFrame = portfolio_results["order_results"]
    order_results.to_sql(
        schema="backtesting",
        name="order_results",
        con=SQL_ENGINE,
        if_exists="append",
        index=False,
    )

    order_details: pd.DataFrame = portfolio_results["order_details"]
    order_details.to_sql(
        schema="backtesting",
        name="order_details",
        con=SQL_ENGINE,
        if_exists="append",
        index=False,
    )


# ----------------------------------------------------------------------------------------------------------------------
@dag(
    dag_id=DAG_NAME,
    default_args=DEFAULT_ARGS,
    catchup=False,
    params=CONFIG_PARAMS,
    on_failure_callback=drop_temp_tables(temp_table_prefix="sm", drop_tables=DROP_TABLES),
    tags=["btr"],
)
def main() -> None:
    """The main DAG flow."""
    from airflow.providers.standard.operators.empty import EmptyOperator

    dag_finish: EmptyOperator = EmptyOperator(task_id="dag_finish", trigger_rule=DEFAULT_TRIGGER)

    # noinspection PyTypeChecker,PyUnresolvedReferences
    (store_dag_info() >> execute_run() >> dag_finish)


# ----------------------------------------------------------------------------------------------------------------------
main()
