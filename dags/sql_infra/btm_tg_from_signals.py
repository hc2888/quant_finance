"""Create SQL Tables for `from_signals()` method outputs."""

from typing import Union

from airflow.models import TaskInstance
from airflow.sdk import Context, task, task_group
from global_airflow.airflow_utils import DEFAULT_TRIGGER


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="stat_results", trigger_rule=DEFAULT_TRIGGER)
def stat_results(**context: Union[Context, TaskInstance]) -> None:
    """Creates SQL partitioned table(s).

    :param context: Union[Context, TaskInstance]: Set of variables that are inherent in
        every Airflow Task function that can be utilized.
    """
    from backtest_metrics.const_vars import FROM_SIGNALS_STAT_RESULTS
    from global_utils.develop_vars import END_MARKET_YEAR
    from global_utils.sql_utils import create_sql_partitioned_table

    reset_exec_timestamp: bool = context["params"]["reset_exec_timestamp"]

    create_sql_partitioned_table(
        sql_schema="backtesting",
        sql_table="stat_results",
        columns_data_type_list=FROM_SIGNALS_STAT_RESULTS,
        start_year=2025,
        end_year=END_MARKET_YEAR,
        partition_column_name="begin_market_date",
        index_columns=["begin_market_date", "symbol"],
        monthly_partition=False,
        create_metadata_table=False,
        reset_exec_timestamp=reset_exec_timestamp,
    )


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="order_results", trigger_rule=DEFAULT_TRIGGER)
def order_results(**context: Union[Context, TaskInstance]) -> None:
    """Creates SQL partitioned table(s).

    :param context: Union[Context, TaskInstance]: Set of variables that are inherent in
        every Airflow Task function that can be utilized.
    """
    from backtest_metrics.const_vars import FROM_SIGNALS_ORDER_RESULTS
    from global_utils.develop_vars import END_MARKET_YEAR
    from global_utils.sql_utils import create_sql_partitioned_table

    reset_exec_timestamp: bool = context["params"]["reset_exec_timestamp"]

    create_sql_partitioned_table(
        sql_schema="backtesting",
        sql_table="order_results",
        columns_data_type_list=FROM_SIGNALS_ORDER_RESULTS,
        start_year=2025,
        end_year=END_MARKET_YEAR,
        partition_column_name="begin_market_date",
        index_columns=["begin_market_date", "symbol"],
        monthly_partition=False,
        create_metadata_table=False,
        reset_exec_timestamp=reset_exec_timestamp,
    )


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="order_details", trigger_rule=DEFAULT_TRIGGER)
def order_details(**context: Union[Context, TaskInstance]) -> None:
    """Creates SQL partitioned table(s).

    :param context: Union[Context, TaskInstance]: Set of variables that are inherent in
        every Airflow Task function that can be utilized.
    """
    from backtest_metrics.const_vars import FROM_SIGNALS_ORDER_DETAILS
    from global_utils.develop_vars import END_MARKET_YEAR
    from global_utils.sql_utils import create_sql_partitioned_table

    reset_exec_timestamp: bool = context["params"]["reset_exec_timestamp"]

    create_sql_partitioned_table(
        sql_schema="backtesting",
        sql_table="order_details",
        columns_data_type_list=FROM_SIGNALS_ORDER_DETAILS,
        start_year=2025,
        end_year=END_MARKET_YEAR,
        partition_column_name="begin_market_date",
        index_columns=["begin_market_date", "symbol"],
        monthly_partition=False,
        create_metadata_table=False,
        reset_exec_timestamp=reset_exec_timestamp,
    )


# ----------------------------------------------------------------------------------------------------------------------
@task_group(group_id="btm_from_signals")
def btm_tg_from_signals() -> None:
    """Task group for `from_signals()` method outputs."""
    from airflow.providers.standard.operators.empty import EmptyOperator

    tg_finish: EmptyOperator = EmptyOperator(task_id="tg_finish", trigger_rule=DEFAULT_TRIGGER)

    # noinspection PyTypeChecker,PyUnresolvedReferences,PyStatementEffect
    (
        [
            stat_results(),
            order_results(),
            order_details(),
        ]
        >> tg_finish
    )
