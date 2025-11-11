"""Create SQL Tables for general Backtest-related prepped market data."""

from typing import Union

from airflow.models import TaskInstance
from airflow.sdk import Context, task, task_group
from global_airflow.airflow_utils import DEFAULT_TRIGGER


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="all_market_dates", trigger_rule=DEFAULT_TRIGGER)
def all_market_dates(**context: Union[Context, TaskInstance]) -> None:
    """Creates SQL partitioned table(s).

    :param context: Union[Context, TaskInstance]: Set of variables that are inherent in
        every Airflow Task function that can be utilized.
    """
    from dags_backtest_runs.const_vars import ALL_MARKET_DATES
    from global_utils.develop_vars import END_MARKET_YEAR
    from global_utils.sql_utils import create_sql_partitioned_table

    reset_exec_timestamp: bool = context["params"]["reset_exec_timestamp"]

    create_sql_partitioned_table(
        sql_schema="backtesting",
        sql_table="all_market_dates",
        columns_data_type_list=ALL_MARKET_DATES,
        start_year=2010,
        end_year=END_MARKET_YEAR,
        partition_column_name="market_date",
        index_columns=["market_date", "market_timestamp"],
        monthly_partition=False,
        create_metadata_table=False,
        reset_exec_timestamp=reset_exec_timestamp,
    )


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="intraday", trigger_rule=DEFAULT_TRIGGER)
def intraday(**context: Union[Context, TaskInstance]) -> None:
    """Creates SQL partitioned table(s).

    :param context: Union[Context, TaskInstance]: Set of variables that are inherent in
        every Airflow Task function that can be utilized.
    """
    from dags_backtest_runs.const_vars import BACKTESTING_INTRADAY
    from global_utils.develop_vars import END_MARKET_YEAR
    from global_utils.sql_utils import create_sql_partitioned_table

    reset_exec_timestamp: bool = context["params"]["reset_exec_timestamp"]

    create_sql_partitioned_table(
        sql_schema="backtesting",
        sql_table="intraday",
        columns_data_type_list=BACKTESTING_INTRADAY,
        start_year=2010,
        end_year=END_MARKET_YEAR,
        index_columns=["market_date", "symbol", "market_timestamp"],
        monthly_partition=True,
        create_metadata_table=True,
        reset_exec_timestamp=reset_exec_timestamp,
    )


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="target_vars", trigger_rule=DEFAULT_TRIGGER)
def target_vars(**context: Union[Context, TaskInstance]) -> None:
    """Creates SQL partitioned table(s).

    :param context: Union[Context, TaskInstance]: Set of variables that are inherent in
        every Airflow Task function that can be utilized.
    """
    from dags_backtest_runs.const_vars import TARGET_VARS
    from global_utils.develop_vars import END_MARKET_YEAR
    from global_utils.sql_utils import create_sql_partitioned_table

    reset_exec_timestamp: bool = context["params"]["reset_exec_timestamp"]

    create_sql_partitioned_table(
        sql_schema="backtesting",
        sql_table="target_vars",
        columns_data_type_list=TARGET_VARS,
        start_year=2010,
        end_year=END_MARKET_YEAR,
        index_columns=["market_date", "symbol", "market_timestamp"],
        monthly_partition=True,
        create_metadata_table=True,
        reset_exec_timestamp=reset_exec_timestamp,
    )


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="model_features", trigger_rule=DEFAULT_TRIGGER)
def model_features(**context: Union[Context, TaskInstance]) -> None:
    """Creates SQL partitioned table(s).

    :param context: Union[Context, TaskInstance]: Set of variables that are inherent in
        every Airflow Task function that can be utilized.
    """
    from dags_backtest_runs.const_vars import MODEL_FEATURES
    from global_utils.develop_vars import END_MARKET_YEAR
    from global_utils.sql_utils import create_sql_partitioned_table

    reset_exec_timestamp: bool = context["params"]["reset_exec_timestamp"]

    create_sql_partitioned_table(
        sql_schema="backtesting",
        sql_table="model_features",
        columns_data_type_list=MODEL_FEATURES,
        start_year=2010,
        end_year=END_MARKET_YEAR,
        index_columns=["market_date", "symbol", "market_timestamp"],
        monthly_partition=True,
        create_metadata_table=True,
        reset_exec_timestamp=reset_exec_timestamp,
    )


# ----------------------------------------------------------------------------------------------------------------------
@task_group(group_id="btr_market_data")
def btr_tg_market_data() -> None:
    """Task group for general Backtest-related prepped market data."""
    from airflow.providers.standard.operators.empty import EmptyOperator

    tg_finish: EmptyOperator = EmptyOperator(task_id="tg_finish", trigger_rule=DEFAULT_TRIGGER)

    # noinspection PyTypeChecker,PyUnresolvedReferences,PyStatementEffect
    (
        [
            all_market_dates(),
            intraday(),
            target_vars(),
            model_features(),
        ]
        >> tg_finish
    )
