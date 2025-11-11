"""Create SQL Tables for general Market data."""

from typing import Union

from airflow.models import TaskInstance
from airflow.sdk import Context, task, task_group
from global_airflow.airflow_utils import DEFAULT_TRIGGER


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="fred", trigger_rule=DEFAULT_TRIGGER)
def fred(**context: Union[Context, TaskInstance]) -> None:
    """Creates SQL partitioned table(s).

    :param context: Union[Context, TaskInstance]: Set of variables that are inherent in
        every Airflow Task function that can be utilized.
    """
    from dags_stock_market.const_vars import FRED_COLUMNS_LIST
    from global_utils.develop_vars import END_MARKET_YEAR
    from global_utils.sql_utils import create_sql_partitioned_table

    reset_exec_timestamp: bool = context["params"]["reset_exec_timestamp"]

    target_var: str = "fred"

    create_sql_partitioned_table(
        sql_schema=target_var,
        sql_table="raw_data",
        columns_data_type_list=FRED_COLUMNS_LIST,
        start_year=1985,
        end_year=END_MARKET_YEAR,
        reset_exec_timestamp=reset_exec_timestamp,
    )


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="sharadar_daily_prices", trigger_rule=DEFAULT_TRIGGER)
def sharadar_daily_prices(**context: Union[Context, TaskInstance]) -> None:
    """Creates SQL partitioned table(s).

    :param context: Union[Context, TaskInstance]: Set of variables that are inherent in
        every Airflow Task function that can be utilized.
    """
    from dags_stock_market.const_vars import SHARADAR_DAILY_PRICES_COLUMNS_LIST
    from global_utils.develop_vars import END_MARKET_YEAR
    from global_utils.sql_utils import create_sql_partitioned_table

    reset_exec_timestamp: bool = context["params"]["reset_exec_timestamp"]

    target_var: str = "sharadar"

    create_sql_partitioned_table(
        sql_schema=target_var,
        sql_table="daily_prices",
        columns_data_type_list=SHARADAR_DAILY_PRICES_COLUMNS_LIST,
        start_year=1990,
        end_year=END_MARKET_YEAR,
        index_columns=["market_date", "symbol"],
        monthly_partition=True,
        create_metadata_table=True,
        reset_exec_timestamp=reset_exec_timestamp,
    )


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="sharadar_exclude_symbols", trigger_rule=DEFAULT_TRIGGER)
def sharadar_exclude_symbols(**context: Union[Context, TaskInstance]) -> None:
    """Creates SQL partitioned table(s).

    :param context: Union[Context, TaskInstance]: Set of variables that are inherent in
        every Airflow Task function that can be utilized.
    """
    from typing import Dict, List, Union

    import pandas as pd
    from dags_stock_market.const_vars import SHARADAR_EXCLUDE_SYMBOLS_COLUMNS_LIST
    from global_utils.develop_vars import END_MARKET_YEAR, START_MARKET_YEAR
    from global_utils.sql_utils import (
        SQL_CHUNKSIZE_PANDAS,
        SQL_ENGINE,
        create_sql_partitioned_table,
    )

    reset_exec_timestamp: bool = context["params"]["reset_exec_timestamp"]

    target_var: str = "sharadar"

    create_sql_partitioned_table(
        sql_schema=target_var,
        sql_table="exclude_symbols",
        columns_data_type_list=SHARADAR_EXCLUDE_SYMBOLS_COLUMNS_LIST,
        start_year=START_MARKET_YEAR,
        end_year=END_MARKET_YEAR,
        index_columns=["market_date", "symbol"],
        monthly_partition=False,
        create_metadata_table=False,
        reset_exec_timestamp=reset_exec_timestamp,
    )
    data_dict: List[Dict[str, Union[str, int]]] = [
        {
            "exec_timestamp": "PLACEHOLDER",
            "market_date": "1990-01-01",
            "market_year": 1990,
            "market_month": 1,
            "symbol": "PLACEHOLDER",
            "lastpricedate": "1990-01-01",
        },
    ]
    temp_df: pd.DataFrame = pd.DataFrame(data=data_dict)
    temp_df.to_sql(
        schema="sharadar",
        name="exclude_symbols",
        con=SQL_ENGINE,
        if_exists="append",
        index=False,
        chunksize=int(SQL_CHUNKSIZE_PANDAS / len(temp_df.columns)),
    )


# ----------------------------------------------------------------------------------------------------------------------
@task_group(group_id="sm_daily_data")
def sm_tg_daily_data() -> None:
    """Task group for Market Data."""
    from airflow.providers.standard.operators.empty import EmptyOperator

    tg_finish: EmptyOperator = EmptyOperator(task_id="tg_finish", trigger_rule=DEFAULT_TRIGGER)

    # noinspection PyTypeChecker,PyUnresolvedReferences,PyStatementEffect
    (
        [
            fred(),
            sharadar_daily_prices(),
            sharadar_exclude_symbols(),
        ]
        >> tg_finish
    )
