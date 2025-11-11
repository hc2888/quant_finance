"""Create SQL Tables for general Market data."""

from typing import List, Union

from airflow.models import TaskInstance
from airflow.sdk import Context, task, task_group
from global_airflow.airflow_utils import DEFAULT_TRIGGER


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="alpha_vantage", trigger_rule=DEFAULT_TRIGGER)
def alpha_vantage(**context: Union[Context, TaskInstance]) -> None:
    """Creates SQL partitioned table(s).

    :param context: Union[Context, TaskInstance]: Set of variables that are inherent in
        every Airflow Task function that can be utilized.
    """
    from dags_stock_market.const_vars import ALPHA_VANTAGE_REAL_TIME_COLUMNS_LIST
    from global_utils.develop_vars import END_MARKET_YEAR
    from global_utils.sql_utils import create_sql_partitioned_table

    reset_exec_timestamp: bool = context["params"]["reset_exec_timestamp"]

    # NOTE: In case you want to collect `adjusted` real time price data; but not reliable
    # price_type_list: List[str] = ["adj", "non_adj"]
    price_type_list: List[str] = ["non_adj"]
    time_intervals_list: List[str] = ["1min", "5min", "15min", "30min", "60min"]
    # time_intervals_list: List[str] = ["1min"]

    price_type: str
    for price_type in price_type_list:
        time_interval: str
        for time_interval in time_intervals_list:
            create_sql_partitioned_table(
                sql_schema="alpha_vantage",
                sql_table=rf"{price_type}_{time_interval}",
                columns_data_type_list=ALPHA_VANTAGE_REAL_TIME_COLUMNS_LIST,
                start_year=2000,
                end_year=END_MARKET_YEAR,
                index_columns=["market_date", "symbol", "market_timestamp"],
                monthly_partition=True,
                reset_exec_timestamp=reset_exec_timestamp,
            )


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="model_prep_api_etf", trigger_rule=DEFAULT_TRIGGER)
def model_prep_api_etf(**context: Union[Context, TaskInstance]) -> None:
    """Creates SQL partitioned table(s).

    :param context: Union[Context, TaskInstance]: Set of variables that are inherent in
        every Airflow Task function that can be utilized.
    """
    from dags_stock_market.const_vars import MODEL_PREP_INTRA_COLUMNS_LIST
    from global_utils.develop_vars import END_MARKET_YEAR
    from global_utils.sql_utils import create_sql_partitioned_table

    reset_exec_timestamp: bool = context["params"]["reset_exec_timestamp"]

    price_type_list: List[str] = ["non_adj", "split_only", "split_div"]
    time_intervals_list: List[str] = ["1min", "5min", "30min", "60min", "daily"]

    price_type: str
    for price_type in price_type_list:
        time_interval: str
        for time_interval in time_intervals_list:
            if time_interval in ["5min", "30min", "60min"] and price_type == "non_adj":
                pass
            else:
                index_columns: List[str] = ["market_date", "symbol", "market_timestamp"]
                columns_type_list: List[str] = MODEL_PREP_INTRA_COLUMNS_LIST.copy()
                if time_interval == "daily":
                    index_columns.remove("market_timestamp")

                    remove_items: List[str] = [
                        "market_timestamp TEXT NOT NULL",
                        "market_clock TEXT NOT NULL",
                        "market_hour INTEGER NOT NULL",
                        "market_minute INTEGER NOT NULL",
                    ]
                    columns_type_list: List[str] = [col for col in columns_type_list if col not in remove_items]

                create_sql_partitioned_table(
                    sql_schema="model_prep_api",
                    sql_table=f"etf_{price_type}_{time_interval}",
                    columns_data_type_list=columns_type_list,
                    start_year=2000,
                    end_year=END_MARKET_YEAR,
                    index_columns=index_columns,
                    monthly_partition=True,
                    reset_exec_timestamp=reset_exec_timestamp,
                )


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="model_prep_api_index", trigger_rule=DEFAULT_TRIGGER)
def model_prep_api_index(**context: Union[Context, TaskInstance]) -> None:
    """Creates SQL partitioned table(s).

    :param context: Union[Context, TaskInstance]: Set of variables that are inherent in
        every Airflow Task function that can be utilized.
    """
    from dags_stock_market.const_vars import MODEL_PREP_INTRA_COLUMNS_LIST
    from global_utils.develop_vars import END_MARKET_YEAR
    from global_utils.sql_utils import create_sql_partitioned_table

    reset_exec_timestamp: bool = context["params"]["reset_exec_timestamp"]

    time_intervals_list: List[str] = ["1min", "5min", "30min", "60min", "daily"]

    time_interval: str
    for time_interval in time_intervals_list:
        index_columns: List[str] = ["market_date", "symbol", "market_timestamp"]
        columns_type_list: List[str] = MODEL_PREP_INTRA_COLUMNS_LIST.copy()
        if time_interval == "daily":
            index_columns.remove("market_timestamp")

            remove_items: List[str] = [
                "market_timestamp TEXT NOT NULL",
                "market_clock TEXT NOT NULL",
                "market_hour INTEGER NOT NULL",
                "market_minute INTEGER NOT NULL",
            ]
            columns_type_list: List[str] = [col for col in columns_type_list if col not in remove_items]

        create_sql_partitioned_table(
            sql_schema="model_prep_api",
            sql_table=f"index_{time_interval}",
            columns_data_type_list=columns_type_list,
            start_year=2000,
            end_year=END_MARKET_YEAR,
            index_columns=index_columns,
            monthly_partition=True,
            reset_exec_timestamp=reset_exec_timestamp,
        )


# ----------------------------------------------------------------------------------------------------------------------
@task_group(group_id="sm_intraday")
def sm_tg_intraday() -> None:
    """Task group for Market Data."""
    from airflow.providers.standard.operators.empty import EmptyOperator

    tg_finish: EmptyOperator = EmptyOperator(task_id="tg_finish", trigger_rule=DEFAULT_TRIGGER)

    # noinspection PyTypeChecker,PyUnresolvedReferences,PyStatementEffect
    (
        [
            alpha_vantage(),
            model_prep_api_etf(),
            model_prep_api_index(),
        ]
        >> tg_finish
    )
