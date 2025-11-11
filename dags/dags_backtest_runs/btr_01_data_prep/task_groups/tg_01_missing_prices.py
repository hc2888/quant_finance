"""Task Group for adding missing price data."""

from typing import List, Union

from airflow.models import TaskInstance
from airflow.sdk import Context, task, task_group
from global_airflow.airflow_utils import DEFAULT_TRIGGER


# ----------------------------------------------------------------------------------------------------------------------
@task.branch(task_id="create_date_ranges", trigger_rule=DEFAULT_TRIGGER)
def create_date_ranges(**context: Union[Context, TaskInstance]) -> str:
    """Create temp SQL table with initial set of price data for all symbols.

    :param context: Union[Context, TaskInstance]: Set of variables that are inherent in
        every Airflow Task function that can be utilized.

    :return: str: Airflow Task to trigger.
    """
    from global_utils.sql_utils import SQL_ENGINE
    from sqlalchemy import text

    schema_table: str = "temp_tables.btr_date_ranges"

    with SQL_ENGINE.connect().execution_options(isolation_level="AUTOCOMMIT") as sql_connection:
        drop_query: str = f"DROP TABLE IF EXISTS {schema_table} CASCADE"
        sql_connection.execute(text(drop_query))

        backtest_begin_date: str = context["params"]["backtest_begin_date"]
        create_query: str = f"""
        CREATE TABLE {schema_table} AS

        WITH begin_dates AS (
            SELECT symbol, MAX(market_date) AS begin_date
            FROM backtesting.intraday
            GROUP BY symbol
        ),
        base_data AS (
            SELECT
                prep_api.symbol
                , begin_dates.begin_date
                , MIN(prep_api.market_date) AS min_index_date
                , MAX(prep_api.market_date) AS max_index_date
            FROM model_prep_api.index_1min AS prep_api
            LEFT JOIN begin_dates
                ON prep_api.symbol = begin_dates.symbol
            GROUP BY prep_api.symbol, begin_dates.begin_date
            --HAVING MIN(prep_api.market_date) <= '{backtest_begin_date}'
        )
        SELECT
            symbol
            , COALESCE(begin_date, GREATEST('{backtest_begin_date}', min_index_date)) AS begin_date
            , max_index_date AS end_date
            , CASE WHEN begin_date IS NULL THEN '1900-01-01'
                ELSE begin_date
                END AS cutoff_date
        FROM base_data
        WHERE COALESCE(begin_date, GREATEST('{backtest_begin_date}', min_index_date)) < max_index_date
        """
        sql_connection.execute(text(create_query))

        idx_name: str = schema_table.replace(".", "_")
        sql_query: str = rf"CREATE INDEX {idx_name}_idx ON {schema_table} (symbol, begin_date, end_date)"
        sql_connection.execute(text(sql_query))
        sql_connection.execute(text(f"VACUUM FULL ANALYZE {schema_table}"))

        count_query: str = f"SELECT COUNT(*) FROM {schema_table}"
        row_count: int = sql_connection.execute(text(count_query)).scalar()

        if row_count > 0:
            task_var: str = "missing_prices.create_market_ranges"
        else:
            task_var: str = "missing_prices.no_data_to_process"

        return task_var


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="create_market_ranges", trigger_rule=DEFAULT_TRIGGER)
def create_market_ranges(**context: Union[Context, TaskInstance]) -> None:
    """Create temp SQL table with initial set of price data for all symbols.

    :param context: Union[Context, TaskInstance]: Set of variables that are inherent in
        every Airflow Task function that can be utilized.
    """
    from global_utils.sql_utils import SQL_ENGINE
    from sqlalchemy import text

    schema_table: str = "temp_tables.btr_market_ranges"

    with SQL_ENGINE.connect().execution_options(isolation_level="AUTOCOMMIT") as sql_connection:
        drop_query: str = f"DROP TABLE IF EXISTS {schema_table} CASCADE"
        sql_connection.execute(text(drop_query))

        begin_date_query: str = "SELECT MIN(begin_date) AS begin_date FROM temp_tables.btr_date_ranges"
        begin_date: str = sql_connection.execute(text(begin_date_query)).scalar()

        end_date_query: str = "SELECT MAX(end_date) AS end_date FROM temp_tables.btr_date_ranges"
        end_date: str = sql_connection.execute(text(end_date_query)).scalar()

        context["ti"].xcom_push(key="begin_date", value=begin_date)
        print(rf"BEGIN DATE: {begin_date}")
        context["ti"].xcom_push(key="end_date", value=end_date)
        print(rf"END DATE: {end_date}")

        create_query: str = f"""
        CREATE TABLE {schema_table} AS

        SELECT market_date, market_timestamp
        FROM backtesting.all_market_dates
        WHERE market_date BETWEEN '{begin_date}' AND '{end_date}'
        """
        sql_connection.execute(text(create_query))

        idx_name: str = schema_table.replace(".", "_")
        sql_query: str = rf"CREATE INDEX {idx_name}_idx ON {schema_table} (market_date, market_timestamp)"
        sql_connection.execute(text(sql_query))
        sql_connection.execute(text(f"VACUUM FULL ANALYZE {schema_table}"))


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="create_missing_prices", trigger_rule=DEFAULT_TRIGGER)
def create_missing_prices() -> None:
    """Create temp SQL table with initial set of price data for all symbols."""
    from global_utils.sql_utils import SQL_ENGINE, SQL_PARTITION_CORES, sql_big_query_prep
    from sqlalchemy import text

    schema_table: str = "temp_tables.btr_missing_prices"

    with SQL_ENGINE.connect().execution_options(isolation_level="AUTOCOMMIT") as sql_connection:
        sql_big_query_prep(sql_connection=sql_connection)  # NOTE: Set these for BIG queries

        drop_query: str = f"DROP TABLE IF EXISTS {schema_table} CASCADE"
        sql_connection.execute(text(drop_query))

        create_query: str = f"""
        CREATE TABLE {schema_table} AS

        SELECT
            DR.symbol
            , MR.market_date
            , MR.market_timestamp
            , AV.open
            , AV.high
            , AV.low
            , AV.close
            , AV.volume
            , DR.cutoff_date
            , (ROW_NUMBER() OVER () % {SQL_PARTITION_CORES}) AS cx_partition

        FROM temp_tables.btr_date_ranges AS DR

        INNER JOIN temp_tables.btr_market_ranges AS MR
            ON MR.market_date BETWEEN DR.begin_date AND DR.end_date

        LEFT JOIN model_prep_api.index_1min AS AV
            ON AV.market_date = MR.market_date
            AND AV.symbol = DR.symbol
            AND AV.market_timestamp = MR.market_timestamp
        """
        sql_connection.execute(text(create_query))

        idx_name: str = schema_table.replace(".", "_")
        sql_query: str = rf"CREATE INDEX {idx_name}_idx ON {schema_table} (cx_partition, symbol)"
        sql_connection.execute(text(sql_query))
        sql_connection.execute(text(f"VACUUM FULL ANALYZE {schema_table}"))


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="fill_missing_price_data", trigger_rule=DEFAULT_TRIGGER)
def fill_missing_price_data() -> None:
    """Invoke Modin API to fill missing price data."""
    import requests
    from global_utils.develop_vars import DOCKER_IP, MODIN_FASTAPI_PORT
    from requests.models import Response

    sub_api_name: str = "btr_01_data_prep"
    sub_api_target_func: str = "fill_missing_price_data"

    target_api_route: str = rf"{sub_api_name}/{sub_api_target_func}/"
    http_response: Response = requests.post(url=rf"http://{DOCKER_IP}:{MODIN_FASTAPI_PORT}/{target_api_route}")
    http_response.raise_for_status()


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="dedupe_intraday", trigger_rule=DEFAULT_TRIGGER)
def dedupe_intraday(**context: Union[Context, TaskInstance]) -> None:
    """De-duplicate the target SQL table.

    :param context: Union[Context, TaskInstance]: Set of variables that are inherent in
        every Airflow Task function that can be utilized.
    """
    from global_utils.sql_utils import dedupe_sql_table

    begin_date: str = context["ti"].xcom_pull(task_ids=rf"missing_prices.create_market_ranges", key="begin_date")
    end_date: str = context["ti"].xcom_pull(task_ids=rf"missing_prices.create_market_ranges", key="end_date")

    # Deduplicate the SQL Table
    partition_by_cols: List[str] = ["market_date", "symbol", "market_timestamp"]
    dedupe_sql_table(
        sql_schema="backtesting",
        sql_table="intraday",
        partition_by_cols=partition_by_cols,
        begin_market_date=begin_date,
        end_market_date=end_date,
    )


# ----------------------------------------------------------------------------------------------------------------------
@task_group(group_id="missing_prices")
def tg_01_missing_prices() -> None:
    """Task Group for adding missing price data."""
    from airflow.providers.standard.operators.empty import EmptyOperator

    branch_create_date_ranges: str = create_date_ranges()

    no_data_to_process: EmptyOperator = EmptyOperator(task_id="no_data_to_process", trigger_rule=DEFAULT_TRIGGER)
    tg_finish: EmptyOperator = EmptyOperator(task_id="tg_finish", trigger_rule=DEFAULT_TRIGGER)

    """MAIN FLOW"""
    # noinspection PyTypeChecker,PyUnresolvedReferences
    (
        branch_create_date_ranges
        >> create_market_ranges()
        >> create_missing_prices()
        >> fill_missing_price_data()
        >> dedupe_intraday()
        >> tg_finish
    )
    """NO DATA FLOWS."""
    # noinspection PyTypeChecker
    (branch_create_date_ranges >> no_data_to_process >> tg_finish)
