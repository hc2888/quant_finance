"""Task Group for adding missing price data."""

from typing import List, Union

from airflow.models import TaskInstance
from airflow.sdk import Context, task, task_group
from global_airflow.airflow_utils import DEFAULT_TRIGGER


# ----------------------------------------------------------------------------------------------------------------------
@task.branch(task_id="create_target_ranges", trigger_rule=DEFAULT_TRIGGER)
def create_target_ranges(**context: Union[Context, TaskInstance]) -> str:
    """Create temp SQL table with initial set of price data for all symbols.

    :param context: Union[Context, TaskInstance]: Set of variables that are inherent in
        every Airflow Task function that can be utilized.

    :return: str: Airflow Task to trigger.
    """
    from global_utils.sql_utils import SQL_ENGINE
    from sqlalchemy import text

    schema_table: str = "temp_tables.btr_target_ranges"

    with SQL_ENGINE.connect().execution_options(isolation_level="AUTOCOMMIT") as sql_connection:
        drop_query: str = f"DROP TABLE IF EXISTS {schema_table} CASCADE"
        sql_connection.execute(text(drop_query))

        backtest_begin_date: str = context["params"]["backtest_begin_date"]
        create_query: str = f"""
        CREATE TABLE {schema_table} AS

        WITH begin_dates AS (
            SELECT symbol, MAX(market_date) AS begin_date
            FROM backtesting.target_vars
            GROUP BY symbol
        ),
        base_data AS (
            SELECT
                prep_api.symbol
                , begin_dates.begin_date
                , MIN(prep_api.market_date) AS min_index_date
                , MAX(prep_api.market_date) AS max_index_date
            FROM model_prep_api.index_1minn AS prep_api
            LEFT JOIN begin_dates
                ON prep_api.symbol = begin_dates.symbol
            GROUP BY prep_api.symbol, begin_dates.begin_date
            HAVING MIN(prep_api.market_date) <= '{backtest_begin_date}'
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
            begin_date_query: str = f"SELECT MIN(begin_date) AS begin_date FROM {schema_table}"
            begin_date: str = sql_connection.execute(text(begin_date_query)).scalar()

            end_date_query: str = f"SELECT MAX(end_date) AS end_date FROM {schema_table}"
            end_date: str = sql_connection.execute(text(end_date_query)).scalar()

            context["ti"].xcom_push(key="begin_date", value=begin_date)
            print(rf"BEGIN DATE: {begin_date}")
            context["ti"].xcom_push(key="end_date", value=end_date)
            print(rf"END DATE: {end_date}")
            task_var: str = "target_vars.create_parsed_intraday"
        else:
            task_var: str = "target_vars.no_data_to_process"

        return task_var


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="create_parsed_intraday", trigger_rule=DEFAULT_TRIGGER)
def create_parsed_intraday(**context: Union[Context, TaskInstance]) -> None:
    """Create temp SQL table with all intraday data with a TIMESTAMP type `market_timestamp` column.

    :param context: Union[Context, TaskInstance]: Set of variables that are inherent in
        every Airflow Task function that can be utilized.

    :return: str: Airflow Task to trigger.
    """
    from global_utils.sql_utils import SQL_ENGINE, sql_big_query_prep
    from sqlalchemy import text

    begin_date: str = context["ti"].xcom_pull(task_ids=rf"target_vars.create_target_ranges", key="begin_date")
    end_date: str = context["ti"].xcom_pull(task_ids=rf"target_vars.create_target_ranges", key="end_date")

    schema_table: str = "temp_tables.parsed_intraday"

    with SQL_ENGINE.connect().execution_options(isolation_level="AUTOCOMMIT") as sql_connection:
        sql_big_query_prep(sql_connection=sql_connection)  # NOTE: Set these for BIG queries

        drop_query: str = f"DROP TABLE IF EXISTS {schema_table} CASCADE"
        sql_connection.execute(text(drop_query))

        parsed_intraday_query: str = f"""
        CREATE TABLE {schema_table} AS

        SELECT
            INT.symbol
            , INT.market_date
            , TO_TIMESTAMP(INT.market_timestamp, 'YYYY-MM-DD HH24:MI:SS') AS new_market_timestamp
            , INT.open
            , INT.high
            , INT.low
            , INT.close
            , INT.volume
        FROM backtesting.intraday AS INT
        WHERE INT.market_date BETWEEN '{begin_date}' AND '{end_date}'
        """
        sql_connection.execute(text(parsed_intraday_query))

        idx_name: str = schema_table.replace(".", "_")
        index_query: str = rf"CREATE INDEX {idx_name}_idx ON {schema_table} (symbol, market_date)"
        sql_connection.execute(text(index_query))
        sql_connection.execute(text(f"VACUUM FULL ANALYZE {schema_table}"))


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="timestamp_intervals", trigger_rule=DEFAULT_TRIGGER)
def timestamp_intervals() -> None:
    """Create temp SQL table with the set of all `market_timestamps` and their future timestamps."""
    from global_utils.sql_utils import SQL_ENGINE, sql_big_query_prep
    from sqlalchemy import text

    schema_table: str = "temp_tables.btr_timestamp_intervals"

    with SQL_ENGINE.connect().execution_options(isolation_level="AUTOCOMMIT") as sql_connection:
        sql_big_query_prep(sql_connection=sql_connection)  # NOTE: Set these for BIG queries

        drop_query: str = f"DROP TABLE IF EXISTS {schema_table} CASCADE"
        sql_connection.execute(text(drop_query))

        create_query: str = f"""
        CREATE TABLE {schema_table} AS

        SELECT
            P.symbol
            , P.market_date
            , TO_CHAR(P.new_market_timestamp, 'YYYY-MM-DD HH24:MI:SS') AS market_timestamp
            , P.open
            , P.high
            , P.low
            , P.close
            , P.volume

            , TO_CHAR(P.new_market_timestamp + INTERVAL '5 minutes',  'YYYY-MM-DD HH24:MI:SS') AS market_ts_5
            , TO_CHAR(P.new_market_timestamp + INTERVAL '15 minutes', 'YYYY-MM-DD HH24:MI:SS') AS market_ts_15
            , TO_CHAR(P.new_market_timestamp + INTERVAL '30 minutes', 'YYYY-MM-DD HH24:MI:SS') AS market_ts_30
            , TO_CHAR(P.new_market_timestamp + INTERVAL '60 minutes', 'YYYY-MM-DD HH24:MI:SS') AS market_ts_60

            , TO_CHAR(P.new_market_timestamp - INTERVAL '5 minutes',  'YYYY-MM-DD HH24:MI:SS') AS prev_market_ts_5
            , TO_CHAR(P.new_market_timestamp - INTERVAL '15 minutes', 'YYYY-MM-DD HH24:MI:SS') AS prev_market_ts_15
            , TO_CHAR(P.new_market_timestamp - INTERVAL '30 minutes', 'YYYY-MM-DD HH24:MI:SS') AS prev_market_ts_30
            , TO_CHAR(P.new_market_timestamp - INTERVAL '60 minutes', 'YYYY-MM-DD HH24:MI:SS') AS prev_market_ts_60

            , DR.cutoff_date

            FROM temp_tables.btr_target_ranges AS DR
            INNER JOIN temp_tables.parsed_intraday AS P
                ON P.symbol = DR.symbol
                AND P.market_date BETWEEN DR.begin_date AND DR.end_date
        """
        sql_connection.execute(text(create_query))

        idx_name: str = schema_table.replace(".", "_")

        ts_5_query: str = rf"CREATE INDEX {idx_name}_idx_5 ON {schema_table} (symbol, market_ts_5)"
        sql_connection.execute(text(ts_5_query))

        ts_15_query: str = rf"CREATE INDEX {idx_name}_idx_15 ON {schema_table} (symbol, market_ts_15)"
        sql_connection.execute(text(ts_15_query))

        ts_30_query: str = rf"CREATE INDEX {idx_name}_idx_30 ON {schema_table} (symbol, market_ts_30)"
        sql_connection.execute(text(ts_30_query))

        ts_60_query: str = rf"CREATE INDEX {idx_name}_idx_60 ON {schema_table} (symbol, market_ts_60)"
        sql_connection.execute(text(ts_60_query))

        prev_ts_5_query: str = rf"CREATE INDEX {idx_name}_prev_idx_5 ON {schema_table} (symbol, prev_market_ts_5)"
        sql_connection.execute(text(prev_ts_5_query))

        prev_ts_15_query: str = rf"CREATE INDEX {idx_name}_prev_idx_15 ON {schema_table} (symbol, prev_market_ts_15)"
        sql_connection.execute(text(prev_ts_15_query))

        prev_ts_30_query: str = rf"CREATE INDEX {idx_name}_prev_idx_30 ON {schema_table} (symbol, prev_market_ts_30)"
        sql_connection.execute(text(prev_ts_30_query))

        prev_ts_60_query: str = rf"CREATE INDEX {idx_name}_prev_idx_60 ON {schema_table} (symbol, prev_market_ts_60)"
        sql_connection.execute(text(prev_ts_60_query))

        sql_connection.execute(text(f"VACUUM FULL ANALYZE {schema_table}"))


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="create_base_intraday", trigger_rule=DEFAULT_TRIGGER)
def create_base_intraday(**context: Union[Context, TaskInstance]) -> None:
    """Create temp SQL table with all intraday data indexed on `market_timestamp` column.

    :param context: Union[Context, TaskInstance]: Set of variables that are inherent in
        every Airflow Task function that can be utilized.

    :return: str: Airflow Task to trigger.
    """
    from global_utils.sql_utils import SQL_ENGINE, sql_big_query_prep
    from sqlalchemy import text

    begin_date: str = context["ti"].xcom_pull(task_ids=rf"target_vars.create_target_ranges", key="begin_date")
    end_date: str = context["ti"].xcom_pull(task_ids=rf"target_vars.create_target_ranges", key="end_date")

    schema_table: str = "temp_tables.base_intraday"

    with SQL_ENGINE.connect().execution_options(isolation_level="AUTOCOMMIT") as sql_connection:
        sql_big_query_prep(sql_connection=sql_connection)  # NOTE: Set these for BIG queries

        drop_query: str = f"DROP TABLE IF EXISTS {schema_table} CASCADE"
        sql_connection.execute(text(drop_query))

        parsed_intraday_query: str = f"""
        CREATE TABLE {schema_table} AS

        SELECT
            symbol
            , market_date
            , market_timestamp
            , open
            , high
            , low
            , close
        FROM backtesting.intraday
        WHERE market_date BETWEEN '{begin_date}' AND '{end_date}'
        """
        sql_connection.execute(text(parsed_intraday_query))

        idx_name: str = schema_table.replace(".", "_")
        index_query: str = rf"CREATE INDEX {idx_name}_idx ON {schema_table} (symbol, market_timestamp)"
        sql_connection.execute(text(index_query))
        sql_connection.execute(text(f"VACUUM FULL ANALYZE {schema_table}"))


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="predictive_prices", trigger_rule=DEFAULT_TRIGGER)
def predictive_prices() -> None:
    """Create temp SQL table with the set of all future prices."""
    from global_utils.sql_utils import SQL_ENGINE, SQL_PARTITION_CORES, sql_big_query_prep
    from sqlalchemy import text

    schema_table: str = "temp_tables.btr_predictive_prices"

    with SQL_ENGINE.connect().execution_options(isolation_level="AUTOCOMMIT") as sql_connection:
        sql_big_query_prep(sql_connection=sql_connection)  # NOTE: Set these for BIG queries

        drop_query: str = f"DROP TABLE IF EXISTS {schema_table} CASCADE"
        sql_connection.execute(text(drop_query))

        create_query: str = f"""
        CREATE TABLE {schema_table} AS

        WITH joined_data AS (
            SELECT
                TI.symbol
                , TI.cutoff_date
                , TI.market_date
                , TI.market_timestamp
                , TI.open
                , TI.high
                , TI.low
                , TI.close
                , TI.volume

                , INT5.open AS open_5
                , INT5.high AS high_5
                , INT5.low AS low_5
                , INT5.close AS close_5

                , INT15.open AS open_15
                , INT15.high AS high_15
                , INT15.low AS low_15
                , INT15.close AS close_15

                , INT30.open AS open_30
                , INT30.high AS high_30
                , INT30.low AS low_30
                , INT30.close AS close_30

                , INT60.open AS open_60
                , INT60.high AS high_60
                , INT60.low AS low_60
                , INT60.close AS close_60

                , PREV_INT5.open AS prev_open_5
                , PREV_INT5.high AS prev_high_5
                , PREV_INT5.low AS prev_low_5
                , PREV_INT5.close AS prev_close_5

                , PREV_INT15.open AS prev_open_15
                , PREV_INT15.high AS prev_high_15
                , PREV_INT15.low AS prev_low_15
                , PREV_INT15.close AS prev_close_15

                , PREV_INT30.open AS prev_open_30
                , PREV_INT30.high AS prev_high_30
                , PREV_INT30.low AS prev_low_30
                , PREV_INT30.close AS prev_close_30

                , PREV_INT60.open AS prev_open_60
                , PREV_INT60.high AS prev_high_60
                , PREV_INT60.low AS prev_low_60
                , PREV_INT60.close AS prev_close_60

            FROM temp_tables.btr_timestamp_intervals AS TI

            LEFT JOIN temp_tables.base_intraday AS INT5
                ON INT5.symbol = TI.symbol
                AND INT5.market_timestamp = TI.market_ts_5
            LEFT JOIN temp_tables.base_intraday AS INT15
                ON INT15.symbol = TI.symbol
                AND INT15.market_timestamp = TI.market_ts_15
            LEFT JOIN temp_tables.base_intraday AS INT30
                ON INT30.symbol = TI.symbol
                AND INT30.market_timestamp = TI.market_ts_30
            LEFT JOIN temp_tables.base_intraday AS INT60
                ON INT60.symbol = TI.symbol
                AND INT60.market_timestamp = TI.market_ts_60
            LEFT JOIN temp_tables.base_intraday AS PREV_INT5
                ON PREV_INT5.symbol = TI.symbol
                AND PREV_INT5.market_timestamp = TI.prev_market_ts_5
            LEFT JOIN temp_tables.base_intraday AS PREV_INT15
                ON PREV_INT15.symbol = TI.symbol
                AND PREV_INT15.market_timestamp = TI.prev_market_ts_15
            LEFT JOIN temp_tables.base_intraday AS PREV_INT30
                ON PREV_INT30.symbol = TI.symbol
                AND PREV_INT30.market_timestamp = TI.prev_market_ts_30
            LEFT JOIN temp_tables.base_intraday AS PREV_INT60
                ON PREV_INT60.symbol = TI.symbol
                AND PREV_INT60.market_timestamp = TI.prev_market_ts_60
        )

        SELECT
            symbol
            , cutoff_date
            , market_date
            , market_timestamp
            , open
            , high
            , low
            , close
            , volume

            , open_5
            , high_5
            , low_5
            , close_5

            , open_15
            , high_15
            , low_15
            , close_15

            , open_30
            , high_30
            , low_30
            , close_30

            , open_60
            , high_60
            , low_60
            , close_60

            , prev_open_5
            , prev_high_5
            , prev_low_5
            , prev_close_5

            , prev_open_15
            , prev_high_15
            , prev_low_15
            , prev_close_15

            , prev_open_30
            , prev_high_30
            , prev_low_30
            , prev_close_30

            , prev_open_60
            , prev_high_60
            , prev_low_60
            , prev_close_60

            , (ROW_NUMBER() OVER () % {SQL_PARTITION_CORES}) AS cx_partition
        FROM joined_data
        """
        sql_connection.execute(text(create_query))

        idx_name: str = schema_table.replace(".", "_")
        sql_query: str = rf"CREATE INDEX {idx_name}_idx ON {schema_table} (cx_partition, symbol)"
        sql_connection.execute(text(sql_query))
        sql_connection.execute(text(f"VACUUM FULL ANALYZE {schema_table}"))


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="create_predictions", trigger_rule=DEFAULT_TRIGGER)
def create_predictions() -> None:
    """Invoke Modin API to create price predictions for various future minute intervals."""
    import requests
    from global_utils.develop_vars import DOCKER_IP, MODIN_FASTAPI_PORT
    from requests.models import Response

    sub_api_name: str = "btr_01_data_prep"
    sub_api_target_func: str = "create_predictions"

    target_api_route: str = rf"{sub_api_name}/{sub_api_target_func}/"
    http_response: Response = requests.post(url=rf"http://{DOCKER_IP}:{MODIN_FASTAPI_PORT}/{target_api_route}")
    http_response.raise_for_status()


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="dedupe_target_vars", trigger_rule=DEFAULT_TRIGGER)
def dedupe_target_vars(**context: Union[Context, TaskInstance]) -> None:
    """De-duplicate the target SQL table.

    :param context: Union[Context, TaskInstance]: Set of variables that are inherent in
        every Airflow Task function that can be utilized.
    """
    from global_utils.sql_utils import dedupe_sql_table

    begin_date: str = context["ti"].xcom_pull(task_ids=rf"target_vars.create_target_ranges", key="begin_date")
    end_date: str = context["ti"].xcom_pull(task_ids=rf"target_vars.create_target_ranges", key="end_date")

    # Deduplicate the SQL Table
    partition_by_cols: List[str] = ["market_date", "symbol", "market_timestamp"]
    dedupe_sql_table(
        sql_schema="backtesting",
        sql_table="target_vars",
        partition_by_cols=partition_by_cols,
        begin_market_date=begin_date,
        end_market_date=end_date,
    )


# ----------------------------------------------------------------------------------------------------------------------
@task_group(group_id="target_vars")
def tg_02_target_vars() -> None:
    """Task Group for adding missing price data."""
    from airflow.providers.standard.operators.empty import EmptyOperator

    branch_create_target_ranges: str = create_target_ranges()

    no_data_to_process: EmptyOperator = EmptyOperator(task_id="no_data_to_process", trigger_rule=DEFAULT_TRIGGER)
    tg_finish: EmptyOperator = EmptyOperator(task_id="tg_finish", trigger_rule=DEFAULT_TRIGGER)

    """MAIN FLOW"""
    # noinspection PyTypeChecker,PyUnresolvedReferences
    (
        branch_create_target_ranges
        >> create_parsed_intraday()
        >> timestamp_intervals()
        >> create_base_intraday()
        >> predictive_prices()
        >> create_predictions()
        >> dedupe_target_vars()
        >> tg_finish
    )
    """NO DATA FLOWS."""
    # noinspection PyTypeChecker
    (branch_create_target_ranges >> no_data_to_process >> tg_finish)
