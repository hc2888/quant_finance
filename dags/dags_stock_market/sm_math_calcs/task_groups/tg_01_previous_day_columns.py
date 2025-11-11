"""Gets all the data for a given column of the previous day and turns it into a new column for the present-day row."""

from airflow.sdk import task, task_group
from global_airflow.airflow_utils import DEFAULT_TRIGGER
from sqlalchemy.engine import Engine


# ----------------------------------------------------------------------------------------------------------------------
def add_previous_day_columns(
    sql_schema: str,
    sql_table: str,
    sql_engine: Engine,
    date_column_name: str,
    begin_date: str,
    end_date: str,
) -> None:
    """Gets all the data for a given column of the previous day and turns it into a new column for the present-day row.

    :param sql_schema: str: name of SQL schema.
    :param sql_table: str: name of SQL table.
    :param sql_engine: Engine: SQL engine object.
    :param date_column_name: str: name of the column that the function is being applied to.
    :param begin_date: str: YYYY-MM-DD format of the beginning market date.
    :param end_date: str: YYYY-MM-DD format of the end market date.
    """
    from typing import Dict, List, Tuple

    import pandas as pd
    from dags_stock_market.sm_math_calcs.dag_vars import (
        prev_day_column_suffix,
        prev_day_table_name,
    )
    from global_utils.develop_vars import PROCESS_ALL_DATA
    from global_utils.sql_utils import SQL_CHUNKSIZE_PANDAS, TEMP_SCHEMA

    if PROCESS_ALL_DATA:
        all_data_sql_query: str = f"""
        SELECT
        MIN({date_column_name}) AS begin_market_date
        , MAX({date_column_name}) AS end_market_date
        FROM {sql_schema}.{sql_table}
        """

        all_data_df: pd.DataFrame = pd.read_sql(sql=all_data_sql_query, con=sql_engine)

        begin_date: str = all_data_df.at[0, "begin_market_date"]
        end_date: str = all_data_df.at[0, "end_market_date"]

    sql_query: str = (
        rf"""
        SELECT * FROM {sql_schema}.{sql_table}
        WHERE {date_column_name} BETWEEN '{begin_date}' AND '{end_date}'
        """.replace(
            "%", "%%"
        )
    )

    main_df: pd.DataFrame = pd.read_sql(sql=sql_query, con=sql_engine)

    column_list: List[str] = list(main_df.columns)
    column_list.remove(date_column_name)
    modified_column_list: List[str] = [col_name + prev_day_column_suffix for col_name in column_list]
    column_combined_tuples: List[Tuple[str, str]] = list(zip(column_list, modified_column_list))

    column_dictionary: Dict[str, str] = {}
    orig_name: str
    modified_name: str
    for orig_name, modified_name in column_combined_tuples:
        column_dictionary[orig_name] = modified_name

    prev_df: pd.DataFrame = main_df.copy()
    prev_df[column_list]: pd.Series = prev_df[column_list].shift(periods=1)

    prev_df.dropna(inplace=True, ignore_index=True)
    prev_df.rename(columns=column_dictionary, inplace=True)

    combined_df: pd.DataFrame = main_df.merge(right=prev_df, how="inner", on=date_column_name)

    temp_sql_table_name: str = rf"sm_{sql_schema}_{prev_day_table_name}"

    combined_df.to_sql(
        schema=TEMP_SCHEMA,
        name=temp_sql_table_name,
        con=sql_engine,
        if_exists="replace",
        index=False,
        chunksize=int(SQL_CHUNKSIZE_PANDAS / len(combined_df.columns)),
    )


# ----------------------------------------------------------------------------------------------------------------------
@task_group(group_id="previous_day_columns")
def tg_01_previous_day_columns() -> None:
    """Gets all the data for a given column of the previous day and turns it into a new column.

    for the present-day row.
    """
    from typing import List, Union

    from airflow.models import TaskInstance
    from airflow.providers.standard.operators.empty import EmptyOperator
    from airflow.providers.standard.operators.python import PythonOperator
    from airflow.sdk import Context
    from dags_stock_market.const_vars import MATH_INDEX_LIST
    from global_utils.sql_utils import SQL_ENGINE

    tg_finish: EmptyOperator = EmptyOperator(task_id="tg_finish", trigger_rule=DEFAULT_TRIGGER)

    previous_day_analyze_list: List[Union[PythonOperator, None]] = []
    index: str
    for index in MATH_INDEX_LIST:
        active_table: str = "raw_data"

        @task(task_id=f"{index}", trigger_rule=DEFAULT_TRIGGER)
        def previous_day_analyze(index_symbol: str, **context: Union[Context, TaskInstance]) -> None:
            """Analyzes previous day data.

            :param index_symbol: str: symbol of the market index.
            :param context: Union[Context, TaskInstance]: Set of variables that are inherent in
                every Airflow Task function that can be utilized.
            """
            begin_date: str = context["params"]["begin_date"]
            end_date: str = context["params"]["end_date"]

            add_previous_day_columns(
                sql_schema=index_symbol,
                sql_table=active_table,
                sql_engine=SQL_ENGINE,
                date_column_name="market_date",
                begin_date=begin_date,
                end_date=end_date,
            )

        previous_day_analyze_list.append(previous_day_analyze(index_symbol=index))

    # noinspection PyRedundantParentheses,PyStatementEffect
    (previous_day_analyze_list >> tg_finish)
