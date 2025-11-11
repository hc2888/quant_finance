"""Executes algebra (non-calculus) math on stock_market data and etc."""

from typing import List

from airflow.sdk import task, task_group
from global_airflow.airflow_utils import DEFAULT_TRIGGER


# ----------------------------------------------------------------------------------------------------------------------
def add_algebra_values(sql_schema: str, columns_list: List[str]) -> None:
    """Executes algebra (non-calculus) math on stock_market data.

    :param sql_schema: str: name of SQL schema.
    :param columns_list: List[str]: list of all the columns from the SQL table.
    """
    import numpy as np
    import pandas as pd
    from dags_stock_market.sm_math_calcs.dag_vars import (
        algebra_values_table_name,
        prev_day_column_suffix,
        prev_day_table_name,
        value_multiplier,
    )
    from global_utils.sql_utils import SQL_CHUNKSIZE_PANDAS, SQL_ENGINE, TEMP_SCHEMA

    columns_list: List[str] = [elem.lower() for elem in columns_list]
    sql_columns_list: List[str] = []

    column: str
    for column in columns_list:
        sql_columns_list.append(column)
        sql_columns_list.append(column + prev_day_column_suffix)

    sql_string: str = ",".join(sql_columns_list)
    sql_query: str = f"""SELECT market_date, {sql_string} FROM {TEMP_SCHEMA}.sm_{sql_schema}_{prev_day_table_name}"""

    main_df: pd.DataFrame = pd.read_sql(sql=sql_query, con=SQL_ENGINE)

    column: str
    for column in columns_list:
        main_df[[column, column + prev_day_column_suffix]]: pd.DataFrame = (
            main_df[[column, column + prev_day_column_suffix]] * value_multiplier
        )

        main_df[f"{column}_slope"]: pd.Series = np.round(
            a=main_df[column] - main_df[column + prev_day_column_suffix],
            decimals=2,
        )

        main_df.loc[main_df[f"{column}_slope"] >= 0, f"{column}_change"]: pd.Series = 1
        main_df.loc[main_df[f"{column}_slope"] < 0, f"{column}_change"]: pd.Series = -1

        distance_formula: np.ndarray = (1 + ((main_df[column] - main_df[column + prev_day_column_suffix]) ** 2)) ** 0.5
        main_df[f"{column}_length"]: pd.Series = np.round(a=distance_formula, decimals=4)
        # Do NOT explicitly input a parameter name for `np.abs()` or else ERROR happens.
        main_df[f"{column}_area_relative"]: pd.Series = np.abs(main_df[f"{column}_slope"]) / 2
        main_df[f"{column}_area_nominal"]: pd.Series = (
            main_df[[column, column + prev_day_column_suffix]].min(axis=1) + main_df[f"{column}_area_relative"]
        )

        main_df[f"{column}_length"]: pd.Series = np.round(
            a=main_df[f"{column}_length"] * main_df[f"{column}_change"],
            decimals=2,
        )
        main_df[f"{column}_area_relative"]: pd.Series = np.round(
            a=main_df[f"{column}_area_relative"] * main_df[f"{column}_change"],
            decimals=2,
        )
        main_df[f"{column}_area_nominal"]: pd.Series = np.round(
            a=main_df[f"{column}_area_nominal"] * main_df[f"{column}_change"],
            decimals=2,
        )

    main_df.drop(columns=sql_columns_list, inplace=True)

    temp_sql_table_name: str = rf"sm_{sql_schema}_{algebra_values_table_name}"

    main_df.to_sql(
        schema=TEMP_SCHEMA,
        name=temp_sql_table_name,
        con=SQL_ENGINE,
        if_exists="replace",
        index=False,
        chunksize=int(SQL_CHUNKSIZE_PANDAS / len(main_df.columns)),
    )


# ----------------------------------------------------------------------------------------------------------------------
@task_group(group_id="algebra_values")
def tg_02_algebra_values() -> None:
    """Executes algebra (non-calculus) math on stock_market data."""
    from typing import List, Union

    from airflow.providers.standard.operators.empty import EmptyOperator
    from airflow.providers.standard.operators.python import PythonOperator
    from dags_stock_market.const_vars import FRED_SYMBOLS_LIST, MATH_INDEX_LIST
    from dags_stock_market.sm_math_calcs.dag_vars import algebra_columns_dict

    tg_finish: EmptyOperator = EmptyOperator(task_id="tg_finish", trigger_rule=DEFAULT_TRIGGER)

    algebra_analyze_list: List[Union[PythonOperator, None]] = []
    index: str
    for index in MATH_INDEX_LIST:
        # LOOP THROUGH MAKING TASKS
        @task(task_id=f"{index}", trigger_rule=DEFAULT_TRIGGER)
        def algebra_analyze(index_symbol: str) -> None:
            """Executes algebra math of the dataset.

            :param index_symbol: str: symbol of the market index.
            """
            if index_symbol != "fred":
                add_algebra_values(sql_schema=index_symbol, columns_list=algebra_columns_dict[index_symbol])
            else:
                add_algebra_values(sql_schema=index_symbol, columns_list=FRED_SYMBOLS_LIST)

        algebra_analyze_list.append(algebra_analyze(index_symbol=index))

    # noinspection PyStatementEffect,PyRedundantParentheses
    (algebra_analyze_list >> tg_finish)
