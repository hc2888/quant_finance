"""Executes correlation statistical analysis on data."""

from airflow.sdk import task, task_group
from global_airflow.airflow_utils import DEFAULT_TRIGGER


# ----------------------------------------------------------------------------------------------------------------------
def correlation_analysis(
    schema: str,
    table: str,
    begin_date: str,
    end_date: str,
    exec_timestamp: str,
    ingest_all_data: bool = False,
    coeff_min_num: float = 0.50,
    method: str = "pearson",
) -> None:
    """Executes correlation analysis on numerical data.

    :param schema: str: name of SQL schema.
    :param table: str: name of SQL table.
    :param begin_date: str: beginning market date of the analysis.
    :param end_date: str: ending market date of the analysis.
    :param exec_timestamp: str: DAG Run execution time in `YYYY-MM-DD HH:MM:SS` format.
    :param ingest_all_data: bool: whether to analyze the entire dataset.
    :param coeff_min_num: float: the cutoff value of the minimum coefficient to input into the SQL table.
    :param method: str: correlation analysis method you want to execute; choice between Pearson, Spearman, and Kendall.
    """
    from typing import List, Tuple

    import numpy as np
    import pandas as pd
    from global_utils.sql_utils import SQL_CHUNKSIZE_PANDAS, SQL_ENGINE, dedupe_sql_table

    if ingest_all_data:
        ingest_all_data_sql_query: str = f"""
        SELECT
        MIN(market_date) AS begin_market_date
        , MAX(market_date) AS end_market_date
        FROM {schema}.{table}
        """
        ingest_all_data_df: pd.DataFrame = pd.read_sql(sql=ingest_all_data_sql_query, con=SQL_ENGINE)
        begin_date: str = ingest_all_data_df.at[0, "begin_market_date"]
        end_date: str = ingest_all_data_df.at[0, "end_market_date"]
    else:
        date_range_sql_query: str = f"""
        SELECT
        MIN(market_date) AS begin_market_date
        , MAX(market_date) AS end_market_date
        FROM {schema}.{table}
        WHERE market_date BETWEEN '{begin_date}' AND '{end_date}'
        """
        date_range_df: pd.DataFrame = pd.read_sql(sql=date_range_sql_query, con=SQL_ENGINE)
        begin_date: str = date_range_df.at[0, "begin_market_date"]
        end_date: str = date_range_df.at[0, "end_market_date"]

    sql_query: str = f"""
    SELECT * FROM {schema}.{table}
    WHERE market_date BETWEEN '{begin_date}' AND '{end_date}'
    """

    pandas_df: pd.DataFrame = pd.read_sql(sql=sql_query, con=SQL_ENGINE)
    # ACTUALLY EXECUTE THE CALCULATIONS IF THE DATAFRAME IS NOT EMPTY
    if not pandas_df.empty:
        pandas_df.replace(to_replace=[np.inf, -np.inf], value=np.nan, inplace=True)

        non_num_columns: List[str] = [
            "unique_id",
            "exec_timestamp",
            "market_date",
            "market_year",
            "market_month",
            "market_timestamp",
            "market_clock",
            "market_hour",
            "market_minute",
            "symbol",
        ]
        list_of_features: List[str] = list(pandas_df.columns)

        column: str
        for column in non_num_columns:
            if column in list_of_features:
                list_of_features.remove(column)

        correlation_matrix: pd.DataFrame = pandas_df[list_of_features].corr(method=method, numeric_only=False)
        correlation_list: List[Tuple[str, str, float]] = list(
            correlation_matrix.abs().unstack().reset_index().itertuples(name=None, index=None)
        )

        num: int
        for num in range(0, len(correlation_list)):
            temp_tuple: Tuple[str, str, float] = correlation_list[num]
            temp_list: List[str] = sorted([temp_tuple[0], temp_tuple[1]])
            correlation_list[num]: List[Tuple[str, str, float]] = (
                temp_list[0],
                temp_list[1],
                correlation_list[num][2],
            )

        corr_columns: List[str] = ["feature_1", "feature_2", "correlation_coefficient"]
        corr_df: pd.DataFrame = pd.DataFrame(data=correlation_list, columns=corr_columns)
        corr_df.dropna(inplace=True)
        corr_df.drop_duplicates(inplace=True)
        corr_df: pd.DataFrame = corr_df.round(decimals=2)

        corr_df: pd.DataFrame = corr_df[corr_df["feature_1"] != corr_df["feature_2"]]
        corr_df: pd.DataFrame = corr_df[corr_df["correlation_coefficient"] >= coeff_min_num]
        corr_df: pd.DataFrame = corr_df.round(decimals=2)
        corr_df.sort_values(by="correlation_coefficient", ascending=False, inplace=True, ignore_index=True)

        corr_df["begin_market_date"]: pd.Series = begin_date
        corr_df["end_market_date"]: pd.Series = end_date
        print(f"""LENGTH OF DATAFRAME: {len(corr_df)}""")

        # Add `exec_timestamp` value / column
        corr_df["exec_timestamp"]: pd.Series = exec_timestamp
        # NOTE: Drop any rows that have infinite values
        corr_df: pd.DataFrame = corr_df.replace(to_replace=[np.inf, -np.inf], value=np.nan).dropna()
        # Insert data into SQL Table
        corr_df.to_sql(
            schema=f"""{schema}_metadata""",
            name=f"""{table}_correlation_{method}""",
            con=SQL_ENGINE,
            if_exists="append",
            index=False,
            chunksize=int(SQL_CHUNKSIZE_PANDAS / len(corr_df.columns)),
        )
        # Deduplicate the SQL Table
        partition_by_cols: List[str] = ["begin_market_date", "end_market_date", "feature_1", "feature_2"]
        dedupe_sql_table(
            sql_schema=rf"{schema}_metadata",
            sql_table=rf"{table}_correlation_{method}",
            partition_by_cols=partition_by_cols,
            main_index_column="begin_market_date",
        )


# ----------------------------------------------------------------------------------------------------------------------
@task_group(group_id="correlation_analysis")
def tg_01_correlation_analysis() -> None:
    """Executes correlation statistical analysis on data."""
    from typing import List, Tuple, Union

    from airflow.models import TaskInstance
    from airflow.providers.standard.operators.empty import EmptyOperator
    from airflow.providers.standard.operators.python import PythonOperator
    from airflow.sdk import Context
    from dags_stock_market.sm_math_metadata.dag_vars import coeff_minimum
    from global_utils.develop_vars import PROCESS_ALL_DATA
    from global_utils.sql_utils import CORRELATION_METHOD_LIST

    tg_finish: EmptyOperator = EmptyOperator(task_id="tg_finish", trigger_rule=DEFAULT_TRIGGER)

    schema_table_list: List[Tuple[str, str]] = [
        ("fred", "raw_data"),
        ("backtesting", "model_features"),
    ]

    data_analyze_list: List[Union[PythonOperator, None]] = []
    schema: str
    table: str
    for schema, table in schema_table_list:
        method: str
        for method in CORRELATION_METHOD_LIST:
            # LOOP THROUGH MAKING TASKS
            @task(task_id=f"{schema}_{table}_{method}", trigger_rule=DEFAULT_TRIGGER)
            def data_analyze(
                sql_schema: str,
                sql_table: str,
                corr_method: str,
                **context: Union[Context, TaskInstance],
            ) -> None:
                """Executes correlation analysis.

                :param sql_schema: str: The SQL schema name.
                :param sql_table: str: The SQL table name.
                :param corr_method: str: correlation method applied.
                :param context: Union[Context, TaskInstance]: Set of variables that are inherent in
                    every Airflow Task function that can be utilized.
                """
                begin_date: str = context["params"]["begin_date"]
                end_date: str = context["params"]["end_date"]

                exec_timestamp: str = context["ti"].xcom_pull(task_ids=r"store_dag_info", key="exec_timestamp")

                correlation_analysis(
                    schema=sql_schema,
                    table=sql_table,
                    begin_date=begin_date,
                    end_date=end_date,
                    exec_timestamp=exec_timestamp,
                    ingest_all_data=PROCESS_ALL_DATA,
                    coeff_min_num=coeff_minimum,
                    method=corr_method,
                )

            data_analyze_list.append(data_analyze(sql_schema=schema, sql_table=table, corr_method=method))
    # noinspection PyStatementEffect,PyRedundantParentheses
    (data_analyze_list >> tg_finish)
