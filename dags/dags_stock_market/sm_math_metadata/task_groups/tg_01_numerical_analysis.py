"""Executes numerical statistical analysis on data."""

from airflow.sdk import task, task_group
from global_airflow.airflow_utils import DEFAULT_TRIGGER


# ----------------------------------------------------------------------------------------------------------------------
def numerical_analysis(
    schema: str,
    table: str,
    begin_date: str,
    end_date: str,
    exec_timestamp: str,
    ingest_all_data: bool = False,
) -> None:
    """Executes various statistical analysis on numerical data.

    :param schema: str: name of SQL schema.
    :param table: str: name of SQL table.
    :param begin_date: str: beginning market date of the analysis.
    :param end_date: str: ending market date of the analysis.
    :param exec_timestamp: str: DAG Run execution time in `YYYY-MM-DD HH:MM:SS` format.
    :param ingest_all_data: bool: whether to analyze the entire dataset.
    """
    from typing import List, Optional

    import numpy as np
    import pandas as pd
    import scipy.stats as sci_stats
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

        total_count: int = len(pandas_df)
        list_of_features: List[str] = list(pandas_df.columns)

        final_non_num_columns: List[Optional[str]] = []
        column: str
        for column in non_num_columns:
            if column in list_of_features:
                list_of_features.remove(column)
                final_non_num_columns.append(column)

        stats_df: pd.DataFrame = pandas_df[list_of_features].describe().round(decimals=2)

        skewness_values: pd.DataFrame = pandas_df[list_of_features].skew(axis=0, skipna=True, numeric_only=True)
        skewness_values: pd.DataFrame = skewness_values.round(decimals=2)

        kurtosis_values: pd.DataFrame = pandas_df[list_of_features].kurt(axis=0, skipna=True, numeric_only=True)
        kurtosis_values: pd.DataFrame = kurtosis_values.round(decimals=2)

        zscore_df: pd.DataFrame = pandas_df.drop(columns=final_non_num_columns).apply(func=sci_stats.zscore)

        feature: str
        for feature in list_of_features:
            std_1_68_perc: float = np.round(a=len(zscore_df[zscore_df[feature] <= 1]) / total_count * 100, decimals=2)
            std_2_95_perc: float = np.round(a=len(zscore_df[zscore_df[feature] <= 2]) / total_count * 100, decimals=2)
            std_3_99_perc: float = np.round(a=len(zscore_df[zscore_df[feature] <= 3]) / total_count * 100, decimals=2)

            stats_df.at["std_1_68_perc", feature]: float = std_1_68_perc
            stats_df.at["std_2_95_perc", feature]: float = std_2_95_perc
            stats_df.at["std_3_99_perc", feature]: float = std_3_99_perc

            stats_df.at["std_dev_mean_perc", feature]: float = (
                stats_df.at["std", feature] / stats_df.at["mean", feature] * 100
            ).round(decimals=2)
            stats_df.at["skewness", feature]: float = skewness_values[feature]
            stats_df.at["kurtosis", feature]: float = kurtosis_values[feature]

        transposed_df: pd.DataFrame = stats_df.transpose()
        transposed_df.rename_axis(mapper="column_name", inplace=True)
        transposed_df.reset_index(inplace=True)
        transposed_df.drop(columns=["count"], inplace=True)

        transposed_df: pd.DataFrame = transposed_df.rename(
            columns={
                "std": "std_dev",
                "25%": "percentile_25",
                "50%": "median",
                "75%": "percentile_75",
            }
        )

        transposed_df["begin_market_date"]: pd.Series = begin_date
        transposed_df["end_market_date"]: pd.Series = end_date
        # Add `exec_timestamp` value / column
        transposed_df["exec_timestamp"]: pd.Series = exec_timestamp
        # NOTE: Drop any rows that have infinite values
        transposed_df: pd.DataFrame = transposed_df.replace(to_replace=[np.inf, -np.inf], value=np.nan).dropna()
        # Insert data into SQL Table
        transposed_df.to_sql(
            schema=f"""{schema}_metadata""",
            name=f"""{table}_numerical""",
            con=SQL_ENGINE,
            if_exists="append",
            index=False,
            chunksize=int(SQL_CHUNKSIZE_PANDAS / len(transposed_df.columns)),
        )
        # Deduplicate the SQL Table
        partition_by_cols: List[str] = ["begin_market_date", "end_market_date", "column_name"]
        dedupe_sql_table(
            sql_schema=rf"{schema}_metadata",
            sql_table=rf"{table}_numerical",
            partition_by_cols=partition_by_cols,
            main_index_column="begin_market_date",
        )


# ----------------------------------------------------------------------------------------------------------------------
@task_group(group_id="numerical_analysis")
def tg_01_numerical_analysis() -> None:
    """Executes numerical statistical analysis on data."""
    from typing import List, Tuple, Union

    from airflow.models import TaskInstance
    from airflow.providers.standard.operators.empty import EmptyOperator
    from airflow.providers.standard.operators.python import PythonOperator
    from airflow.sdk import Context
    from global_utils.develop_vars import PROCESS_ALL_DATA

    tg_finish: EmptyOperator = EmptyOperator(task_id="tg_finish", trigger_rule=DEFAULT_TRIGGER)

    schema_table_list: List[Tuple[str, str]] = [
        ("fred", "raw_data"),
        ("backtesting", "model_features"),
    ]

    data_analyze_list: List[Union[PythonOperator, None]] = []
    schema: str
    table: str
    for schema, table in schema_table_list:
        # CREATE DATA TABLES
        @task(task_id=f"{schema}_{table}", trigger_rule=DEFAULT_TRIGGER)
        def data_analyze(sql_schema: str, sql_table: str, **context: Union[Context, TaskInstance]) -> None:
            """Executes numerical analysis.

            :param sql_schema: str: The SQL schema name.
            :param sql_table: str: The SQL table name.
            :param context: Union[Context, TaskInstance]: Set of variables that are inherent in
                every Airflow Task function that can be utilized.
            """
            begin_date: str = context["params"]["begin_date"]
            end_date: str = context["params"]["end_date"]

            exec_timestamp: str = context["ti"].xcom_pull(task_ids=r"store_dag_info", key="exec_timestamp")

            numerical_analysis(
                schema=sql_schema,
                table=sql_table,
                begin_date=begin_date,
                end_date=end_date,
                exec_timestamp=exec_timestamp,
                ingest_all_data=PROCESS_ALL_DATA,
            )

        data_analyze_list.append(data_analyze(sql_schema=schema, sql_table=table))
    # noinspection PyStatementEffect,PyRedundantParentheses
    (data_analyze_list >> tg_finish)
