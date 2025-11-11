"""Task Group for creating single row calculation features."""

from typing import Dict, List, Union

from airflow.models import TaskInstance
from airflow.sdk import Context, task, task_group
from global_airflow.airflow_utils import DEFAULT_TRIGGER

# ----------------------------------------------------------------------------------------------------------------------
SQL_SCHEMA: str = "temp_tables"
SQL_TABLE: str = "btr_features_single_calcs"


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="create_temp_table", trigger_rule=DEFAULT_TRIGGER)
def create_temp_table() -> None:
    """Create temp SQL Table used for next set of Airflow Tasks."""
    from global_utils.sql_utils import SQL_ENGINE
    from sqlalchemy import text

    schema_table: str = f"{SQL_SCHEMA}.{SQL_TABLE}"
    with SQL_ENGINE.connect().execution_options(isolation_level="AUTOCOMMIT") as sql_connection:
        drop_query: str = f"DROP TABLE IF EXISTS {schema_table}"
        sql_connection.execute(text(drop_query))

        create_query: str = f"""
        CREATE TABLE {schema_table} (
        market_date TEXT NOT NULL
        , market_timestamp TEXT NOT NULL
        , symbol TEXT NOT NULL
        , open NUMERIC(10,2) NOT NULL
        , high NUMERIC(10,2) NOT NULL
        , low NUMERIC(10,2) NOT NULL
        , close NUMERIC(10,2) NOT NULL
        , volume BIGINT NOT NULL

        -- `dags/algo_metrics/trade_methods/noise.py`
        , efficiency_ratio NUMERIC(3,2) NOT NULL
        , price_density_tanh NUMERIC(4,3) NOT NULL
        , fractal_dimension NUMERIC(3,2) NOT NULL

        -- `dags/algo_metrics/trade_methods/momentums.py`
        , momentum_divergence INTEGER NOT NULL

        -- `dags/algo_metrics/trade_methods/regression_analysis.py`
        , is_upward_trend INTEGER NOT NULL
        , eq_slope NUMERIC(10,2) NOT NULL
        , eq_intercept NUMERIC(10,2) NOT NULL
        , resistance_line NUMERIC(10,2) NOT NULL
        , support_line NUMERIC(10,2) NOT NULL
        , top_channel_bias NUMERIC(10,2) NOT NULL
        , bottom_channel_bias NUMERIC(10,2) NOT NULL
        , trend_line_pass INTEGER NOT NULL
        , trade_signal INTEGER NOT NULL
        )
        """
        sql_connection.execute(text(create_query))


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="single_features", trigger_rule=DEFAULT_TRIGGER)
def single_features(group_num: int = 0, **context: Union[Context, TaskInstance]) -> None:
    """Create features calculated from `dags/algo_metrics/trade_methods/regression_analysis.py` functions.

    :param group_num: int: Designated number from distributed set of temp SP500 symbol SQL tables.
    :param context: Union[Context, TaskInstance]: Set of variables that are inherent in
        every Airflow Task function that can be utilized.
    """
    import numpy as np
    import pandas as pd
    from algo_metrics.trade_methods.momentums import momentum_divergence
    from algo_metrics.trade_methods.noise import (
        efficiency_ratio,
        fractal_dimension,
        price_density,
    )
    from algo_metrics.trade_methods.regression_analysis import (
        regression_trade_signal,
        trend_line_eval,
        trend_line_vars,
    )
    from global_utils.sql_utils import SQL_CHUNKSIZE_PANDAS, SQL_ENGINE

    timesteps: int = context["params"]["timesteps"]
    time_shift: int = context["params"]["time_shift"]

    # Use this value to determine if this task will be activated or not
    split_num: int = context["ti"].xcom_pull(
        task_ids=rf"data_distro",
        key="split_num",
        map_indexes=[None],
    )[0]
    # Only activate this task if there is a distro table to pull from
    if group_num < split_num:
        sql_query: str = f"""
        SELECT start_date, begin_date, end_date, symbol
        FROM temp_tables.btr_02_features_prep_dates_{group_num}
        """
        prep_df: pd.DataFrame = pd.read_sql(sql=sql_query, con=SQL_ENGINE)

        num: int
        for num in range(len(prep_df)):
            start_date: str = prep_df.at[num, "start_date"]
            begin_date: str = prep_df.at[num, "begin_date"]
            end_date: str = prep_df.at[num, "end_date"]
            symbol: str = prep_df.at[num, "symbol"]

            message_1: str = rf"ROW {num + 1} OF {len(prep_df)};"
            message_2: str = f"SYMBOL: {symbol}; START_DATE: {start_date}; BEGIN_DATE: {begin_date};"
            message_3: str = rf"END_DATE: {end_date}"
            print(f"{message_1} {message_2} {message_3}")

            sql_query: str = f"""
            SELECT

            market_date
            , market_timestamp
            , symbol
            , open
            , high
            , low
            , close
            , volume

            FROM backtesting.intraday

            WHERE market_date BETWEEN '{start_date}' AND '{end_date}'
            AND symbol = '{symbol}'
            ORDER BY market_timestamp ASC
            """
            main_df: pd.DataFrame = pd.read_sql(sql=sql_query, con=SQL_ENGINE)

            trade_columns: List[str] = ["symbol", "market_timestamp", "trade_signal"]
            trade_signal_df: pd.DataFrame = pd.DataFrame(columns=trade_columns)

            begin_row_num: int
            for begin_row_num in range(len(main_df) - timesteps + 1):
                end_row_num: int = begin_row_num + timesteps
                # NOTE: Create `temp_df` based on timesteps parameter
                temp_df: pd.DataFrame = main_df[begin_row_num:end_row_num].reset_index(drop=True)

                # NOTE: `dags/algo_metrics/trade_methods/noise.py`
                efficiency_ratio_val: float = efficiency_ratio(
                    main_df=temp_df,
                    time_column="market_timestamp",
                    time_shift=time_shift,
                )
                main_df.at[end_row_num - 1, "efficiency_ratio"]: float = efficiency_ratio_val

                price_density_val: float = price_density(main_df=temp_df, time_column="market_timestamp")
                price_density_tanh: float = round(np.tanh(price_density_val), 3)
                main_df.at[end_row_num - 1, "price_density_tanh"]: float = price_density_tanh

                fractal_dimension_val: float = fractal_dimension(main_df=temp_df, time_column="market_timestamp")
                fractal_dimension_val: float = round((fractal_dimension_val - 1.5) / 0.5, 2)
                main_df.at[end_row_num - 1, "fractal_dimension"]: float = fractal_dimension_val

                # NOTE: `dags/algo_metrics/trade_methods/momentums.py`
                momentum_divergence_val: int = momentum_divergence(
                    main_df=temp_df,
                    time_column="market_timestamp",
                    window_slow=int(timesteps / 2),
                    window_fast=int(timesteps / 4),
                    window_sign=int(timesteps / 6),
                )
                main_df.at[end_row_num - 1, "momentum_divergence"]: float = momentum_divergence_val

                # NOTE: `dags/algo_metrics/trade_methods/regression_analysis.py`
                target_array: np.ndarray = temp_df["close"].to_numpy()
                coeff_dict: Dict[str, Union[float, int]] = trend_line_vars(target_array=target_array)

                index_array: np.ndarray = np.arange(start=0, stop=target_array.size)
                # Multiply each constant by its index
                predict_array: np.ndarray = coeff_dict["eq_slope"] * index_array + coeff_dict["eq_intercept"]
                temp_df["predict_values"]: pd.Series = predict_array

                trend_line_pass: int = trend_line_eval(
                    main_df=temp_df,
                    true_column="close",
                    pred_column="predict_values",
                    time_column="market_timestamp",
                )

                main_df.at[end_row_num - 1, "is_upward_trend"]: int = coeff_dict["is_upward_trend"]
                main_df.at[end_row_num - 1, "eq_slope"]: float = coeff_dict["eq_slope"]
                main_df.at[end_row_num - 1, "eq_intercept"]: float = coeff_dict["eq_intercept"]
                main_df.at[end_row_num - 1, "resistance_line"]: float = coeff_dict["resistance_line"]
                main_df.at[end_row_num - 1, "support_line"]: float = coeff_dict["support_line"]
                main_df.at[end_row_num - 1, "top_channel_bias"]: float = coeff_dict["top_channel_bias"]
                main_df.at[end_row_num - 1, "bottom_channel_bias"]: float = coeff_dict["bottom_channel_bias"]
                main_df.at[end_row_num - 1, "trend_line_pass"]: int = trend_line_pass

                trade_signal_series: pd.Series = regression_trade_signal(
                    target_column=temp_df["close"],
                    eq_slope=coeff_dict["eq_slope"],
                    eq_intercept=coeff_dict["eq_intercept"],
                )
                temp_df["trade_signal"]: pd.Series = trade_signal_series

                trade_signal_df: pd.DataFrame = pd.concat(
                    objs=[trade_signal_df, temp_df[trade_columns].tail(n=1)],
                    axis=0,
                )

            main_df: pd.DataFrame = main_df.merge(right=trade_signal_df, on=["symbol", "market_timestamp"], how="left")

            main_df: pd.DataFrame = main_df.ffill().reset_index(drop=True)
            main_df["trade_signal"]: pd.Series = main_df["trade_signal"].fillna(value=0).astype(dtype=int)
            # Ensure control column has no gaps; rows before the first computed window are NaN
            main_df["trend_line_pass"]: pd.Series = main_df["trend_line_pass"].fillna(value=-1)
            # Zero-out trade decisions when trend line fails OR hasn't been computed yet (=-1)
            main_df.loc[main_df["trend_line_pass"] == -1, "trade_signal"]: int = 0
            main_df: pd.DataFrame = main_df.fillna(value=0)

            if main_df.empty:
                raise ValueError("EMPTY DATAFRAME; TROUBLESHOOT & MODIFY CALC LOGIC")
            else:
                # NOTE: Get rid of the `market_date`s that are already in the SQL table
                main_df: pd.DataFrame = main_df[main_df["market_date"] >= begin_date].reset_index(drop=True)
                main_df.to_sql(
                    schema=SQL_SCHEMA,
                    name=SQL_TABLE,
                    con=SQL_ENGINE,
                    if_exists="append",
                    index=False,
                    chunksize=int(SQL_CHUNKSIZE_PANDAS / len(main_df.columns)),
                )


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="finalize_temp_table", trigger_rule=DEFAULT_TRIGGER)
def finalize_temp_table() -> None:
    """Create temp SQL Table used for next set of Airflow Tasks."""
    from global_utils.sql_utils import SQL_ENGINE
    from sqlalchemy import text

    schema_table: str = f"{SQL_SCHEMA}.{SQL_TABLE}"
    with SQL_ENGINE.connect().execution_options(isolation_level="AUTOCOMMIT") as sql_connection:
        index_query: str = rf"CREATE INDEX ON {schema_table} (market_date, symbol, market_timestamp)"
        sql_connection.execute(text(index_query))
        sql_connection.execute(text(f"VACUUM FULL ANALYZE {schema_table}"))


# ----------------------------------------------------------------------------------------------------------------------
@task_group(group_id="single_calcs")
def tg_01_single_calcs() -> None:
    """Task Group for the main ETL process."""
    from airflow.providers.standard.operators.empty import EmptyOperator
    from airflow.providers.standard.operators.python import PythonOperator
    from global_utils.develop_vars import TOTAL_CPU_CORES
    from global_utils.general_utils import calc_cpu_cores_per_task

    tg_finish: EmptyOperator = EmptyOperator(task_id="tg_finish", trigger_rule=DEFAULT_TRIGGER)

    # What is the maximum number of PARALLEL-IZED Dynamic Airflow Tasks that occur at any point in the DAG?
    num_max_parallel_dynamic_tasks: int = 1
    # NOTE: Cannot use all CPU cores because the data operations are too intensive
    num_cpu_cores: int = int(TOTAL_CPU_CORES / 2)
    cpu_cores_per_task: int = calc_cpu_cores_per_task(
        num_max_parallel_dynamic_tasks=num_max_parallel_dynamic_tasks,
        num_cpu_cores=num_cpu_cores,
    )
    distro_range_list: List[int] = [num for num in range(cpu_cores_per_task)]
    # noinspection PyUnresolvedReferences
    single_features_tasks: PythonOperator = single_features.expand(group_num=distro_range_list)

    """MAIN FLOW."""
    # noinspection PyTypeChecker,PyUnresolvedReferences
    (create_temp_table() >> single_features_tasks >> finalize_temp_table() >> tg_finish)
