"""Task Group for creating single row calculation features."""

from typing import List, Union

from airflow.models import TaskInstance
from airflow.sdk import Context, task, task_group
from global_airflow.airflow_utils import DEFAULT_TRIGGER

# ----------------------------------------------------------------------------------------------------------------------
SQL_SCHEMA: str = "temp_tables"
SQL_TABLE: str = "btr_features_range_calcs"


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
        , market_year INTEGER NOT NULL
        , market_month INTEGER NOT NULL
        , market_timestamp TEXT NOT NULL
        , market_clock TEXT NOT NULL
        , market_hour INTEGER NOT NULL
        , market_minute INTEGER NOT NULL
        , symbol TEXT NOT NULL
        , open NUMERIC(10,2) NOT NULL
        , high NUMERIC(10,2) NOT NULL
        , low NUMERIC(10,2) NOT NULL
        , close NUMERIC(10,2) NOT NULL
        , volume BIGINT NOT NULL

        -- `dags/algo_metrics/trade_methods/adaptive_techniques.py`
        , chande_dynamic INTEGER NOT NULL
        , ehlers_mama_fama NUMERIC(2,1) NOT NULL
        , ehlers_frama INTEGER NOT NULL
        , mcginley_dynamic INTEGER NOT NULL
        , kaufman_efficiency INTEGER NOT NULL
        , dynamic_momentum_tanh NUMERIC(4,3) NOT NULL
        , adaptive_trend INTEGER NOT NULL

        -- `dags/algo_metrics/trade_methods/advanced_techniques.py`
        , bierovic_true_range INTEGER NOT NULL
        , conners_vix_reversal INTEGER NOT NULL
        , bookstaber_volatility_breakout INTEGER NOT NULL
        , fractal_patterns INTEGER NOT NULL

        -- `dags/algo_metrics/trade_methods/behavioral.py`
        , vol_ratio INTEGER NOT NULL
        , vol_ratio_tanh NUMERIC(4,3) NOT NULL

        -- `dags/algo_metrics/trade_methods/charting.py`
        , detect_spike INTEGER NOT NULL
        , island_reversal INTEGER NOT NULL
        , pivot_point_top INTEGER NOT NULL
        , pivot_point_low INTEGER NOT NULL
        , pivot_point_volatile INTEGER NOT NULL
        , pivot_point_buy INTEGER NOT NULL
        , pivot_point_sell INTEGER NOT NULL
        , reversal_day INTEGER NOT NULL
        , key_reversal_down INTEGER NOT NULL
        , key_reversal_up INTEGER NOT NULL
        , wide_range_day INTEGER NOT NULL
        , outside_day INTEGER NOT NULL
        , inside_day INTEGER NOT NULL
        , doji INTEGER NOT NULL
        , double_doji INTEGER NOT NULL
        , engulfing_pattern INTEGER NOT NULL

        -- `dags/algo_metrics/trade_methods/cycles.py`
        , hilbert_transform NUMERIC(2,1) NOT NULL
        , fisher_transform_signal INTEGER NOT NULL
        , fisher_transform_tanh NUMERIC(4,3) NOT NULL
        , ehlers_universal_tanh NUMERIC(4,3) NOT NULL
        , short_cycle INTEGER NOT NULL

        -- `dags/algo_metrics/trade_methods/day_trading.py`
        , first_hour_breakout INTEGER NOT NULL
        , momentum_pinball INTEGER NOT NULL
        , raschke_range_breakout INTEGER NOT NULL
        , williams_range_breakout INTEGER NOT NULL
        , williams_second_method INTEGER NOT NULL
        , true_upward_gap INTEGER NOT NULL
        , intraday_sell_shock INTEGER NOT NULL
        , intraday_buy_shock INTEGER NOT NULL

        -- `dags/algo_metrics/trade_methods/event_driven.py`
        , swing_high_low INTEGER NOT NULL
        , swing_high_low_entry INTEGER NOT NULL
        , wilder_swing_index INTEGER NOT NULL
        , n_day_breakout INTEGER NOT NULL
        , turtle_strategy_enter INTEGER NOT NULL
        , turtle_strategy_exit INTEGER NOT NULL
        , turtle_strategy_net_position INTEGER NOT NULL
        , turtle_strategy_stop INTEGER NOT NULL
        , turtle_strategy_net_stop INTEGER NOT NULL

        -- `dags/algo_metrics/trade_methods/momentums.py`
        , div_index_slow INTEGER NOT NULL
        , div_index_fast INTEGER NOT NULL
        , ultim_osc_1 INTEGER NOT NULL
        , ultim_osc_2 INTEGER NOT NULL
        , relative_vigor_idx INTEGER NOT NULL
        , relative_vigor_idx_tanh NUMERIC(4,3) NOT NULL
        , raschke_first_cross INTEGER NOT NULL
        , strength_oscillator_tanh NUMERIC(4,3) NOT NULL
        , velocity_accel NUMERIC(3,2) NOT NULL
        , cambridge_hook INTEGER NOT NULL

        -- `dags/algo_metrics/trade_methods/multi_time_frames.py`
        , elder_triple_intraday_timing INTEGER NOT NULL
        , elder_triple_force_index INTEGER NOT NULL
        , elder_triple_ray INTEGER NOT NULL

        -- `dags/algo_metrics/trade_methods/price_distribution.py`
        , kase_devstop NUMERIC(3,2) NOT NULL
        , jackson_zone NUMERIC(3,2) NOT NULL
        , chande_kroll_zone NUMERIC(2,1) NOT NULL
        , scorpio_zone NUMERIC(3,2) NOT NULL
        , mcnicholl_skewness_tanh NUMERIC(4,3) NOT NULL
        , skew_kurt_volatility INTEGER NOT NULL

        -- `dags/algo_metrics/trade_methods/trends.py`
        , smooth_expon_single INTEGER NOT NULL
        , smooth_expon_double INTEGER NOT NULL
        , simple_volatility INTEGER NOT NULL
        , single_system_momentum INTEGER NOT NULL
        , single_system_mov_avg INTEGER NOT NULL
        , single_system_exponential INTEGER NOT NULL
        , single_system_slope INTEGER NOT NULL
        , single_system_n_day_breakout INTEGER NOT NULL
        , donchian_5_20 INTEGER NOT NULL
        , golden_cross INTEGER NOT NULL
        , woodshedder_long_term INTEGER NOT NULL
        , three_crossover INTEGER NOT NULL
        , ehlers_onset NUMERIC(3,2) NOT NULL
        , trix_signal INTEGER NOT NULL
        , kestner_adx INTEGER NOT NULL
        , donchian_breakout INTEGER NOT NULL

        -- `dags/algo_metrics/trade_methods/volumes.py`
        , volume_price_combo NUMERIC(2,1) NOT NULL
        , mcclellan_oscillator_tanh NUMERIC(4,3) NOT NULL
        , volume_spike INTEGER NOT NULL
        , pseudo_volume INTEGER NOT NULL
        , low_volume_period INTEGER NOT NULL
        , low_price_move INTEGER NOT NULL

        -- `dags/algo_metrics/tech_analysis/momentum.py`
        , relative_strength_index NUMERIC(3,2) NOT NULL
        , true_strength_index NUMERIC(3,2) NOT NULL
        , stoch_osc_k NUMERIC(3,2) NOT NULL
        , stoch_osc_d NUMERIC(3,2) NOT NULL
        , stoch_osc_d_slow NUMERIC(3,2) NOT NULL
        , kaufman_adapt_dynamic INTEGER NOT NULL
        , kaufman_adapt_regular INTEGER NOT NULL
        , rate_of_change_tanh NUMERIC(4,3) NOT NULL
        , williams_perc_r NUMERIC(3,2) NOT NULL
        , stoch_rsi NUMERIC(3,2) NOT NULL
        , stoch_rsi_k NUMERIC(3,2) NOT NULL
        , stoch_rsi_d NUMERIC(3,2) NOT NULL
        , perc_price_osc_tanh NUMERIC(4,3) NOT NULL
        , perc_price_osc_signal_tanh NUMERIC(4,3) NOT NULL
        , perc_price_osc_hist_tanh NUMERIC(4,3) NOT NULL
        , perc_volume_osc_tanh NUMERIC(4,3) NOT NULL
        , perc_volume_osc_signal_tanh NUMERIC(4,3) NOT NULL
        , perc_volume_osc_hist_tanh NUMERIC(4,3) NOT NULL
        , awesome_osc_tanh NUMERIC(4,3) NOT NULL

        -- `dags/algo_metrics/tech_analysis/trend.py`
        , aroon_up NUMERIC(3,2) NOT NULL
        , aroon_down NUMERIC(3,2) NOT NULL
        , aroon_indicator NUMERIC(3,2) NOT NULL
        , adx_signal NUMERIC(3,2) NOT NULL
        , adx_pos_signal NUMERIC(3,2) NOT NULL
        , adx_neg_signal NUMERIC(3,2) NOT NULL
        , macd_tanh NUMERIC(4,3) NOT NULL
        , macd_signal_tanh NUMERIC(4,3) NOT NULL
        , macd_diff_tanh NUMERIC(4,3) NOT NULL
        , ichimoku_span INTEGER NOT NULL
        , ichimoku_close INTEGER NOT NULL
        , ichimoku_base INTEGER NOT NULL
        , kst_tanh NUMERIC(4,3) NOT NULL
        , kst_signal_tanh NUMERIC(4,3) NOT NULL
        , kst_diff_tanh NUMERIC(4,3) NOT NULL
        , detrend_price_osc_tanh NUMERIC(4,3) NOT NULL
        , cci_tanh NUMERIC(4,3) NOT NULL
        , vortex_diff_tanh NUMERIC(4,3) NOT NULL
        , psar_up_indicator INTEGER NOT NULL
        , psar_down_indicator INTEGER NOT NULL
        , schaff_trend_cycle NUMERIC(3,2) NOT NULL

        -- `dags/algo_metrics/tech_analysis/volatility.py`
        , bollinger_bands_high INTEGER NOT NULL
        , bollinger_bands_low INTEGER NOT NULL
        , keltner_channel_high INTEGER NOT NULL
        , keltner_channel_low INTEGER NOT NULL
        , donchian_channel_perc_band NUMERIC(3,2) NOT NULL
        , ulcer_index_tanh NUMERIC(4,3) NOT NULL
        , bookstaber_std_dev_tanh NUMERIC(4,3) NOT NULL
        , bookstaber_log_squared_tanh NUMERIC(4,3) NOT NULL
        , bookstaber_high_low_tanh NUMERIC(4,3) NOT NULL

        -- `dags/algo_metrics/tech_analysis/volume.py`
        , chaikin_money_flow NUMERIC(3,2) NOT NULL
        , force_index_tanh NUMERIC(4,3) NOT NULL
        , ease_of_move_tanh NUMERIC(4,3) NOT NULL
        , ease_of_move_sma_tanh NUMERIC(4,3) NOT NULL
        , money_flow_index NUMERIC(3,2) NOT NULL
        )
        """
        sql_connection.execute(text(create_query))


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="range_features", trigger_rule=DEFAULT_TRIGGER)
def range_features(group_num: int = 0, **context: Union[Context, TaskInstance]) -> None:
    """Create features calculated from `dags/algo_metrics/trade_methods/range_features.py` functions.

    :param group_num: int: Designated number from distributed set of temp SP500 symbol SQL tables.
    :param context: Union[Context, TaskInstance]: Set of variables that are inherent in
        every Airflow Task function that can be utilized.
    """
    import pandas as pd
    from dags_backtest_runs.btr_02_features_prep.feature_params.set_1 import (
        set_1_features,
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
            , market_year
            , market_month
            , market_timestamp
            , market_clock
            , market_hour
            , market_minute
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
            # NOTE: The set of `algo_metrics` function calculations
            main_df: pd.DataFrame = set_1_features(
                main_df=main_df,
                time_column="market_timestamp",
                timesteps=timesteps,
                time_shift=time_shift,
            )
            main_df: pd.DataFrame = main_df.ffill().fillna(value=0)

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
@task_group(group_id="range_calcs")
def tg_02_range_calcs() -> None:
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
    range_features_tasks: PythonOperator = range_features.expand(group_num=distro_range_list)

    """MAIN FLOW."""
    # noinspection PyTypeChecker,PyUnresolvedReferences
    (create_temp_table() >> range_features_tasks >> finalize_temp_table() >> tg_finish)
