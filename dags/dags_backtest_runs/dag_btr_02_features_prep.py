"""Create features for predictive model."""

from typing import Dict, List, Union

from airflow.models import TaskInstance
from airflow.sdk import Context, dag, task
from global_airflow.airflow_utils import DEFAULT_ARGS, DEFAULT_TRIGGER, drop_temp_tables, store_dag_info

# ----------------------------------------------------------------------------------------------------------------------
DAG_NAME: str = "btr_02_features_prep"

DROP_TABLES: bool = False

CONFIG_PARAMS: Dict[str, Union[str, int, bool, List[str]]] = {
    "execute_backfill": False,
    "use_testing_date_ranges": False,  # Use the `begin_date` & `end_date` params
    "force_insert_into_sql_table": False,  # INSERT into `backtesting.model_features` even if this is NOT a `normal_run`
    "begin_date": "2025-01-01",
    "end_date": "2025-01-07",
    "process_all_symbols": True,  # Whether to process all available symbols
    "target_symbols": [
        "AGG",  # iShares Core US Aggregate Bond ETF
        "BND",  # Vanguard Total Bond Market Index Fund ETF
        "EDV",  # Vanguard Extended Duration Treasury ETF
        "HYG",  # iShares iBoxx $ High Yield Corporate Bond ETF
        "IEF",  # iShares 7-10 Year Treasury Bond ETF
        "IYR",  # iShares U.S. Real Estate ETF
        "JNK",  # SPDR Bloomberg High Yield Bond ETF
        "PHB",  # Invesco Fundamental High Yield Corporate Bond ETF
        "SPAB",  # SPDR Portfolio Aggregate Bond ETF
        "SPY",  # NOTE: SPDR S&P 500; The fund is the largest and oldest ETF in the USA.
        "TLT",  # iShares 20+ Year Treasury Bond ETF
        "UUP",  # Invesco DB US Dollar Index Bullish Fund
        "VGLT",  # Vanguard Long-Term Treasury Index Fund ETF
        "VGSH",  # Vanguard Short-Term Treasury Index Fund ETF
        "VT",  # Vanguard Total World Stock ETF
        "^VIN",  # CBOE NEAR-TERM VIX INDEX
        "^VIX",  # CBOE VOLATILITY INDEX
    ],
    "timesteps": 120,  # How many minutes to utilize for vectorized / aggregate calculations
    "time_shift": 60,  # How many minutes before to look at prices; i.e, immediate previous prices are useless noise
}


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="check_backfill", trigger_rule=DEFAULT_TRIGGER)
def check_backfill(**context: Union[Context, TaskInstance]) -> None:
    """Check if this is a backfill run.

    :param context: Union[Context, TaskInstance]: Set of variables that are inherent in
        every Airflow Task function that can be utilized.

    :return: str: Airflow Task to trigger.
    """
    from global_utils.sql_utils import SQL_ENGINE
    from sqlalchemy import text

    execute_backfill: bool = context["params"]["execute_backfill"]

    with SQL_ENGINE.connect().execution_options(isolation_level="AUTOCOMMIT") as sql_connection:
        if execute_backfill:
            truncate_model_features: str = rf"TRUNCATE TABLE backtesting.model_features"
            sql_connection.execute(text(truncate_model_features))


# ----------------------------------------------------------------------------------------------------------------------
@task.branch(task_id="data_distro", trigger_rule=DEFAULT_TRIGGER)
def data_distro(cpu_cores_per_task: int = 0, **context: Union[Context, TaskInstance]) -> str:
    """Create distributed set of SQL temp tables to create features.

    :param cpu_cores_per_task: int: max distributed CPU cores allowed for the DAG.
    :param context: Union[Context, TaskInstance]: Set of variables that are inherent in
        every Airflow Task function that can be utilized.

    :return: str: Airflow Task to trigger.
    """
    import pandas as pd
    from dags_stock_market.const_vars import ALPHA_VANTAGE_ETF_DICT, MODEL_PREP_API_SYMBOLS_LIST
    from global_utils.sql_utils import SQL_CHUNKSIZE_PANDAS, SQL_ENGINE

    use_testing_date_ranges: bool = context["params"]["use_testing_date_ranges"]

    process_all_symbols: bool = context["params"]["process_all_symbols"]
    # NOTE: The market `symbol`s to process
    if process_all_symbols:
        target_symbols: str = str(list(ALPHA_VANTAGE_ETF_DICT.keys()) + MODEL_PREP_API_SYMBOLS_LIST)[1:-1]
    else:
        target_symbols: str = str(context["params"]["target_symbols"])[1:-1]

    if use_testing_date_ranges:
        normal_run: bool = False
        begin_date: str = context["params"]["begin_date"]
        end_date: str = context["params"]["end_date"]
        date_range_clause: str = f"""
        INT1.symbol IN ('SPY', '^VIX')
        AND INT1.market_date BETWEEN '{begin_date}' AND '{end_date}'
        """
    else:
        normal_run: bool = True
        date_range_clause: str = f"INT1.symbol IN ({target_symbols}) AND MF1.symbol IS NULL"

    context["ti"].xcom_push(key="normal_run", value=normal_run)

    dates_query: str = f"""
    SELECT
        INT1.symbol
        , INT1.market_year
        , COALESCE(
            (
                SELECT MAX(INT2.market_date)
                FROM backtesting.intraday AS INT2
                WHERE INT2.symbol = INT1.symbol
                AND INT2.market_date < MIN(INT1.market_date)
            )
            , MIN(INT1.market_date)
        ) AS start_date
        , MIN(INT1.market_date) AS begin_date
        , MAX(INT1.market_date) AS end_date
    FROM backtesting.intraday AS INT1
    LEFT JOIN backtesting.model_features AS MF1
        ON INT1.symbol = MF1.symbol AND INT1.market_date = MF1.market_date
    WHERE {date_range_clause}
    GROUP BY INT1.symbol, INT1.market_year
    ORDER BY INT1.symbol, INT1.market_year ASC
    """
    dates_df: pd.DataFrame = pd.read_sql(sql=dates_query, con=SQL_ENGINE)

    if not dates_df.empty:
        final_begin_date: str = dates_df["begin_date"].min()
        context["ti"].xcom_push(key="final_begin_date", value=final_begin_date)
        final_end_date: str = dates_df["end_date"].max()
        context["ti"].xcom_push(key="final_end_date", value=final_end_date)

        split_num: int = min(len(dates_df), cpu_cores_per_task)
        context["ti"].xcom_push(key="split_num", value=split_num)
        print(rf"TOTAL SPLIT NUM COUNT: {split_num}")

        group_num: int
        for group_num in range(split_num):
            temp_df: pd.DataFrame = dates_df.iloc[group_num::cpu_cores_per_task].copy()
            temp_df.to_sql(
                schema="temp_tables",
                name=f"btr_02_features_prep_dates_{group_num}",
                con=SQL_ENGINE,
                if_exists="replace",
                index=False,
                chunksize=int(SQL_CHUNKSIZE_PANDAS / len(temp_df.columns)),
            )
        task_var: str = r"single_calcs.create_temp_table"
    else:
        task_var: str = r"no_data_to_process"

    return task_var


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="insert_features_data", trigger_rule=DEFAULT_TRIGGER)
def insert_features_data(**context: Union[Context, TaskInstance]) -> None:
    """Combine all temp SQL Tables.

    :param context: Union[Context, TaskInstance]: Set of variables that are inherent in
        every Airflow Task function that can be utilized.
    """
    from dags_backtest_runs.btr_02_features_prep.const_vars import MODEL_FEATURES_COLUMNS_STR
    from global_utils.sql_utils import SQL_ENGINE, sql_big_query_prep
    from sqlalchemy import text

    normal_run: bool = context["ti"].xcom_pull(task_ids=rf"data_distro", key="normal_run")
    force_insert_into_sql_table: bool = context["params"]["force_insert_into_sql_table"]

    if normal_run or force_insert_into_sql_table:
        begin_sql_query: str = "INSERT INTO backtesting.model_features"
        target_columns_query: str = MODEL_FEATURES_COLUMNS_STR
    else:
        begin_sql_query: str = "CREATE TABLE temp_tables.model_features AS"
        target_columns_query: str = ""

    exec_timestamp: str = context["ti"].xcom_pull(task_ids=r"store_dag_info", key="exec_timestamp")
    print(rf"EXEC_TIMESTAMP: {exec_timestamp}")

    with SQL_ENGINE.connect().execution_options(isolation_level="AUTOCOMMIT") as sql_connection:
        sql_big_query_prep(sql_connection=sql_connection)  # NOTE: Set these for BIG queries

        drop_query: str = "DROP TABLE IF EXISTS temp_tables.model_features"
        sql_connection.execute(text(drop_query))

        insert_query: str = f"""
        {begin_sql_query} {target_columns_query}

        SELECT
        '{exec_timestamp}' AS exec_timestamp
        , RC.market_date
        , RC.market_year
        , RC.market_month
        , RC.market_timestamp
        , RC.market_clock
        , RC.market_hour
        , RC.market_minute
        , RC.symbol
        , RC.open
        , RC.high
        , RC.low
        , RC.close
        , RC.volume

        -- `dags/algo_metrics/trade_methods/noise.py`
        , SC.efficiency_ratio
        , SC.price_density_tanh
        , SC.fractal_dimension

        -- `dags/algo_metrics/trade_methods/momentums.py`
        , SC.momentum_divergence

        -- `dags/algo_metrics/trade_methods/regression_analysis.py`
        , SC.is_upward_trend
        , SC.trend_line_pass
        , SC.trade_signal

        -- `dags/algo_metrics/trade_methods/adaptive_techniques.py`
        , RC.chande_dynamic
        , RC.ehlers_mama_fama
        , RC.ehlers_frama
        , RC.mcginley_dynamic
        , RC.kaufman_efficiency
        , RC.dynamic_momentum_tanh
        , RC.adaptive_trend

        -- `dags/algo_metrics/trade_methods/advanced_techniques.py`
        , RC.bierovic_true_range
        , RC.conners_vix_reversal
        , RC.bookstaber_volatility_breakout
        , RC.fractal_patterns

        -- `dags/algo_metrics/trade_methods/behavioral.py`
        , RC.vol_ratio
        , RC.vol_ratio_tanh

        -- `dags/algo_metrics/trade_methods/charting.py`
        , RC.detect_spike
        , RC.island_reversal
        , RC.pivot_point_top
        , RC.pivot_point_low
        , RC.pivot_point_volatile
        , RC.pivot_point_buy
        , RC.pivot_point_sell
        , RC.reversal_day
        , RC.key_reversal_down
        , RC.key_reversal_up
        , RC.wide_range_day
        , RC.outside_day
        , RC.inside_day
        , RC.doji
        , RC.double_doji
        , RC.engulfing_pattern

        -- `dags/algo_metrics/trade_methods/cycles.py`
        , RC.hilbert_transform
        , RC.fisher_transform_signal
        , RC.fisher_transform_tanh
        , RC.ehlers_universal_tanh
        , RC.short_cycle

        -- `dags/algo_metrics/trade_methods/day_trading.py`
        , RC.first_hour_breakout
        , RC.momentum_pinball
        , RC.raschke_range_breakout
        , RC.williams_range_breakout
        , RC.williams_second_method
        , RC.true_upward_gap
        , RC.intraday_sell_shock
        , RC.intraday_buy_shock

        -- `dags/algo_metrics/trade_methods/event_driven.py`
        , RC.swing_high_low
        , RC.swing_high_low_entry
        , RC.wilder_swing_index
        , RC.n_day_breakout
        , RC.turtle_strategy_enter
        , RC.turtle_strategy_exit
        , RC.turtle_strategy_net_position
        , RC.turtle_strategy_stop
        , RC.turtle_strategy_net_stop

        -- `dags/algo_metrics/trade_methods/momentums.py`
        , RC.div_index_slow
        , RC.div_index_fast
        , RC.ultim_osc_1
        , RC.ultim_osc_2
        , RC.relative_vigor_idx
        , RC.relative_vigor_idx_tanh
        , RC.raschke_first_cross
        , RC.strength_oscillator_tanh
        , RC.velocity_accel
        , RC.cambridge_hook

        -- `dags/algo_metrics/trade_methods/multi_time_frames.py`
        , RC.elder_triple_intraday_timing
        , RC.elder_triple_force_index
        , RC.elder_triple_ray

        -- `dags/algo_metrics/trade_methods/price_distribution.py`
        , RC.kase_devstop
        , RC.jackson_zone
        , RC.chande_kroll_zone
        , RC.scorpio_zone
        , RC.mcnicholl_skewness_tanh
        , RC.skew_kurt_volatility

        -- `dags/algo_metrics/trade_methods/trends.py`
        , RC.smooth_expon_single
        , RC.smooth_expon_double
        , RC.simple_volatility
        , RC.single_system_momentum
        , RC.single_system_mov_avg
        , RC.single_system_exponential
        , RC.single_system_slope
        , RC.single_system_n_day_breakout
        , RC.donchian_5_20
        , RC.golden_cross
        , RC.woodshedder_long_term
        , RC.three_crossover
        , RC.ehlers_onset
        , RC.trix_signal
        , RC.kestner_adx
        , RC.donchian_breakout

        -- `dags/algo_metrics/trade_methods/volumes.py`
        , RC.volume_price_combo
        , RC.mcclellan_oscillator_tanh
        , RC.volume_spike
        , RC.pseudo_volume
        , RC.low_volume_period
        , RC.low_price_move

        -- `dags/algo_metrics/tech_analysis/momentum.py`
        , RC.relative_strength_index
        , RC.true_strength_index
        , RC.stoch_osc_k
        , RC.stoch_osc_d
        , RC.stoch_osc_d_slow
        , RC.kaufman_adapt_dynamic
        , RC.kaufman_adapt_regular
        , RC.rate_of_change_tanh
        , RC.williams_perc_r
        , RC.stoch_rsi
        , RC.stoch_rsi_k
        , RC.stoch_rsi_d
        , RC.perc_price_osc_tanh
        , RC.perc_price_osc_signal_tanh
        , RC.perc_price_osc_hist_tanh
        , RC.perc_volume_osc_tanh
        , RC.perc_volume_osc_signal_tanh
        , RC.perc_volume_osc_hist_tanh
        , RC.awesome_osc_tanh

        -- `dags/algo_metrics/tech_analysis/trend.py`
        , RC.aroon_up
        , RC.aroon_down
        , RC.aroon_indicator
        , RC.adx_signal
        , RC.adx_pos_signal
        , RC.adx_neg_signal
        , RC.macd_tanh
        , RC.macd_signal_tanh
        , RC.macd_diff_tanh
        , RC.ichimoku_span
        , RC.ichimoku_close
        , RC.ichimoku_base
        , RC.kst_tanh
        , RC.kst_signal_tanh
        , RC.kst_diff_tanh
        , RC.detrend_price_osc_tanh
        , RC.cci_tanh
        , RC.vortex_diff_tanh
        , RC.psar_up_indicator
        , RC.psar_down_indicator
        , RC.schaff_trend_cycle

        -- `dags/algo_metrics/tech_analysis/volatility.py`
        , RC.bollinger_bands_high
        , RC.bollinger_bands_low
        , RC.keltner_channel_high
        , RC.keltner_channel_low
        , RC.donchian_channel_perc_band
        , RC.ulcer_index_tanh
        , RC.bookstaber_std_dev_tanh
        , RC.bookstaber_log_squared_tanh
        , RC.bookstaber_high_low_tanh

        -- `dags/algo_metrics/tech_analysis/volume.py`
        , RC.chaikin_money_flow
        , RC.force_index_tanh
        , RC.ease_of_move_tanh
        , RC.ease_of_move_sma_tanh
        , RC.money_flow_index

        , TV.time_sin
        , TV.time_cos

        , TV.predict_5
        , TV.predict_15
        , TV.predict_30
        , TV.predict_60

        FROM temp_tables.btr_features_range_calcs AS RC

        INNER JOIN temp_tables.btr_features_single_calcs AS SC
            ON RC.market_date = SC.market_date
            AND RC.symbol = SC.symbol
            AND RC.market_timestamp = SC.market_timestamp

        INNER JOIN backtesting.target_vars AS TV
            ON RC.market_date = TV.market_date
            AND RC.symbol = TV.symbol
            AND RC.market_timestamp = TV.market_timestamp
        """
        sql_connection.execute(text(insert_query))


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="new_features_dates", trigger_rule=DEFAULT_TRIGGER)
def new_features_dates(**context: Union[Context, TaskInstance]) -> None:
    """Add new `market_date`s into `backtesting.features_dates` SQL Table.

    :param context: Union[Context, TaskInstance]: Set of variables that are inherent in
        every Airflow Task function that can be utilized.
    """
    from global_utils.sql_utils import SQL_ENGINE
    from sqlalchemy import text

    normal_run: bool = context["ti"].xcom_pull(task_ids=rf"data_distro", key="normal_run")
    exec_timestamp: str = context["ti"].xcom_pull(task_ids=r"store_dag_info", key="exec_timestamp")

    if normal_run:
        with SQL_ENGINE.connect().execution_options(isolation_level="AUTOCOMMIT") as sql_connection:
            sql_query: str = f"""
            INSERT INTO backtesting.features_dates

            (
            exec_timestamp
            , market_date
            , market_year
            , market_month
            , market_timestamp
            )

            SELECT
            '{exec_timestamp}' AS exec_timestamp
            , market_date
            , market_year
            , market_month
            , market_timestamp

            FROM backtesting.all_market_dates
            WHERE market_date NOT IN (SELECT DISTINCT market_date FROM backtesting.features_dates)
            """
            sql_connection.execute(text(sql_query))


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="dedupe_features_dates", trigger_rule=DEFAULT_TRIGGER)
def dedupe_features_dates(**context: Union[Context, TaskInstance]) -> None:
    """De-duplicate the target SQL table.

    :param context: Union[Context, TaskInstance]: Set of variables that are inherent in
        every Airflow Task function that can be utilized.
    """
    from global_utils.sql_utils import dedupe_sql_table

    normal_run: bool = context["ti"].xcom_pull(task_ids=rf"data_distro", key="normal_run")

    if normal_run:
        final_begin_date: str = context["ti"].xcom_pull(task_ids=rf"data_distro", key="final_begin_date")
        final_end_date: str = context["ti"].xcom_pull(task_ids=rf"data_distro", key="final_end_date")

        # Deduplicate the SQL Table
        partition_by_cols: List[str] = ["market_date", "market_timestamp"]
        dedupe_sql_table(
            sql_schema="backtesting",
            sql_table="features_dates",
            partition_by_cols=partition_by_cols,
            begin_market_date=final_begin_date,
            end_market_date=final_end_date,
        )


# ----------------------------------------------------------------------------------------------------------------------
@task(task_id="dedupe_model_features", trigger_rule=DEFAULT_TRIGGER)
def dedupe_model_features(**context: Union[Context, TaskInstance]) -> None:
    """De-duplicate the target SQL table.

    :param context: Union[Context, TaskInstance]: Set of variables that are inherent in
        every Airflow Task function that can be utilized.
    """
    from global_utils.sql_utils import dedupe_sql_table

    normal_run: bool = context["ti"].xcom_pull(task_ids=rf"data_distro", key="normal_run")
    force_insert_into_sql_table: bool = context["params"]["force_insert_into_sql_table"]

    if normal_run or force_insert_into_sql_table:
        final_begin_date: str = context["ti"].xcom_pull(task_ids=rf"data_distro", key="final_begin_date")
        final_end_date: str = context["ti"].xcom_pull(task_ids=rf"data_distro", key="final_end_date")
        exec_timestamp: str = context["ti"].xcom_pull(task_ids=r"store_dag_info", key="exec_timestamp")

        query_1: str = "DELETE FROM backtesting.model_features"
        query_2: str = rf"WHERE market_date BETWEEN '{final_begin_date}' AND '{final_end_date}'"
        query_3: str = rf"AND exec_timestamp = '{exec_timestamp}'"
        delete_query: str = f"{query_1} {query_2} {query_3}"
        print(delete_query)

        # Deduplicate the SQL Table
        partition_by_cols: List[str] = ["market_date", "symbol", "market_timestamp"]
        dedupe_sql_table(
            sql_schema="backtesting",
            sql_table="model_features",
            partition_by_cols=partition_by_cols,
            begin_market_date=final_begin_date,
            end_market_date=final_end_date,
        )


# ----------------------------------------------------------------------------------------------------------------------
@dag(
    dag_id=DAG_NAME,
    default_args=DEFAULT_ARGS,
    # `max_active_runs` defines how many running concurrent instances of a DAG there are allowed to be.
    max_active_runs=1,
    catchup=False,
    params=CONFIG_PARAMS,
    on_failure_callback=drop_temp_tables(temp_table_prefix="sm", drop_tables=DROP_TABLES),
    tags=["btr"],
)
def main() -> None:
    """The main DAG flow."""
    from airflow.providers.standard.operators.empty import EmptyOperator
    from airflow.sdk import TaskGroup
    from dags_backtest_runs.btr_02_features_prep.task_groups.tg_01_single_calcs import tg_01_single_calcs
    from dags_backtest_runs.btr_02_features_prep.task_groups.tg_02_range_calcs import tg_02_range_calcs
    from global_utils.develop_vars import TOTAL_CPU_CORES
    from global_utils.general_utils import calc_cpu_cores_per_task

    dag_finish: EmptyOperator = EmptyOperator(task_id="dag_finish", trigger_rule=DEFAULT_TRIGGER)
    no_data_to_process: EmptyOperator = EmptyOperator(task_id="no_data_to_process", trigger_rule=DEFAULT_TRIGGER)

    # What is the maximum number of PARALLEL-IZED Dynamic Airflow Tasks that occur at any point in the DAG?
    num_max_parallel_dynamic_tasks: int = 1
    # NOTE: Cannot use all CPU cores because the data operations are too intensive
    num_cpu_cores: int = int(TOTAL_CPU_CORES / 2)
    cpu_cores_per_task: int = calc_cpu_cores_per_task(
        num_max_parallel_dynamic_tasks=num_max_parallel_dynamic_tasks,
        num_cpu_cores=num_cpu_cores,
    )

    branch_data_distro: str = data_distro(cpu_cores_per_task=cpu_cores_per_task)

    # noinspection PyNoneFunctionAssignment,PyTypeChecker
    tg_01_single_calcs: TaskGroup = tg_01_single_calcs()
    # noinspection PyNoneFunctionAssignment,PyTypeChecker
    tg_02_range_calcs: TaskGroup = tg_02_range_calcs()

    """BEGIN FLOW"""
    # noinspection PyTypeChecker,PyUnresolvedReferences
    (store_dag_info() >> check_backfill() >> branch_data_distro)
    """MAIN FLOW."""
    # noinspection PyTypeChecker,PyUnresolvedReferences
    (
        branch_data_distro
        >> tg_01_single_calcs
        >> tg_02_range_calcs
        >> insert_features_data()
        >> dedupe_model_features()
        >> dag_finish
    )
    """NO DATA FLOW."""
    # noinspection PyTypeChecker,PyUnresolvedReferences
    (branch_data_distro >> no_data_to_process >> dag_finish)


# ----------------------------------------------------------------------------------------------------------------------
main()
