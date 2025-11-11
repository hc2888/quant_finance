"""All static / constant variables used throughout Backtesting System."""

from typing import List

from sql_infra.universal_vars import DEFAULT_COLUMNS_LIST

# ----------------------------------------------------------------------------------------------------------------------
FROM_SIGNALS_STAT_RESULTS: List[str] = [
    "begin_market_date TEXT NOT NULL",
    "symbol TEXT NOT NULL",
    "start_index BIGINT NOT NULL",
    "end_index BIGINT NOT NULL",
    "total_duration TEXT NOT NULL",
    "start_value NUMERIC(10,2) NOT NULL",
    "min_value NUMERIC(10,2) NOT NULL",
    "max_value NUMERIC(10,2) NOT NULL",
    "end_value NUMERIC(10,2) NOT NULL",
    "total_return_perc NUMERIC(10,2) NOT NULL",
    "benchmark_return_perc NUMERIC(10,2) NOT NULL",
    "position_coverage_perc NUMERIC(10,2) NOT NULL",
    "max_exposure_perc NUMERIC(10,2) NOT NULL",
    "max_drawdown_perc NUMERIC(10,2) NOT NULL",
    "max_drawdown_duration TEXT NOT NULL",
    "total_orders BIGINT NOT NULL",
    "total_fees_paid BIGINT NOT NULL",
    "total_trades BIGINT NOT NULL",
    "win_rate_perc NUMERIC(5,2) NOT NULL",
    "best_trade_perc NUMERIC(5,2) NOT NULL",
    "worse_trade_perc NUMERIC(5,2) NOT NULL",
    "avg_winning_trade_perc NUMERIC(5,2) NOT NULL",
    "avg_losing_trade_perc NUMERIC(5,2) NOT NULL",
    "avg_winning_trade_duration TEXT NOT NULL",
    "avg_losing_trade_duration TEXT NOT NULL",
    "profit_factor NUMERIC(10,2) NOT NULL",
    "expectancy NUMERIC(10,2) NOT NULL",
    "sharpe_ratio NUMERIC(10,2) NOT NULL",
    "calmar_ratio NUMERIC(10,2) NOT NULL",
    "omega_ratio NUMERIC(10,2) NOT NULL",
    "sortino_ratio NUMERIC(10,2) NOT NULL",
]
FROM_SIGNALS_STAT_RESULTS: List[str] = DEFAULT_COLUMNS_LIST + FROM_SIGNALS_STAT_RESULTS

FROM_SIGNALS_ORDER_RESULTS: List[str] = [
    "begin_market_date TEXT NOT NULL",
    "symbol TEXT NOT NULL",
    "start_index BIGINT NOT NULL",
    "end_index BIGINT NOT NULL",
    "total_duration TEXT NOT NULL",
    "total_records BIGINT NOT NULL",
    "side_counts_buy BIGINT NOT NULL",
    "side_counts_sell BIGINT NOT NULL",
    "type_counts_market BIGINT NOT NULL",
    "type_counts_limit BIGINT NOT NULL",
    "stop_type_counts_none BIGINT NOT NULL",
    "stop_type_counts_sl BIGINT NOT NULL",
    "stop_type_counts_tsl BIGINT NOT NULL",
    "stop_type_counts_ttp BIGINT NOT NULL",
    "stop_type_counts_tp BIGINT NOT NULL",
    "stop_type_counts_td BIGINT NOT NULL",
    "stop_type_counts_dt BIGINT NOT NULL",
    "size_min NUMERIC(10,2) NOT NULL",
    "size_median NUMERIC(10,2) NOT NULL",
    "size_max NUMERIC(10,2) NOT NULL",
    "fees_min NUMERIC(10,2) NOT NULL",
    "fees_median NUMERIC(10,2) NOT NULL",
    "fees_max NUMERIC(10,2) NOT NULL",
    "weighted_buy_price NUMERIC(10,2) NOT NULL",
    "weighted_sell_price NUMERIC(10,2) NOT NULL",
    "avg_signal_creation_duration TEXT NOT NULL",
    "avg_creation_fill_duration TEXT NOT NULL",
    "avg_signal_fill_duration TEXT NOT NULL",
]
FROM_SIGNALS_ORDER_RESULTS: List[str] = DEFAULT_COLUMNS_LIST + FROM_SIGNALS_ORDER_RESULTS

FROM_SIGNALS_ORDER_DETAILS: List[str] = [
    "begin_market_date TEXT NOT NULL",
    "symbol TEXT NOT NULL",
    "order_id BIGINT NOT NULL",
    "signal_index BIGINT NOT NULL",
    "creation_index BIGINT NOT NULL",
    "fill_index BIGINT NOT NULL",
    "size NUMERIC(10,2) NOT NULL",
    "price NUMERIC(10,2) NOT NULL",
    "fees NUMERIC(10,2) NOT NULL",
    "side TEXT NOT NULL",
    "type TEXT NOT NULL",
    "stop_type TEXT NOT NULL",
]
FROM_SIGNALS_ORDER_DETAILS: List[str] = DEFAULT_COLUMNS_LIST + FROM_SIGNALS_ORDER_DETAILS
