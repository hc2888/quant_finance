"""Various Portfolio-run functions revolving around using `from_signals()` method."""

from typing import Dict, List, Literal

import numpy as np
import pandas as pd
import vectorbtpro as vbt
from pandas.core.groupby import DataFrameGroupBy


# ----------------------------------------------------------------------------------------------------------------------
def _groupby_portfolio_performance(
    sub_df: pd.DataFrame,
    time_column: str,
    close_column: str,
) -> pd.DataFrame:
    """Calculate BUY SELL signals for each `symbol`.

    :param sub_df: pd.DataFrame: The sub-grouped Pandas DataFrame.
    :param time_column: str: The timestamp column.
    :param close_column: str: The closing price column.
    :return: pd.DataFrame: DataFrame with new columns.
    """
    from algo_metrics.trade_methods.adaptive_techniques import chande_dynamic

    sub_df: pd.DataFrame = sub_df.sort_values(by=time_column, ascending=True, ignore_index=True)
    # NOTE: `chande_dynamic()` is a PLACEHOLDER until universal MODEL signal function is made
    sub_df: pd.DataFrame = chande_dynamic(main_df=sub_df, time_column=time_column, close_column=close_column)
    return sub_df


# ----------------------------------------------------------------------------------------------------------------------
def portfolio_performance(
    main_df: pd.DataFrame,
    symbol_list: List[str],
    begin_market_date: str,
    exec_timestamp: str,
    symbol_column: str = "symbol",
    time_column: str = "market_timestamp",
    close_column: str = "close",
    investment_amount: int = 1_000_000,
    per_buy_quantity: int = 100_000,
    per_buy_type: Literal["value", "amount"] = "value",
    data_time_freq: str = "1min",
) -> Dict[str, pd.DataFrame]:
    """Execute a simulated historical market price run that measure the performance of a Portfolio.

    :param main_df: pd.DataFrame: The DataFrame containing the data.
    :param symbol_list: List[str]: List of incoming symbols.
    :param begin_market_date: str: The beginning `market_date`.
    :param exec_timestamp: str: DAG Run execution time in `YYYY-MM-DD HH:MM:SS` format.
    :param symbol_column: str: The target column with the stock symbols.
    :param time_column: str: The timestamp column.
    :param close_column: str: The closing price column.
    :param investment_amount: int: How much simulated $ money for portfolio run.
    :param per_buy_quantity: int: How many dollars or number of shares to buy per purchase.
    :param per_buy_type: Literal["value", "amount"]: `value` == $ AMOUNT TO BUY; `amount` == NUMBER OF SHARES.
    :param data_time_freq: str: The frequency of the data; 1-minute == `1min`, 1-hour == `1h`, daily data, etc.
    :return: Dict[str, pd.DataFrame]: Dictionary with the 3 output Pandas DataFrames.
    """
    # noinspection PyProtectedMember
    from pandas.api.types import is_numeric_dtype

    # noinspection PyUnresolvedReferences,PyTypeChecker
    main_df: DataFrameGroupBy = main_df.groupby(by=[symbol_column])
    main_df: pd.DataFrame = main_df.apply(
        func=_groupby_portfolio_performance,
        time_column=time_column,
        close_column=close_column,
    ).reset_index(drop=True)

    grouped_dates: pd.Series = main_df.groupby(by=symbol_column)[time_column].apply(lambda market_ts: set(market_ts))
    # `*` is an unpacking operator.
    # It takes the list of sets and passes them as separate arguments into `set.intersection`
    common_timestamps: List[str] = list(set.intersection(*grouped_dates.tolist()))

    main_df: pd.DataFrame = main_df[main_df[time_column].isin(values=common_timestamps)].reset_index(drop=True)

    close_arrays_list: List[np.ndarray] = []
    entries_arrays_list: List[np.ndarray] = []
    exits_arrays_list: List[np.ndarray] = []

    index_dict: Dict[int, str] = {}

    index_num: int
    symbol: str
    for index_num, symbol in enumerate(symbol_list):
        temp_df: pd.DataFrame = main_df[main_df[symbol_column] == symbol].reset_index(drop=True)
        close_array: np.ndarray = temp_df[close_column].to_numpy()
        # noinspection PyUnresolvedReferences
        entries_array: np.ndarray = (temp_df["chande_dynamic"] == 1).to_numpy()
        # noinspection PyUnresolvedReferences
        exits_array: np.ndarray = (temp_df["chande_dynamic"] == -1).to_numpy()

        close_arrays_list.append(close_array)
        entries_arrays_list.append(entries_array)
        exits_arrays_list.append(exits_array)

        index_dict[index_num]: str = symbol

    close_array_stack: np.ndarray = np.column_stack(tup=close_arrays_list)
    entries_array_stack: np.ndarray = np.column_stack(tup=entries_arrays_list)
    exits_array_stack: np.ndarray = np.column_stack(tup=exits_arrays_list)
    # Run the backtest using optimized arrays
    portfolio: vbt.Portfolio = vbt.Portfolio.from_signals(
        close=close_array_stack,
        entries=entries_array_stack,
        exits=exits_array_stack,
        init_cash=investment_amount,  # How much money to start out with
        size=per_buy_quantity,  # $ AMOUNT TO BUY OR NUMBER OF SHARES
        size_type=per_buy_type,  # NOTE:  `value` == $ AMOUNT TO BUY; `amount` == NUMBER OF SHARES
        freq=data_time_freq,  # NOTE: `1min` is for 1-minute intraday data, `1h` for hourly data
    )

    # Analyze the results
    # Only calculate aggregate `np.mean` to metrics that make sense per asset
    # Ex. Total Return, Expectancy, Sharpe Ratio
    stat_results: pd.DataFrame = portfolio.stats(agg_func=None).reset_index()
    stat_results: pd.DataFrame = stat_results.round(decimals=2)
    stat_column_renames: Dict[str, str] = {
        "index": "symbol",
        "Start Index": "start_index",
        "End Index": "end_index",
        "Total Duration": "total_duration",
        "Start Value": "start_value",
        "Min Value": "min_value",
        "Max Value": "max_value",
        "End Value": "end_value",
        "Total Return [%]": "total_return_perc",
        "Benchmark Return [%]": "benchmark_return_perc",
        "Position Coverage [%]": "position_coverage_perc",
        "Max Gross Exposure [%]": "max_exposure_perc",
        "Max Drawdown [%]": "max_drawdown_perc",
        "Max Drawdown Duration": "max_drawdown_duration",
        "Total Orders": "total_orders",
        "Total Fees Paid": "total_fees_paid",
        "Total Trades": "total_trades",
        "Win Rate [%]": "win_rate_perc",
        "Best Trade [%]": "best_trade_perc",
        "Worst Trade [%]": "worse_trade_perc",
        "Avg Winning Trade [%]": "avg_winning_trade_perc",
        "Avg Losing Trade [%]": "avg_losing_trade_perc",
        "Avg Winning Trade Duration": "avg_winning_trade_duration",
        "Avg Losing Trade Duration": "avg_losing_trade_duration",
        "Profit Factor": "profit_factor",
        "Expectancy": "expectancy",
        "Sharpe Ratio": "sharpe_ratio",
        "Calmar Ratio": "calmar_ratio",
        "Omega Ratio": "omega_ratio",
        "Sortino Ratio": "sortino_ratio",
    }
    stat_results: pd.DataFrame = stat_results.rename(columns=stat_column_renames)
    # `.map()` will convert unmatched values to `NaN`
    # `.replace()` leaves unmatched values unchanged
    stat_results["symbol"]: pd.Series = stat_results["symbol"].map(arg=index_dict)
    stat_results["exec_timestamp"]: pd.Series = exec_timestamp
    stat_results["begin_market_date"]: pd.Series = begin_market_date
    stat_results["market_date"]: pd.Series = begin_market_date
    stat_results["market_year"]: pd.Series = pd.to_datetime(arg=stat_results["market_date"]).dt.strftime("%Y")
    stat_results["market_year"]: pd.Series = stat_results["market_year"].astype(dtype=int)
    stat_results["market_month"]: pd.Series = pd.to_datetime(arg=stat_results["market_date"]).dt.month
    stat_results["market_month"]: pd.Series = stat_results["market_month"].astype(dtype=int)
    column_name: str
    for column_name in stat_results.columns:
        column_data: pd.Series = stat_results[column_name]
        if not is_numeric_dtype(arr_or_dtype=column_data):
            stat_results[column_name]: pd.Series = column_data.astype(dtype=str)

    # noinspection PyUnresolvedReferences
    order_results: pd.DataFrame = portfolio.orders.stats(agg_func=None).reset_index()
    order_results: pd.DataFrame = order_results.round(decimals=2)
    order_results_renames: Dict[str, str] = {
        "index": "symbol",
        "Start Index": "start_index",
        "End Index": "end_index",
        "Total Duration": "total_duration",
        "Total Records": "total_records",
        "Side Counts: Buy": "side_counts_buy",
        "Side Counts: Sell": "side_counts_sell",
        "Type Counts: Market": "type_counts_market",
        "Type Counts: Limit": "type_counts_limit",
        "Stop Type Counts: None": "stop_type_counts_none",
        "Stop Type Counts: SL": "stop_type_counts_sl",
        "Stop Type Counts: TSL": "stop_type_counts_tsl",
        "Stop Type Counts: TTP": "stop_type_counts_ttp",
        "Stop Type Counts: TP": "stop_type_counts_tp",
        "Stop Type Counts: TD": "stop_type_counts_td",
        "Stop Type Counts: DT": "stop_type_counts_dt",
        "Size: Min": "size_min",
        "Size: Median": "size_median",
        "Size: Max": "size_max",
        "Fees: Min": "fees_min",
        "Fees: Median": "fees_median",
        "Fees: Max": "fees_max",
        "Weighted Buy Price": "weighted_buy_price",
        "Weighted Sell Price": "weighted_sell_price",
        "Avg Signal-Creation Duration": "avg_signal_creation_duration",
        "Avg Creation-Fill Duration": "avg_creation_fill_duration",
        "Avg Signal-Fill Duration": "avg_signal_fill_duration",
    }
    order_results: pd.DataFrame = order_results.rename(columns=order_results_renames)
    # `.map()` will convert unmatched values to `NaN`
    # `.replace()` leaves unmatched values unchanged
    order_results["symbol"]: pd.Series = order_results["symbol"].map(arg=index_dict)
    order_results["exec_timestamp"]: pd.Series = exec_timestamp
    order_results["begin_market_date"]: pd.Series = begin_market_date
    order_results["market_date"]: pd.Series = begin_market_date
    order_results["market_year"]: pd.Series = pd.to_datetime(arg=order_results["market_date"]).dt.strftime("%Y")
    order_results["market_year"]: pd.Series = order_results["market_year"].astype(dtype=int)
    order_results["market_month"]: pd.Series = pd.to_datetime(arg=order_results["market_date"]).dt.month
    order_results["market_month"]: pd.Series = order_results["market_month"].astype(dtype=int)
    column_name: str
    for column_name in order_results.columns:
        column_data: pd.Series = order_results[column_name]
        if not is_numeric_dtype(arr_or_dtype=column_data):
            order_results[column_name]: pd.Series = column_data.astype(dtype=str)

    # noinspection PyUnresolvedReferences
    order_details: pd.DataFrame = portfolio.orders.records_readable
    order_details: pd.DataFrame = order_details.round(decimals=2)
    order_details_renames: Dict[str, str] = {
        "Order Id": "order_id",
        "Column": "symbol",
        "Signal Index": "signal_index",
        "Creation Index": "creation_index",
        "Fill Index": "fill_index",
        "Size": "size",
        "Price": "price",
        "Fees": "fees",
        "Side": "side",
        "Type": "type",
        "Stop Type": "stop_type",
    }
    order_details: pd.DataFrame = order_details.rename(columns=order_details_renames)
    # `.map()` will convert unmatched values to `NaN`
    # `.replace()` leaves unmatched values unchanged
    order_details["symbol"]: pd.Series = order_details["symbol"].map(arg=index_dict)
    order_details["exec_timestamp"]: pd.Series = exec_timestamp
    order_details["begin_market_date"]: pd.Series = begin_market_date
    order_details["market_date"]: pd.Series = begin_market_date
    order_details["market_year"]: pd.Series = pd.to_datetime(arg=order_details["market_date"]).dt.strftime("%Y")
    order_details["market_year"]: pd.Series = order_details["market_year"].astype(dtype=int)
    order_details["market_month"]: pd.Series = pd.to_datetime(arg=order_details["market_date"]).dt.month
    order_details["market_month"]: pd.Series = order_details["market_month"].astype(dtype=int)
    column_name: str
    for column_name in order_details.columns:
        column_data: pd.Series = order_details[column_name]
        if not is_numeric_dtype(arr_or_dtype=column_data):
            order_details[column_name]: pd.Series = column_data.astype(dtype=str)
    results_dict: Dict[str, pd.DataFrame] = {
        "stat_results": stat_results,
        "order_results": order_results,
        "order_details": order_details,
    }
    return results_dict
