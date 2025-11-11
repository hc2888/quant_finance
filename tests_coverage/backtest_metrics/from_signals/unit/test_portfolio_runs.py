"""Python Test(s) for `dags/backtest_metrics/from_signals.py`."""

from typing import Dict

import pandas as pd
from global_utils.develop_secrets import QUANT_PROJ_PATH

# ----------------------------------------------------------------------------------------------------------------------
FILE_PATH: str = rf"{QUANT_PROJ_PATH}/tests_coverage/backtest_metrics/from_signals"
TARGET_SUBFOLDER: str = "portfolio_runs"
DATA_FOLDER: str = rf"{FILE_PATH}/data/{TARGET_SUBFOLDER}"
EXPECTED_FOLDER: str = rf"{FILE_PATH}/expected_results/{TARGET_SUBFOLDER}"


# ----------------------------------------------------------------------------------------------------------------------
def test_portfolio_performance() -> None:
    """Test the `portfolio_portfolio_performance` Calculation."""
    from backtest_metrics.from_signals.portfolio_runs import portfolio_performance

    file_name: str = r"portfolio_performance.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: Dict[str, pd.DataFrame] = portfolio_performance(
        main_df=test_df,
        symbol_list=["SPY", "GOVT", "QTUM"],
        begin_market_date="2025-01-13",
        exec_timestamp="placeholder",
    )
    stat_results_test: pd.DataFrame = test_result["stat_results"]
    stat_results_test: pd.DataFrame = stat_results_test.drop(columns=["exec_timestamp"])
    stat_results_csv: str = "portfolio_performance_stat_results.csv"
    stat_results_expected: pd.DataFrame = pd.read_csv(filepath_or_buffer=rf"{EXPECTED_FOLDER}/{stat_results_csv}")
    pd.testing.assert_frame_equal(
        left=stat_results_test,
        right=stat_results_expected,
        check_like=True,
        check_dtype=False,
    )

    order_results_test: pd.DataFrame = test_result["order_results"]
    order_results_test: pd.DataFrame = order_results_test.drop(columns=["exec_timestamp"])
    order_results_csv: str = "portfolio_performance_order_results.csv"
    order_results_expected: pd.DataFrame = pd.read_csv(filepath_or_buffer=rf"{EXPECTED_FOLDER}/{order_results_csv}")
    pd.testing.assert_frame_equal(
        left=order_results_test,
        right=order_results_expected,
        check_like=True,
        check_dtype=False,
    )

    order_details_test: pd.DataFrame = test_result["order_details"]
    order_details_test: pd.DataFrame = order_details_test.drop(columns=["exec_timestamp", "stop_type"])
    order_details_csv: str = "portfolio_performance_order_details.csv"
    order_details_expected: pd.DataFrame = pd.read_csv(filepath_or_buffer=rf"{EXPECTED_FOLDER}/{order_details_csv}")
    pd.testing.assert_frame_equal(
        left=order_details_test,
        right=order_details_expected,
        check_like=True,
        check_dtype=False,
    )
