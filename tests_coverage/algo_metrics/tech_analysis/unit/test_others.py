"""Python Test(s) for `dags/tech_analysis/others.py`."""

import pandas as pd
from global_utils.develop_secrets import QUANT_PROJ_PATH

# ----------------------------------------------------------------------------------------------------------------------
FILE_PATH: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis"
TARGET_SUBFOLDER: str = "others"
DATA_FOLDER: str = rf"{FILE_PATH}/data/{TARGET_SUBFOLDER}"
EXPECTED_FOLDER: str = rf"{FILE_PATH}/expected_results/{TARGET_SUBFOLDER}"


# ----------------------------------------------------------------------------------------------------------------------
def test_daily_regular_return() -> None:
    """Test the `daily_regular_return` Calculation."""
    from algo_metrics.tech_analysis.others import daily_regular_return

    file_name: str = r"daily_regular_return.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = daily_regular_return(main_df=test_df)
    test_result: pd.DataFrame = test_result.drop(columns=["symbol", "market_date"])

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_daily_log_return() -> None:
    """Test the `daily_log_return` Calculation."""
    from algo_metrics.tech_analysis.others import daily_log_return

    file_name: str = r"daily_log_return.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = daily_log_return(main_df=test_df)
    test_result: pd.DataFrame = test_result.drop(columns=["symbol", "market_date"])

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_cumulative_return() -> None:
    """Test the `cumulative_return` Calculation."""
    from algo_metrics.tech_analysis.others import cumulative_return

    file_name: str = r"cumulative_return.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = cumulative_return(main_df=test_df)
    test_result: pd.DataFrame = test_result.drop(columns=["symbol", "market_date"])

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)
