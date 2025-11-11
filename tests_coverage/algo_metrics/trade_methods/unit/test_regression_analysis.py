"""Python Test(s) for `dags/algo_metrics/regression_analysis.py`."""

from typing import Dict

import numpy as np
import pandas as pd
from global_utils.develop_secrets import QUANT_PROJ_PATH

# ----------------------------------------------------------------------------------------------------------------------
FILE_PATH: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/trade_methods"
TARGET_SUBFOLDER: str = "regression_analysis"
DATA_FOLDER: str = rf"{FILE_PATH}/data/{TARGET_SUBFOLDER}"
EXPECTED_FOLDER: str = rf"{FILE_PATH}/expected_results/{TARGET_SUBFOLDER}"


# ----------------------------------------------------------------------------------------------------------------------
def test_linear_regression_fit() -> None:
    """Test the linear_regression_fit function."""
    from algo_metrics.trade_methods.regression_analysis import linear_regression_fit

    target_array: np.ndarray = np.array(object=[2, 4, 6, 8, 10])
    # Expected slope and intercept for y = 2x should be slope (m) = 2 and intercept (b) = 2
    expected_slope: float = 2.0
    expected_intercept: float = 2.0
    # Calculate slope and intercept using the function

    linear_regression_dict: Dict[str, float] = linear_regression_fit(target_array=target_array)
    calculated_slope: float = linear_regression_dict["eq_slope"]
    calculated_intercept: float = linear_regression_dict["eq_intercept"]

    # Assert the calculated slope and intercept match the expected values
    assert np.isclose(a=calculated_slope, b=expected_slope, atol=0.001)
    assert np.isclose(a=calculated_intercept, b=expected_intercept, atol=0.001)


# ----------------------------------------------------------------------------------------------------------------------
def test_trend_line_vars_downward() -> None:
    """Test the trend_line_vars function."""
    from algo_metrics.trade_methods.regression_analysis import trend_line_vars

    target_array: np.ndarray = np.array(object=[7, 8, 6, 7, 5, 6, 4, 5, 2, 3, 1])
    expected_coeffs: Dict[str, float] = {
        "is_upward_trend": -1,
        "eq_slope": -0.61,
        "eq_intercept": 7.95,
        "resistance_line": 6,
        "support_line": 2,
        "top_channel_bias": 9.27,
        "bottom_channel_bias": 6.88,
    }
    calculated_coeffs: Dict[str, float] = trend_line_vars(target_array=target_array)
    print(rf"calculated_coeffs: {calculated_coeffs}")

    assert np.isclose(a=calculated_coeffs["is_upward_trend"], b=expected_coeffs["is_upward_trend"], atol=0.1)
    assert np.isclose(a=calculated_coeffs["eq_slope"], b=expected_coeffs["eq_slope"], atol=0.1)
    assert np.isclose(a=calculated_coeffs["eq_intercept"], b=expected_coeffs["eq_intercept"], atol=0.1)
    assert np.isclose(a=calculated_coeffs["resistance_line"], b=expected_coeffs["resistance_line"], atol=0.1)
    assert np.isclose(a=calculated_coeffs["support_line"], b=expected_coeffs["support_line"], atol=0.1)
    assert np.isclose(a=calculated_coeffs["top_channel_bias"], b=expected_coeffs["top_channel_bias"], atol=0.1)
    assert np.isclose(a=calculated_coeffs["bottom_channel_bias"], b=expected_coeffs["bottom_channel_bias"], atol=0.1)


# ----------------------------------------------------------------------------------------------------------------------
def test_trend_line_vars_upward() -> None:
    """Test the trend_line_vars function."""
    from algo_metrics.trade_methods.regression_analysis import trend_line_vars

    target_array: np.ndarray = np.array(object=[1, 3, 2, 5, 4, 6, 5, 7, 6, 8, 7])
    expected_coeffs: Dict[str, float] = {
        "is_upward_trend": 1,
        "eq_slope": 0.61,
        "eq_intercept": 1.86,
        "resistance_line": 6,
        "support_line": 2,
        "top_channel_bias": 3.17,
        "bottom_channel_bias": 0.78,
    }
    calculated_coeffs: Dict[str, float] = trend_line_vars(target_array=target_array)
    print(rf"calculated_coeffs: {calculated_coeffs}")

    assert np.isclose(a=calculated_coeffs["is_upward_trend"], b=expected_coeffs["is_upward_trend"], atol=0.1)
    assert np.isclose(a=calculated_coeffs["eq_slope"], b=expected_coeffs["eq_slope"], atol=0.1)
    assert np.isclose(a=calculated_coeffs["eq_intercept"], b=expected_coeffs["eq_intercept"], atol=0.1)
    assert np.isclose(a=calculated_coeffs["resistance_line"], b=expected_coeffs["resistance_line"], atol=0.1)
    assert np.isclose(a=calculated_coeffs["support_line"], b=expected_coeffs["support_line"], atol=0.1)
    assert np.isclose(a=calculated_coeffs["top_channel_bias"], b=expected_coeffs["top_channel_bias"], atol=0.1)
    assert np.isclose(a=calculated_coeffs["bottom_channel_bias"], b=expected_coeffs["bottom_channel_bias"], atol=0.1)


# ----------------------------------------------------------------------------------------------------------------------
def test_trend_line_vars_inconsistent_slopes() -> None:
    """Test the trend_line_vars function when top and bottom slopes are inconsistent."""
    from algo_metrics.trade_methods.regression_analysis import trend_line_vars

    target_array: np.ndarray = np.array([7, 8, 6, 7, 5, 6, 4, 5, 100, 3, 1])
    expected_coeffs: Dict[str, float] = {
        "is_upward_trend": 0,
        "eq_slope": 2.06,
        "eq_intercept": 3.5,
        "resistance_line": 100,
        "support_line": 4,
        "top_channel_bias": 83.52,
        "bottom_channel_bias": -19.6,
    }
    calculated_coeffs: Dict[str, float] = trend_line_vars(target_array=target_array)
    print(rf"calculated_coeffs: {calculated_coeffs}")

    assert np.isclose(a=calculated_coeffs["is_upward_trend"], b=expected_coeffs["is_upward_trend"], atol=0.1)
    assert np.isclose(a=calculated_coeffs["eq_slope"], b=expected_coeffs["eq_slope"], atol=0.1)
    assert np.isclose(a=calculated_coeffs["eq_intercept"], b=expected_coeffs["eq_intercept"], atol=0.1)
    assert np.isclose(a=calculated_coeffs["resistance_line"], b=expected_coeffs["resistance_line"], atol=0.1)
    assert np.isclose(a=calculated_coeffs["support_line"], b=expected_coeffs["support_line"], atol=0.1)
    assert np.isclose(a=calculated_coeffs["top_channel_bias"], b=expected_coeffs["top_channel_bias"], atol=0.1)
    assert np.isclose(a=calculated_coeffs["bottom_channel_bias"], b=expected_coeffs["bottom_channel_bias"], atol=0.1)


# ----------------------------------------------------------------------------------------------------------------------
def test_trend_line_vars_no_slopes() -> None:
    """Test the trend_line_vars function when top and bottom slopes are inconsistent."""
    from algo_metrics.trade_methods.regression_analysis import trend_line_vars

    target_array: np.ndarray = np.array([7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7])
    expected_coeffs: Dict[str, int] = {
        "is_upward_trend": 0,
        "eq_slope": 0,
        "eq_intercept": 0,
        "resistance_line": 0,
        "support_line": 0,
        "top_channel_bias": 0,
        "bottom_channel_bias": 0,
    }
    calculated_coeffs: Dict[str, float] = trend_line_vars(target_array=target_array)
    print(rf"calculated_coeffs: {calculated_coeffs}")

    assert np.isclose(a=calculated_coeffs["is_upward_trend"], b=expected_coeffs["is_upward_trend"], atol=0.1)
    assert np.isclose(a=calculated_coeffs["eq_slope"], b=expected_coeffs["eq_slope"], atol=0.1)
    assert np.isclose(a=calculated_coeffs["eq_intercept"], b=expected_coeffs["eq_intercept"], atol=0.1)
    assert np.isclose(a=calculated_coeffs["resistance_line"], b=expected_coeffs["resistance_line"], atol=0.1)
    assert np.isclose(a=calculated_coeffs["support_line"], b=expected_coeffs["support_line"], atol=0.1)
    assert np.isclose(a=calculated_coeffs["top_channel_bias"], b=expected_coeffs["top_channel_bias"], atol=0.1)
    assert np.isclose(a=calculated_coeffs["bottom_channel_bias"], b=expected_coeffs["bottom_channel_bias"], atol=0.1)


# ----------------------------------------------------------------------------------------------------------------------
def test_trend_line_vars_no_data_troughs() -> None:
    """Test the trend_line_vars function."""
    from algo_metrics.trade_methods.regression_analysis import trend_line_vars

    target_array: np.ndarray = np.array(object=[23.98, 22.99, 21.99, 26.99, 23.99, 23.99, 23.99])
    expected_coeffs: Dict[str, float] = {
        "is_upward_trend": 0,
        "eq_slope": 0.14,
        "eq_intercept": 23.56,
        "resistance_line": 26.99,
        "support_line": 21.99,
        "top_channel_bias": 26.57,
        "bottom_channel_bias": 21.71,
    }
    calculated_coeffs: Dict[str, float] = trend_line_vars(target_array=target_array)
    print(rf"calculated_coeffs: {calculated_coeffs}")

    assert np.isclose(a=calculated_coeffs["is_upward_trend"], b=expected_coeffs["is_upward_trend"], atol=0.1)
    assert np.isclose(a=calculated_coeffs["eq_slope"], b=expected_coeffs["eq_slope"], atol=0.1)
    assert np.isclose(a=calculated_coeffs["eq_intercept"], b=expected_coeffs["eq_intercept"], atol=0.1)
    assert np.isclose(a=calculated_coeffs["resistance_line"], b=expected_coeffs["resistance_line"], atol=0.1)
    assert np.isclose(a=calculated_coeffs["support_line"], b=expected_coeffs["support_line"], atol=0.1)
    assert np.isclose(a=calculated_coeffs["top_channel_bias"], b=expected_coeffs["top_channel_bias"], atol=0.1)
    assert np.isclose(a=calculated_coeffs["bottom_channel_bias"], b=expected_coeffs["bottom_channel_bias"], atol=0.1)


# ----------------------------------------------------------------------------------------------------------------------
def test_auto_correl_durbin_watson() -> None:
    """Test the Durbin-Watson d-statistic calculation."""
    from algo_metrics.trade_methods.regression_analysis import auto_correl_durbin_watson

    data_array: np.ndarray = np.array(object=[0.5, -0.2, 0.3, -0.4, 0.1])

    # Calculate expected result manually
    diff_residuals: np.ndarray = np.diff(a=data_array, n=1, axis=0)
    sum_squared_diff: float = np.sum(a=diff_residuals**2, axis=0)
    sum_squared_residuals: float = np.sum(a=data_array**2, axis=0)
    expected_result: float = round(sum_squared_diff / sum_squared_residuals, 3)

    test_result: float = auto_correl_durbin_watson(target_array=data_array)

    assert np.isclose(a=expected_result, b=test_result, atol=0.001)


# ----------------------------------------------------------------------------------------------------------------------
def test_trend_line_eval_true() -> None:
    """Test the Trend Line Evaluation calculation."""
    from algo_metrics.trade_methods.regression_analysis import trend_line_eval

    data_df: pd.DataFrame = pd.DataFrame(
        data={
            "true_values": [3.0, 0.5, 2.0, 7.0],
            "predicted_values": [3.1, 0.4, 1.9, 7.1],
            "symbol": ["SPY", "SPY", "SPY", "SPY"],
            "market_date": ["01", "02", "03", "04"],
        },
    )
    test_result: int = trend_line_eval(main_df=data_df, true_column="true_values", pred_column="predicted_values")

    assert test_result == 1


# ----------------------------------------------------------------------------------------------------------------------
def test_trend_line_eval_false() -> None:
    """Test the Trend Line Evaluation calculation."""
    from algo_metrics.trade_methods.regression_analysis import trend_line_eval

    data_df: pd.DataFrame = pd.DataFrame(
        data={
            "true_values": [3.0, -0.5, 2.0, 7.0],
            "predicted_values": [22.5, 0.0, 23.0, 18.0],
            "symbol": ["SPY", "SPY", "SPY", "SPY"],
            "market_date": ["01", "02", "03", "04"],
        },
    )
    test_result: int = trend_line_eval(main_df=data_df, true_column="true_values", pred_column="predicted_values")

    assert test_result == -1


# ----------------------------------------------------------------------------------------------------------------------
def test_regression_trade_signal() -> None:
    """Test the Regression_Trade_signal Calculation."""
    from algo_metrics.trade_methods.regression_analysis import (
        linear_regression_fit,
        regression_trade_signal,
    )

    file_name: str = r"regression_trade_signal.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    target_column: pd.Series = pd.read_csv(filepath_or_buffer=test_data).head(n=200)["close"]

    linear_regression_dict: Dict[str, float] = linear_regression_fit(target_array=target_column.to_numpy())
    eq_slope: float = linear_regression_dict["eq_slope"]
    eq_intercept: float = linear_regression_dict["eq_intercept"]

    results_column: pd.Series = pd.read_csv(filepath_or_buffer=test_data).tail(n=100)["close"]
    test_result: pd.Series = regression_trade_signal(
        target_column=results_column,
        eq_slope=eq_slope,
        eq_intercept=eq_intercept,
    )

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.Series = pd.read_csv(filepath_or_buffer=expected_data)["trade_signal"]

    pd.testing.assert_series_equal(left=test_result, right=expected_result, check_like=True)
