"""Python Test(s) for `dags/algo_metrics/event_driven.py`."""

from typing import Dict, List

import pandas as pd
from global_utils.develop_secrets import QUANT_PROJ_PATH

# ----------------------------------------------------------------------------------------------------------------------
FILE_PATH: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/trade_methods"
TARGET_SUBFOLDER: str = "event_driven"
DATA_FOLDER: str = rf"{FILE_PATH}/data/{TARGET_SUBFOLDER}"
EXPECTED_FOLDER: str = rf"{FILE_PATH}/expected_results/{TARGET_SUBFOLDER}"


# ----------------------------------------------------------------------------------------------------------------------
def test_swing_high_low_points_v1() -> None:
    """Test function."""
    from algo_metrics.trade_methods.event_driven import swing_high_low_points

    file_name: str = r"swing_high_low_points_v1.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = swing_high_low_points(main_df=test_df)
    test_result: Dict[str, List[float]] = test_result[["swing_high_low", "swing_high_low_entry"]].to_dict(orient="list")

    expected_result: Dict[str, List[float]] = {
        "swing_high_low_entry": [0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, -1.0, -1.0, -1.0, -1.0, -1.0],
        "swing_high_low": [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
    }

    assert test_result == expected_result


# ----------------------------------------------------------------------------------------------------------------------
def test_swing_high_low_points_v2() -> None:
    """Test function."""
    from algo_metrics.trade_methods.event_driven import swing_high_low_points

    file_name: str = r"swing_high_low_points_v2.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = swing_high_low_points(main_df=test_df)
    test_result: Dict[str, List[float]] = test_result[["swing_high_low", "swing_high_low_entry"]].to_dict(orient="list")

    expected_result: Dict[str, List[float]] = {
        "swing_high_low_entry": [0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
        "swing_high_low": [-1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0],
    }

    assert test_result == expected_result


# ----------------------------------------------------------------------------------------------------------------------
def test_swing_high_low_points_v3() -> None:
    """Test function."""
    from algo_metrics.trade_methods.event_driven import swing_high_low_points

    file_name: str = r"swing_high_low_points_v3.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = swing_high_low_points(main_df=test_df)
    test_result: Dict[str, List[float]] = test_result[["swing_high_low", "swing_high_low_entry"]].to_dict(orient="list")

    expected_result: Dict[str, List[float]] = {
        "swing_high_low_entry": [0.0, 0.0, -1.0, -1.0, 1.0, 1.0, 0.0, -1.0, -1.0, -1.0, -1.0, 0.0],
        "swing_high_low": [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, -1.0, -1.0, -1.0, -1.0, -1.0],
    }

    assert test_result == expected_result


# ----------------------------------------------------------------------------------------------------------------------
def test_swing_high_low_points_v4() -> None:
    """Test function."""
    from algo_metrics.trade_methods.event_driven import swing_high_low_points

    file_name: str = r"swing_high_low_points_v4.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = swing_high_low_points(main_df=test_df)
    test_result: Dict[str, List[float]] = test_result[["swing_high_low", "swing_high_low_entry"]].to_dict(orient="list")

    expected_result: Dict[str, List[float]] = {
        "swing_high_low_entry": [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
        "swing_high_low": [-1.0, -1.0, -1.0, -1.0, -1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
    }

    assert test_result == expected_result


# ----------------------------------------------------------------------------------------------------------------------
def test_swing_high_low_points_v5() -> None:
    """Test function."""
    from algo_metrics.trade_methods.event_driven import swing_high_low_points

    file_name: str = r"swing_high_low_points_v5.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = swing_high_low_points(main_df=test_df)
    test_result: Dict[str, List[float]] = test_result[["swing_high_low", "swing_high_low_entry"]].to_dict(orient="list")

    expected_result: Dict[str, List[float]] = {
        "swing_high_low_entry": [-1.0, -1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
        "swing_high_low": [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
    }

    assert test_result == expected_result


# ----------------------------------------------------------------------------------------------------------------------
def test_wilder_swing_index() -> None:
    """Test function."""
    from algo_metrics.trade_methods.event_driven import wilder_swing_index

    file_name: str = r"wilder_swing_index.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = wilder_swing_index(main_df=test_df)

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_n_day_breakout() -> None:
    """Test function."""
    from algo_metrics.trade_methods.event_driven import n_day_breakout

    file_name: str = r"n_day_breakout.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = n_day_breakout(main_df=test_df)

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_turtle_strategy() -> None:
    """Test function."""
    from algo_metrics.trade_methods.event_driven import turtle_strategy

    file_name: str = r"turtle_strategy.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = turtle_strategy(main_df=test_df)

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)
