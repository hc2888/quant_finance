"""Python Test(s) for `dags/algo_metrics/trade_methods/trends.py`."""

import pandas as pd
from global_utils.develop_secrets import QUANT_PROJ_PATH

# ----------------------------------------------------------------------------------------------------------------------
FILE_PATH: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/trade_methods"
TARGET_SUBFOLDER: str = "momentums"
DATA_FOLDER: str = rf"{FILE_PATH}/data/{TARGET_SUBFOLDER}"
EXPECTED_FOLDER: str = rf"{FILE_PATH}/expected_results/{TARGET_SUBFOLDER}"


# ----------------------------------------------------------------------------------------------------------------------
def test_divergence_index() -> None:
    """Test the `divergence_index` Calculation."""
    from algo_metrics.trade_methods.momentums import divergence_index

    file_name: str = r"divergence_index.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = divergence_index(main_df=test_df, target_column="close")

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_ultim_osc_signal() -> None:
    """Test the `ultim_osc_signal` Calculation."""
    from algo_metrics.trade_methods.momentums import ultim_osc_signal

    file_name: str = r"ultim_osc_signal.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = ultim_osc_signal(main_df=test_df)

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_relative_vigor_idx() -> None:
    """Test the `relative_vigor_idx` Calculation."""
    from algo_metrics.trade_methods.momentums import relative_vigor_idx

    file_name: str = r"relative_vigor_idx.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = relative_vigor_idx(main_df=test_df)

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_raschke_first_cross() -> None:
    """Test the `raschke_first_cross` Calculation."""
    from algo_metrics.trade_methods.momentums import raschke_first_cross

    file_name: str = r"raschke_first_cross.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = raschke_first_cross(main_df=test_df)

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_strength_oscillator() -> None:
    """Test the `strength_oscillator` Calculation."""
    from algo_metrics.trade_methods.momentums import strength_oscillator

    file_name: str = r"strength_oscillator.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = strength_oscillator(main_df=test_df)

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_velocity_accel() -> None:
    """Test the `velocity_accel` Calculation."""
    from algo_metrics.trade_methods.momentums import velocity_accel

    file_name: str = r"velocity_accel.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = velocity_accel(main_df=test_df)

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_cambridge_hook() -> None:
    """Test the `cambridge_hook` Calculation."""
    from algo_metrics.trade_methods.momentums import cambridge_hook

    file_name: str = r"cambridge_hook.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = cambridge_hook(main_df=test_df)

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_momentum_divergence_sell() -> None:
    """Test the `momentum_divergence` Calculation; SELL signal."""
    from algo_metrics.trade_methods.momentums import momentum_divergence

    file_name: str = r"momentum_divergence_sell.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: int = momentum_divergence(main_df=test_df)

    assert test_result == -1


# ----------------------------------------------------------------------------------------------------------------------
def test_momentum_divergence_buy() -> None:
    """Test the `momentum_divergence` Calculation; BUY signal."""
    from algo_metrics.trade_methods.momentums import momentum_divergence

    file_name: str = r"momentum_divergence_buy.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: int = momentum_divergence(main_df=test_df)

    assert test_result == 1


# ----------------------------------------------------------------------------------------------------------------------
def test_momentum_divergence_neutral() -> None:
    """Test the `momentum_divergence` Calculation; NEUTRAL signal."""
    from algo_metrics.trade_methods.momentums import momentum_divergence

    file_name: str = r"momentum_divergence_neutral.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: int = momentum_divergence(main_df=test_df)

    assert test_result == 0
