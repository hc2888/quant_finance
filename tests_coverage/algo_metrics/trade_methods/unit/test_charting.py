"""Python Test(s) for `dags/algo_metrics/charting.py`."""

from typing import Dict, List, Union

import pandas as pd
from global_utils.develop_secrets import QUANT_PROJ_PATH

# ----------------------------------------------------------------------------------------------------------------------
FILE_PATH: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/trade_methods"
TARGET_SUBFOLDER: str = "charting"
DATA_FOLDER: str = rf"{FILE_PATH}/data/{TARGET_SUBFOLDER}"
EXPECTED_FOLDER: str = rf"{FILE_PATH}/expected_results/{TARGET_SUBFOLDER}"


# ----------------------------------------------------------------------------------------------------------------------
def test_detect_spike_true() -> None:
    """Test function."""
    from algo_metrics.trade_methods.charting import detect_spike

    data_dict: Dict[str, List[Union[str, int]]] = {
        "open": [100, 110, 115, 113, 111, 109, 108, 107, 106, 105],
        "high": [110, 115, 117, 114, 112, 110, 109, 108, 125, 106],
        "low": [95, 105, 110, 109, 107, 106, 105, 104, 103, 102],
        "close": [108, 113, 116, 112, 110, 109, 107, 106, 105, 104],
        "volume": [1000, 1500, 1400, 1300, 1200, 1100, 1000, 900, 800, 700],
        "symbol": ["SPY", "SPY", "SPY", "SPY", "SPY", "SPY", "SPY", "SPY", "SPY", "SPY"],
        "market_date": ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10"],
    }
    main_df: pd.DataFrame = pd.DataFrame(data=data_dict)

    spike_detected: int = detect_spike(main_df=main_df)["detect_spike"].iloc[-1]

    assert spike_detected == 1


# ----------------------------------------------------------------------------------------------------------------------
def test_detect_spike_false() -> None:
    """Test function."""
    from algo_metrics.trade_methods.charting import detect_spike

    data_dict: Dict[str, List[Union[str, int]]] = {
        "open": [100, 102, 101, 103, 102, 101, 99, 98, 97, 96],
        "high": [101, 103, 102, 104, 103, 102, 100, 99, 98, 97],
        "low": [99, 101, 100, 102, 101, 100, 98, 97, 96, 95],
        "close": [100, 102, 101, 103, 102, 101, 99, 98, 97, 96],
        "volume": [1000, 1500, 1200, 1300, 1400, 1100, 1000, 900, 800, 700],
        "symbol": ["SPY", "SPY", "SPY", "SPY", "SPY", "SPY", "SPY", "SPY", "SPY", "SPY"],
        "market_date": ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10"],
    }
    main_df: pd.DataFrame = pd.DataFrame(data=data_dict)

    spike_detected: int = detect_spike(main_df=main_df)["detect_spike"].iloc[-1]

    assert spike_detected == -1


# ----------------------------------------------------------------------------------------------------------------------
def test_island_reversal_true() -> None:
    """Test function."""
    from algo_metrics.trade_methods.charting import island_reversal

    data_dict: Dict[str, List[Union[str, int]]] = {
        "open": [100, 120, 115],
        "high": [110, 125, 117],
        "low": [95, 125, 110],
        "close": [108, 123, 116],
        "symbol": ["SPY", "SPY", "SPY"],
        "market_date": ["01", "02", "03"],
    }
    main_df: pd.DataFrame = pd.DataFrame(data=data_dict)

    island_reversal_detected: int = island_reversal(main_df=main_df)["island_reversal"].iloc[-1]

    assert island_reversal_detected == 1


# ----------------------------------------------------------------------------------------------------------------------
def test_island_reversal_false() -> None:
    """Test function."""
    from algo_metrics.trade_methods.charting import island_reversal

    data_dict: Dict[str, List[Union[str, int]]] = {
        "open": [100, 110, 115],
        "high": [110, 115, 117],
        "low": [95, 105, 110],
        "close": [108, 113, 116],
        "symbol": ["SPY", "SPY", "SPY"],
        "market_date": ["01", "02", "03"],
    }
    main_df: pd.DataFrame = pd.DataFrame(data=data_dict)

    island_reversal_detected: int = island_reversal(main_df=main_df)["island_reversal"].iloc[-1]

    assert island_reversal_detected == -1


# ----------------------------------------------------------------------------------------------------------------------
def test_pivot_points_buy_signal() -> None:
    """Test function."""
    from algo_metrics.trade_methods.charting import pivot_points

    data_dict: Dict[str, List[Union[str, int]]] = {
        "high": [110, 110, 110],
        "low": [125, 120, 125],
        "close": [108, 123, 116],
        "symbol": ["SPY", "SPY", "SPY"],
        "market_date": ["01", "02", "03"],
    }
    main_df: pd.DataFrame = pd.DataFrame(data=data_dict)

    target_columns: List[str] = [
        "pivot_point_top",
        "pivot_point_low",
        "pivot_point_volatile",
        "pivot_point_buy",
        "pivot_point_sell",
    ]
    test_result: Dict[str, int] = pivot_points(main_df=main_df)[target_columns].iloc[-1].to_dict()

    expected_result: Dict[str, int] = {
        "pivot_point_top": -1,
        "pivot_point_low": 1,
        "pivot_point_volatile": -1,
        "pivot_point_buy": 1,
        "pivot_point_sell": -1,
    }

    assert expected_result == test_result


# ----------------------------------------------------------------------------------------------------------------------
def test_pivot_points_sell_signal() -> None:
    """Test function."""
    from algo_metrics.trade_methods.charting import pivot_points

    data_dict: Dict[str, List[Union[str, int]]] = {
        "high": [110, 125, 117],
        "low": [95, 125, 110],
        "close": [108, 108, 116],
        "symbol": ["SPY", "SPY", "SPY"],
        "market_date": ["01", "02", "03"],
    }
    main_df: pd.DataFrame = pd.DataFrame(data=data_dict)

    target_columns: List[str] = [
        "pivot_point_top",
        "pivot_point_low",
        "pivot_point_volatile",
        "pivot_point_buy",
        "pivot_point_sell",
    ]
    test_result: Dict[str, int] = pivot_points(main_df=main_df)[target_columns].iloc[-1].to_dict()

    expected_result: Dict[str, int] = {
        "pivot_point_top": 1,
        "pivot_point_low": -1,
        "pivot_point_volatile": -1,
        "pivot_point_buy": -1,
        "pivot_point_sell": 1,
    }

    assert expected_result == test_result


# ----------------------------------------------------------------------------------------------------------------------
def test_pivot_points_null() -> None:
    """Test function."""
    from algo_metrics.trade_methods.charting import pivot_points

    data_dict: Dict[str, List[Union[str, int]]] = {
        "high": [110, 110, 110],
        "low": [125, 125, 125],
        "close": [108, 123, 116],
        "symbol": ["SPY", "SPY", "SPY"],
        "market_date": ["01", "02", "03"],
    }
    main_df: pd.DataFrame = pd.DataFrame(data=data_dict)

    target_columns: List[str] = [
        "pivot_point_top",
        "pivot_point_low",
        "pivot_point_volatile",
        "pivot_point_buy",
        "pivot_point_sell",
    ]
    test_result: Dict[str, int] = pivot_points(main_df=main_df)[target_columns].iloc[-1].to_dict()

    expected_result: Dict[str, int] = {
        "pivot_point_top": -1,
        "pivot_point_low": -1,
        "pivot_point_volatile": -1,
        "pivot_point_buy": -1,
        "pivot_point_sell": -1,
    }

    assert expected_result == test_result


# ----------------------------------------------------------------------------------------------------------------------
def test_pivot_points_top() -> None:
    """Test function."""
    from algo_metrics.trade_methods.charting import pivot_points

    data_dict: Dict[str, List[Union[str, int]]] = {
        "high": [110, 125, 117],
        "low": [95, 125, 110],
        "close": [108, 123, 116],
        "symbol": ["SPY", "SPY", "SPY"],
        "market_date": ["01", "02", "03"],
    }
    main_df: pd.DataFrame = pd.DataFrame(data=data_dict)

    target_columns: List[str] = [
        "pivot_point_top",
        "pivot_point_low",
        "pivot_point_volatile",
        "pivot_point_buy",
        "pivot_point_sell",
    ]
    test_result: Dict[str, int] = pivot_points(main_df=main_df)[target_columns].iloc[-1].to_dict()

    expected_result: Dict[str, int] = {
        "pivot_point_top": 1,
        "pivot_point_low": -1,
        "pivot_point_volatile": -1,
        "pivot_point_buy": -1,
        "pivot_point_sell": -1,
    }

    assert expected_result == test_result


# ----------------------------------------------------------------------------------------------------------------------
def test_pivot_points_volatile() -> None:
    """Test function."""
    from algo_metrics.trade_methods.charting import pivot_points

    data_dict: Dict[str, List[Union[str, int]]] = {
        "high": [110, 125, 110],
        "low": [125, 120, 125],
        "close": [108, 108, 116],
        "symbol": ["SPY", "SPY", "SPY"],
        "market_date": ["01", "02", "03"],
    }
    main_df: pd.DataFrame = pd.DataFrame(data=data_dict)

    target_columns: List[str] = [
        "pivot_point_top",
        "pivot_point_low",
        "pivot_point_volatile",
        "pivot_point_buy",
        "pivot_point_sell",
    ]
    test_result: Dict[str, int] = pivot_points(main_df=main_df)[target_columns].iloc[-1].to_dict()

    expected_result: Dict[str, int] = {
        "pivot_point_top": 1,
        "pivot_point_low": 1,
        "pivot_point_volatile": 1,
        "pivot_point_buy": -1,
        "pivot_point_sell": -1,
    }

    assert expected_result == test_result


# ----------------------------------------------------------------------------------------------------------------------
def test_reversal_day_downward() -> None:
    """Test function."""
    from algo_metrics.trade_methods.charting import reversal_day

    data_dict: Dict[str, List[Union[str, int]]] = {
        "high": [110, 125],
        "low": [100, 100],
        "close": [108, 100],
        "symbol": ["SPY", "SPY"],
        "market_date": ["01", "02"],
    }
    main_df: pd.DataFrame = pd.DataFrame(data=data_dict)
    test_result: int = reversal_day(main_df=main_df)["reversal_day"].iloc[-1]

    expected_result: int = -1
    assert expected_result == test_result


# ----------------------------------------------------------------------------------------------------------------------
def test_reversal_day_upward() -> None:
    """Test function."""
    from algo_metrics.trade_methods.charting import reversal_day

    data_dict: Dict[str, List[Union[str, int]]] = {
        "high": [100, 100],
        "low": [100, 90],
        "close": [108, 125],
        "symbol": ["SPY", "SPY"],
        "market_date": ["01", "02"],
    }
    main_df: pd.DataFrame = pd.DataFrame(data=data_dict)
    test_result: int = reversal_day(main_df=main_df)["reversal_day"].iloc[-1]

    expected_result: int = 1
    assert expected_result == test_result


# ----------------------------------------------------------------------------------------------------------------------
def test_reversal_day_none() -> None:
    """Test function."""
    from algo_metrics.trade_methods.charting import reversal_day

    data_dict: Dict[str, List[Union[str, int]]] = {
        "high": [110, 125],
        "low": [100, 100],
        "close": [108, 125],
        "symbol": ["SPY", "SPY"],
        "market_date": ["01", "02"],
    }
    main_df: pd.DataFrame = pd.DataFrame(data=data_dict)
    test_result: int = reversal_day(main_df=main_df)["reversal_day"].iloc[-1]

    expected_result: int = 0
    assert expected_result == test_result


# ----------------------------------------------------------------------------------------------------------------------
def test_key_reversal_down() -> None:
    """Test function."""
    from algo_metrics.trade_methods.charting import key_reversal_down

    file_name: str = r"key_reversal_down.csv"

    data_dict: Dict[str, List[Union[str, int]]] = {
        "close": [100, 102, 105, 107, 110, 108, 106, 104],
        "high": [101, 103, 106, 108, 111, 112, 113, 114],
        "low": [99, 101, 104, 106, 109, 107, 105, 103],
        "symbol": ["SPY", "SPY", "SPY", "SPY", "SPY", "SPY", "SPY", "SPY"],
        "market_date": ["01", "02", "03", "04", "05", "06", "07", "08"],
    }
    main_df: pd.DataFrame = pd.DataFrame(data=data_dict)

    test_result: pd.DataFrame = key_reversal_down(main_df=main_df)
    test_result: pd.DataFrame = test_result.drop(columns=["symbol", "market_date"])

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_key_reversal_up() -> None:
    """Test function."""
    from algo_metrics.trade_methods.charting import key_reversal_up

    file_name: str = r"key_reversal_up.csv"

    data_dict: Dict[str, List[Union[str, int]]] = {
        "close": [106, 105, 104, 103, 104, 101, 100, 101],
        "high": [101, 103, 106, 108, 111, 112, 113, 114],
        "low": [110, 109, 108, 107, 106, 105, 104, 103],
        "symbol": ["SPY", "SPY", "SPY", "SPY", "SPY", "SPY", "SPY", "SPY"],
        "market_date": ["01", "02", "03", "04", "05", "06", "07", "08"],
    }
    main_df: pd.DataFrame = pd.DataFrame(data=data_dict)

    test_result: pd.DataFrame = key_reversal_up(main_df=main_df)
    test_result: pd.DataFrame = test_result.drop(columns=["symbol", "market_date"])

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_wide_range_day() -> None:
    """Test function."""
    from algo_metrics.trade_methods.charting import wide_range_day

    file_name: str = r"wide_range_day.csv"

    data_dict: Dict[str, List[Union[str, int]]] = {
        "high": [10, 11, 10, 12, 13, 15, 14, 20, 12, 13, 18, 17, 16, 17, 18],
        "low": [9, 9, 9, 10, 11, 12, 12, 11, 5, 11, 13, 3, 15, 16, 16],
        "symbol": ["S", "S", "S", "S", "S", "S", "S", "S", "S", "S", "S", "S", "S", "S", "S"],
        "market_date": ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15"],
    }
    main_df: pd.DataFrame = pd.DataFrame(data=data_dict)

    test_result: pd.DataFrame = wide_range_day(main_df=main_df)
    test_result: pd.DataFrame = test_result.drop(columns=["symbol", "market_date"])

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_inside_outside_day() -> None:
    """Test function."""
    from algo_metrics.trade_methods.charting import inside_outside_day

    file_name: str = r"inside_outside_day.csv"

    data_dict: Dict[str, List[Union[str, int]]] = {
        "high": [110, 125, 120],
        "low": [100, 90, 105],
        "symbol": ["SPY", "SPY", "SPY"],
        "market_date": ["01", "02", "03"],
    }
    main_df: pd.DataFrame = pd.DataFrame(data=data_dict)

    test_result: pd.DataFrame = inside_outside_day(main_df=main_df)
    test_result: pd.DataFrame = test_result.drop(columns=["symbol", "market_date"])

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_candlestick_signals() -> None:
    """Test the `candlestick_signals` Calculation."""
    from algo_metrics.trade_methods.charting import candlestick_signals

    file_name: str = r"candlestick_signals.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = candlestick_signals(main_df=test_df)

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)
