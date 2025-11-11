"""Python Test(s) for `dags/tech_analysis/volume.py`."""

import pandas as pd
from global_utils.develop_secrets import QUANT_PROJ_PATH

# ----------------------------------------------------------------------------------------------------------------------
FILE_PATH: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis"
TARGET_SUBFOLDER: str = "volume"
DATA_FOLDER: str = rf"{FILE_PATH}/data/{TARGET_SUBFOLDER}"
EXPECTED_FOLDER: str = rf"{FILE_PATH}/expected_results/{TARGET_SUBFOLDER}"


# ----------------------------------------------------------------------------------------------------------------------
def test_acc_dist_index_indicator() -> None:
    """Test the Accumulation/Distribution Line (ADL) calculation using the function."""
    from algo_metrics.tech_analysis.volume import acc_dist_index_indicator

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-accum.csv"
    target_col: str = "ADLine"
    test_col: str = "adi"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = acc_dist_index_indicator(
        main_df=main_df,
        high_column="High",
        low_column="Low",
        close_column="Close",
        volume_column="Volume",
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(left=main_results, right=test_results, check_names=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_on_balance_volume_indicator() -> None:
    """Test the On-Balance Volume (OBV) calculation using the function."""
    from algo_metrics.tech_analysis.volume import on_balance_volume_indicator

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-obv.csv"
    target_col: str = "OBV"
    test_col: str = "obv"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = on_balance_volume_indicator(
        main_df=main_df,
        close_column="Close",
        volume_column="Volume",
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(left=main_results, right=test_results, check_names=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_chaikin_money_flow_indicator() -> None:
    """Test the `chaikin_money_flow_indicator` Calculation."""
    from algo_metrics.tech_analysis.volume import chaikin_money_flow_indicator

    file_name: str = r"chaikin_money_flow_indicator.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = chaikin_money_flow_indicator(main_df=test_df)
    test_result: pd.DataFrame = test_result.drop(columns=["symbol", "market_date"])

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_chaikin_money_flow_indicator_signal() -> None:
    """Test the `chaikin_money_flow_indicator` Calculation."""
    from algo_metrics.tech_analysis.volume import chaikin_money_flow_indicator

    file_name: str = r"chaikin_money_flow_indicator_signal.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = chaikin_money_flow_indicator(main_df=test_df, output_signal=True)

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_force_index_indicator() -> None:
    """Test the Force Index (FI) calculation using the function."""
    from algo_metrics.tech_analysis.volume import force_index_indicator

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-fi.csv"
    target_col: str = "FI"
    test_col: str = "fi_13"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = force_index_indicator(
        main_df=main_df,
        close_column="Close",
        volume_column="Volume",
        window=13,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(left=main_results, right=test_results, check_names=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_force_index_indicator_signal() -> None:
    """Test the `force_index_indicator` Calculation."""
    from algo_metrics.tech_analysis.volume import force_index_indicator

    file_name: str = r"force_index_indicator.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = force_index_indicator(main_df=test_df, output_signal=True)

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_ease_of_movement_indicator() -> None:
    """Test the Ease of Movement (EMV) calculation using the function."""
    from algo_metrics.tech_analysis.volume import ease_of_movement_indicator

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-easeofmovement.csv"
    target_col: str = "EMV"
    test_col: str = "eom_14"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = ease_of_movement_indicator(
        main_df=main_df,
        high_column="High",
        low_column="Low",
        volume_column="Volume",
        window=14,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(left=main_results, right=test_results, check_names=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_ease_of_movement_indicator_sma() -> None:
    """Test the simple moving average (SMA) of Ease of Movement (EMV) calculation using the function."""
    from algo_metrics.tech_analysis.volume import ease_of_movement_indicator

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-easeofmovement.csv"
    target_col: str = "SMA_EMV"
    test_col: str = "sma_eom_14"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = ease_of_movement_indicator(
        main_df=main_df,
        high_column="High",
        low_column="Low",
        volume_column="Volume",
        window=14,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(left=main_results, right=test_results, check_names=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_ease_of_movement_indicator_signal() -> None:
    """Test the `ease_of_movement_indicator` Calculation."""
    from algo_metrics.tech_analysis.volume import ease_of_movement_indicator

    file_name: str = r"ease_of_movement_indicator.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = ease_of_movement_indicator(main_df=test_df, output_signal=True)

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_volume_price_trend_indicator_unsmoothed() -> None:
    """Test the unsmoothed Volume Price Trend (VPT) calculation using the function."""
    from algo_metrics.tech_analysis.volume import volume_price_trend_indicator

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-vpt.csv"
    target_col: str = "unsmoothed vpt"
    test_col: str = "vpt_smooth_0"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = volume_price_trend_indicator(
        main_df=main_df,
        close_column="Close",
        volume_column="Volume",
        smoothing_factor=None,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(left=main_results, right=test_results, check_names=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_volume_price_trend_indicator_smoothed_14() -> None:
    """Test the unsmoothed Volume Price Trend (VPT) calculation using the function."""
    from algo_metrics.tech_analysis.volume import volume_price_trend_indicator

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-vpt.csv"
    target_col: str = "14-smoothed vpt"
    test_col: str = "vpt_smooth_14"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = volume_price_trend_indicator(
        main_df=main_df,
        close_column="Close",
        volume_column="Volume",
        smoothing_factor=14,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(left=main_results, right=test_results, check_names=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_negative_volume_index_indicator() -> None:
    """Test the `negative_volume_index_indicator` Calculation."""
    from algo_metrics.tech_analysis.volume import negative_volume_index_indicator

    file_name: str = r"negative_volume_index_indicator.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = negative_volume_index_indicator(main_df=test_df)
    test_result: pd.DataFrame = test_result.drop(columns=["symbol", "market_date"])

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_money_flow_index_indicator() -> None:
    """Test the Money Flow Index (MFI) calculation using the indicator method."""
    from algo_metrics.tech_analysis.volume import money_flow_index_indicator

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-mfi.csv"
    target_col: str = "MFI"
    test_col: str = "mfi_14"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = money_flow_index_indicator(
        main_df=main_df,
        high_column="High",
        low_column="Low",
        close_column="Close",
        volume_column="Volume",
        window=14,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(left=main_results, right=test_results, check_names=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_money_flow_index_indicator_signal() -> None:
    """Test the `money_flow_index_indicator` Calculation."""
    from algo_metrics.tech_analysis.volume import money_flow_index_indicator

    file_name: str = r"money_flow_index_indicator.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = money_flow_index_indicator(main_df=test_df, output_signal=True)

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_volume_weighted_average_price() -> None:
    """Test the Volume Weighted Average Price (VWAP) calculation using the function."""
    from algo_metrics.tech_analysis.volume import volume_weighted_average_price

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-vwap.csv"
    target_col: str = "vwap"
    test_col: str = "vwap_14"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = volume_weighted_average_price(
        main_df=main_df,
        high_column="High",
        low_column="Low",
        close_column="Close",
        volume_column="Volume",
        window=14,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(left=main_results, right=test_results, check_names=False)
