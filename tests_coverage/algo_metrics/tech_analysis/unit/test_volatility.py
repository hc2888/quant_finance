"""Python Test(s) for `dags/tech_analysis/volatility.py`."""

import pandas as pd
from global_utils.develop_secrets import QUANT_PROJ_PATH

# ----------------------------------------------------------------------------------------------------------------------
FILE_PATH: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis"
TARGET_SUBFOLDER: str = "volatility"
DATA_FOLDER: str = rf"{FILE_PATH}/data/{TARGET_SUBFOLDER}"
EXPECTED_FOLDER: str = rf"{FILE_PATH}/expected_results/{TARGET_SUBFOLDER}"


# ----------------------------------------------------------------------------------------------------------------------
def test_annualized_volatility() -> None:
    """Test the `annualized_volatility` Calculation."""
    from algo_metrics.tech_analysis.volatility import annualized_volatility

    file_name: str = r"annualized_volatility.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = annualized_volatility(main_df=test_df)
    test_result: pd.DataFrame = test_result.drop(columns=["symbol"])

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_average_true_range_1() -> None:
    """Test the Average True Range (ATR) calculation using the function."""
    from algo_metrics.tech_analysis.volatility import average_true_range

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-atr.csv"
    target_col: str = "ATR"
    test_col: str = "atr"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = average_true_range(
        main_df=main_df,
        high_column="High",
        low_column="Low",
        close_column="Close",
        window=14,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(left=main_results, right=test_results, check_names=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_average_true_range_2() -> None:
    """Test the Average True Range (ATR) calculation using the indicator method with different parameters."""
    from algo_metrics.tech_analysis.volatility import average_true_range

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-atr2.csv"
    target_col: str = "ATR"
    test_col: str = "atr"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = average_true_range(
        main_df=main_df,
        high_column="High",
        low_column="Low",
        close_column="Close",
        window=10,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(left=main_results, right=test_results, check_names=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_bollinger_bands_mavg() -> None:
    """Test the Bollinger Bands middle average calculation using the indicator method."""
    from algo_metrics.tech_analysis.volatility import bollinger_bands

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-bbands.csv"
    target_col: str = "MiddleBand"
    test_col: str = "mavg"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = bollinger_bands(
        main_df=main_df,
        close_column="Close",
        window=20,
        window_dev=2,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(left=main_results, right=test_results, check_names=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_bollinger_bands_hband() -> None:
    """Test the Bollinger Bands high band calculation using the indicator method."""
    from algo_metrics.tech_analysis.volatility import bollinger_bands

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-bbands.csv"
    target_col: str = "HighBand"
    test_col: str = "hband"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = bollinger_bands(
        main_df=main_df,
        close_column="Close",
        window=20,
        window_dev=2,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(left=main_results, right=test_results, check_names=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_bollinger_bands_lband() -> None:
    """Test the Bollinger Bands low band calculation using the indicator method."""
    from algo_metrics.tech_analysis.volatility import bollinger_bands

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-bbands.csv"
    target_col: str = "LowBand"
    test_col: str = "lband"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = bollinger_bands(
        main_df=main_df,
        close_column="Close",
        window=20,
        window_dev=2,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(left=main_results, right=test_results, check_names=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_bollinger_bands_wband() -> None:
    """Test the Bollinger Bands width band calculation using the indicator method."""
    from algo_metrics.tech_analysis.volatility import bollinger_bands

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-bbands.csv"
    target_col: str = "WidthBand"
    test_col: str = "wband"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = bollinger_bands(
        main_df=main_df,
        close_column="Close",
        window=20,
        window_dev=2,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(left=main_results, right=test_results, check_names=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_bollinger_bands_pband() -> None:
    """Test the Bollinger Bands percentage band calculation using the indicator method."""
    from algo_metrics.tech_analysis.volatility import bollinger_bands

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-bbands.csv"
    target_col: str = "PercentageBand"
    test_col: str = "pband"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = bollinger_bands(
        main_df=main_df,
        close_column="Close",
        window=20,
        window_dev=2,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(left=main_results, right=test_results, check_names=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_bollinger_bands_hband_indicator() -> None:
    """Test the Bollinger Bands high band indicator calculation using the indicator method."""
    from algo_metrics.tech_analysis.volatility import bollinger_bands

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-bbands.csv"
    target_col: str = "CrossUp"
    test_col: str = "hband_indicator"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = bollinger_bands(
        main_df=main_df,
        close_column="Close",
        window=20,
        window_dev=2,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(left=main_results, right=test_results, check_names=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_bollinger_bands_lband_indicator() -> None:
    """Test the Bollinger Bands low band indicator calculation using the indicator method."""
    from algo_metrics.tech_analysis.volatility import bollinger_bands

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-bbands.csv"
    target_col: str = "CrossDown"
    test_col: str = "lband_indicator"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = bollinger_bands(
        main_df=main_df,
        close_column="Close",
        window=20,
        window_dev=2,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(left=main_results, right=test_results, check_names=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_bollinger_bands_signal() -> None:
    """Test the `bollinger_bands` Calculation."""
    from algo_metrics.tech_analysis.volatility import bollinger_bands

    file_name: str = r"bollinger_bands.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = bollinger_bands(main_df=test_df, output_signal=True)

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_keltner_channel_mavg() -> None:
    """Test the Keltner Channel middle band calculation using the indicator method."""
    from algo_metrics.tech_analysis.volatility import keltner_channel

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-kc.csv"
    target_col: str = "middle_band"
    test_col: str = "kc_mband"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = keltner_channel(
        main_df=main_df,
        high_column="High",
        low_column="Low",
        close_column="Close",
        window=20,
        window_atr=10,
        multiplier=2,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(left=main_results, right=test_results, check_names=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_keltner_channel_hband() -> None:
    """Test the Keltner Channel high band calculation using the indicator method."""
    from algo_metrics.tech_analysis.volatility import keltner_channel

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-kc.csv"
    target_col: str = "upper_band"
    test_col: str = "kc_hband"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = keltner_channel(
        main_df=main_df,
        high_column="High",
        low_column="Low",
        close_column="Close",
        window=20,
        window_atr=10,
        multiplier=2,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(left=main_results, right=test_results, check_names=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_keltner_channel_lband() -> None:
    """Test the Keltner Channel low band calculation using the indicator method."""
    from algo_metrics.tech_analysis.volatility import keltner_channel

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-kc.csv"
    target_col: str = "lower_band"
    test_col: str = "kc_lband"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = keltner_channel(
        main_df=main_df,
        high_column="High",
        low_column="Low",
        close_column="Close",
        window=20,
        window_atr=10,
        multiplier=2,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(left=main_results, right=test_results, check_names=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_keltner_channel_wband() -> None:
    """Test the Keltner Channel width band calculation using the indicator method."""
    from algo_metrics.tech_analysis.volatility import keltner_channel

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-kc.csv"
    target_col: str = "kc_band_width"
    test_col: str = "kc_wband"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = keltner_channel(
        main_df=main_df,
        high_column="High",
        low_column="Low",
        close_column="Close",
        window=20,
        window_atr=10,
        multiplier=2,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(left=main_results, right=test_results, check_names=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_keltner_channel_pband() -> None:
    """Test the Keltner Channel percentage band calculation using the indicator method."""
    from algo_metrics.tech_analysis.volatility import keltner_channel

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-kc.csv"
    target_col: str = "kc_percentage"
    test_col: str = "kc_pband"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = keltner_channel(
        main_df=main_df,
        high_column="High",
        low_column="Low",
        close_column="Close",
        window=20,
        window_atr=10,
        multiplier=2,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(left=main_results, right=test_results, check_names=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_keltner_channel_hband_indicator() -> None:
    """Test the Keltner Channel high band indicator calculation using the indicator method."""
    from algo_metrics.tech_analysis.volatility import keltner_channel

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-kc.csv"
    target_col: str = "kc_high_indicator"
    test_col: str = "kc_hband_indicator"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = keltner_channel(
        main_df=main_df,
        high_column="High",
        low_column="Low",
        close_column="Close",
        window=20,
        window_atr=10,
        multiplier=2,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(left=main_results, right=test_results, check_names=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_keltner_channel_lband_indicator() -> None:
    """Test the Keltner Channel low band indicator calculation using the indicator method."""
    from algo_metrics.tech_analysis.volatility import keltner_channel

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-kc.csv"
    target_col: str = "kc_low_indicator"
    test_col: str = "kc_lband_indicator"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = keltner_channel(
        main_df=main_df,
        high_column="High",
        low_column="Low",
        close_column="Close",
        window=20,
        window_atr=10,
        multiplier=2,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(left=main_results, right=test_results, check_names=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_keltner_channel_signal() -> None:
    """Test the `keltner_channel` Calculation."""
    from algo_metrics.tech_analysis.volatility import keltner_channel

    file_name: str = r"keltner_channel.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = keltner_channel(main_df=test_df, output_signal=True)

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_donchian_channel_1_mavg() -> None:
    """Test the Donchian Channel middle band calculation using the indicator method."""
    from algo_metrics.tech_analysis.volatility import donchian_channel

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-dc.csv"
    target_col: str = "middle_band"
    test_col: str = "dcmband"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = donchian_channel(
        main_df=main_df,
        high_column="high",
        low_column="low",
        close_column="close",
        window=20,
        offset=0,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(left=main_results, right=test_results, check_names=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_donchian_channel_1_hband() -> None:
    """Test the Donchian Channel middle band calculation using the indicator method."""
    from algo_metrics.tech_analysis.volatility import donchian_channel

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-dc.csv"
    target_col: str = "upper_band"
    test_col: str = "dchband"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = donchian_channel(
        main_df=main_df,
        high_column="high",
        low_column="low",
        close_column="close",
        window=20,
        offset=0,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(left=main_results, right=test_results, check_names=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_donchian_channel_1_lband() -> None:
    """Test the Donchian Channel low band calculation using the indicator method."""
    from algo_metrics.tech_analysis.volatility import donchian_channel

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-dc.csv"
    target_col: str = "lower_band"
    test_col: str = "dclband"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = donchian_channel(
        main_df=main_df,
        high_column="high",
        low_column="low",
        close_column="close",
        window=20,
        offset=0,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(left=main_results, right=test_results, check_names=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_donchian_channel_1_wband() -> None:
    """Test the Donchian Channel low band calculation using the indicator method."""
    from algo_metrics.tech_analysis.volatility import donchian_channel

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-dc.csv"
    target_col: str = "dc_band_width"
    test_col: str = "dcwband"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = donchian_channel(
        main_df=main_df,
        high_column="high",
        low_column="low",
        close_column="close",
        window=20,
        offset=0,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(left=main_results, right=test_results, check_names=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_donchian_channel_1_pband() -> None:
    """Test the Donchian Channel low band calculation using the indicator method."""
    from algo_metrics.tech_analysis.volatility import donchian_channel

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-dc.csv"
    target_col: str = "dc_percentage"
    test_col: str = "dcpband"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = donchian_channel(
        main_df=main_df,
        high_column="high",
        low_column="low",
        close_column="close",
        window=20,
        offset=0,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(left=main_results, right=test_results, check_names=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_donchian_channel_2_mavg() -> None:
    """Test the Donchian Channel middle band calculation using the indicator method."""
    from algo_metrics.tech_analysis.volatility import donchian_channel

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-dc2.csv"
    target_col: str = "middle_band"
    test_col: str = "dcmband"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = donchian_channel(
        main_df=main_df,
        high_column="high",
        low_column="low",
        close_column="close",
        window=20,
        offset=1,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(
        left=main_results,
        right=test_results,
        check_names=False,
    )


# ----------------------------------------------------------------------------------------------------------------------
def test_donchian_channel_2_hband() -> None:
    """Test the Donchian Channel middle band calculation using the indicator method."""
    from algo_metrics.tech_analysis.volatility import donchian_channel

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-dc2.csv"
    target_col: str = "upper_band"
    test_col: str = "dchband"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = donchian_channel(
        main_df=main_df,
        high_column="high",
        low_column="low",
        close_column="close",
        window=20,
        offset=1,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(left=main_results, right=test_results, check_names=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_donchian_channel_2_lband() -> None:
    """Test the Donchian Channel low band calculation using the indicator method."""
    from algo_metrics.tech_analysis.volatility import donchian_channel

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-dc2.csv"
    target_col: str = "lower_band"
    test_col: str = "dclband"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = donchian_channel(
        main_df=main_df,
        high_column="high",
        low_column="low",
        close_column="close",
        window=20,
        offset=1,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(left=main_results, right=test_results, check_names=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_donchian_channel_2_wband() -> None:
    """Test the Donchian Channel low band calculation using the indicator method."""
    from algo_metrics.tech_analysis.volatility import donchian_channel

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-dc2.csv"
    target_col: str = "dc_band_width"
    test_col: str = "dcwband"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = donchian_channel(
        main_df=main_df,
        high_column="high",
        low_column="low",
        close_column="close",
        window=20,
        offset=1,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(left=main_results, right=test_results, check_names=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_donchian_channel_2_pband() -> None:
    """Test the Donchian Channel low band calculation using the indicator method."""
    from algo_metrics.tech_analysis.volatility import donchian_channel

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-dc2.csv"
    target_col: str = "dc_percentage"
    test_col: str = "dcpband"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = donchian_channel(
        main_df=main_df,
        high_column="high",
        low_column="low",
        close_column="close",
        window=20,
        offset=1,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(left=main_results, right=test_results, check_names=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_donchian_channel_signal() -> None:
    """Test the `donchian_channel` Calculation."""
    from algo_metrics.tech_analysis.volatility import donchian_channel

    file_name: str = r"donchian_channel.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = donchian_channel(main_df=test_df, output_signal=True)

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_ulcer_index_indicator() -> None:
    """Test the Ulcer Index calculation using the indicator method."""
    from algo_metrics.tech_analysis.volatility import ulcer_index_indicator

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-ui.csv"
    target_col: str = "ulcer_index"
    test_col: str = "ulcer_index"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = ulcer_index_indicator(
        main_df=main_df,
        close_column="Close",
        window=14,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(left=main_results, right=test_results, check_names=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_ulcer_index_indicator_signal() -> None:
    """Test the `ulcer_index_indicator` Calculation."""
    from algo_metrics.tech_analysis.volatility import ulcer_index_indicator

    file_name: str = r"ulcer_index_indicator.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = ulcer_index_indicator(main_df=test_df, output_signal=True)

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_bookstaber_volatility() -> None:
    """Test the `bookstaber_volatility` Calculation."""
    from algo_metrics.tech_analysis.volatility import bookstaber_volatility

    file_name: str = r"bookstaber_volatility.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = bookstaber_volatility(main_df=test_df)

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_bookstaber_volatility_signal() -> None:
    """Test the `bookstaber_volatility` Calculation."""
    from algo_metrics.tech_analysis.volatility import bookstaber_volatility

    file_name: str = r"bookstaber_volatility_signal.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = bookstaber_volatility(main_df=test_df, output_signal=True)

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_relative_volatility() -> None:
    """Test the `relative_volatility` Calculation."""
    from algo_metrics.tech_analysis.volatility import relative_volatility

    file_name: str = r"relative_volatility.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = relative_volatility(main_df=test_df)

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)
