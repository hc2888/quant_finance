"""Python Test(s) for `dags/tech_analysis/trend.py`."""

import pandas as pd
from global_utils.develop_secrets import QUANT_PROJ_PATH

# ----------------------------------------------------------------------------------------------------------------------
FILE_PATH: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis"
TARGET_SUBFOLDER: str = "trend"
DATA_FOLDER: str = rf"{FILE_PATH}/data/{TARGET_SUBFOLDER}"
EXPECTED_FOLDER: str = rf"{FILE_PATH}/expected_results/{TARGET_SUBFOLDER}"


# ----------------------------------------------------------------------------------------------------------------------
def test_aroon_indicator() -> None:
    """Test the `aroon_indicator` Calculation."""
    from algo_metrics.tech_analysis.trend import aroon_indicator

    file_name: str = r"aroon_indicator.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = aroon_indicator(main_df=test_df)
    test_result: pd.DataFrame = test_result.drop(columns=["symbol", "market_date"])

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_aroon_indicator_signal() -> None:
    """Test the `aroon_indicator` Calculation."""
    from algo_metrics.tech_analysis.trend import aroon_indicator

    file_name: str = r"aroon_indicator_signal.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = aroon_indicator(main_df=test_df, output_signal=True)

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_adx_indicator() -> None:
    """Test the Average Directional Index (ADX) calculation."""
    from algo_metrics.tech_analysis.trend import adx_indicator

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-adx.csv"
    target_col: str = "ADX"
    test_col: str = "adx"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = adx_indicator(
        main_df=main_df,
        high_column="High",
        low_column="Low",
        close_column="Close",
        window=14,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(left=main_results, right=test_results, check_names=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_adx_indicator_signal() -> None:
    """Test the `adx_indicator` Calculation."""
    from algo_metrics.tech_analysis.trend import adx_indicator

    file_name: str = r"adx_indicator.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = adx_indicator(main_df=test_df, output_signal=True)

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_adx_indicator_pos() -> None:
    """Test the +DI (Positive Directional Indicator) calculation."""
    from algo_metrics.tech_analysis.trend import adx_indicator

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-adx.csv"
    target_col: str = "+DI14"
    test_col: str = "adx_pos"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = adx_indicator(
        main_df=main_df,
        high_column="High",
        low_column="Low",
        close_column="Close",
        window=14,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(left=main_results, right=test_results, check_names=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_adx_indicator_neg() -> None:
    """Test the -DI (Negative Directional Indicator) calculation."""
    from algo_metrics.tech_analysis.trend import adx_indicator

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-adx.csv"
    target_col: str = "-DI14"
    test_col: str = "adx_neg"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = adx_indicator(
        main_df=main_df,
        high_column="High",
        low_column="Low",
        close_column="Close",
        window=14,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(left=main_results, right=test_results, check_names=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_macd_indicator() -> None:
    """Test the MACD line calculation."""
    from algo_metrics.tech_analysis.trend import macd_indicator

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-macd.csv"
    target_col: str = "MACD_line"
    test_col: str = "MACD_12_26"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = macd_indicator(
        main_df=main_df,
        close_column="Close",
        window_slow=26,
        window_fast=12,
        window_sign=9,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(left=main_results, right=test_results, check_names=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_macd_indicator_sign() -> None:
    """Test the MACD signal line calculation."""
    from algo_metrics.tech_analysis.trend import macd_indicator

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-macd.csv"
    target_col: str = "MACD_signal"
    test_col: str = "MACD_sign_12_26"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = macd_indicator(
        main_df=main_df,
        close_column="Close",
        window_slow=26,
        window_fast=12,
        window_sign=9,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(left=main_results, right=test_results, check_names=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_macd_indicator_diff() -> None:
    """Test the MACD diff calculation."""
    from algo_metrics.tech_analysis.trend import macd_indicator

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-macd.csv"
    target_col: str = "MACD_diff"
    test_col: str = "MACD_diff_12_26"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = macd_indicator(
        main_df=main_df,
        close_column="Close",
        window_slow=26,
        window_fast=12,
        window_sign=9,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(left=main_results, right=test_results, check_names=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_macd_indicator_signal() -> None:
    """Test the `macd_indicator` Calculation."""
    from algo_metrics.tech_analysis.trend import macd_indicator

    file_name: str = r"macd_indicator.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = macd_indicator(main_df=test_df, output_signal=True)

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_expon_mov_avg() -> None:
    """Test the `expon_mov_avg` Calculation."""
    from algo_metrics.tech_analysis.trend import expon_mov_avg

    file_name: str = r"expon_mov_avg.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = expon_mov_avg(main_df=test_df)
    test_result: pd.DataFrame = test_result.drop(columns=["symbol", "market_date"])

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_wma_indicator() -> None:
    """Test the Weighted Moving Average (WMA) calculation."""
    from algo_metrics.tech_analysis.trend import wma_indicator

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-wma.csv"
    target_col: str = "WMA"
    test_col: str = "wma_9"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = wma_indicator(
        main_df=main_df,
        close_column="Close",
        window=9,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(left=main_results, right=test_results, check_names=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_mass_index() -> None:
    """Test the `mass_index` Calculation."""
    from algo_metrics.tech_analysis.trend import mass_index

    file_name: str = r"mass_index.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = mass_index(main_df=test_df)
    test_result: pd.DataFrame = test_result.drop(columns=["symbol", "market_date"])

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_ichimoku_indicator() -> None:
    """Test the `ichimoku_indicator` Calculation."""
    from algo_metrics.tech_analysis.trend import ichimoku_indicator

    file_name: str = r"ichimoku_indicator.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = ichimoku_indicator(main_df=test_df)
    test_result: pd.DataFrame = test_result.drop(columns=["symbol", "market_date"])

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_ichimoku_indicator_signal() -> None:
    """Test the `ichimoku_indicator` Calculation."""
    from algo_metrics.tech_analysis.trend import ichimoku_indicator

    file_name: str = r"ichimoku_indicator_signal.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = ichimoku_indicator(main_df=test_df, output_signal=True)

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_kst_indicator() -> None:
    """Test the `kst_indicator` Calculation."""
    from algo_metrics.tech_analysis.trend import kst_indicator

    file_name: str = r"kst_indicator.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = kst_indicator(main_df=test_df)
    test_result: pd.DataFrame = test_result.drop(columns=["symbol", "market_date"])

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_kst_indicator_signal() -> None:
    """Test the `kst_indicator` Calculation."""
    from algo_metrics.tech_analysis.trend import kst_indicator

    file_name: str = r"kst_indicator_signal.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = kst_indicator(main_df=test_df, output_signal=True)

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_dpo_indicator() -> None:
    """Test the `dpo_indicator` Calculation."""
    from algo_metrics.tech_analysis.trend import dpo_indicator

    file_name: str = r"dpo_indicator.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = dpo_indicator(main_df=test_df)
    test_result: pd.DataFrame = test_result.drop(columns=["symbol", "market_date"])

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_dpo_indicator_signal() -> None:
    """Test the `dpo_indicator` Calculation."""
    from algo_metrics.tech_analysis.trend import dpo_indicator

    file_name: str = r"dpo_indicator_signal.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = dpo_indicator(main_df=test_df, output_signal=True)

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_cci_indicator() -> None:
    """Test the Commodity Channel Index (CCI) calculation."""
    from algo_metrics.tech_analysis.trend import cci_indicator

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-cci.csv"
    target_col: str = "CCI"
    test_col: str = "cci"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = cci_indicator(
        main_df=main_df,
        high_column="High",
        low_column="Low",
        close_column="Close",
        window=20,
        constant=0.015,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(left=main_results, right=test_results, check_names=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_cci_indicator_signal() -> None:
    """Test the `cci_indicator` Calculation."""
    from algo_metrics.tech_analysis.trend import cci_indicator

    file_name: str = r"cci_indicator.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = cci_indicator(main_df=test_df, output_signal=True)

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_vortex_indicator_pos() -> None:
    """Test the Vortex positive indicator calculation."""
    from algo_metrics.tech_analysis.trend import vortex_indicator

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-vortex.csv"
    target_col: str = "+VI14"
    test_col: str = "vip"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = vortex_indicator(
        main_df=main_df,
        high_column="High",
        low_column="Low",
        close_column="Close",
        window=14,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(left=main_results, right=test_results, check_names=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_vortex_indicator_neg() -> None:
    """Test the Vortex negative indicator calculation."""
    from algo_metrics.tech_analysis.trend import vortex_indicator

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-vortex.csv"
    target_col: str = "-VI14"
    test_col: str = "vin"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = vortex_indicator(
        main_df=main_df,
        high_column="High",
        low_column="Low",
        close_column="Close",
        window=14,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(left=main_results, right=test_results, check_names=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_vortex_indicator_signal() -> None:
    """Test the `vortex_indicator` Calculation."""
    from algo_metrics.tech_analysis.trend import vortex_indicator

    file_name: str = r"vortex_indicator.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = vortex_indicator(main_df=test_df, output_signal=True)

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_psar_indicator_up() -> None:
    """Test the PSAR up calculation."""
    from algo_metrics.tech_analysis.trend import psar_indicator

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-psar.csv"
    target_col: str = "psar_up"
    test_col: str = "psar_up"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = psar_indicator(
        main_df=main_df,
        high_column="High",
        low_column="Low",
        close_column="Close",
        step=0.02,
        max_step=0.20,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(left=main_results, right=test_results, check_names=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_psar_indicator_down() -> None:
    """Test the PSAR down calculation."""
    from algo_metrics.tech_analysis.trend import psar_indicator

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-psar.csv"
    target_col: str = "psar_down"
    test_col: str = "psar_down"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = psar_indicator(
        main_df=main_df,
        high_column="High",
        low_column="Low",
        close_column="Close",
        step=0.02,
        max_step=0.20,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(left=main_results, right=test_results, check_names=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_psar_indicator_up_ind_column() -> None:
    """Test the PSAR up indicator calculation."""
    from algo_metrics.tech_analysis.trend import psar_indicator

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-psar.csv"
    target_col: str = "psar_up_ind"
    test_col: str = "psar_up_indicator"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = psar_indicator(
        main_df=main_df,
        high_column="High",
        low_column="Low",
        close_column="Close",
        step=0.02,
        max_step=0.20,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(left=main_results, right=test_results, check_names=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_psar_indicator_down_ind_column() -> None:
    """Test the PSAR down indicator calculation."""
    from algo_metrics.tech_analysis.trend import psar_indicator

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-psar.csv"
    target_col: str = "psar_down_ind"
    test_col: str = "psar_down_indicator"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = psar_indicator(
        main_df=main_df,
        high_column="High",
        low_column="Low",
        close_column="Close",
        step=0.02,
        max_step=0.20,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(left=main_results, right=test_results, check_names=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_psar_indicator_signal() -> None:
    """Test the `psar_indicator` Calculation."""
    from algo_metrics.tech_analysis.trend import psar_indicator

    file_name: str = r"psar_indicator.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = psar_indicator(main_df=test_df, output_signal=True)

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_stc_indicator() -> None:
    """Test the Schaff Trend Cycle (STC) calculation."""
    from algo_metrics.tech_analysis.trend import stc_indicator

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-stc.csv"
    target_col: str = "stc"
    test_col: str = "stc"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = stc_indicator(
        main_df=main_df,
        close_column="Close",
        window_slow=50,
        window_fast=23,
        cycle=10,
        smooth_1=3,
        smooth_2=3,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(left=main_results, right=test_results, check_names=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_stc_indicator_signal() -> None:
    """Test the `stc_indicator` Calculation."""
    from algo_metrics.tech_analysis.trend import stc_indicator

    file_name: str = r"stc_indicator.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = stc_indicator(main_df=test_df, output_signal=True)

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)
