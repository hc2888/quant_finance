"""Python Test(s) for `dags/tech_analysis/momentum.py`."""

import pandas as pd
from global_utils.develop_secrets import QUANT_PROJ_PATH

# ----------------------------------------------------------------------------------------------------------------------
FILE_PATH: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis"
TARGET_SUBFOLDER: str = "momentum"
DATA_FOLDER: str = rf"{FILE_PATH}/data/{TARGET_SUBFOLDER}"
EXPECTED_FOLDER: str = rf"{FILE_PATH}/expected_results/{TARGET_SUBFOLDER}"


# ----------------------------------------------------------------------------------------------------------------------
def test_relative_strength_index() -> None:
    """Test the Relative Strength Index (RSI) calculation using the indicator method."""
    from algo_metrics.tech_analysis.momentum import relative_strength_index

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-rsi.csv"
    target_col: str = "RSI"
    test_col: str = "rsi"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = relative_strength_index(
        main_df=main_df,
        close_column="Close",
        window=14,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(left=main_results, right=test_results, check_names=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_relative_strength_index_signal() -> None:
    """Test the `relative_strength_index` Calculation."""
    from algo_metrics.tech_analysis.momentum import relative_strength_index

    file_name: str = r"relative_strength_index.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = relative_strength_index(main_df=test_df, output_signal=True)

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_true_strength_index() -> None:
    """Test the True Strength Index (TSI) calculation using the indicator method."""
    from algo_metrics.tech_analysis.momentum import true_strength_index

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-tsi.csv"
    target_col: str = "TSI"
    test_col: str = "tsi"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = true_strength_index(
        main_df=main_df,
        close_column="Close",
        window_slow=25,
        window_fast=13,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(
        left=main_results,
        right=test_results,
        check_names=False,
        check_exact=False,
        rtol=0.1,
        atol=0.1,
    )


# ----------------------------------------------------------------------------------------------------------------------
def test_true_strength_index_signal() -> None:
    """Test the `true_strength_index` Calculation."""
    from algo_metrics.tech_analysis.momentum import true_strength_index

    file_name: str = r"true_strength_index.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = true_strength_index(main_df=test_df, output_signal=True)

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_ultimate_oscillator() -> None:
    """Test the Ultimate Oscillator calculation using the indicator method."""
    from algo_metrics.tech_analysis.momentum import ultimate_oscillator

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-ultosc.csv"
    target_col: str = "Ult_Osc"
    test_col: str = "ultim_osc"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = ultimate_oscillator(
        main_df=main_df,
        high_column="High",
        low_column="Low",
        close_column="Close",
        window_1=7,
        window_2=14,
        window_3=28,
        weight_1=4.0,
        weight_2=2.0,
        weight_3=1.0,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(left=main_results, right=test_results, check_names=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_stochastic_oscillator() -> None:
    """Test the Stochastic Oscillator calculation using the indicator method."""
    from algo_metrics.tech_analysis.momentum import stochastic_oscillator

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-soo.csv"
    target_col: str = "SO"
    test_col: str = "stoch_k"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = stochastic_oscillator(
        main_df=main_df,
        high_column="High",
        low_column="Low",
        close_column="Close",
        window=14,
        smooth_window=3,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(left=main_results, right=test_results, check_names=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_stochastic_oscillator_signal() -> None:
    """Test the `stochastic_oscillator` Calculation."""
    from algo_metrics.tech_analysis.momentum import stochastic_oscillator

    file_name: str = r"stochastic_oscillator.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = stochastic_oscillator(main_df=test_df, output_signal=True)

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_kaufman_adapt_mov_avg_regular_smoothing() -> None:
    """Test the Kaufman's Adaptive Moving Average (KAMA) calculation using the indicator method."""
    from algo_metrics.tech_analysis.momentum import kaufman_adapt_mov_avg

    file_name: str = r"kaufman_adapt_mov_avg_regular.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = kaufman_adapt_mov_avg(main_df=test_df, dynamic_smoothing=False)

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_kaufman_adapt_mov_avg_dynamic_smoothing() -> None:
    """Test the `kaufman_adapt_mov_avg` Dynamic Smoothing Calculation."""
    from algo_metrics.tech_analysis.momentum import kaufman_adapt_mov_avg

    file_name: str = r"kaufman_adapt_mov_avg_dynamic.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = kaufman_adapt_mov_avg(main_df=test_df, dynamic_smoothing=True)

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_kaufman_adapt_signal() -> None:
    """Test the `kaufman_adapt_signal` Calculation."""
    from algo_metrics.tech_analysis.momentum import kaufman_adapt_signal

    file_name: str = r"kaufman_adapt_signal.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = kaufman_adapt_signal(main_df=test_df, dynamic_smoothing=True)
    test_result: pd.DataFrame = kaufman_adapt_signal(main_df=test_result, dynamic_smoothing=False)

    expected_data: str = rf"{EXPECTED_FOLDER}/kaufman_adapt_signal.csv"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_rate_of_change() -> None:
    """Test the Rate of Change (ROC) calculation using the function."""
    from algo_metrics.tech_analysis.momentum import rate_of_change

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-roc.csv"
    target_col: str = "ROC"
    test_col: str = "roc"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = rate_of_change(
        main_df=main_df,
        close_column="Close",
        window=12,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(left=main_results, right=test_results, check_names=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_rate_of_change_signal() -> None:
    """Test the `rate_of_change` Calculation."""
    from algo_metrics.tech_analysis.momentum import rate_of_change

    file_name: str = r"rate_of_change.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = rate_of_change(main_df=test_df, output_signal=True)

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_williams_perc_r() -> None:
    """Test the Williams %R calculation using the indicator method."""
    from algo_metrics.tech_analysis.momentum import williams_perc_r

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-percentr.csv"
    target_col: str = "Williams_%R"
    test_col: str = "will_r"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = williams_perc_r(
        main_df=main_df,
        high_column="High",
        low_column="Low",
        close_column="Close",
        lookback_period=14,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(left=main_results, right=test_results, check_names=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_williams_perc_r_signal() -> None:
    """Test the `williams_perc_r` Calculation."""
    from algo_metrics.tech_analysis.momentum import williams_perc_r

    file_name: str = r"williams_perc_r.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = williams_perc_r(main_df=test_df, output_signal=True)

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_stoch_relative_strength_index() -> None:
    """Test the Stochastic RSI calculation using the indicator method."""
    from algo_metrics.tech_analysis.momentum import stoch_relative_strength_index

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-stochrsi.csv"
    target_col: str = "StochRSI(14)"
    test_col: str = "stochrsi"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = stoch_relative_strength_index(
        main_df=main_df,
        close_column="Close",
        window=14,
        smooth_1=3,
        smooth_2=3,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(left=main_results, right=test_results, check_names=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_stoch_relative_strength_index_signal() -> None:
    """Test the `stoch_relative_strength_index` Calculation."""
    from algo_metrics.tech_analysis.momentum import stoch_relative_strength_index

    file_name: str = r"stoch_relative_strength_index.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = stoch_relative_strength_index(main_df=test_df, output_signal=True)

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_percentage_price_oscillator() -> None:
    """Test the Percentage Price Oscillator (PPO) calculation using the indicator method."""
    from algo_metrics.tech_analysis.momentum import percentage_price_oscillator

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-ppo.csv"
    target_col: str = "PPO"
    test_col: str = "PPO_12_26"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = percentage_price_oscillator(
        main_df=main_df,
        close_column="Close",
        window_slow=26,
        window_fast=12,
        window_sign=9,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(
        left=main_results,
        right=test_results,
        check_names=False,
    )


# ----------------------------------------------------------------------------------------------------------------------
def test_percentage_price_oscillator_signal() -> None:
    """Test the PPO Signal Line calculation using the indicator method."""
    from algo_metrics.tech_analysis.momentum import percentage_price_oscillator

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-ppo.csv"
    target_col: str = "PPO_Signal_Line"
    test_col: str = "PPO_sign_12_26"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = percentage_price_oscillator(
        main_df=main_df,
        close_column="Close",
        window_slow=26,
        window_fast=12,
        window_sign=9,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(
        left=main_results,
        right=test_results,
        check_names=False,
    )


# ----------------------------------------------------------------------------------------------------------------------
def test_percentage_price_oscillator_hist() -> None:
    """Test the PPO Histogram calculation using the indicator method."""
    from algo_metrics.tech_analysis.momentum import percentage_price_oscillator

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-ppo.csv"
    target_col: str = "PPO_Histogram"
    test_col: str = "PPO_hist_12_26"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = percentage_price_oscillator(
        main_df=main_df,
        close_column="Close",
        window_slow=26,
        window_fast=12,
        window_sign=9,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(
        left=main_results,
        right=test_results,
        check_names=False,
    )


# ----------------------------------------------------------------------------------------------------------------------
def test_percentage_price_oscillator_binary() -> None:
    """Test the `percentage_price_oscillator` Calculation."""
    from algo_metrics.tech_analysis.momentum import percentage_price_oscillator

    file_name: str = r"percentage_price_oscillator.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = percentage_price_oscillator(main_df=test_df, output_signal=True)

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_percentage_volume_oscillator() -> None:
    """Test the Percentage Volume Oscillator (PVO) calculation using the indicator method."""
    from algo_metrics.tech_analysis.momentum import percentage_volume_oscillator

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-pvo.csv"
    target_col: str = "PVO"
    test_col: str = "PVO_12_26"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = percentage_volume_oscillator(
        main_df=main_df,
        volume_column="Volume",
        window_slow=26,
        window_fast=12,
        window_sign=9,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(
        left=main_results,
        right=test_results,
        check_names=False,
    )


# ----------------------------------------------------------------------------------------------------------------------
def test_percentage_volume_oscillator_signal() -> None:
    """Test the PVO Signal Line calculation using the indicator method."""
    from algo_metrics.tech_analysis.momentum import percentage_volume_oscillator

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-pvo.csv"
    target_col: str = "PVO_Signal_Line"
    test_col: str = "PVO_sign_12_26"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = percentage_volume_oscillator(
        main_df=main_df,
        volume_column="Volume",
        window_slow=26,
        window_fast=12,
        window_sign=9,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(
        left=main_results,
        right=test_results,
        check_names=False,
    )


# ----------------------------------------------------------------------------------------------------------------------
def test_percentage_volume_oscillator_hist() -> None:
    """Test the PVO Histogram calculation using the indicator method."""
    from algo_metrics.tech_analysis.momentum import percentage_volume_oscillator

    filename: str = rf"{QUANT_PROJ_PATH}/tests_coverage/algo_metrics/tech_analysis/data/cs-pvo.csv"
    target_col: str = "PVO_Histogram"
    test_col: str = "PVO_hist_12_26"

    main_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=filename, sep=",")
    main_results: pd.Series = main_df[target_col].tail()

    test_df: pd.DataFrame = percentage_volume_oscillator(
        main_df=main_df,
        volume_column="Volume",
        window_slow=26,
        window_fast=12,
        window_sign=9,
    )
    test_results: pd.Series = test_df[test_col].tail()

    pd.testing.assert_series_equal(
        left=main_results,
        right=test_results,
        check_names=False,
    )


# ----------------------------------------------------------------------------------------------------------------------
def test_percentage_volume_oscillator_binary() -> None:
    """Test the `williams_perc_r` Calculation."""
    from algo_metrics.tech_analysis.momentum import percentage_volume_oscillator

    file_name: str = r"percentage_volume_oscillator.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = percentage_volume_oscillator(main_df=test_df, output_signal=True)

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_awesome_oscillator() -> None:
    """Test the `awesome_oscillator` Calculation."""
    from algo_metrics.tech_analysis.momentum import awesome_oscillator

    file_name: str = r"awesome_oscillator.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = awesome_oscillator(main_df=test_df)
    test_result: pd.DataFrame = test_result.drop(columns=["symbol", "market_date"])

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)


# ----------------------------------------------------------------------------------------------------------------------
def test_awesome_oscillator_signal() -> None:
    """Test the `awesome_oscillator_signal` Calculation."""
    from algo_metrics.tech_analysis.momentum import awesome_oscillator

    file_name: str = r"awesome_oscillator_signal.csv"

    test_data: str = rf"{DATA_FOLDER}/{file_name}"
    test_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=test_data)
    test_result: pd.DataFrame = awesome_oscillator(main_df=test_df, output_signal=True)

    expected_data: str = rf"{EXPECTED_FOLDER}/{file_name}"
    expected_result: pd.DataFrame = pd.read_csv(filepath_or_buffer=expected_data)

    pd.testing.assert_frame_equal(left=test_result, right=expected_result, check_like=True, check_dtype=False)
