"""Python Test(s) for `dags/algo_metrics/basic_calcs.py`."""

from typing import Dict, List, Union

import numpy as np
import pandas as pd
from algo_metrics.const_vars import ROUND_DECIMAL_PLACES


# ----------------------------------------------------------------------------------------------------------------------
def test_sample_error() -> None:
    """Test the sample error based on a sample DataFrame."""
    from algo_metrics.trade_methods.basic_calcs import sample_error

    data_dict: Dict[str, List[Union[str, float]]] = {
        "TargetColumn": [10, 12, 23, 23, 16, 23, 21, 16],
        "symbol": ["SPY", "SPY", "SPY", "SPY", "SPY", "SPY", "SPY", "SPY"],
        "market_date": ["01", "02", "03", "04", "05", "06", "07", "08"],
    }
    main_df: pd.DataFrame = pd.DataFrame(data=data_dict)

    main_result: float = 0.35  # Expected sample error value (pre-calculated)
    test_result: float = sample_error(main_df=main_df, target_column="TargetColumn")

    assert main_result == test_result


# ----------------------------------------------------------------------------------------------------------------------
def test_relative_standard_error() -> None:
    """Test the Relative Standard Error (RSE) based on a sample DataFrame."""
    from algo_metrics.trade_methods.basic_calcs import relative_standard_error

    data_dict: Dict[str, List[Union[str, float]]] = {
        "TargetColumn": [10, 12, 23, 23, 16, 23, 21, 16],
        "symbol": ["SPY", "SPY", "SPY", "SPY", "SPY", "SPY", "SPY", "SPY"],
        "market_date": ["01", "02", "03", "04", "05", "06", "07", "08"],
    }
    main_df: pd.DataFrame = pd.DataFrame(data=data_dict)

    main_result: float = 10.29  # Expected RSE value (pre-calculated)
    test_result: float = relative_standard_error(main_df=main_df, target_column="TargetColumn")

    assert main_result == test_result


# ----------------------------------------------------------------------------------------------------------------------
def test_relative_standard_error_zero() -> None:
    """Test the Relative Standard Error (RSE) based on a sample DataFrame."""
    from algo_metrics.trade_methods.basic_calcs import relative_standard_error

    data_dict: Dict[str, List[Union[str, float]]] = {
        "TargetColumn": [0, 0, 0, 0, 0, 0, 0, 0],
        "symbol": ["SPY", "SPY", "SPY", "SPY", "SPY", "SPY", "SPY", "SPY"],
        "market_date": ["01", "02", "03", "04", "05", "06", "07", "08"],
    }
    main_df: pd.DataFrame = pd.DataFrame(data=data_dict)

    main_result: float = 0.0  # Expected RSE value (pre-calculated)
    test_result: float = relative_standard_error(main_df=main_df, target_column="TargetColumn")

    assert main_result == test_result


# ----------------------------------------------------------------------------------------------------------------------
def test_relative_standard_error_signal() -> None:
    """Test the Relative Standard Error (RSE) based on a sample DataFrame."""
    from algo_metrics.trade_methods.basic_calcs import relative_standard_error

    data_dict: Dict[str, List[Union[str, float]]] = {
        "TargetColumn": [10, 12, 23, 23, 16, 23, 21, 16],
        "symbol": ["SPY", "SPY", "SPY", "SPY", "SPY", "SPY", "SPY", "SPY"],
        "market_date": ["01", "02", "03", "04", "05", "06", "07", "08"],
    }
    main_df: pd.DataFrame = pd.DataFrame(data=data_dict)

    main_result: float = 0.1  # Expected RSE value (pre-calculated)
    test_result: float = relative_standard_error(main_df=main_df, target_column="TargetColumn", output_signal=True)

    assert main_result == test_result


# ----------------------------------------------------------------------------------------------------------------------
def test_geometric_mean() -> None:
    """Test the Geometric Mean calculation."""
    from algo_metrics.trade_methods.basic_calcs import geometric_mean

    data_dict: Dict[str, List[Union[str, int]]] = {
        "Values": [1, 3, 9, 27, 81],
        "symbol": ["SPY", "SPY", "SPY", "SPY", "SPY"],
        "market_date": ["01", "02", "03", "04", "05"],
    }
    main_df: pd.DataFrame = pd.DataFrame(data=data_dict)

    main_result: float = 9.0  # Calculated manually: (1*3*9*27*81)^(1/5) = 9
    test_result: float = geometric_mean(main_df=main_df, target_column="Values")

    assert np.isclose(a=main_result, b=test_result, atol=0.1)


# ----------------------------------------------------------------------------------------------------------------------
def test_quadratic_mean() -> None:
    """Test the Quadratic Mean calculation."""
    from algo_metrics.trade_methods.basic_calcs import quadratic_mean

    data_dict: Dict[str, List[Union[str, int]]] = {
        "Values": [1, 2, 3, 4, 5],
        "symbol": ["SPY", "SPY", "SPY", "SPY", "SPY"],
        "market_date": ["01", "02", "03", "04", "05"],
    }
    main_df: pd.DataFrame = pd.DataFrame(data=data_dict)

    main_result: float = (
        3.32  # Calculated manually: sqrt((1^2 + 2^2 + 3^2 + 4^2 + 5^2) / 5) = sqrt(55 / 5) = sqrt(11) ≈ 3.316
    )
    test_result: float = quadratic_mean(main_df=main_df, target_column="Values")

    assert np.isclose(a=main_result, b=test_result, atol=0.001)


# ----------------------------------------------------------------------------------------------------------------------
def test_harmonic_mean() -> None:
    """Test the Harmonic Mean calculation."""
    from algo_metrics.trade_methods.basic_calcs import harmonic_mean

    data_dict: Dict[str, List[Union[str, int]]] = {
        "Values": [1, 2, 3, 4, 5],
        "symbol": ["SPY", "SPY", "SPY", "SPY", "SPY"],
        "market_date": ["01", "02", "03", "04", "05"],
    }
    main_df: pd.DataFrame = pd.DataFrame(data=data_dict)

    main_result: float = 2.19  # Calculated manually: 5 / (1/1 + 1/2 + 1/3 + 1/4 + 1/5) = 5 / 2.283 ≈ 2.189
    test_result: float = harmonic_mean(main_df=main_df, target_column="Values")

    assert np.isclose(a=main_result, b=test_result, atol=0.001)


# ----------------------------------------------------------------------------------------------------------------------
def test_trade_algo_integrity_both_true() -> None:
    """Test the evaluate_trading_strategy function."""
    from algo_metrics.trade_methods.basic_calcs import trade_algo_integrity

    period_1_returns: pd.Series = pd.Series(data=[0.01, 0.02, 0.015, 0.03, 0.025])
    period_2_returns: pd.Series = pd.Series(data=[0.02, 0.03, 0.025, 0.035, 0.03])

    # Expected results based on the t-test
    expected_trade_algo_result: bool = True  # Hypothetical expectation

    # Calculate the consistency using the function
    trade_algo_result: bool = trade_algo_integrity(
        period_1_returns=period_1_returns,
        period_2_returns=period_2_returns,
    )

    # Assert the calculated consistency matches the expected values
    assert trade_algo_result == expected_trade_algo_result


# ----------------------------------------------------------------------------------------------------------------------
def test_trade_algo_integrity_both_false() -> None:
    """Test the evaluate_trading_strategy function."""
    from algo_metrics.trade_methods.basic_calcs import trade_algo_integrity

    period_1_returns: pd.Series = pd.Series(data=[0.01, 0.02, 0.015, 0.03, 0.025])
    period_2_returns: pd.Series = pd.Series(data=[0.05, 0.06, 0.07, 0.08, 0.09])

    # Expected results based on the t-test
    expected_trade_algo_result: bool = False  # Hypothetical expectation

    # Calculate the consistency using the function
    trade_algo_result: bool = trade_algo_integrity(
        period_1_returns=period_1_returns,
        period_2_returns=period_2_returns,
    )

    # Assert the calculated consistency matches the expected values
    assert trade_algo_result == expected_trade_algo_result


# ----------------------------------------------------------------------------------------------------------------------
def test_trade_algo_integrity_value_error() -> None:
    """Test the evaluate_trading_strategy function."""
    import pytest
    from algo_metrics.trade_methods.basic_calcs import trade_algo_integrity

    period_1_returns: pd.Series = pd.Series(data=[0.01, 0.02, 0.015, 0.03, 0.025])
    period_2_returns: pd.Series = pd.Series(data=[0.01, 0.02, 0.03, 0.04, 0.05])

    with pytest.raises(
        expected_exception=ValueError,
        match="INCONSISTENT STATISTICAL ANALYSIS: THOROUGHLY REVIEW MATH LOGIC",
    ):
        # Force inconsistency by using an extreme critical value
        trade_algo_integrity(
            period_1_returns=period_1_returns,
            period_2_returns=period_2_returns,
            alpha=0.05,
            critical_value=0.01,  # Very low critical value to cause inconsistency
        )


# ----------------------------------------------------------------------------------------------------------------------
# Test for standard_return
def test_standard_return() -> None:
    """Test the standard return calculation."""
    from algo_metrics.trade_methods.basic_calcs import standard_return

    data_dict: Dict[str, List[Union[str, float]]] = {
        "Price": [100, 110, 105, 115],
        "symbol": ["SPY", "SPY", "SPY", "SPY"],
        "market_date": ["01", "02", "03", "04"],
    }
    main_df: pd.DataFrame = pd.DataFrame(data=data_dict)

    # Expected result: (115 / 100) - 1 = 0.15
    expected_result: float = 0.15
    test_result: float = standard_return(main_df=main_df, target_column="Price")

    assert np.isclose(a=expected_result, b=test_result, atol=0.001)


# ----------------------------------------------------------------------------------------------------------------------
# Test for log_return
def test_log_return() -> None:
    """Test the log return calculation."""
    from algo_metrics.trade_methods.basic_calcs import log_return

    data_dict: Dict[str, List[Union[str, float]]] = {
        "Price": [100, 110, 105, 115],
        "symbol": ["SPY", "SPY", "SPY", "SPY"],
        "market_date": ["01", "02", "03", "04"],
    }
    main_df: pd.DataFrame = pd.DataFrame(data=data_dict)

    # Expected result: np.log(115 / 100) ≈ 0.14
    expected_result: float = round(np.log(115 / 100), 3)
    test_result: float = log_return(main_df=main_df, target_column="Price")

    assert np.isclose(a=expected_result, b=test_result, atol=0.001)


# ----------------------------------------------------------------------------------------------------------------------
# Test for annualized_rate_return
def test_annualized_rate_return() -> None:
    """Test the annualized rate of return calculation."""
    from algo_metrics.trade_methods.basic_calcs import annualized_rate_return

    data_dict: Dict[str, List[Union[str, float]]] = {
        "Price": [100, 110, 105, 115],
        "symbol": ["SPY", "SPY", "SPY", "SPY"],
        "market_date": ["01", "02", "03", "04"],
    }
    main_df: pd.DataFrame = pd.DataFrame(data=data_dict)

    # Compounded rate calculation
    # Assuming 252 trading days, 4 data points mean ~4 days
    expected_result_compounded: float = (115 / 100) ** (252 / 4) - 1
    test_result_compounded: float = annualized_rate_return(
        main_df=main_df, target_column="Price", compounded_rate=True, trading_days=252
    )

    assert np.isclose(a=expected_result_compounded, b=test_result_compounded, atol=0.001)

    # Non-compounded rate calculation
    expected_result_non_compounded: float = (115 / 100) * (252 / 4)
    test_result_non_compounded: float = annualized_rate_return(
        main_df=main_df, target_column="Price", compounded_rate=False, trading_days=252
    )

    assert np.isclose(a=expected_result_non_compounded, b=test_result_non_compounded, atol=0.001)


# ----------------------------------------------------------------------------------------------------------------------
def test_rate_return_probability() -> None:
    """Test the rate return probability calculation."""
    from algo_metrics.trade_methods.basic_calcs import rate_return_probability
    from scipy.stats import norm

    # Sample data
    data_dict: Dict[str, List[Union[str, float]]] = {
        "Price": [100, 105, 102, 110, 115],
        "symbol": ["SPY", "SPY", "SPY", "SPY", "SPY"],
        "market_date": ["01", "02", "03", "04", "05"],
    }
    main_df: pd.DataFrame = pd.DataFrame(data=data_dict)

    target_return: float = 120.0
    begin_invest_dollars: float = 100.0
    n_timesteps: int = 5

    # Calculate periodic returns
    main_df["returns"] = main_df["Price"].pct_change().dropna()

    # Calculate geometric average of periodic returns
    geo_mean_return: float = np.exp(np.mean(np.log(1 + main_df["returns"]))) - 1

    # Calculate standard deviation of the logarithms of 1 plus the periodic returns
    log_std_dev: float = np.std(np.log(1 + main_df["returns"]))

    # Calculate z value
    numerator_value: float = np.log(target_return / begin_invest_dollars) - np.log(1 + geo_mean_return) * n_timesteps
    denominator_value: float = np.sqrt(log_std_dev * n_timesteps)
    z_score: float = numerator_value / denominator_value

    # Calculate expected probability of achieving at least the target return
    expected_result: float = round(1 - norm.cdf(z_score), 2)

    # Calculate test result
    test_result: float = rate_return_probability(
        main_df=main_df,
        target_column="Price",
        target_return=target_return,
        begin_invest_dollars=begin_invest_dollars,
        n_timesteps=n_timesteps,
    )

    assert np.isclose(a=expected_result, b=test_result, atol=0.01)


# ----------------------------------------------------------------------------------------------------------------------
def test_annualized_risk() -> None:
    """Test the annualized risk calculation."""
    from algo_metrics.trade_methods.basic_calcs import annualized_risk

    # Sample data
    data_dict: Dict[str, List[Union[str, float]]] = {
        "Price": [100, 102, 101, 105, 107, 110, 108, 112, 115, 117],
        "symbol": ["SPY", "SPY", "SPY", "SPY", "SPY", "SPY", "SPY", "SPY", "SPY", "SPY"],
        "market_date": ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10"],
    }
    main_df: pd.DataFrame = pd.DataFrame(data=data_dict)

    # Calculate expected result
    # Calculate daily returns
    main_df["daily_returns"]: pd.Series = main_df["Price"].pct_change().dropna()

    # Calculate the standard deviation of daily returns
    daily_std_dev: float = np.std(a=main_df["daily_returns"])

    # Calculate the expected annualized risk
    expected_result: float = round(daily_std_dev * np.sqrt(252), ROUND_DECIMAL_PLACES)

    # Calculate test result
    test_result: float = annualized_risk(main_df=main_df, target_column="Price")

    assert np.isclose(a=expected_result, b=test_result, atol=0.001)


# ----------------------------------------------------------------------------------------------------------------------
def test_calculate_beta() -> None:
    """Test the Beta calculation."""
    from algo_metrics.trade_methods.basic_calcs import calculate_beta

    # Sample data
    data_dict: Dict[str, List[Union[str, float]]] = {
        "Returns_A": [0.01, 0.02, -0.01, 0.03, 0.04],
        "Returns_B": [0.02, 0.01, 0.00, 0.04, 0.03],
        "symbol": ["SPY", "SPY", "SPY", "SPY", "SPY"],
        "market_date": ["01", "02", "03", "04", "05"],
    }
    main_df: pd.DataFrame = pd.DataFrame(data=data_dict)

    # Calculate expected result
    # noinspection PyTypeChecker
    covariance_ab: float = np.cov(m=main_df["Returns_A"], y=main_df["Returns_B"])[0, 1]
    variance_b: float = np.var(a=main_df["Returns_B"])
    expected_result: float = covariance_ab / variance_b

    # Calculate test result
    test_result: float = calculate_beta(main_df=main_df, returns_a_column="Returns_A", returns_b_column="Returns_B")

    assert np.isclose(a=expected_result, b=test_result, atol=0.001)


# ----------------------------------------------------------------------------------------------------------------------
def test_downside_semivariance() -> None:
    """Test the downside semivariance calculation."""
    from algo_metrics.trade_methods.basic_calcs import downside_semivariance

    # Sample data
    data_dict: Dict[str, List[Union[str, float]]] = {
        "Price": [100, 102, 101, 105, 107, 110, 108, 112, 115, 117],
        "symbol": ["SPY", "SPY", "SPY", "SPY", "SPY", "SPY", "SPY", "SPY", "SPY", "SPY"],
        "market_date": ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10"],
    }
    main_df: pd.DataFrame = pd.DataFrame(data=data_dict)

    # Calculate expected result
    return_values: pd.Series = main_df["Price"].pct_change().dropna()
    target_value: float = return_values.mean()
    below_target_returns: pd.Series = return_values[return_values < target_value]
    expected_result: float = round(np.sum(a=(target_value - below_target_returns) ** 2) / len(return_values), 2)

    # Calculate test result
    test_result: float = downside_semivariance(main_df=main_df, target_column="Price")

    assert np.isclose(a=expected_result, b=test_result, atol=0.01)
