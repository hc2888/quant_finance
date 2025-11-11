"""Python Test(s) for `dags/algo_metrics/noise.py`."""

from typing import Dict, List, Union

import pandas as pd


# ----------------------------------------------------------------------------------------------------------------------
def test_efficiency_ratio() -> None:
    """Test the Efficiency Ratio (ER) based on a list of moves."""
    from algo_metrics.trade_methods.noise import efficiency_ratio

    data_dict: Dict[str, List[Union[str, int]]] = {
        "open": [1, 3, 2],
        "close": [3, 1, 5],
        "symbol": ["SPY", "SPY", "SPY"],
        "market_date": ["01", "02", "03"],
    }
    main_df: pd.DataFrame = pd.DataFrame(data=data_dict)

    main_result: float = 0.33
    test_result: float = efficiency_ratio(main_df=main_df, close_column="close")

    assert main_result == test_result


# ----------------------------------------------------------------------------------------------------------------------
def test_price_density() -> None:
    """Test the Price Density (PD) calculation."""
    from algo_metrics.trade_methods.noise import price_density

    data_dict: Dict[str, List[Union[str, int]]] = {
        "high": [4, 5, 6],
        "low": [1, 2, 3],
        "symbol": ["SPY", "SPY", "SPY"],
        "market_date": ["01", "02", "03"],
    }
    main_df: pd.DataFrame = pd.DataFrame(data=data_dict)

    main_result: float = 1.8  # Calculated manually: (3+3+3)/(6-1)
    test_result: float = price_density(main_df=main_df, high_column="high", low_column="low")

    assert main_result == test_result


# ----------------------------------------------------------------------------------------------------------------------
def test_fractal_dimension() -> None:
    """Test the Fractal Dimension (FD) calculation."""
    import numpy as np
    from algo_metrics.trade_methods.noise import fractal_dimension

    data_dict: Dict[str, List[Union[str, int]]] = {
        "high": [1, 2, 3, 4, 5],
        "low": [1, 1, 1, 1, 1],
        "close": [1, 2, 3, 4, 5],
        "symbol": ["SPY", "SPY", "SPY", "SPY", "SPY"],
        "market_date": ["01", "02", "03", "04", "05"],
    }
    main_df: pd.DataFrame = pd.DataFrame(data=data_dict)

    main_result: float = 1.5  # This is an expected value calculated manually for testing
    test_result: float = fractal_dimension(main_df=main_df, high_column="high", low_column="low", close_column="close")

    # `np.isclose()` is used to compare two arrays element-wise and determine if they are "close" to
    # each other within a specified tolerance.
    # This function is particularly useful when dealing with floating-point arithmetic,
    # where exact equality is often impractical due to the precision limitations of floating-point numbers.
    assert np.isclose(a=main_result, b=test_result, atol=0.1)


# ----------------------------------------------------------------------------------------------------------------------
def test_fractal_dimension_zero() -> None:
    """Test the Fractal Dimension (FD) calculation."""
    import numpy as np
    from algo_metrics.trade_methods.noise import fractal_dimension

    data_dict: Dict[str, List[Union[str, int]]] = {
        "high": [1, 1, 1, 1, 1],
        "low": [1, 1, 1, 1, 1],
        "close": [1, 1, 1, 1, 1],
        "symbol": ["SPY", "SPY", "SPY", "SPY", "SPY"],
        "market_date": ["01", "02", "03", "04", "05"],
    }
    main_df: pd.DataFrame = pd.DataFrame(data=data_dict)

    main_result: float = 0.0  # This is an expected value calculated manually for testing
    test_result: float = fractal_dimension(main_df=main_df, high_column="high", low_column="low", close_column="close")

    # `np.isclose()` is used to compare two arrays element-wise and determine if they are "close" to
    # each other within a specified tolerance.
    # This function is particularly useful when dealing with floating-point arithmetic,
    # where exact equality is often impractical due to the precision limitations of floating-point numbers.
    assert np.isclose(a=main_result, b=test_result, atol=0.1)
