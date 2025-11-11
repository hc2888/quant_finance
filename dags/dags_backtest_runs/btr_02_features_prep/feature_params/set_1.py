"""Set of features."""

import pandas as pd


# ----------------------------------------------------------------------------------------------------------------------
def features_adaptive_techniques(
    main_df: pd.DataFrame,
    time_column: str,
    timesteps: int,
) -> pd.DataFrame:
    """`dags/algo_metrics/trade_methods/adaptive_techniques.py`.

    :param main_df: pd.DataFrame: The DataFrame containing the data.
    :param time_column: str: The target column with the dates / timestamps.
    :param timesteps: int: Number of timesteps to consider.

    :return: pd.DataFrame: DataFrame with new columns.
    """
    from algo_metrics.trade_methods.adaptive_techniques import (
        adaptive_trend,
        chande_dynamic,
        dynamic_momentum_idx,
        ehlers_frama,
        ehlers_mama_fama,
        kaufman_efficiency,
        mcginley_dynamic,
    )

    main_df: pd.DataFrame = chande_dynamic(
        main_df=main_df,
        time_column=time_column,
        fast_period=int(timesteps / 3),
        slow_period=timesteps,
    )
    main_df: pd.DataFrame = ehlers_mama_fama(main_df=main_df, time_column=time_column)
    main_df: pd.DataFrame = ehlers_frama(main_df=main_df, time_column=time_column, lookback_period=timesteps)
    main_df: pd.DataFrame = mcginley_dynamic(main_df=main_df, time_column=time_column, window=timesteps)
    main_df: pd.DataFrame = kaufman_efficiency(main_df=main_df, time_column=time_column, lookback_period=timesteps)
    main_df: pd.DataFrame = dynamic_momentum_idx(
        main_df=main_df,
        time_column=time_column,
        window=int(timesteps / 3),
        pivotal_period=timesteps,
    )
    main_df: pd.DataFrame = adaptive_trend(main_df=main_df, time_column=time_column)

    return main_df


# ----------------------------------------------------------------------------------------------------------------------
def features_advanced_techniques(
    main_df: pd.DataFrame,
    time_column: str,
    timesteps: int,
    time_shift: int,
) -> pd.DataFrame:
    """`dags/algo_metrics/trade_methods/advanced_techniques.py`.

    :param main_df: pd.DataFrame: The DataFrame containing the data.
    :param time_column: str: The target column with the dates / timestamps.
    :param timesteps: int: Number of timesteps to consider.
    :param time_shift: int: Number of timesteps to look back into for default calculations.

    :return: pd.DataFrame: DataFrame with new columns.
    """
    from algo_metrics.trade_methods.advanced_techniques import (
        bierovic_true_range,
        bookstaber_volatility_breakout,
        conners_vix_reversal,
        fractal_patterns,
    )

    main_df: pd.DataFrame = bierovic_true_range(
        main_df=main_df,
        time_column=time_column,
        smooth_steps=timesteps,
        time_shift=time_shift,
    )
    main_df: pd.DataFrame = conners_vix_reversal(
        main_df=main_df,
        time_column=time_column,
        high_range=timesteps,
        day_range=int(timesteps / 3),
        time_shift=time_shift,
    )
    main_df: pd.DataFrame = bookstaber_volatility_breakout(
        main_df=main_df,
        time_column=time_column,
        lookback_period=timesteps,
        time_shift=time_shift,
    )
    main_df: pd.DataFrame = fractal_patterns(main_df=main_df, time_column=time_column, time_shift=time_shift)

    return main_df


# ----------------------------------------------------------------------------------------------------------------------
def features_behavioral(
    main_df: pd.DataFrame,
    time_column: str,
    timesteps: int,
    time_shift: int,
) -> pd.DataFrame:
    """`dags/algo_metrics/trade_methods/behavioral.py`.

    :param main_df: pd.DataFrame: The DataFrame containing the data.
    :param time_column: str: The target column with the dates / timestamps.
    :param timesteps: int: Number of timesteps to consider.
    :param time_shift: int: Number of timesteps to look back into for default calculations.

    :return: pd.DataFrame: DataFrame with new columns.
    """
    from algo_metrics.trade_methods.behavioral import volatility_ratio

    main_df: pd.DataFrame = volatility_ratio(
        main_df=main_df,
        time_column=time_column,
        timesteps_covered=timesteps,
        timestep_lag=int(timesteps / 2),
        time_shift=time_shift,
    )

    return main_df


# ----------------------------------------------------------------------------------------------------------------------
def features_charting(
    main_df: pd.DataFrame,
    time_column: str,
    timesteps: int,
    time_shift: int,
) -> pd.DataFrame:
    """`dags/algo_metrics/trade_methods/charting.py`.

    :param main_df: pd.DataFrame: The DataFrame containing the data.
    :param time_column: str: The target column with the dates / timestamps.
    :param timesteps: int: Number of timesteps to consider.
    :param time_shift: int: Number of timesteps to look back into for default calculations.

    :return: pd.DataFrame: DataFrame with new columns.
    """
    from algo_metrics.trade_methods.charting import (
        candlestick_signals,
        detect_spike,
        inside_outside_day,
        island_reversal,
        key_reversal_down,
        key_reversal_up,
        pivot_points,
        reversal_day,
        wide_range_day,
    )

    # NOTE: Universal parameter constants specific to this group of metrics
    perc_diff_min: float = 0.01

    main_df: pd.DataFrame = detect_spike(
        main_df=main_df,
        time_column=time_column,
        n_steps=timesteps,
        time_shift=time_shift,
    )
    main_df: pd.DataFrame = island_reversal(
        main_df=main_df,
        time_column=time_column,
        perc_diff_min=perc_diff_min,
        time_shift=time_shift,
    )
    main_df: pd.DataFrame = pivot_points(
        main_df=main_df,
        time_column=time_column,
        perc_diff_min=perc_diff_min,
        time_shift=time_shift,
    )
    main_df: pd.DataFrame = reversal_day(
        main_df=main_df,
        time_column=time_column,
        perc_diff_min=perc_diff_min,
        time_shift=time_shift,
    )
    main_df: pd.DataFrame = key_reversal_down(
        main_df=main_df,
        time_column=time_column,
        n_steps=timesteps,
        time_shift=time_shift,
    )
    main_df: pd.DataFrame = key_reversal_up(
        main_df=main_df,
        time_column=time_column,
        n_steps=timesteps,
        time_shift=time_shift,
    )
    main_df: pd.DataFrame = wide_range_day(main_df=main_df, time_column=time_column, n_steps=timesteps)
    main_df: pd.DataFrame = inside_outside_day(
        main_df=main_df,
        time_column=time_column,
        perc_diff_min=perc_diff_min,
        time_shift=time_shift,
    )
    main_df: pd.DataFrame = candlestick_signals(
        main_df=main_df,
        time_column=time_column,
        perc_diff_min=0.0001,  # NOTE: The default `perc_diff_min` causes too many `doji` & `double_doji`s to == 1
        time_shift=time_shift,
    )

    return main_df


# ----------------------------------------------------------------------------------------------------------------------
def features_cycles(
    main_df: pd.DataFrame,
    time_column: str,
    timesteps: int,
    time_shift: int,
) -> pd.DataFrame:
    """`dags/algo_metrics/trade_methods/cycles.py`.

    :param main_df: pd.DataFrame: The DataFrame containing the data.
    :param time_column: str: The target column with the dates / timestamps.
    :param timesteps: int: Number of timesteps to consider.
    :param time_shift: int: Number of timesteps to look back into for default calculations.

    :return: pd.DataFrame: DataFrame with new columns.
    """
    from algo_metrics.trade_methods.cycles import (
        ehlers_universal,
        fisher_transform,
        hilbert_transform,
        short_cycle,
    )

    main_df: pd.DataFrame = hilbert_transform(main_df=main_df, time_column=time_column, time_shift=time_shift)
    main_df: pd.DataFrame = fisher_transform(main_df=main_df, time_column=time_column, n_steps=timesteps)
    main_df: pd.DataFrame = ehlers_universal(main_df=main_df, time_column=time_column, time_shift=time_shift)
    main_df: pd.DataFrame = short_cycle(
        main_df=main_df,
        time_column=time_column,
        slow_period=timesteps,
        fast_period=int(timesteps / 2),
        time_shift=time_shift,
    )

    return main_df


# ----------------------------------------------------------------------------------------------------------------------
def features_day_trading(
    main_df: pd.DataFrame,
    time_column: str,
    timesteps: int,
    time_shift: int,
) -> pd.DataFrame:
    """`dags/algo_metrics/trade_methods/day_trading.py`.

    :param main_df: pd.DataFrame: The DataFrame containing the data.
    :param time_column: str: The target column with the dates / timestamps.
    :param timesteps: int: Number of timesteps to consider.
    :param time_shift: int: Number of timesteps to look back into for default calculations.

    :return: pd.DataFrame: DataFrame with new columns.
    """
    from algo_metrics.trade_methods.day_trading import (
        first_hour_breakout,
        intraday_price_shock,
        momentum_pinball,
        raschke_range_breakout,
        true_upward_gap,
        williams_range_breakout,
    )

    main_df: pd.DataFrame = first_hour_breakout(main_df=main_df)
    main_df: pd.DataFrame = momentum_pinball(
        main_df=main_df,
        timesteps=timesteps,
        time_shift=time_shift,
        intraday_action=True,
    )
    main_df: pd.DataFrame = raschke_range_breakout(main_df=main_df, time_column=time_column, timesteps=timesteps)
    main_df: pd.DataFrame = williams_range_breakout(main_df=main_df, time_column=time_column, time_shift=time_shift)
    main_df: pd.DataFrame = true_upward_gap(main_df=main_df, time_shift=time_shift)
    main_df: pd.DataFrame = intraday_price_shock(main_df=main_df, time_shift=time_shift)

    return main_df


# ----------------------------------------------------------------------------------------------------------------------
def features_event_driven(
    main_df: pd.DataFrame,
    time_column: str,
    timesteps: int,
    time_shift: int,
) -> pd.DataFrame:
    """`dags/algo_metrics/trade_methods/event_driven.py`.

    :param main_df: pd.DataFrame: The DataFrame containing the data.
    :param time_column: str: The target column with the dates / timestamps.
    :param timesteps: int: Number of timesteps to consider.
    :param time_shift: int: Number of timesteps to look back into for default calculations.

    :return: pd.DataFrame: DataFrame with new columns.
    """
    from algo_metrics.trade_methods.event_driven import (
        n_day_breakout,
        swing_high_low_points,
        turtle_strategy,
        wilder_swing_index,
    )

    main_df: pd.DataFrame = swing_high_low_points(main_df=main_df, time_column=time_column)
    main_df: pd.DataFrame = wilder_swing_index(main_df=main_df, time_column=time_column, time_shift=time_shift)
    main_df: pd.DataFrame = n_day_breakout(main_df=main_df, time_column=time_column, past_n_steps=timesteps)
    main_df: pd.DataFrame = turtle_strategy(
        main_df=main_df,
        time_column=time_column,
        enter_n_steps=timesteps,
        exit_n_steps=int(timesteps / 3),
        volatility_n_steps=int(timesteps / 3),
        time_shift=time_shift,
    )
    return main_df


# ----------------------------------------------------------------------------------------------------------------------
def features_momentum(
    main_df: pd.DataFrame,
    time_column: str,
    timesteps: int,
    time_shift: int,
) -> pd.DataFrame:
    """`dags/algo_metrics/trade_methods/volumes.py`.

    :param main_df: pd.DataFrame: The DataFrame containing the data.
    :param time_column: str: The target column with the dates / timestamps.
    :param timesteps: int: Number of timesteps to consider.
    :param time_shift: int: Number of timesteps to look back into for default calculations.

    :return: pd.DataFrame: DataFrame with new columns.
    """
    from algo_metrics.tech_analysis.momentum import (
        awesome_oscillator,
        kaufman_adapt_signal,
        percentage_price_oscillator,
        percentage_volume_oscillator,
        rate_of_change,
        relative_strength_index,
        stoch_relative_strength_index,
        stochastic_oscillator,
        true_strength_index,
        williams_perc_r,
    )

    main_df: pd.DataFrame = relative_strength_index(
        main_df=main_df,
        time_column=time_column,
        window=timesteps,
        output_signal=True,
        time_shift=time_shift,
    )
    main_df: pd.DataFrame = true_strength_index(
        main_df=main_df,
        time_column=time_column,
        window_slow=timesteps,
        window_fast=int(timesteps / 2),
        output_signal=True,
        time_shift=time_shift,
    )
    main_df: pd.DataFrame = stochastic_oscillator(
        main_df=main_df,
        time_column=time_column,
        window=timesteps,
        smooth_window=int(timesteps * 0.2),
        output_signal=True,
    )
    main_df: pd.DataFrame = kaufman_adapt_signal(
        main_df=main_df,
        time_column=time_column,
        dynamic_smoothing=False,
        window=timesteps,
        pow1=int(timesteps / 5),
        pow2=int(timesteps * 3),
    )
    main_df: pd.DataFrame = kaufman_adapt_signal(
        main_df=main_df,
        time_column=time_column,
        dynamic_smoothing=True,
        window=timesteps,
        pow1=int(timesteps / 5),
        pow2=int(timesteps * 3),
    )
    main_df: pd.DataFrame = rate_of_change(
        main_df=main_df,
        time_column=time_column,
        window=timesteps,
        output_signal=True,
    )
    main_df: pd.DataFrame = williams_perc_r(
        main_df=main_df,
        time_column=time_column,
        lookback_period=timesteps,
        output_signal=True,
    )
    main_df: pd.DataFrame = stoch_relative_strength_index(
        main_df=main_df,
        time_column=time_column,
        window=timesteps,
        smooth_1=int(timesteps * 0.2),
        smooth_2=int(timesteps * 0.2),
        output_signal=True,
        time_shift=time_shift,
    )
    main_df: pd.DataFrame = percentage_price_oscillator(
        main_df=main_df,
        time_column=time_column,
        window_slow=timesteps,
        window_fast=int(timesteps * 0.5),
        window_sign=int(timesteps / 3),
        output_signal=True,
    )
    main_df: pd.DataFrame = percentage_volume_oscillator(
        main_df=main_df,
        time_column=time_column,
        window_slow=timesteps,
        window_fast=int(timesteps * 0.5),
        window_sign=int(timesteps / 3),
        output_signal=True,
    )
    main_df: pd.DataFrame = awesome_oscillator(
        main_df=main_df,
        time_column=time_column,
        window_1=int(timesteps / 6),
        window_2=timesteps,
        output_signal=True,
    )
    return main_df


# ----------------------------------------------------------------------------------------------------------------------
def features_momentums(
    main_df: pd.DataFrame,
    time_column: str,
    timesteps: int,
    time_shift: int,
) -> pd.DataFrame:
    """`dags/algo_metrics/trade_methods/momentums.py`.

    :param main_df: pd.DataFrame: The DataFrame containing the data.
    :param time_column: str: The target column with the dates / timestamps.
    :param timesteps: int: Number of timesteps to consider.
    :param time_shift: int: Number of timesteps to look back into for default calculations.

    :return: pd.DataFrame: DataFrame with new columns.
    """
    from algo_metrics.trade_methods.momentums import (
        cambridge_hook,
        divergence_index,
        raschke_first_cross,
        relative_vigor_idx,
        strength_oscillator,
        ultim_osc_signal,
        velocity_accel,
    )

    main_df: pd.DataFrame = divergence_index(
        main_df=main_df,
        time_column=time_column,
        slow_period=timesteps,
        fast_period=int(timesteps / 4),
        time_shift=time_shift,
    )
    main_df: pd.DataFrame = ultim_osc_signal(
        main_df=main_df,
        time_column=time_column,
        window_1=int(timesteps / 4),
        window_2=int(timesteps / 2),
        window_3=timesteps,
        time_shift=time_shift,
    )
    main_df: pd.DataFrame = relative_vigor_idx(
        main_df=main_df,
        time_column=time_column,
        n_timesteps=timesteps,
        time_shift=time_shift,
    )
    main_df: pd.DataFrame = raschke_first_cross(
        main_df=main_df,
        time_column=time_column,
        slow_period=timesteps,
        fast_period=int(timesteps / 4),
        trend_period=int(timesteps * 0.375),
        smooth_window=int(timesteps * 0.05),
    )
    main_df: pd.DataFrame = strength_oscillator(
        main_df=main_df,
        time_column=time_column,
        smooth_window=timesteps,
        time_shift=time_shift,
    )
    main_df: pd.DataFrame = velocity_accel(main_df=main_df, time_column=time_column)
    main_df: pd.DataFrame = cambridge_hook(
        main_df=main_df,
        time_column=time_column,
        timesteps=timesteps,
        time_shift=time_shift,
    )
    return main_df


# ----------------------------------------------------------------------------------------------------------------------
def features_multi_time_frames(
    main_df: pd.DataFrame,
    time_column: str,
    timesteps: int,
    time_shift: int,
) -> pd.DataFrame:
    """`dags/algo_metrics/trade_methods/multi_time_frames.py`.

    :param main_df: pd.DataFrame: The DataFrame containing the data.
    :param time_column: str: The target column with the dates / timestamps.
    :param timesteps: int: Number of timesteps to consider.
    :param time_shift: int: Number of timesteps to look back into for default calculations.

    :return: pd.DataFrame: DataFrame with new columns.
    """
    from algo_metrics.trade_methods.multi_time_frames import elder_triple_screen

    main_df: pd.DataFrame = elder_triple_screen(
        main_df=main_df,
        time_column=time_column,
        intraday_action=True,
        window_slow=timesteps,
        window_fast=int(timesteps / 2),
        window_sign=int(timesteps / 3),
        time_shift=time_shift,
    )
    return main_df


# ----------------------------------------------------------------------------------------------------------------------
def features_price_distribution(
    main_df: pd.DataFrame,
    time_column: str,
    timesteps: int,
    time_shift: int,
) -> pd.DataFrame:
    """`dags/algo_metrics/trade_methods/price_distribution.py`.

    :param main_df: pd.DataFrame: The DataFrame containing the data.
    :param time_column: str: The target column with the dates / timestamps.
    :param timesteps: int: Number of timesteps to consider.
    :param time_shift: int: Number of timesteps to look back into for default calculations.

    :return: pd.DataFrame: DataFrame with new columns.
    """
    from algo_metrics.trade_methods.price_distribution import (
        chande_kroll_zone,
        jackson_zone,
        kase_devstop,
        mcnicholl_skewness,
        scorpio_zone,
        skew_kurt_volatility,
    )

    main_df: pd.DataFrame = kase_devstop(
        main_df=main_df,
        time_column=time_column,
        window=timesteps,
        lookback_period=int(timesteps / 20),
        intraday_action=True,
        time_shift=time_shift,
    )
    main_df: pd.DataFrame = jackson_zone(
        main_df=main_df,
        time_column=time_column,
        intraday_action=True,
        time_shift=time_shift,
    )
    main_df: pd.DataFrame = chande_kroll_zone(
        main_df=main_df,
        time_column=time_column,
        window=timesteps,
        intraday_action=True,
        time_shift=time_shift,
    )
    main_df: pd.DataFrame = scorpio_zone(
        main_df=main_df,
        time_column=time_column,
        window=timesteps,
        intraday_action=True,
        time_shift=time_shift,
    )
    main_df: pd.DataFrame = mcnicholl_skewness(main_df=main_df, time_column=time_column)
    main_df: pd.DataFrame = skew_kurt_volatility(
        main_df=main_df,
        time_column=time_column,
        lookback_period=timesteps,
        time_shift=time_shift,
    )

    return main_df


# ----------------------------------------------------------------------------------------------------------------------
def features_trend(
    main_df: pd.DataFrame,
    time_column: str,
    timesteps: int,
    time_shift: int,
) -> pd.DataFrame:
    """`dags/algo_metrics/tech_analysis/trend.py`.

    :param main_df: pd.DataFrame: The DataFrame containing the data.
    :param time_column: str: The target column with the dates / timestamps.
    :param timesteps: int: Number of timesteps to consider.
    :param time_shift: int: Number of timesteps to look back into for default calculations.

    :return: pd.DataFrame: DataFrame with new columns.
    """
    from algo_metrics.tech_analysis.trend import (
        adx_indicator,
        aroon_indicator,
        cci_indicator,
        dpo_indicator,
        ichimoku_indicator,
        kst_indicator,
        macd_indicator,
        psar_indicator,
        stc_indicator,
        vortex_indicator,
    )

    main_df: pd.DataFrame = aroon_indicator(
        main_df=main_df,
        time_column=time_column,
        window=timesteps,
        output_signal=True,
    )
    main_df: pd.DataFrame = adx_indicator(
        main_df=main_df,
        time_column=time_column,
        window=timesteps,
        output_signal=True,
        time_shift=time_shift,
    )
    main_df: pd.DataFrame = macd_indicator(
        main_df=main_df,
        time_column=time_column,
        window_slow=timesteps,
        window_fast=int(timesteps / 2),
        window_sign=int(timesteps / 3),
        output_signal=True,
    )
    main_df: pd.DataFrame = ichimoku_indicator(
        main_df=main_df,
        time_column=time_column,
        output_signal=True,
        window_1=int(timesteps / 3),
        window_2=int(timesteps / 2),
        window_3=timesteps,
    )
    main_df: pd.DataFrame = kst_indicator(
        main_df=main_df,
        time_column=time_column,
        output_signal=True,
        roc1=int(timesteps / 3),
        roc2=int(timesteps / 2),
        roc3=int(timesteps * (2 / 3)),
        roc4=timesteps,
        window_1=int(timesteps / 3),
        window_2=int(timesteps / 3),
        window_3=int(timesteps / 3),
        window4=int(timesteps / 2),
        nsig=int(timesteps / 3),
    )
    main_df: pd.DataFrame = dpo_indicator(
        main_df=main_df,
        time_column=time_column,
        window=timesteps,
        output_signal=True,
    )
    main_df: pd.DataFrame = cci_indicator(
        main_df=main_df,
        time_column=time_column,
        window=timesteps,
        output_signal=True,
    )
    main_df: pd.DataFrame = vortex_indicator(
        main_df=main_df,
        time_column=time_column,
        window=timesteps,
        output_signal=True,
        time_shift=time_shift,
    )
    main_df: pd.DataFrame = psar_indicator(main_df=main_df, time_column=time_column, output_signal=True)
    main_df: pd.DataFrame = stc_indicator(
        main_df=main_df,
        time_column=time_column,
        output_signal=True,
        window_slow=timesteps,
        window_fast=int(timesteps / 2),
        cycle=int(timesteps / 5),
        smooth_1=int(timesteps * 0.05),
        smooth_2=int(timesteps * 0.05),
    )
    return main_df


# ----------------------------------------------------------------------------------------------------------------------
def features_trends(
    main_df: pd.DataFrame,
    time_column: str,
    timesteps: int,
    time_shift: int,
) -> pd.DataFrame:
    """`dags/algo_metrics/trade_methods/trends.py`.

    :param main_df: pd.DataFrame: The DataFrame containing the data.
    :param time_column: str: The target column with the dates / timestamps.
    :param timesteps: int: Number of timesteps to consider.
    :param time_shift: int: Number of timesteps to look back into for default calculations.

    :return: pd.DataFrame: DataFrame with new columns.
    """
    from algo_metrics.trade_methods.trends import (
        donchian_5_20,
        donchian_breakout,
        ehlers_onset,
        golden_cross,
        kestner_adx,
        simple_volatility,
        single_system,
        smoothing_exponential,
        three_crossover,
        trix_signal,
        woodshedder_long_term,
    )

    main_df: pd.DataFrame = smoothing_exponential(main_df=main_df, time_column=time_column)
    main_df: pd.DataFrame = simple_volatility(
        main_df=main_df,
        time_column=time_column,
        window=timesteps,
        time_shift=time_shift,
    )
    main_df: pd.DataFrame = single_system(main_df=main_df, time_column=time_column, n_steps=timesteps)
    main_df: pd.DataFrame = donchian_5_20(
        main_df=main_df,
        time_column=time_column,
        fast_move_avg=int(timesteps / 4),
        slow_move_avg=timesteps,
        time_shift=time_shift,
    )
    main_df: pd.DataFrame = golden_cross(main_df=main_df, time_column=time_column)
    main_df: pd.DataFrame = woodshedder_long_term(
        main_df=main_df,
        time_column=time_column,
        fast_period=int(timesteps * 0.05),
        slow_period=timesteps,
    )
    main_df: pd.DataFrame = three_crossover(
        main_df=main_df,
        time_column=time_column,
        fast_trend=int(timesteps / 4),
        medium_trend=int(timesteps / 2),
        slow_trend=timesteps,
    )
    main_df: pd.DataFrame = ehlers_onset(main_df=main_df, time_column=time_column, low_pass_period=timesteps)
    main_df: pd.DataFrame = trix_signal(main_df=main_df, time_column=time_column, n_timesteps=timesteps)
    main_df: pd.DataFrame = kestner_adx(
        main_df=main_df,
        time_column=time_column,
        fast_window=int(timesteps / 5),
        slow_window=timesteps,
        adx_window=int(timesteps / 4),
        time_shift=time_shift,
    )
    main_df: pd.DataFrame = donchian_breakout(main_df=main_df, time_column=time_column, intraday_action=True)
    return main_df


# ----------------------------------------------------------------------------------------------------------------------
def features_volatility(
    main_df: pd.DataFrame,
    time_column: str,
    timesteps: int,
    time_shift: int,
) -> pd.DataFrame:
    """`dags/algo_metrics/tech_analysis/volatility.py`.

    :param main_df: pd.DataFrame: The DataFrame containing the data.
    :param time_column: str: The target column with the dates / timestamps.
    :param timesteps: int: Number of timesteps to consider.
    :param time_shift: int: Number of timesteps to look back into for default calculations.

    :return: pd.DataFrame: DataFrame with new columns.
    """
    from algo_metrics.tech_analysis.volatility import (
        bollinger_bands,
        bookstaber_volatility,
        donchian_channel,
        keltner_channel,
        ulcer_index_indicator,
    )

    main_df: pd.DataFrame = bollinger_bands(
        main_df=main_df,
        time_column=time_column,
        window=timesteps,
        window_dev=int(timesteps / 10),
        output_signal=True,
    )
    main_df: pd.DataFrame = keltner_channel(
        main_df=main_df,
        time_column=time_column,
        window=timesteps,
        window_atr=int(timesteps / 2),
        multiplier=int(timesteps / 10),
        output_signal=True,
    )
    main_df: pd.DataFrame = donchian_channel(
        main_df=main_df,
        time_column=time_column,
        window=timesteps,
        output_signal=True,
    )
    main_df: pd.DataFrame = ulcer_index_indicator(
        main_df=main_df,
        time_column=time_column,
        window=timesteps,
        output_signal=True,
    )
    main_df: pd.DataFrame = bookstaber_volatility(
        main_df=main_df,
        time_column=time_column,
        timesteps=timesteps,
        annualized_time=98_280,  # NOTE: Number of trading minutes in a year
        output_signal=True,
        time_shift=time_shift,
    )

    return main_df


# ----------------------------------------------------------------------------------------------------------------------
def features_volume(
    main_df: pd.DataFrame,
    time_column: str,
    timesteps: int,
    time_shift: int,
) -> pd.DataFrame:
    """`dags/algo_metrics/tech_analysis/volume.py`.

    :param main_df: pd.DataFrame: The DataFrame containing the data.
    :param time_column: str: The target column with the dates / timestamps.
    :param timesteps: int: Number of timesteps to consider.
    :param time_shift: int: Number of timesteps to look back into for default calculations.

    :return: pd.DataFrame: DataFrame with new columns.
    """
    from algo_metrics.tech_analysis.volume import (
        chaikin_money_flow_indicator,
        ease_of_movement_indicator,
        force_index_indicator,
        money_flow_index_indicator,
    )

    main_df: pd.DataFrame = chaikin_money_flow_indicator(
        main_df=main_df,
        time_column=time_column,
        window=timesteps,
        output_signal=True,
    )
    main_df: pd.DataFrame = force_index_indicator(
        main_df=main_df,
        time_column=time_column,
        window=timesteps,
        output_signal=True,
        time_shift=time_shift,
    )
    main_df: pd.DataFrame = ease_of_movement_indicator(
        main_df=main_df,
        time_column=time_column,
        window=timesteps,
        output_signal=True,
        time_shift=time_shift,
    )
    main_df: pd.DataFrame = money_flow_index_indicator(
        main_df=main_df,
        time_column=time_column,
        window=timesteps,
        output_signal=True,
        time_shift=time_shift,
    )

    return main_df


# ----------------------------------------------------------------------------------------------------------------------
def features_volumes(
    main_df: pd.DataFrame,
    time_column: str,
    timesteps: int,
    time_shift: int,
) -> pd.DataFrame:
    """`dags/algo_metrics/trade_methods/volumes.py`.

    :param main_df: pd.DataFrame: The DataFrame containing the data.
    :param time_column: str: The target column with the dates / timestamps.
    :param timesteps: int: Number of timesteps to consider.
    :param time_shift: int: Number of timesteps to look back into for default calculations.

    :return: pd.DataFrame: DataFrame with new columns.
    """
    from algo_metrics.trade_methods.volumes import (
        low_price_move,
        low_volume_period,
        mcclellan_oscillator,
        pseudo_volume,
        volume_price_combo,
        volume_spike,
    )

    main_df: pd.DataFrame = volume_price_combo(main_df=main_df, time_column=time_column, time_shift=time_shift)
    main_df: pd.DataFrame = mcclellan_oscillator(
        main_df=main_df,
        time_column=time_column,
        smoothing_window_1=int(timesteps / 2),
        smoothing_window_2=timesteps,
        time_shift=time_shift,
    )
    main_df: pd.DataFrame = volume_spike(
        main_df=main_df,
        time_column=time_column,
        timesteps=timesteps,
        timestep_lag=int(timesteps / 6),
    )
    main_df: pd.DataFrame = pseudo_volume(
        main_df=main_df,
        time_column=time_column,
        timesteps=timesteps,
        trading_days=98_280,  # NOTE: Number of trading minutes in a year
        time_shift=time_shift,
    )
    main_df: pd.DataFrame = low_volume_period(main_df=main_df, time_column=time_column, timesteps=timesteps)
    main_df: pd.DataFrame = low_price_move(
        main_df=main_df,
        time_column=time_column,
        timesteps=timesteps,
        time_shift=time_shift,
    )
    return main_df


# ----------------------------------------------------------------------------------------------------------------------
def set_1_features(
    main_df: pd.DataFrame,
    time_column: str,
    timesteps: int,
    time_shift: int,
) -> pd.DataFrame:
    """Create features from `algo_metrics`.

    :param main_df: pd.DataFrame: The DataFrame containing the data.
    :param time_column: str: The target column with the dates / timestamps.
    :param timesteps: int: Number of timesteps to consider.
    :param time_shift: int: Number of timesteps to look back into for default calculations.

    :return: pd.DataFrame: DataFrame with new columns.
    """
    main_df: pd.DataFrame = features_adaptive_techniques(main_df=main_df, time_column=time_column, timesteps=timesteps)
    main_df: pd.DataFrame = features_advanced_techniques(
        main_df=main_df,
        time_column=time_column,
        timesteps=timesteps,
        time_shift=time_shift,
    )
    main_df: pd.DataFrame = features_behavioral(
        main_df=main_df,
        time_column=time_column,
        timesteps=timesteps,
        time_shift=time_shift,
    )
    main_df: pd.DataFrame = features_charting(
        main_df=main_df,
        time_column=time_column,
        timesteps=timesteps,
        time_shift=time_shift,
    )
    main_df: pd.DataFrame = features_cycles(
        main_df=main_df,
        time_column=time_column,
        timesteps=timesteps,
        time_shift=time_shift,
    )
    main_df: pd.DataFrame = features_day_trading(
        main_df=main_df,
        time_column=time_column,
        timesteps=timesteps,
        time_shift=time_shift,
    )
    main_df: pd.DataFrame = features_event_driven(
        main_df=main_df,
        time_column=time_column,
        timesteps=timesteps,
        time_shift=time_shift,
    )
    main_df: pd.DataFrame = features_momentum(
        main_df=main_df,
        time_column=time_column,
        timesteps=timesteps,
        time_shift=time_shift,
    )
    main_df: pd.DataFrame = features_momentums(
        main_df=main_df,
        time_column=time_column,
        timesteps=timesteps,
        time_shift=time_shift,
    )
    main_df: pd.DataFrame = features_multi_time_frames(
        main_df=main_df,
        time_column=time_column,
        timesteps=timesteps,
        time_shift=time_shift,
    )
    main_df: pd.DataFrame = features_price_distribution(
        main_df=main_df,
        time_column=time_column,
        timesteps=timesteps,
        time_shift=time_shift,
    )
    main_df: pd.DataFrame = features_trend(
        main_df=main_df,
        time_column=time_column,
        timesteps=timesteps,
        time_shift=time_shift,
    )
    main_df: pd.DataFrame = features_trends(
        main_df=main_df,
        time_column=time_column,
        timesteps=timesteps,
        time_shift=time_shift,
    )
    main_df: pd.DataFrame = features_volatility(
        main_df=main_df,
        time_column=time_column,
        timesteps=timesteps,
        time_shift=time_shift,
    )
    main_df: pd.DataFrame = features_volume(
        main_df=main_df,
        time_column=time_column,
        timesteps=timesteps,
        time_shift=time_shift,
    )
    main_df: pd.DataFrame = features_volumes(
        main_df=main_df,
        time_column=time_column,
        timesteps=timesteps,
        time_shift=time_shift,
    )

    return main_df
