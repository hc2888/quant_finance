"""Constant variables used throughout the DAG."""

MODEL_FEATURES_COLUMNS_STR: str = """
(
exec_timestamp
, market_date
, market_year
, market_month
, market_timestamp
, market_clock
, market_hour
, market_minute
, symbol
, open
, high
, low
, close
, volume

-- `dags/algo_metrics/trade_methods/noise.py`
, efficiency_ratio
, price_density_tanh
, fractal_dimension

-- `dags/algo_metrics/trade_methods/momentums.py`
, momentum_divergence

-- `dags/algo_metrics/trade_methods/regression_analysis.py`
, is_upward_trend
, trend_line_pass
, trade_signal

-- `dags/algo_metrics/trade_methods/adaptive_techniques.py`
, chande_dynamic
, ehlers_mama_fama
, ehlers_frama
, mcginley_dynamic
, kaufman_efficiency
, dynamic_momentum_tanh
, adaptive_trend

-- `dags/algo_metrics/trade_methods/advanced_techniques.py`
, bierovic_true_range
, conners_vix_reversal
, bookstaber_volatility_breakout
, fractal_patterns

-- `dags/algo_metrics/trade_methods/behavioral.py`
, vol_ratio
, vol_ratio_tanh

-- `dags/algo_metrics/trade_methods/charting.py`
, detect_spike
, island_reversal
, pivot_point_top
, pivot_point_low
, pivot_point_volatile
, pivot_point_buy
, pivot_point_sell
, reversal_day
, key_reversal_down
, key_reversal_up
, wide_range_day
, outside_day
, inside_day
, doji
, double_doji
, engulfing_pattern

-- `dags/algo_metrics/trade_methods/cycles.py`
, hilbert_transform
, fisher_transform_signal
, fisher_transform_tanh
, ehlers_universal_tanh
, short_cycle

-- `dags/algo_metrics/trade_methods/day_trading.py`
, first_hour_breakout
, momentum_pinball
, raschke_range_breakout
, williams_range_breakout
, williams_second_method
, true_upward_gap
, intraday_sell_shock
, intraday_buy_shock

-- `dags/algo_metrics/trade_methods/event_driven.py`
, swing_high_low
, swing_high_low_entry
, wilder_swing_index
, n_day_breakout
, turtle_strategy_enter
, turtle_strategy_exit
, turtle_strategy_net_position
, turtle_strategy_stop
, turtle_strategy_net_stop

-- `dags/algo_metrics/trade_methods/momentums.py`
, div_index_slow
, div_index_fast
, ultim_osc_1
, ultim_osc_2
, relative_vigor_idx
, relative_vigor_idx_tanh
, raschke_first_cross
, strength_oscillator_tanh
, velocity_accel
, cambridge_hook

-- `dags/algo_metrics/trade_methods/multi_time_frames.py`
, elder_triple_intraday_timing
, elder_triple_force_index
, elder_triple_ray

-- `dags/algo_metrics/trade_methods/price_distribution.py`
, kase_devstop
, jackson_zone
, chande_kroll_zone
, scorpio_zone
, mcnicholl_skewness_tanh
, skew_kurt_volatility

-- `dags/algo_metrics/trade_methods/trends.py`
, smooth_expon_single
, smooth_expon_double
, simple_volatility
, single_system_momentum
, single_system_mov_avg
, single_system_exponential
, single_system_slope
, single_system_n_day_breakout
, donchian_5_20
, golden_cross
, woodshedder_long_term
, three_crossover
, ehlers_onset
, trix_signal
, kestner_adx
, donchian_breakout

-- `dags/algo_metrics/trade_methods/volumes.py`
, volume_price_combo
, mcclellan_oscillator_tanh
, volume_spike
, pseudo_volume
, low_volume_period
, low_price_move

-- `dags/algo_metrics/tech_analysis/momentum.py`
, relative_strength_index
, true_strength_index
, stoch_osc_k
, stoch_osc_d
, stoch_osc_d_slow
, kaufman_adapt_dynamic
, kaufman_adapt_regular
, rate_of_change_tanh
, williams_perc_r
, stoch_rsi
, stoch_rsi_k
, stoch_rsi_d
, perc_price_osc_tanh
, perc_price_osc_signal_tanh
, perc_price_osc_hist_tanh
, perc_volume_osc_tanh
, perc_volume_osc_signal_tanh
, perc_volume_osc_hist_tanh
, awesome_osc_tanh

-- `dags/algo_metrics/tech_analysis/trend.py`
, aroon_up
, aroon_down
, aroon_indicator
, adx_signal
, adx_pos_signal
, adx_neg_signal
, macd_tanh
, macd_signal_tanh
, macd_diff_tanh
, ichimoku_span
, ichimoku_close
, ichimoku_base
, kst_tanh
, kst_signal_tanh
, kst_diff_tanh
, detrend_price_osc_tanh
, cci_tanh
, vortex_diff_tanh
, psar_up_indicator
, psar_down_indicator
, schaff_trend_cycle

-- `dags/algo_metrics/tech_analysis/volatility.py`
, bollinger_bands_high
, bollinger_bands_low
, keltner_channel_high
, keltner_channel_low
, donchian_channel_perc_band
, ulcer_index_tanh
, bookstaber_std_dev_tanh
, bookstaber_log_squared_tanh
, bookstaber_high_low_tanh

-- `dags/algo_metrics/tech_analysis/volume.py`
, chaikin_money_flow
, force_index_tanh
, ease_of_move_tanh
, ease_of_move_sma_tanh
, money_flow_index

, time_sin
, time_cos

, predict_5
, predict_15
, predict_30
, predict_60
)
"""
