from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class OperatorDefaults:
    capital: float = 20000.0
    risk_pct: float = 1.0
    rr_ratio: float = 2.0
    mode: str = 'Balanced'


@dataclass(frozen=True, slots=True)
class RuntimeStrategyPreset:
    trailing_sl_pct: float = 0.0
    strike_step: int = 50
    moneyness: str = 'ATM'
    strike_steps: int = 0
    fetch_option_metrics: bool = False
    mtf_ema_period: int = 3
    mtf_setup_mode: str = 'either'
    mtf_retest_strength: bool = True
    mtf_max_trades_per_day: int = 3
    amd_swing_window: int = 3
    amd_min_fvg_size: float = 0.35
    amd_min_bvg_size: float = 0.25
    amd_zone_fresh_bars: int = 24
    amd_retest_tolerance_pct: float = 0.0015
    amd_max_retest_bars: int = 6
    amd_min_score_conservative: float = 7.0
    amd_min_score_balanced: float = 5.0
    amd_min_score_aggressive: float = 3.0


OPERATOR_DEFAULTS = OperatorDefaults()
RUNTIME_STRATEGY_PRESET = RuntimeStrategyPreset()


def normalize_runtime_mode(mode: str) -> str:
    raw = str(mode or '').strip().lower()
    if raw == 'conservative':
        return 'Conservative'
    if raw == 'aggressive':
        return 'Aggressive'
    return 'Balanced'


def operator_default_values() -> dict[str, object]:
    return {
        'capital': OPERATOR_DEFAULTS.capital,
        'risk_pct': OPERATOR_DEFAULTS.risk_pct,
        'rr_ratio': OPERATOR_DEFAULTS.rr_ratio,
        'mode': normalize_runtime_mode(OPERATOR_DEFAULTS.mode),
    }


def runtime_strategy_kwargs(mode: str) -> dict[str, object]:
    preset = RUNTIME_STRATEGY_PRESET
    normalized_mode = normalize_runtime_mode(mode)
    return {
        'trailing_sl_pct': preset.trailing_sl_pct,
        'strike_step': preset.strike_step,
        'moneyness': preset.moneyness,
        'strike_steps': preset.strike_steps,
        'fetch_option_metrics': preset.fetch_option_metrics,
        'mtf_ema_period': preset.mtf_ema_period,
        'mtf_setup_mode': preset.mtf_setup_mode,
        'mtf_retest_strength': preset.mtf_retest_strength,
        'mtf_max_trades_per_day': preset.mtf_max_trades_per_day,
        'amd_mode': normalized_mode,
        'amd_swing_window': preset.amd_swing_window,
        'amd_min_fvg_size': preset.amd_min_fvg_size,
        'amd_min_bvg_size': preset.amd_min_bvg_size,
        'amd_zone_fresh_bars': preset.amd_zone_fresh_bars,
        'amd_retest_tolerance_pct': preset.amd_retest_tolerance_pct,
        'amd_max_retest_bars': preset.amd_max_retest_bars,
        'amd_min_score_conservative': preset.amd_min_score_conservative,
        'amd_min_score_balanced': preset.amd_min_score_balanced,
        'amd_min_score_aggressive': preset.amd_min_score_aggressive,
    }
