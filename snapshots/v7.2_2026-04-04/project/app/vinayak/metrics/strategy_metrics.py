from __future__ import annotations

from typing import Any

import pandas as pd

from vinayak.metrics.utils import coerce_trade_records, safe_divide


def _subset(frame: pd.DataFrame, strategy_name: str) -> pd.DataFrame:
    strategies = frame.get('strategy', pd.Series([''] * len(frame), index=frame.index)).astype(str).str.lower()
    key = strategy_name.lower()
    if key == 'supply_demand':
        mask = strategies.str.contains('demand') | strategies.str.contains('supply')
    else:
        mask = strategies.str.contains(key)
    return frame.loc[mask].copy()


def _risk_per_trade(frame: pd.DataFrame) -> pd.Series:
    entry = pd.to_numeric(frame.get('entry_price', pd.Series(dtype=float)), errors='coerce').fillna(0.0)
    stop = pd.to_numeric(frame.get('stop_loss', pd.Series(dtype=float)), errors='coerce').fillna(0.0)
    return (entry - stop).abs().replace(0.0, pd.NA)


def _breakout_metrics(frame: pd.DataFrame) -> dict[str, float]:
    if frame.empty:
        return {}
    pnl = pd.to_numeric(frame.get('pnl', pd.Series(dtype=float)), errors='coerce').fillna(0.0)
    risk = _risk_per_trade(frame)
    followthrough = pnl.div(risk).replace([pd.NA, pd.NaT], 0.0).fillna(0.0)
    holding_minutes = (frame.get('exit_time') - frame.get('entry_time')).dt.total_seconds().div(60.0) if 'exit_time' in frame.columns and 'entry_time' in frame.columns else pd.Series(dtype=float)
    false_breakouts = ((pnl < 0) & (holding_minutes.fillna(9999) <= 30)).sum() if not holding_minutes.empty else int((pnl < 0).sum())
    retest_success = frame.get('retest_confirmed', pd.Series([None] * len(frame), index=frame.index)).fillna(False).astype(bool) & (pnl > 0)
    return {
        'false_breakout_rate': round(safe_divide(int(false_breakouts), len(frame)), 4),
        'retest_success_rate': round(safe_divide(int(retest_success.sum()), max(int(frame.get('retest_confirmed', pd.Series([False] * len(frame), index=frame.index)).fillna(False).astype(bool).sum()), 1)), 4),
        'breakout_followthrough_score': round(float(followthrough.clip(lower=0).mean() * 100.0) if not followthrough.empty else 0.0, 4),
        'opening_range_quality_score': round(float(pd.to_numeric(frame.get('structure_clarity', pd.Series(dtype=float)), errors='coerce').fillna(0.0).mean()), 4),
        'breakout_failure_count': int(false_breakouts),
    }


def _supply_demand_metrics(frame: pd.DataFrame) -> dict[str, float]:
    if frame.empty:
        return {}
    pnl = pd.to_numeric(frame.get('pnl', pd.Series(dtype=float)), errors='coerce').fillna(0.0)
    freshness = pd.to_numeric(frame.get('freshness_score', pd.Series(dtype=float)), errors='coerce').fillna(0.0)
    move_away = pd.to_numeric(frame.get('move_away_score', pd.Series(dtype=float)), errors='coerce').fillna(0.0)
    reaction = pd.to_numeric(frame.get('rejection_strength', pd.Series(dtype=float)), errors='coerce').fillna(0.0)
    first_touch = frame.get('retest_confirmed', pd.Series([None] * len(frame), index=frame.index)).fillna(False).astype(bool)
    return {
        'zone_respect_rate': round(safe_divide(int((pnl > 0).sum()), len(frame)), 4),
        'first_touch_reaction_rate': round(safe_divide(int((first_touch & (pnl > 0)).sum()), max(int(first_touch.sum()), 1)), 4),
        'freshness_quality_avg': round(float(freshness.mean()) if not freshness.empty else 0.0, 4),
        'move_away_quality_avg': round(float(move_away.mean()) if not move_away.empty else 0.0, 4),
        'zone_failure_rate': round(safe_divide(int((pnl < 0).sum()), len(frame)), 4),
        'reaction_strength_avg': round(float(reaction.mean()) if not reaction.empty else 0.0, 4),
    }


def _indicator_metrics(frame: pd.DataFrame) -> dict[str, float]:
    if frame.empty:
        return {}
    pnl = pd.to_numeric(frame.get('pnl', pd.Series(dtype=float)), errors='coerce').fillna(0.0)
    trend_ok = frame.get('trend_ok', pd.Series([None] * len(frame), index=frame.index)).fillna(False).astype(bool)
    vwap_alignment = frame.get('vwap_alignment', pd.Series([None] * len(frame), index=frame.index)).fillna(False).astype(bool)
    adx = pd.to_numeric(frame.get('adx_value', pd.Series(dtype=float)), errors='coerce').fillna(0.0)
    return {
        'indicator_alignment_rate': round(safe_divide(int((trend_ok & vwap_alignment).sum()), len(frame)), 4),
        'overbought_oversold_accuracy': round(safe_divide(int((pnl > 0).sum()), len(frame)), 4),
        'adx_trend_quality': round(float(adx.mean()) if not adx.empty else 0.0, 4),
        'vwap_signal_quality': round(safe_divide(int((vwap_alignment & (pnl > 0)).sum()), max(int(vwap_alignment.sum()), 1)), 4),
    }


def calculate_strategy_metrics(trades: Any) -> dict[str, Any]:
    frame = coerce_trade_records(trades)
    breakdown = {
        'breakout': _breakout_metrics(_subset(frame, 'breakout')),
        'supply_demand': _supply_demand_metrics(_subset(frame, 'supply_demand')),
        'indicator': _indicator_metrics(_subset(frame, 'indicator')),
    }
    comparison_rows: list[dict[str, Any]] = []
    for name, metrics in breakdown.items():
        if not metrics:
            continue
        comparison_rows.append({'strategy': name, **metrics})
    return {
        'by_strategy': breakdown,
        'comparison_rows': comparison_rows,
    }
