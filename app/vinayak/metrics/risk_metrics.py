from __future__ import annotations

from typing import Any

import pandas as pd

from vinayak.metrics.utils import closed_trades_only, coerce_trade_records, safe_divide


def calculate_equity_curve(trades: Any, starting_capital: float = 100000.0) -> pd.DataFrame:
    frame = closed_trades_only(coerce_trade_records(trades))
    if frame.empty:
        return pd.DataFrame(columns=['timestamp', 'pnl', 'equity'])
    ordered = frame.sort_values('exit_time').copy()
    ordered['pnl'] = pd.to_numeric(ordered.get('pnl'), errors='coerce').fillna(0.0)
    ordered['timestamp'] = ordered['exit_time']
    ordered['equity'] = float(starting_capital) + ordered['pnl'].cumsum()
    return ordered[['timestamp', 'pnl', 'equity']].reset_index(drop=True)


def calculate_drawdown_series(equity_curve: pd.DataFrame) -> pd.DataFrame:
    if equity_curve.empty:
        return pd.DataFrame(columns=['timestamp', 'equity', 'peak', 'drawdown', 'drawdown_pct'])
    curve = equity_curve.copy()
    curve['peak'] = curve['equity'].cummax()
    curve['drawdown'] = curve['equity'] - curve['peak']
    curve['drawdown_pct'] = curve['drawdown'].div(curve['peak'].replace(0, pd.NA)).fillna(0.0)
    return curve[['timestamp', 'equity', 'peak', 'drawdown', 'drawdown_pct']]


def detect_daily_loss_limit_hits(trades: Any, max_daily_loss: float | None) -> dict[str, Any]:
    closed = closed_trades_only(coerce_trade_records(trades))
    if closed.empty or not max_daily_loss or max_daily_loss <= 0:
        return {'daily_loss_limit_hit_count': 0, 'daily_loss_limit_hit_rate': 0.0, 'daily_pnl': {}}
    daily = closed.copy()
    daily['exit_date'] = daily['exit_time'].dt.strftime('%Y-%m-%d')
    grouped = daily.groupby('exit_date')['pnl'].sum().to_dict()
    hit_count = sum(1 for value in grouped.values() if float(value) <= -abs(float(max_daily_loss)))
    return {
        'daily_loss_limit_hit_count': int(hit_count),
        'daily_loss_limit_hit_rate': round(safe_divide(hit_count, len(grouped)), 4),
        'daily_pnl': {str(key): round(float(value), 4) for key, value in grouped.items()},
    }


def _streak_counts(pnl_values: list[float], positive: bool) -> int:
    best = 0
    current = 0
    for pnl in pnl_values:
        condition = pnl > 0 if positive else pnl < 0
        if condition:
            current += 1
            best = max(best, current)
        else:
            current = 0
    return best


def calculate_risk_metrics(trades: Any, starting_capital: float = 100000.0, max_daily_loss: float | None = None) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame]:
    frame = coerce_trade_records(trades)
    closed = closed_trades_only(frame)
    equity_curve = calculate_equity_curve(frame, starting_capital=starting_capital)
    drawdown_curve = calculate_drawdown_series(equity_curve)
    pnl_values = pd.to_numeric(closed.get('pnl', pd.Series(dtype=float)), errors='coerce').fillna(0.0)

    if not closed.empty:
        daily = closed.copy()
        daily['exit_date'] = daily['exit_time'].dt.strftime('%Y-%m-%d')
        daily_pnl = daily.groupby('exit_date')['pnl'].sum()
    else:
        daily_pnl = pd.Series(dtype=float)

    entry_series = pd.to_numeric(frame.get('entry_price', pd.Series([0.0] * len(frame), index=frame.index)), errors='coerce').fillna(0.0)
    stop_series = pd.to_numeric(frame.get('stop_loss', pd.Series([0.0] * len(frame), index=frame.index)), errors='coerce').fillna(0.0)
    quantity_series = pd.to_numeric(frame.get('quantity', pd.Series([0.0] * len(frame), index=frame.index)), errors='coerce').fillna(0.0)
    risk_per_trade = (entry_series - stop_series).abs() * quantity_series
    exposure_per_trade = entry_series * quantity_series
    daily_loss = detect_daily_loss_limit_hits(frame, max_daily_loss)

    max_drawdown = abs(float(drawdown_curve['drawdown'].min())) if not drawdown_curve.empty else 0.0
    max_drawdown_pct = abs(float(drawdown_curve['drawdown_pct'].min())) if not drawdown_curve.empty else 0.0
    current_drawdown = abs(float(drawdown_curve['drawdown'].iloc[-1])) if not drawdown_curve.empty else 0.0

    metrics = {
        'max_drawdown': round(max_drawdown, 4),
        'max_drawdown_pct': round(max_drawdown_pct * 100.0, 4),
        'current_drawdown': round(current_drawdown, 4),
        'consecutive_wins': int(_streak_counts(pnl_values.tolist(), positive=True)),
        'consecutive_losses': int(_streak_counts(pnl_values.tolist(), positive=False)),
        'average_daily_pnl': round(float(daily_pnl.mean()) if not daily_pnl.empty else 0.0, 4),
        'worst_day_pnl': round(float(daily_pnl.min()) if not daily_pnl.empty else 0.0, 4),
        'best_day_pnl': round(float(daily_pnl.max()) if not daily_pnl.empty else 0.0, 4),
        'daily_loss_limit_hit_count': daily_loss['daily_loss_limit_hit_count'],
        'daily_loss_limit_hit_rate': daily_loss['daily_loss_limit_hit_rate'],
        'risk_per_trade_avg': round(float(risk_per_trade.mean()) if not risk_per_trade.empty else 0.0, 4),
        'risk_per_trade_max': round(float(risk_per_trade.max()) if not risk_per_trade.empty else 0.0, 4),
        'exposure_per_trade_avg': round(float(exposure_per_trade.mean()) if not exposure_per_trade.empty else 0.0, 4),
        'capital_utilization_pct': round(safe_divide(float(exposure_per_trade.mean()) if not exposure_per_trade.empty else 0.0, float(starting_capital or 1.0)) * 100.0, 4),
    }
    return metrics, equity_curve, drawdown_curve

