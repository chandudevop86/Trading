from __future__ import annotations

from typing import Any

import pandas as pd

from vinayak.metrics.utils import closed_trades_only, coerce_trade_records, safe_divide


def calculate_performance_metrics(trades: Any) -> dict[str, Any]:
    frame = coerce_trade_records(trades)
    closed = closed_trades_only(frame)
    pnl = pd.to_numeric(closed.get('pnl', pd.Series(dtype=float)), errors='coerce').dropna()
    wins = pnl[pnl > 0]
    losses = pnl[pnl < 0]
    holding = None
    if not closed.empty and 'entry_time' in closed.columns and 'exit_time' in closed.columns:
        holding = (closed['exit_time'] - closed['entry_time']).dt.total_seconds().div(60.0)
        holding = holding.dropna()

    winning_trades = int((pnl > 0).sum())
    losing_trades = int((pnl < 0).sum())
    gross_profit = float(wins.sum()) if not wins.empty else 0.0
    gross_loss = float(losses.sum()) if not losses.empty else 0.0
    average_win = float(wins.mean()) if not wins.empty else 0.0
    average_loss = abs(float(losses.mean())) if not losses.empty else 0.0
    win_rate = safe_divide(winning_trades, len(closed))
    risk_reward_ratio = safe_divide(average_win, average_loss)
    expectancy = (win_rate * average_win) - ((1.0 - win_rate) * average_loss)

    return {
        'total_trades': int(len(frame)),
        'closed_trades': int(len(closed)),
        'open_trades': max(0, int(len(frame) - len(closed))),
        'winning_trades': winning_trades,
        'losing_trades': losing_trades,
        'win_rate': round(win_rate, 4),
        'average_win': round(average_win, 4),
        'average_loss': round(average_loss, 4),
        'risk_reward_ratio': round(risk_reward_ratio, 4),
        'expectancy': round(expectancy, 4),
        'gross_profit': round(gross_profit, 4),
        'gross_loss': round(gross_loss, 4),
        'net_profit': round(float(pnl.sum()) if not pnl.empty else 0.0, 4),
        'profit_factor': round(safe_divide(gross_profit, abs(gross_loss)), 4),
        'average_trade_pnl': round(float(pnl.mean()) if not pnl.empty else 0.0, 4),
        'median_trade_pnl': round(float(pnl.median()) if not pnl.empty else 0.0, 4),
        'pnl_std_dev': round(float(pnl.std(ddof=0)) if not pnl.empty else 0.0, 4),
        'best_trade': round(float(pnl.max()) if not pnl.empty else 0.0, 4),
        'worst_trade': round(float(pnl.min()) if not pnl.empty else 0.0, 4),
        'average_holding_minutes': round(float(holding.mean()) if holding is not None and not holding.empty else 0.0, 4),
    }
