from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from src.trading_core import prepare_trading_data, write_rows


@dataclass(slots=True)
class BacktestConfig:
    capital: float = 100000.0
    risk_pct: float = 0.01
    rr_ratio: float = 2.0
    trades_output: Path = Path('data/trades.csv')
    summary_output: Path = Path('data/backtest_results.csv')
    strategy_name: str = 'STRATEGY'
    strategy_config: Any = None


@dataclass(slots=True)
class BacktestSummary:
    total_trades: int
    win_rate: float
    total_pnl: float
    max_drawdown: float
    avg_rr: float
    trades_output: str
    summary_output: str

    def to_dict(self) -> dict[str, object]:
        return {
            'strategy': self.__dict__.get('strategy_name', ''),
            'total_trades': self.total_trades,
            'win_rate': round(self.win_rate, 2),
            'total_pnl': round(self.total_pnl, 2),
            'max_drawdown': round(self.max_drawdown, 2),
            'avg_rr': round(self.avg_rr, 2),
            'trades_output': self.trades_output,
            'summary_output': self.summary_output,
        }


def _trade_exit_metrics(df: pd.DataFrame, trade: dict[str, object]) -> dict[str, object]:
    side = str(trade.get('side', '')).upper()
    entry_time = pd.to_datetime(trade.get('timestamp') or trade.get('entry_time'), errors='coerce')
    stop_loss = float(trade.get('stop_loss', 0.0) or 0.0)
    target = float(trade.get('target', trade.get('target_price', 0.0)) or 0.0)
    entry = float(trade.get('entry', trade.get('entry_price', 0.0)) or 0.0)
    quantity = int(float(trade.get('quantity', 1) or 1))
    future = df[df['timestamp'] >= entry_time].copy() if not pd.isna(entry_time) else df.copy()
    exit_price = float(future['close'].iloc[-1]) if not future.empty else entry
    exit_time = future['timestamp'].iloc[-1] if not future.empty else entry_time
    exit_reason = 'EOD'
    for row in future.itertuples(index=False):
        if side == 'BUY':
            if float(row.low) <= stop_loss:
                exit_price = stop_loss
                exit_time = row.timestamp
                exit_reason = 'STOP_LOSS'
                break
            if float(row.high) >= target:
                exit_price = target
                exit_time = row.timestamp
                exit_reason = 'TARGET'
                break
        elif side == 'SELL':
            if float(row.high) >= stop_loss:
                exit_price = stop_loss
                exit_time = row.timestamp
                exit_reason = 'STOP_LOSS'
                break
            if float(row.low) <= target:
                exit_price = target
                exit_time = row.timestamp
                exit_reason = 'TARGET'
                break
    pnl = (exit_price - entry) * quantity if side == 'BUY' else (entry - exit_price) * quantity
    risk_per_unit = abs(entry - stop_loss)
    rr = 0.0 if risk_per_unit <= 0 else abs(exit_price - entry) / risk_per_unit
    enriched = dict(trade)
    enriched.update(
        {
            'exit_time': '' if pd.isna(exit_time) else pd.Timestamp(exit_time).strftime('%Y-%m-%d %H:%M:%S'),
            'exit_price': round(float(exit_price), 4),
            'exit_reason': exit_reason,
            'pnl': round(float(pnl), 2),
            'rr_achieved': round(float(rr), 2),
        }
    )
    return enriched


def _equity_metrics(trades: list[dict[str, object]], starting_capital: float) -> tuple[float, float]:
    equity = float(starting_capital)
    peak = equity
    max_drawdown = 0.0
    for trade in trades:
        equity += float(trade.get('pnl', 0.0) or 0.0)
        peak = max(peak, equity)
        max_drawdown = min(max_drawdown, equity - peak)
    return equity, abs(max_drawdown)


def run_backtest(df: Any, strategy_func: Callable[..., list[dict[str, object]]], config: BacktestConfig | dict[str, object] | None = None) -> dict[str, object]:
    cfg = config if isinstance(config, BacktestConfig) else BacktestConfig(**(config or {}))
    prepared = prepare_trading_data(df)
    strategy_config = getattr(cfg, 'strategy_config', None)
    trades = strategy_func(prepared, capital=float(cfg.capital), risk_pct=float(cfg.risk_pct), rr_ratio=float(cfg.rr_ratio), config=strategy_config)
    enriched = [_trade_exit_metrics(prepared, trade) for trade in trades if str(trade.get('side', '')).upper() in {'BUY', 'SELL'}]
    total_trades = len(enriched)
    wins = sum(1 for trade in enriched if float(trade.get('pnl', 0.0) or 0.0) > 0)
    total_pnl = sum(float(trade.get('pnl', 0.0) or 0.0) for trade in enriched)
    avg_rr = sum(float(trade.get('rr_achieved', 0.0) or 0.0) for trade in enriched) / total_trades if total_trades else 0.0
    _, max_drawdown = _equity_metrics(enriched, float(cfg.capital))
    win_rate = (wins / total_trades) * 100.0 if total_trades else 0.0
    write_rows(cfg.trades_output, enriched)
    summary = {
        'strategy': cfg.strategy_name,
        'total_trades': total_trades,
        'win_rate': round(win_rate, 2),
        'total_pnl': round(total_pnl, 2),
        'max_drawdown': round(max_drawdown, 2),
        'avg_rr': round(avg_rr, 2),
        'trades_output': str(cfg.trades_output),
    }
    write_rows(cfg.summary_output, [summary])
    return summary
