from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from src.runtime_config import RuntimeConfig
from src.trade_validation_service import build_trade_evaluation_summary
from src.trading_core import append_log, prepare_trading_data, write_rows
from src.validation_engine import required_win_rate_pct

RUNTIME_CONFIG = RuntimeConfig.load()
BACKTEST_TRADES_OUTPUT = RUNTIME_CONFIG.paths.backtest_trades_csv
BACKTEST_SUMMARY_OUTPUT = RUNTIME_CONFIG.paths.backtest_summary_csv
BACKTEST_VALIDATION_OUTPUT = RUNTIME_CONFIG.paths.backtest_validation_csv


@dataclass(slots=True)
class BacktestValidationConfig:
    min_trades: int = 100
    target_trades: int = 150
    max_trades: int = 200
    min_profit_factor: float = 1.2
    min_expectancy_per_trade: float = 0.0
    min_win_rate: float = 0.0
    min_avg_rr: float = 0.8
    max_drawdown_pct: float = 15.0
    max_duplicate_rejections: int = 0
    require_positive_expectancy: bool = True
    max_expectancy_stability_gap_ratio: float = 0.5
    require_second_half_positive_expectancy: bool = True
    require_drawdown_proof: bool = True
    min_loss_trades_for_drawdown_proof: int = 1



def nifty_intraday_validation_config() -> BacktestValidationConfig:
    """Validation preset for Nifty intraday paper/backtest promotion."""
    return BacktestValidationConfig(
        min_trades=100,
        target_trades=150,
        max_trades=200,
        min_profit_factor=1.2,
        min_expectancy_per_trade=0.0,
        min_win_rate=38.0,
        min_avg_rr=1.0,
        max_drawdown_pct=15.0,
        require_positive_expectancy=True,
        max_expectancy_stability_gap_ratio=0.5,
        require_second_half_positive_expectancy=True,
        require_drawdown_proof=True,
        min_loss_trades_for_drawdown_proof=1,
    )


def nifty_intraday_backtest_config(
    *,
    capital: float = 100000.0,
    risk_pct: float = 0.005,
    rr_ratio: float = 2.0,
    strategy_name: str = "NIFTY_INTRADAY",
    trades_output: Path = BACKTEST_TRADES_OUTPUT,
    summary_output: Path = BACKTEST_SUMMARY_OUTPUT,
    validation_output: Path = BACKTEST_VALIDATION_OUTPUT,
) -> BacktestConfig:
    """Backtest preset aligned with Nifty intraday validation and risk controls."""
    return BacktestConfig(
        capital=float(capital),
        risk_pct=float(risk_pct),
        rr_ratio=float(rr_ratio),
        trades_output=trades_output,
        summary_output=summary_output,
        validation_output=validation_output,
        strategy_name=strategy_name,
        max_trades_per_day=3,
        max_daily_loss=max(float(capital) * 0.02, 0.0),
        duplicate_cooldown_minutes=15,
        commission_per_trade=20.0,
        slippage_bps=3.0,
        validation=nifty_intraday_validation_config(),
    )

@dataclass(slots=True)
class BacktestConfig:
    capital: float = 100000.0
    risk_pct: float = 0.01
    rr_ratio: float = 2.0
    trades_output: Path = BACKTEST_TRADES_OUTPUT
    summary_output: Path = BACKTEST_SUMMARY_OUTPUT
    validation_output: Path = BACKTEST_VALIDATION_OUTPUT
    strategy_name: str = 'STRATEGY'
    strategy_config: Any = None
    allow_overlap: bool = False
    close_open_positions_at_end: bool = True
    max_trades_per_day: int | None = None
    max_daily_loss: float | None = None
    duplicate_cooldown_minutes: int = 0
    commission_per_trade: float = 0.0
    slippage_bps: float = 0.0
    validation: BacktestValidationConfig = field(default_factory=BacktestValidationConfig)


@dataclass(slots=True)
class BacktestResult:
    trades: list[dict[str, object]]
    summary: dict[str, object]
    validation: dict[str, object]


@dataclass(slots=True)
class _TradeLifecycle:
    index: int
    strategy: str
    side: str
    entry_time: pd.Timestamp
    entry_price: float
    stop_loss: float
    target_price: float
    quantity: int
    score: float
    reason: str
    status: str = 'candidate'
    exit_time: pd.Timestamp | None = None
    exit_price: float | None = None
    exit_reason: str = ''
    pnl: float = 0.0
    gross_pnl: float = 0.0
    trading_cost: float = 0.0
    rr_achieved: float = 0.0
    setup_type: str = ''
    rejection_reason: str = ''
    extra: dict[str, object] = field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        payload = {
            'trade_index': self.index,
            'strategy': self.strategy,
            'side': self.side,
            'timestamp': self.entry_time.strftime('%Y-%m-%d %H:%M:%S'),
            'entry_time': self.entry_time.strftime('%Y-%m-%d %H:%M:%S'),
            'entry': round(self.entry_price, 4),
            'entry_price': round(self.entry_price, 4),
            'stop_loss': round(self.stop_loss, 4),
            'target': round(self.target_price, 4),
            'target_price': round(self.target_price, 4),
            'quantity': int(self.quantity),
            'score': round(self.score, 2),
            'reason': self.reason,
            'trade_status': self.status,
            'execution_status': 'CLOSED' if self.status == 'closed' else 'EXECUTED',
            'position_status': 'CLOSED' if self.status == 'closed' else 'OPEN',
            'exit_time': '' if self.exit_time is None else self.exit_time.strftime('%Y-%m-%d %H:%M:%S'),
            'exit_price': '' if self.exit_price is None else round(self.exit_price, 4),
            'exit_reason': self.exit_reason,
            'gross_pnl': round(self.gross_pnl, 2),
            'trading_cost': round(self.trading_cost, 2),
            'pnl': round(self.pnl, 2),
            'rr_achieved': round(self.rr_achieved, 2),
            'setup_type': self.setup_type,
            'rejection_reason': self.rejection_reason,
        }
        payload.update(self.extra)
        return payload


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        if value is None or str(value).strip() == '':
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: object, default: int = 0) -> int:
    try:
        if value is None or str(value).strip() == '':
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default

def _normalize_text(value: object) -> str:
    return str(value or '').strip().upper()

def _merge_shared_validation(summary: dict[str, object], rows: list[dict[str, object]], strategy_name: str) -> dict[str, object]:
    shared = build_trade_evaluation_summary(rows, strategy_name=strategy_name)
    keep_existing = {
        'strategy', 'total_trades', 'closed_trades', 'wins', 'losses', 'win_rate', 'total_pnl',
        'avg_pnl', 'avg_win', 'avg_loss', 'profit_factor', 'expectancy_per_trade',
        'max_drawdown', 'max_drawdown_pct', 'duplicate_trade_count', 'invalid_trade_count', 'execution_error_count'
    }
    for key, value in shared.items():
        if key not in keep_existing:
            summary[key] = value
    return summary

def _trade_key(record: dict[str, object]) -> str:
    strategy = _normalize_text(record.get('strategy', 'STRATEGY'))
    symbol = _normalize_text(record.get('symbol', 'UNKNOWN'))
    signal_time = pd.Timestamp(record['timestamp']).strftime('%Y-%m-%d %H:%M:%S')
    side = _normalize_text(record.get('side'))
    entry_price = f"{_safe_float(record.get('entry')):.6f}"
    payload = '|'.join([strategy, symbol, signal_time, side, entry_price])
    return hashlib.sha1(payload.encode('utf-8')).hexdigest()[:20]


def _trade_id(record: dict[str, object]) -> str:
    strategy = _normalize_text(record.get('strategy', 'STRATEGY'))
    symbol = _normalize_text(record.get('symbol', 'UNKNOWN'))
    signal_time = pd.Timestamp(record['timestamp']).strftime('%Y-%m-%d %H:%M:%S')
    side = _normalize_text(record.get('side'))
    entry_price = f"{_safe_float(record.get('entry')):.6f}"
    stop_loss = f"{_safe_float(record.get('stop_loss')):.6f}"
    target = f"{_safe_float(record.get('target')):.6f}"
    payload = '|'.join([strategy, symbol, signal_time, side, entry_price, stop_loss, target])
    return hashlib.sha1(payload.encode('utf-8')).hexdigest()

def _cost_for_trade(entry_price: float, exit_price: float, quantity: int, cfg: BacktestConfig) -> float:
    traded_notional = (abs(float(entry_price)) + abs(float(exit_price))) * max(int(quantity), 0)
    slippage_cost = traded_notional * (max(float(cfg.slippage_bps), 0.0) / 10000.0)
    commission_cost = max(float(cfg.commission_per_trade), 0.0)
    return slippage_cost + commission_cost


def _validate_trade_candidate(trade: dict[str, object], index: int) -> tuple[bool, str, dict[str, object]]:
    record = dict(trade)
    side = str(record.get('side', '')).strip().upper()
    if side not in {'BUY', 'SELL'}:
        return False, 'invalid_side', record
    timestamp = pd.to_datetime(record.get('timestamp', record.get('entry_time')), errors='coerce')
    if pd.isna(timestamp):
        return False, 'invalid_timestamp', record
    entry = _safe_float(record.get('entry', record.get('entry_price')))
    stop_loss = _safe_float(record.get('stop_loss'))
    target = _safe_float(record.get('target', record.get('target_price')))
    if entry <= 0:
        return False, 'invalid_entry', record
    if stop_loss <= 0:
        return False, 'invalid_stop_loss', record
    if target <= 0:
        return False, 'invalid_target', record
    if side == 'BUY' and not (stop_loss < entry < target):
        return False, 'buy_levels_not_ordered', record
    if side == 'SELL' and not (target < entry < stop_loss):
        return False, 'sell_levels_not_ordered', record
    quantity = _safe_int(record.get('quantity'), 1)
    if quantity <= 0:
        quantity = 1
    record['side'] = side
    record['timestamp'] = timestamp
    record['entry'] = entry
    record['entry_price'] = entry
    record['stop_loss'] = stop_loss
    record['target'] = target
    record['target_price'] = target
    record['quantity'] = quantity
    record['strategy'] = str(record.get('strategy', 'STRATEGY') or 'STRATEGY')
    record['symbol'] = str(record.get('symbol', 'UNKNOWN') or 'UNKNOWN')
    record['pattern'] = str(record.get('pattern', record.get('zone_pattern', 'UNKNOWN')) or 'UNKNOWN')
    record['score'] = _safe_float(record.get('score', record.get('mtf_score', record.get('total_score', record.get('zone_strength_score', record.get('zone_gate_score', 0.0))))))
    record['reason'] = str(record.get('reason', record.get('strategy', 'TRADE')) or 'TRADE')
    record['trade_index'] = index
    record['trade_key'] = str(record.get('trade_key', '') or _trade_key(record))
    record['trade_id'] = str(record.get('trade_id', '') or _trade_id(record))
    return True, '', record


def _build_lifecycle(record: dict[str, object]) -> _TradeLifecycle:
    extra = {
        key: value
        for key, value in record.items()
        if key not in {'trade_index', 'strategy', 'side', 'timestamp', 'entry', 'entry_price', 'stop_loss', 'target', 'target_price', 'quantity', 'score', 'reason'}
    }
    lifecycle = _TradeLifecycle(
        index=int(record['trade_index']),
        strategy=str(record.get('strategy', 'STRATEGY') or 'STRATEGY'),
        side=str(record['side']),
        entry_time=pd.Timestamp(record['timestamp']),
        entry_price=float(record['entry']),
        stop_loss=float(record['stop_loss']),
        target_price=float(record['target']),
        quantity=int(record['quantity']),
        score=float(record.get('score', 0.0) or 0.0),
        reason=str(record.get('reason', 'TRADE') or 'TRADE'),
        setup_type=str(record.get('setup_type', '') or ''),
        rejection_reason=str(record.get('rejection_reason', '') or ''),
        extra=extra,
    )
    lifecycle.status = 'entered'
    return lifecycle


def _trade_signature(record: dict[str, object]) -> tuple[str, str, float]:
    return (
        str(record.get('strategy', '')),
        str(record.get('symbol', '')),
        str(record.get('side', '')),
        round(_safe_float(record.get('entry')), 4),
    )


def _dedupe_candidates(candidates: list[dict[str, object]], duplicate_cooldown_minutes: int) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    seen: set[tuple[str, str, str, float]] = set()
    cooldown_seen: dict[tuple[str, str, float], pd.Timestamp] = {}
    unique: list[dict[str, object]] = []
    rejected: list[dict[str, object]] = []
    for candidate in sorted(candidates, key=lambda item: pd.Timestamp(item['timestamp'])):
        timestamp = pd.Timestamp(candidate['timestamp'])
        hard_key = (
            str(candidate.get('strategy', '')),
            str(candidate.get('side', '')),
            str(timestamp.strftime('%Y-%m-%d %H:%M:%S')),
            round(_safe_float(candidate.get('entry')), 4),
        )
        if hard_key in seen:
            duplicate = dict(candidate)
            duplicate['trade_status'] = 'rejected'
            duplicate['rejection_reason'] = 'DUPLICATE_CANDIDATE'
            rejected.append(duplicate)
            continue
        seen.add(hard_key)

        if duplicate_cooldown_minutes > 0:
            cooldown_key = _trade_signature(candidate)
            previous = cooldown_seen.get(cooldown_key)
            if previous is not None:
                elapsed_minutes = abs((timestamp - previous).total_seconds()) / 60.0
                if elapsed_minutes < float(duplicate_cooldown_minutes):
                    duplicate = dict(candidate)
                    duplicate['trade_status'] = 'rejected'
                    duplicate['rejection_reason'] = 'DUPLICATE_COOLDOWN'
                    rejected.append(duplicate)
                    continue
            cooldown_seen[cooldown_key] = timestamp

        unique.append(candidate)
    return unique, rejected


def _finalize_trade_exit(
    trade: _TradeLifecycle,
    *,
    exit_price: float,
    exit_time: pd.Timestamp,
    exit_reason: str,
    cfg: BacktestConfig,
) -> _TradeLifecycle:
    trade.exit_price = float(exit_price)
    trade.exit_time = pd.Timestamp(exit_time)
    trade.exit_reason = str(exit_reason)
    trade.status = 'closed'
    if trade.side == 'BUY':
        trade.gross_pnl = (trade.exit_price - trade.entry_price) * trade.quantity
    else:
        trade.gross_pnl = (trade.entry_price - trade.exit_price) * trade.quantity
    trade.trading_cost = _cost_for_trade(trade.entry_price, trade.exit_price, trade.quantity, cfg)
    trade.pnl = trade.gross_pnl - trade.trading_cost
    risk_per_unit = abs(trade.entry_price - trade.stop_loss)
    if risk_per_unit <= 0:
        trade.rr_achieved = 0.0
    else:
        rr = abs(trade.exit_price - trade.entry_price) / risk_per_unit
        if trade.pnl < 0:
            rr *= -1.0
        trade.rr_achieved = rr
    return trade

def _simulate_trade_exit(prepared: pd.DataFrame, trade: _TradeLifecycle, *, close_open_positions_at_end: bool, cfg: BacktestConfig) -> _TradeLifecycle:
    future = prepared[prepared['timestamp'] > trade.entry_time].copy()
    if future.empty:
        if close_open_positions_at_end:
            current_row = prepared[prepared['timestamp'] == trade.entry_time].tail(1)
            if not current_row.empty:
                last_row = current_row.iloc[-1]
                return _finalize_trade_exit(
                    trade,
                    exit_price=float(last_row['close']),
                    exit_time=pd.Timestamp(last_row['timestamp']),
                    exit_reason='END_OF_DATA',
                    cfg=cfg,
                )
        trade.status = 'invalid'
        trade.exit_reason = 'NO_FUTURE_DATA'
        return trade
    for row in future.itertuples(index=False):
        candle_time = pd.Timestamp(row.timestamp)
        if trade.side == 'BUY':
            if float(row.low) <= trade.stop_loss:
                return _finalize_trade_exit(trade, exit_price=trade.stop_loss, exit_time=candle_time, exit_reason='STOP_LOSS', cfg=cfg)
            if float(row.high) >= trade.target_price:
                return _finalize_trade_exit(trade, exit_price=trade.target_price, exit_time=candle_time, exit_reason='TARGET', cfg=cfg)
        else:
            if float(row.high) >= trade.stop_loss:
                return _finalize_trade_exit(trade, exit_price=trade.stop_loss, exit_time=candle_time, exit_reason='STOP_LOSS', cfg=cfg)
            if float(row.low) <= trade.target_price:
                return _finalize_trade_exit(trade, exit_price=trade.target_price, exit_time=candle_time, exit_reason='TARGET', cfg=cfg)
    if close_open_positions_at_end:
        last_row = future.iloc[-1]
        return _finalize_trade_exit(
            trade,
            exit_price=float(last_row['close']),
            exit_time=pd.Timestamp(last_row['timestamp']),
            exit_reason='END_OF_DATA',
            cfg=cfg,
        )
    trade.status = 'open'
    trade.exit_reason = 'OPEN_AT_END'
    return trade


def _equity_curve_rows(trades: list[dict[str, object]], capital: float) -> list[dict[str, object]]:
    equity = float(capital)
    peak = equity
    rows: list[dict[str, object]] = []
    for trade in trades:
        equity += _safe_float(trade.get('pnl'))
        peak = max(peak, equity)
        drawdown = equity - peak
        drawdown_pct = (abs(drawdown) / peak) * 100.0 if peak > 0 else 0.0
        rows.append(
            {
                'timestamp': str(trade.get('exit_time') or trade.get('entry_time') or trade.get('timestamp') or ''),
                'equity': round(equity, 2),
                'drawdown': round(drawdown, 2),
                'drawdown_pct': round(drawdown_pct, 2),
                'strategy': str(trade.get('strategy', '')),
            }
        )
    return rows


def _score_bucket(score: float) -> str:
    if score >= 13:
        return '13+'
    if score >= 10:
        return '10-12'
    if score >= 7:
        return '7-9'
    return '0-6'


def _trade_score(record: dict[str, Any]) -> float:
    for key in ('mtf_score', 'score', 'total_score', 'zone_score', 'zone_strength_score', 'zone_gate_score'):
        raw = str(record.get(key, '') or '').strip()
        if raw:
            return _safe_float(record.get(key))
    return 0.0


def _has_mtf_scores(trades: list[dict[str, object]]) -> bool:
    return any(str(trade.get('mtf_score', '') or '').strip() for trade in trades)


def _mtf_score_bucket(score: float) -> str:
    if score >= 16:
        return '16-20'
    if score >= 12:
        return '12-15'
    if score >= 9:
        return '9-11'
    return '0-8'


def _mtf_score_bucket_metrics(trades: list[dict[str, object]], capital: float) -> tuple[dict[str, dict[str, float]], str]:
    buckets = ['0-8', '9-11', '12-15', '16-20']
    bucket_map: dict[str, list[dict[str, object]]] = {bucket: [] for bucket in buckets}
    for trade in trades:
        bucket_map[_mtf_score_bucket(_safe_float(trade.get('mtf_score')))].append(trade)

    analysis: dict[str, dict[str, float]] = {}
    for bucket in buckets:
        bucket_trades = bucket_map[bucket]
        pnl_values = [_safe_float(trade.get('pnl')) for trade in bucket_trades]
        wins = sum(1 for value in pnl_values if value > 0)
        losses = sum(1 for value in pnl_values if value < 0)
        total = len(bucket_trades)
        gross_profit = sum(value for value in pnl_values if value > 0)
        gross_loss_abs = abs(sum(value for value in pnl_values if value < 0))
        profit_factor = gross_profit / gross_loss_abs if gross_loss_abs > 0 else float('inf') if gross_profit > 0 else 0.0
        expectancy = (sum(pnl_values) / total) if total else 0.0
        equity_rows = _equity_curve_rows(bucket_trades, float(capital)) if total else []
        max_drawdown_pct = max((row.get('drawdown_pct', 0.0) for row in equity_rows), default=0.0)
        analysis[bucket] = {
            'trades': float(total),
            'wins': float(wins),
            'losses': float(losses),
            'win_rate': round((wins / total) * 100.0, 2) if total else 0.0,
            'total_pnl': round(sum(pnl_values), 2),
            'avg_pnl': round(expectancy, 2),
            'expectancy': round(expectancy, 2),
            'profit_factor': round(profit_factor, 2) if profit_factor != float('inf') else 'inf',
            'max_drawdown_pct': round(float(max_drawdown_pct), 2),
        }

    serialized = '; '.join(
        f"{bucket}=trades:{int(values['trades'])},wins:{int(values['wins'])},losses:{int(values['losses'])},win_rate:{values['win_rate']:.2f},avg_pnl:{values['avg_pnl']:.2f},expectancy:{values['expectancy']:.2f},max_dd_pct:{values['max_drawdown_pct']:.2f},pf:{values['profit_factor']}"
        for bucket, values in analysis.items()
    )
    return analysis, serialized


def _breakout_score_bucket(score: float) -> str:
    if score >= 15:
        return '15+'
    if score >= 12:
        return '12-14'
    if score >= 9:
        return '9-11'
    return '0-8'


def _breakout_score_bucket_metrics(trades: list[dict[str, object]], capital: float) -> tuple[dict[str, dict[str, float]], str]:
    buckets = ['0-8', '9-11', '12-14', '15+']
    breakout_trades = [trade for trade in trades if str(trade.get('strategy', '')).upper() == 'BREAKOUT']
    bucket_map: dict[str, list[dict[str, object]]] = {bucket: [] for bucket in buckets}
    for trade in breakout_trades:
        bucket_map[_breakout_score_bucket(_trade_score(trade))].append(trade)

    analysis: dict[str, dict[str, float]] = {}
    for bucket in buckets:
        bucket_trades = bucket_map[bucket]
        pnl_values = [_safe_float(trade.get('pnl')) for trade in bucket_trades]
        wins = sum(1 for value in pnl_values if value > 0)
        total = len(bucket_trades)
        equity_rows = _equity_curve_rows(bucket_trades, float(capital)) if total else []
        max_drawdown_pct = max((row.get('drawdown_pct', 0.0) for row in equity_rows), default=0.0)
        analysis[bucket] = {
            'trades': float(total),
            'win_rate': round((wins / total) * 100.0, 2) if total else 0.0,
            'avg_pnl': round((sum(pnl_values) / total), 2) if total else 0.0,
            'expectancy': round((sum(pnl_values) / total), 2) if total else 0.0,
            'max_drawdown_pct': round(float(max_drawdown_pct), 2),
        }

    serialized = '; '.join(
        f"{bucket}=trades:{int(values['trades'])},win_rate:{values['win_rate']:.2f},avg_pnl:{values['avg_pnl']:.2f},expectancy:{values['expectancy']:.2f},max_dd_pct:{values['max_drawdown_pct']:.2f}"
        for bucket, values in analysis.items()
    )
    return analysis, serialized


def _score_bucket_metrics(trades: list[dict[str, object]], capital: float) -> tuple[dict[str, dict[str, float]], str]:
    buckets = ['0-6', '7-9', '10-12', '13+']
    bucket_map: dict[str, list[dict[str, object]]] = {bucket: [] for bucket in buckets}
    for trade in trades:
        bucket_map[_score_bucket(_trade_score(trade))].append(trade)

    analysis: dict[str, dict[str, float]] = {}
    for bucket in buckets:
        bucket_trades = bucket_map[bucket]
        pnl_values = [_safe_float(trade.get('pnl')) for trade in bucket_trades]
        wins = sum(1 for value in pnl_values if value > 0)
        losses = sum(1 for value in pnl_values if value < 0)
        total = len(bucket_trades)
        gross_profit = sum(value for value in pnl_values if value > 0)
        gross_loss_abs = abs(sum(value for value in pnl_values if value < 0))
        profit_factor = gross_profit / gross_loss_abs if gross_loss_abs > 0 else float('inf') if gross_profit > 0 else 0.0
        expectancy = (sum(pnl_values) / total) if total else 0.0
        equity_rows = _equity_curve_rows(bucket_trades, float(capital)) if total else []
        max_drawdown_pct = max((row.get('drawdown_pct', 0.0) for row in equity_rows), default=0.0)
        analysis[bucket] = {
            'trades': float(total),
            'wins': float(wins),
            'losses': float(losses),
            'win_rate': round((wins / total) * 100.0, 2) if total else 0.0,
            'total_pnl': round(sum(pnl_values), 2),
            'avg_pnl': round(expectancy, 2),
            'expectancy': round(expectancy, 2),
            'profit_factor': round(profit_factor, 2) if profit_factor != float('inf') else 'inf',
            'max_drawdown_pct': round(float(max_drawdown_pct), 2),
        }

    serialized = '; '.join(
        f"{bucket}=trades:{int(values['trades'])},wins:{int(values['wins'])},losses:{int(values['losses'])},win_rate:{values['win_rate']:.2f},avg_pnl:{values['avg_pnl']:.2f},expectancy:{values['expectancy']:.2f},max_dd_pct:{values['max_drawdown_pct']:.2f},pf:{values['profit_factor']}"
        for bucket, values in analysis.items()
    )
    return analysis, serialized


def _threshold_filter_metrics(trades: list[dict[str, object]], capital: float) -> tuple[dict[str, dict[str, float]], str, str]:
    thresholds = [('ALL', 0.0), ('8+', 8.0), ('10+', 10.0), ('12+', 12.0)]
    analysis: dict[str, dict[str, float]] = {}
    best_label = 'ALL'
    best_key = (-10**9, -10**9, 10**9, -10**9)
    for label, threshold in thresholds:
        filtered = trades if threshold <= 0 else [trade for trade in trades if _trade_score(trade) >= threshold]
        pnl_values = [_safe_float(trade.get('pnl')) for trade in filtered]
        wins = sum(1 for value in pnl_values if value > 0)
        total = len(filtered)
        gross_profit = sum(value for value in pnl_values if value > 0)
        gross_loss_abs = abs(sum(value for value in pnl_values if value < 0))
        profit_factor = gross_profit / gross_loss_abs if gross_loss_abs > 0 else float('inf') if gross_profit > 0 else 0.0
        expectancy = (sum(pnl_values) / total) if total else 0.0
        equity_rows = _equity_curve_rows(filtered, float(capital)) if total else []
        max_drawdown_pct = max((row.get('drawdown_pct', 0.0) for row in equity_rows), default=0.0)
        win_rate = round((wins / total) * 100.0, 2) if total else 0.0
        analysis[label] = {
            'threshold': threshold,
            'trades': float(total),
            'win_rate': win_rate,
            'total_pnl': round(sum(pnl_values), 2),
            'expectancy': round(expectancy, 2),
            'max_drawdown_pct': round(float(max_drawdown_pct), 2),
            'profit_factor': round(profit_factor, 2) if profit_factor != float('inf') else 'inf',
        }
        ranking_pf = 999.0 if profit_factor == float('inf') else float(profit_factor)
        candidate_key = (round(expectancy, 6), ranking_pf, -float(max_drawdown_pct), -float(total))
        if total > 0 and candidate_key > best_key:
            best_key = candidate_key
            best_label = label

    serialized = '; '.join(
        f"{label}=trades:{int(values['trades'])},win_rate:{values['win_rate']:.2f},expectancy:{values['expectancy']:.2f},pnl:{values['total_pnl']:.2f},max_dd_pct:{values['max_drawdown_pct']:.2f},pf:{values['profit_factor']}"
        for label, values in analysis.items()
    )
    return analysis, serialized, best_label


def _binary_segment_metrics(rows: list[dict[str, Any]], key: str, positive_value: str, capital: float) -> str:
    groups = {'YES': [], 'NO': []}
    for row in rows:
        groups['YES' if str(row.get(key, '')).strip().upper() == positive_value else 'NO'].append(row)
    parts = []
    for label in ['YES', 'NO']:
        sample = groups[label]
        pnl_values = [_safe_float(item.get('pnl')) for item in sample]
        total = len(sample)
        wins = sum(1 for value in pnl_values if value > 0)
        expectancy = (sum(pnl_values) / total) if total else 0.0
        equity_rows = _equity_curve_rows(sample, float(capital)) if 'backtest_engine' in __file__ and total else _build_equity_curve_rows('SEGMENT', sample, starting_equity=float(capital)) if total else []
        max_dd_pct = max((row.get('drawdown_pct', 0.0) for row in equity_rows), default=0.0)
        parts.append(f"{label}=trades:{total},win_rate:{((wins / total) * 100.0) if total else 0.0:.2f},expectancy:{expectancy:.2f},max_dd_pct:{float(max_dd_pct):.2f}")
    return '; '.join(parts)


def _score_progression_flags(bucket_analysis: dict[str, dict[str, float]]) -> tuple[str, str]:
    progression = ['0-6', '7-9', '10-12', '13+']
    active = [bucket for bucket in progression if bucket_analysis.get(bucket, {}).get('trades', 0.0) > 0]
    if len(active) < 2:
        return 'INSUFFICIENT_DATA', 'INSUFFICIENT_DATA'
    win_rates = [float(bucket_analysis[bucket]['win_rate']) for bucket in active]
    expectancies = [float(bucket_analysis[bucket]['expectancy']) for bucket in active]
    win_rate_improves = all(curr >= prev for prev, curr in zip(win_rates, win_rates[1:]))
    expectancy_improves = all(curr >= prev for prev, curr in zip(expectancies, expectancies[1:]))
    return ('YES' if win_rate_improves else 'NO'), ('YES' if expectancy_improves else 'NO')


def _summary_from_trades(trades: list[dict[str, object]], cfg: BacktestConfig) -> dict[str, object]:
    total_trades = len(trades)
    wins = sum(1 for trade in trades if _safe_float(trade.get('pnl')) > 0)
    losses = sum(1 for trade in trades if _safe_float(trade.get('pnl')) < 0)
    pnl_values = [_safe_float(trade.get('pnl')) for trade in trades]
    total_pnl = sum(pnl_values)
    gross_total_pnl = sum(_safe_float(trade.get('gross_pnl', trade.get('pnl'))) for trade in trades)
    total_trading_cost = sum(_safe_float(trade.get('trading_cost')) for trade in trades)
    avg_pnl = total_pnl / total_trades if total_trades else 0.0
    avg_rr = sum(_safe_float(trade.get('rr_achieved')) for trade in trades) / total_trades if total_trades else 0.0
    winning_trades = [value for value in pnl_values if value > 0]
    losing_trades = [value for value in pnl_values if value < 0]
    avg_win = sum(winning_trades) / len(winning_trades) if winning_trades else 0.0
    avg_loss = sum(losing_trades) / len(losing_trades) if losing_trades else 0.0
    gross_profit = sum(winning_trades)
    gross_loss = abs(sum(losing_trades))
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else float('inf') if gross_profit > 0 else 0.0
    expectancy_per_trade = avg_pnl
    expectancy_r = sum(_safe_float(trade.get('rr_achieved')) for trade in trades) / total_trades if total_trades else 0.0
    first_half_count = total_trades // 2
    first_half_values = pnl_values[:first_half_count]
    second_half_values = pnl_values[first_half_count:]
    first_half_expectancy = sum(first_half_values) / len(first_half_values) if first_half_values else 0.0
    second_half_expectancy = sum(second_half_values) / len(second_half_values) if second_half_values else 0.0
    expectancy_stability_gap_ratio = abs(first_half_expectancy - second_half_expectancy) / abs(expectancy_per_trade) if abs(expectancy_per_trade) > 0 else 0.0
    equity_rows = _equity_curve_rows(trades, float(cfg.capital))
    max_drawdown = abs(min((row['drawdown'] for row in equity_rows), default=0.0))
    max_drawdown_pct = max((row['drawdown_pct'] for row in equity_rows), default=0.0)
    drawdown_proven = max_drawdown_pct > 0 and losses >= int(cfg.validation.min_loss_trades_for_drawdown_proof)
    retest_only_count = sum(1 for trade in trades if str(trade.get('retest_only_entry', '')).strip().upper() == 'YES')
    vwap_pass_count = sum(1 for trade in trades if str(trade.get('vwap_gate', '')).strip().upper() == 'PASS')
    session_pass_count = sum(1 for trade in trades if str(trade.get('session_gate', '')).strip().upper() == 'PASS')
    zone_gate_scores = [_safe_float(trade.get('zone_gate_score')) for trade in trades if str(trade.get('zone_gate_score', '')).strip() != '']
    avg_zone_gate_score = sum(zone_gate_scores) / len(zone_gate_scores) if zone_gate_scores else 0.0

    pnl_by_strategy: dict[str, float] = {}
    for trade in trades:
        strategy = str(trade.get('strategy', cfg.strategy_name) or cfg.strategy_name)
        pnl_by_strategy[strategy] = pnl_by_strategy.get(strategy, 0.0) + _safe_float(trade.get('pnl'))

    bucket_analysis, score_bucket_analysis = _score_bucket_metrics(trades, float(cfg.capital))
    threshold_analysis, threshold_filter_analysis, best_threshold_label = _threshold_filter_metrics(trades, float(cfg.capital))
    higher_score_improves_win_rate, higher_score_improves_expectancy = _score_progression_flags(bucket_analysis)
    mtf_bucket_analysis, mtf_score_bucket_analysis = _mtf_score_bucket_metrics(trades, float(cfg.capital)) if _has_mtf_scores(trades) else ({}, '')
    breakout_bucket_analysis, breakout_score_bucket_analysis = _breakout_score_bucket_metrics(trades, float(cfg.capital))
    overlap_performance = _binary_segment_metrics(trades, 'overlap_with_fvg', 'YES', float(cfg.capital))
    freshness_performance = _binary_segment_metrics(trades, 'fresh_zone', 'YES', float(cfg.capital))

    trading_days = len({str(trade.get('exit_time') or trade.get('entry_time') or trade.get('timestamp') or '')[:10] for trade in trades if str(trade.get('exit_time') or trade.get('entry_time') or trade.get('timestamp') or '').strip()})
    avg_trades_per_day = total_trades / trading_days if trading_days else 0.0

    return {
        'strategy': cfg.strategy_name,
        'total_trades': total_trades,
        'wins': wins,
        'losses': losses,
        'win_rate': round((wins / total_trades) * 100.0, 2) if total_trades else 0.0,
        'total_pnl': round(total_pnl, 2),
        'gross_total_pnl': round(gross_total_pnl, 2),
        'total_trading_cost': round(total_trading_cost, 2),
        'avg_pnl': round(avg_pnl, 2),
        'avg_win': round(avg_win, 2),
        'avg_loss': round(avg_loss, 2),
        'profit_factor': round(profit_factor, 2) if profit_factor != float('inf') else 'inf',
        'expectancy_per_trade': round(expectancy_per_trade, 2),
        'first_half_expectancy_per_trade': round(first_half_expectancy, 2),
        'second_half_expectancy_per_trade': round(second_half_expectancy, 2),
        'expectancy_stability_gap_ratio': round(expectancy_stability_gap_ratio, 4),
        'expectancy_r': round(expectancy_r, 2),
        'positive_expectancy': 'YES' if expectancy_per_trade > 0 else 'NO',
        'drawdown_proven': 'YES' if drawdown_proven else 'NO',
        'retest_only_trades': retest_only_count,
        'retest_only_trade_pct': round((retest_only_count / total_trades) * 100.0, 2) if total_trades else 0.0,
        'vwap_pass_trades': vwap_pass_count,
        'vwap_pass_pct': round((vwap_pass_count / total_trades) * 100.0, 2) if total_trades else 0.0,
        'session_pass_trades': session_pass_count,
        'session_pass_pct': round((session_pass_count / total_trades) * 100.0, 2) if total_trades else 0.0,
        'avg_zone_gate_score': round(avg_zone_gate_score, 2),
        'max_drawdown': round(max_drawdown, 2),
        'max_drawdown_pct': round(max_drawdown_pct, 2),
        'avg_rr': round(avg_rr, 2),
        'trading_days': trading_days,
        'avg_trades_per_day': round(avg_trades_per_day, 2),
        'pnl_by_strategy': '; '.join(f'{key}:{value:.2f}' for key, value in sorted(pnl_by_strategy.items())),
        'score_bucket_analysis': score_bucket_analysis,
        'threshold_filter_analysis': threshold_filter_analysis,
        'mtf_score_bucket_analysis': mtf_score_bucket_analysis,
        'breakout_score_bucket_analysis': breakout_score_bucket_analysis,
        'overlap_performance': overlap_performance,
        'freshness_performance': freshness_performance,
        'best_min_score_threshold': best_threshold_label,
        'higher_score_improves_win_rate': higher_score_improves_win_rate,
        'higher_score_improves_expectancy': higher_score_improves_expectancy,
        'trades_output': str(cfg.trades_output),
        'summary_output': str(cfg.summary_output),
        'validation_output': str(cfg.validation_output),
        'market_session': '09:15-15:30',
        'duplicate_controls_enforced': 'YES' if int(cfg.duplicate_cooldown_minutes) > 0 else 'NO',
        'cooldown_controls_enforced': 'YES' if int(cfg.duplicate_cooldown_minutes) > 0 else 'NO',
        'duplicate_trade_count': 0,
        'malformed_trade_count': 0,
        'invalid_trade_count': 0,
        'execution_error_count': 0,
    }
def _validation_report(summary: dict[str, object], cfg: BacktestConfig) -> dict[str, object]:
    rules = cfg.validation
    total_trades = _safe_int(summary.get('total_trades'))
    win_rate = _safe_float(summary.get('win_rate'))
    profit_factor = float('inf') if str(summary.get('profit_factor', '')).strip().lower() == 'inf' else _safe_float(summary.get('profit_factor'))
    expectancy = _safe_float(summary.get('expectancy_per_trade'))
    first_half_expectancy = _safe_float(summary.get('first_half_expectancy_per_trade'))
    second_half_expectancy = _safe_float(summary.get('second_half_expectancy_per_trade'))
    expectancy_stability_gap_ratio = _safe_float(summary.get('expectancy_stability_gap_ratio'))
    avg_rr = _safe_float(summary.get('avg_rr'), default=2.0)
    max_drawdown_pct = _safe_float(summary.get('max_drawdown_pct'))
    positive_expectancy = str(summary.get('positive_expectancy', 'NO')).upper() == 'YES' or expectancy > 0
    drawdown_proven = str(summary.get('drawdown_proven', 'NO')).upper() == 'YES'
    losses = _safe_int(summary.get('losses'))
    duplicate_rejections = _safe_int(summary.get('duplicate_rejections', summary.get('duplicate_trade_count')))
    invalid_trade_count = _safe_int(summary.get('invalid_trade_count', summary.get('malformed_trade_count')))
    required_win_rate = max(float(rules.min_win_rate), required_win_rate_pct(avg_rr))

    blockers: list[str] = []
    notes: list[str] = []
    sample_window_passed = int(rules.min_trades) <= total_trades <= int(rules.max_trades)

    if total_trades < int(rules.min_trades):
        blockers.append(f'MIN_TRADES<{int(rules.min_trades)}')
    elif total_trades > int(rules.max_trades):
        blockers.append(f'MAX_TRADES>{int(rules.max_trades)}')
        notes.append('OVERTRADING_SAMPLE_REJECTED')
    else:
        notes.append('SAMPLE_WITHIN_TARGET_WINDOW')

    if rules.require_positive_expectancy and not positive_expectancy:
        blockers.append('NEGATIVE_EXPECTANCY')
    if expectancy <= float(rules.min_expectancy_per_trade):
        blockers.append(f'EXPECTANCY<={float(rules.min_expectancy_per_trade):.2f}')
    if rules.require_second_half_positive_expectancy and second_half_expectancy <= float(rules.min_expectancy_per_trade):
        blockers.append('SECOND_HALF_EXPECTANCY_NOT_POSITIVE')
    if expectancy_stability_gap_ratio > float(rules.max_expectancy_stability_gap_ratio):
        blockers.append(f'EXPECTANCY_STABILITY_GAP>{float(rules.max_expectancy_stability_gap_ratio):.2f}')
    if profit_factor != float('inf') and profit_factor <= float(rules.min_profit_factor):
        blockers.append(f'PROFIT_FACTOR<={float(rules.min_profit_factor):.2f}')
    if win_rate < float(required_win_rate):
        blockers.append(f'WIN_RATE<{float(required_win_rate):.2f}')
    if avg_rr < float(rules.min_avg_rr):
        blockers.append(f'AVG_RR<{float(rules.min_avg_rr):.2f}')
    if max_drawdown_pct > float(rules.max_drawdown_pct):
        blockers.append(f'MAX_DD_PCT>{float(rules.max_drawdown_pct):.2f}')
    if duplicate_rejections > int(rules.max_duplicate_rejections):
        blockers.append(f'DUPLICATES>{int(rules.max_duplicate_rejections)}')
    if invalid_trade_count > 0:
        blockers.append('INVALID_TRADES>0')
    if rules.require_drawdown_proof and not drawdown_proven:
        blockers.append('DRAWDOWN_NOT_PROVEN')

    deployment_ready = 'YES' if not blockers else 'NO'
    target_gap = max(int(rules.target_trades) - total_trades, 0)
    sample_status = 'BELOW_MIN' if total_trades < int(rules.min_trades) else 'ABOVE_TARGET' if total_trades > int(rules.max_trades) else 'TARGET_WINDOW'

    return {
        'strategy': str(summary.get('strategy', cfg.strategy_name) or cfg.strategy_name),
        'sample_status': sample_status,
        'sample_window_passed': 'YES' if sample_window_passed else 'NO',
        'sample_trade_floor': int(rules.min_trades),
        'sample_trade_target': int(rules.target_trades),
        'sample_trade_cap': int(rules.max_trades),
        'trades_evaluated': total_trades,
        'trade_gap_to_target': target_gap,
        'positive_expectancy': 'YES' if positive_expectancy else 'NO',
        'profit_factor': summary.get('profit_factor', 0.0),
        'expectancy_per_trade': round(expectancy, 2),
        'first_half_expectancy_per_trade': round(first_half_expectancy, 2),
        'second_half_expectancy_per_trade': round(second_half_expectancy, 2),
        'expectancy_stability_gap_ratio': round(expectancy_stability_gap_ratio, 4),
        'win_rate': round(win_rate, 2),
        'avg_rr': round(avg_rr, 2),
        'losses': losses,
        'drawdown_proven': 'YES' if drawdown_proven else 'NO',
        'max_drawdown_pct': round(max_drawdown_pct, 2),
        'required_win_rate': round(float(required_win_rate), 2),
        'required_profit_factor': round(float(rules.min_profit_factor), 2),
        'max_duplicate_rejections': int(rules.max_duplicate_rejections),
        'duplicate_rejections': duplicate_rejections,
        'duplicate_trade_count': duplicate_rejections,
        'invalid_trade_count': invalid_trade_count,
        'clean_trade_count': _safe_int(summary.get('clean_trades', total_trades)),
        'clean_trade_metrics_only': 'YES',
        'edge_proof_status': str(summary.get('go_live_status', summary.get('paper_readiness_status', 'PAPER_ONLY'))),
        'readiness_summary': str(summary.get('paper_readiness_summary', '')),
        'promotion_status': str(summary.get('promotion_status', 'RESEARCH_ONLY')),
        'validation_passed': deployment_ready,
        'deployment_ready': deployment_ready,
        'deployment_blockers': '; '.join(blockers),
        'validation_notes': '; '.join(notes),
    }
def _build_rejected_row(record: dict[str, object], reason: str) -> dict[str, object]:
    rejected = dict(record)
    rejected['trade_status'] = 'rejected'
    rejected['rejection_reason'] = reason
    rejected.setdefault('entry_time', str(rejected.get('timestamp', '')))
    rejected.setdefault('execution_status', 'REJECTED')
    rejected.setdefault('position_status', '')
    rejected.setdefault('exit_time', '')
    rejected.setdefault('exit_price', '')
    rejected.setdefault('exit_reason', '')
    rejected.setdefault('pnl', 0.0)
    return rejected


def _apply_backtest_risk_rules(
    records: list[dict[str, object]],
    prepared: pd.DataFrame,
    cfg: BacktestConfig,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    simulated: list[dict[str, object]] = []
    rejected: list[dict[str, object]] = []
    active_exit_cutoff: pd.Timestamp | None = None
    daily_state: dict[str, dict[str, float]] = {}

    for record in sorted(records, key=lambda item: pd.Timestamp(item['timestamp'])):
        lifecycle = _build_lifecycle(record)
        day_key = lifecycle.entry_time.strftime('%Y-%m-%d')
        state = daily_state.setdefault(day_key, {'count': 0.0, 'realized_pnl': 0.0})

        if not cfg.allow_overlap and active_exit_cutoff is not None and lifecycle.entry_time <= active_exit_cutoff:
            rejected.append(_build_rejected_row(lifecycle.to_dict(), 'OVERLAPPING_TRADE'))
            continue

        if cfg.max_trades_per_day is not None and int(cfg.max_trades_per_day) > 0 and int(state['count']) >= int(cfg.max_trades_per_day):
            rejected.append(_build_rejected_row(lifecycle.to_dict(), 'MAX_TRADES_PER_DAY'))
            continue

        if cfg.max_daily_loss is not None and float(cfg.max_daily_loss) > 0 and float(state['realized_pnl']) <= -abs(float(cfg.max_daily_loss)):
            rejected.append(_build_rejected_row(lifecycle.to_dict(), 'MAX_DAILY_LOSS'))
            continue

        lifecycle = _simulate_trade_exit(prepared, lifecycle, close_open_positions_at_end=cfg.close_open_positions_at_end, cfg=cfg)
        lifecycle_row = lifecycle.to_dict()
        simulated.append(lifecycle_row)
        if lifecycle.exit_time is not None and lifecycle.status == 'closed':
            active_exit_cutoff = lifecycle.exit_time
            state['count'] += 1.0
            state['realized_pnl'] += _safe_float(lifecycle_row.get('pnl'))

    return simulated, rejected


def run_backtest(df: Any, strategy_func: Callable[..., list[dict[str, object]]], config: BacktestConfig | dict[str, object] | None = None) -> dict[str, object]:
    """Run a historical trade simulation with validation and live-readiness gating."""
    cfg = config if isinstance(config, BacktestConfig) else BacktestConfig(**(config or {}))
    prepared = prepare_trading_data(df)
    strategy_config = getattr(cfg, 'strategy_config', None)

    try:
        raw_trades = strategy_func(
            prepared,
            capital=float(cfg.capital),
            risk_pct=float(cfg.risk_pct),
            rr_ratio=float(cfg.rr_ratio),
            config=strategy_config,
        )
    except Exception as exc:
        append_log(f'backtest_engine strategy execution failed: {exc}')
        write_rows(cfg.trades_output, [])
        summary = {
            'strategy': cfg.strategy_name,
            'total_trades': 0,
            'wins': 0,
            'losses': 0,
            'win_rate': 0.0,
            'total_pnl': 0.0,
            'avg_pnl': 0.0,
            'max_drawdown': 0.0,
            'max_drawdown_pct': 0.0,
            'avg_rr': 0.0,
            'pnl_by_strategy': '',
            'score_bucket_analysis': '',
            'trades_output': str(cfg.trades_output),
            'summary_output': str(cfg.summary_output),
            'validation_output': str(cfg.validation_output),
            'error': str(exc),
        }
        validation = _validation_report(summary, cfg)
        write_rows(cfg.summary_output, [summary])
        write_rows(cfg.validation_output, [validation])
        return {**summary, **validation}

    validated_candidates: list[dict[str, object]] = []
    rejected_rows: list[dict[str, object]] = []
    for index, trade in enumerate(raw_trades or [], start=1):
        valid, reason, normalized = _validate_trade_candidate(trade, index)
        if not valid:
            append_log(f'backtest_engine rejected malformed trade #{index}: {reason}')
            rejected_rows.append(_build_rejected_row(normalized, reason))
            continue
        validated_candidates.append(normalized)

    candidates, duplicate_rejections = _dedupe_candidates(validated_candidates, int(cfg.duplicate_cooldown_minutes))
    rejected_rows.extend(duplicate_rejections)
    simulated_rows, rule_rejections = _apply_backtest_risk_rules(candidates, prepared, cfg)
    rejected_rows.extend(rule_rejections)

    closed_trades = [row for row in simulated_rows if str(row.get('trade_status', '')) == 'closed']
    all_rows = simulated_rows + rejected_rows
    write_rows(cfg.trades_output, all_rows)

    summary = _summary_from_trades(closed_trades, cfg)
    malformed_reasons = {'invalid_side', 'invalid_timestamp', 'invalid_entry', 'invalid_stop_loss', 'invalid_target', 'buy_levels_not_ordered', 'sell_levels_not_ordered'}
    summary['rejected_candidates'] = len(rejected_rows)
    summary['closed_trades'] = len(closed_trades)
    summary['duplicate_rejections'] = len([row for row in rejected_rows if str(row.get('rejection_reason', '')) in {'DUPLICATE_CANDIDATE', 'DUPLICATE_COOLDOWN'}])
    summary['duplicate_trade_count'] = int(summary['duplicate_rejections'])
    summary['risk_rule_rejections'] = len([row for row in rejected_rows if str(row.get('rejection_reason', '')) in {'MAX_TRADES_PER_DAY', 'MAX_DAILY_LOSS', 'OVERLAPPING_TRADE'}])
    summary['malformed_trade_count'] = len([row for row in rejected_rows if str(row.get('rejection_reason', '')).lower() in malformed_reasons])
    summary['invalid_trade_count'] = int(summary['malformed_trade_count'])
    summary['duplicate_controls_enforced'] = 'YES' if int(cfg.duplicate_cooldown_minutes) > 0 else 'NO'
    summary['cooldown_controls_enforced'] = 'YES' if int(cfg.duplicate_cooldown_minutes) > 0 else 'NO'
    summary = _merge_shared_validation(summary, closed_trades, cfg.strategy_name)
    validation = _validation_report(summary, cfg)
    summary.update(validation)
    summary = _merge_shared_validation(summary, closed_trades, cfg.strategy_name)
    write_rows(cfg.summary_output, [summary])
    write_rows(cfg.validation_output, [validation])
    return summary


def summarize_trade_log(
    trade_log: str | Path | list[dict[str, object]],
    *,
    capital: float = 100000.0,
    strategy_name: str = 'PAPER_VALIDATION',
    summary_output: Path = BACKTEST_SUMMARY_OUTPUT,
    validation_output: Path = BACKTEST_VALIDATION_OUTPUT,
    validation: BacktestValidationConfig | None = None,
    duplicate_controls_enforced: bool | None = None,
) -> dict[str, object]:
    """Summarize paper/backtest rows and return the same live-readiness verdict used by backtests."""
    if isinstance(trade_log, (str, Path)):
        path = Path(trade_log)
        if path.exists() and path.stat().st_size > 0:
            rows = pd.read_csv(path).to_dict(orient='records')
        else:
            rows = []
    else:
        rows = [dict(row) for row in trade_log]

    closed_rows = [row for row in rows if (str(row.get('trade_status', '')).upper() in {'CLOSED', 'EXITED'} or str(row.get('execution_status', '')).upper() in {'CLOSED', 'EXITED'} or str(row.get('exit_time', '')).strip()) and ('pnl' in row or 'gross_pnl' in row)]
    cfg = BacktestConfig(
        capital=float(capital),
        trades_output=BACKTEST_TRADES_OUTPUT,
        summary_output=summary_output,
        validation_output=validation_output,
        strategy_name=strategy_name,
        validation=validation or BacktestValidationConfig(),
    )
    summary = _summary_from_trades(closed_rows, cfg)
    duplicate_reasons = {'DUPLICATE_CANDIDATE', 'DUPLICATE_COOLDOWN', 'DUPLICATE_ACTIVE_TRADE', 'DUPLICATE_EXECUTED_TRADE', 'DUPLICATE_BATCH_TRADE', 'DUPLICATE_SIGNAL_KEY', 'DUPLICATE_SIGNAL_COOLDOWN'}
    malformed_reasons = {'INVALID_SIDE', 'INVALID_TIMESTAMP', 'INVALID_ENTRY', 'INVALID_STOP_LOSS', 'INVALID_TARGET', 'BUY_LEVELS_NOT_ORDERED', 'SELL_LEVELS_NOT_ORDERED', 'MISSING_PRICE', 'MISSING_QUANTITY', 'MISSING_TIMESTAMP', 'MISSING_STOP_LOSS', 'MISSING_TARGET', 'INVALID_TRADE_LEVELS'}
    summary['rejected_candidates'] = len([row for row in rows if str(row.get('trade_status', '')).lower() == 'rejected'])
    summary['closed_trades'] = len(closed_rows)
    summary['duplicate_rejections'] = len([row for row in rows if str(row.get('duplicate_reason', row.get('rejection_reason', ''))).strip().upper() in duplicate_reasons])
    summary['duplicate_trade_count'] = int(summary['duplicate_rejections'])
    summary['malformed_trade_count'] = len([row for row in rows if str(row.get('validation_error', row.get('rejection_reason', ''))).strip().upper() in malformed_reasons])
    summary['invalid_trade_count'] = int(summary['malformed_trade_count'])
    summary['execution_error_count'] = len([row for row in rows if str(row.get('execution_status', '')).strip().upper() == 'ERROR' or str(row.get('trade_status', '')).strip().upper() == 'ERROR'])
    enforced = duplicate_controls_enforced if duplicate_controls_enforced is not None else summary['duplicate_trade_count'] == 0
    summary['duplicate_controls_enforced'] = 'YES' if enforced else 'NO'
    summary['cooldown_controls_enforced'] = 'YES' if enforced else 'NO'
    summary['market_session'] = '09:15-15:30'
    summary = _merge_shared_validation(summary, closed_rows, strategy_name)
    validation_row = _validation_report(summary, cfg)
    summary.update(validation_row)
    summary = _merge_shared_validation(summary, closed_rows, strategy_name)
    write_rows(summary_output, [summary])
    write_rows(validation_output, [validation_row])
    return summary







