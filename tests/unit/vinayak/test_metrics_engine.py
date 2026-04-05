from __future__ import annotations

from datetime import UTC, datetime, timedelta

from vinayak.metrics import run_full_metrics_engine
from vinayak.metrics.utils import coerce_trade_records
from vinayak.metrics.validation_metrics import compute_setup_quality_score


_NOW = datetime(2026, 4, 2, 10, 0, tzinfo=UTC)


def _trade(**overrides):
    base = {
        'trade_id': 'T1',
        'symbol': 'NIFTY',
        'strategy': 'supply_demand',
        'side': 'BUY',
        'entry_time': _NOW - timedelta(minutes=30),
        'exit_time': _NOW,
        'entry_price': 100.0,
        'exit_price': 102.0,
        'stop_loss': 99.0,
        'target_price': 104.0,
        'quantity': 10,
        'pnl': 20.0,
        'gross_pnl': 22.0,
        'fees': 2.0,
        'slippage': 0.1,
        'status': 'FILLED',
        'execution_mode': 'paper',
        'signal_time': _NOW - timedelta(minutes=31),
        'execution_time': _NOW - timedelta(minutes=29),
        'validation_passed': True,
        'rejection_reason': '',
        'zone_score': 85.0,
        'vwap_alignment': True,
        'adx_value': 28.0,
        'trend_ok': True,
        'volatility_ok': True,
        'chop_ok': True,
        'duplicate_blocked': False,
        'retest_confirmed': True,
        'move_away_score': 82.0,
        'freshness_score': 88.0,
        'rejection_strength': 79.0,
        'structure_clarity': 84.0,
    }
    base.update(overrides)
    return base


def _health(ok: bool = True, *, timestamp: datetime | None = None, error_message: str | None = None):
    return {
        'timestamp': timestamp or _NOW,
        'data_latency_ms': 120.0 if ok else 900.0,
        'api_latency_ms': 90.0 if ok else 700.0,
        'signal_generation_success': ok,
        'execution_success': ok,
        'pipeline_ok': ok,
        'telegram_ok': ok,
        'broker_ok': ok,
        'error_message': error_message,
    }


def _candles(stale: bool = False):
    ts = _NOW - timedelta(minutes=30 if stale else 5)
    return [
        {'timestamp': ts, 'open': 100.0, 'high': 101.0, 'low': 99.0, 'close': 100.5, 'volume': 1000, 'vwap': 100.2}
    ]


def test_metrics_engine_handles_empty_trades() -> None:
    result = run_full_metrics_engine([], candles=_candles(), health_snapshots=[])
    assert result['performance']['total_trades'] == 0
    assert result['execution']['signals_generated'] == 0
    assert result['readiness']['overall_status'] == 'FAIL'


def test_metrics_engine_all_winning_trades() -> None:
    trades = [_trade(trade_id='W1', pnl=50.0), _trade(trade_id='W2', pnl=25.0, entry_time=_NOW - timedelta(minutes=90), exit_time=_NOW - timedelta(minutes=60))]
    result = run_full_metrics_engine(trades, candles=_candles(), health_snapshots=[_health()])
    assert result['performance']['winning_trades'] == 2
    assert result['performance']['losing_trades'] == 0
    assert result['performance']['profit_factor'] == 0.0


def test_metrics_engine_all_losing_trades() -> None:
    trades = [_trade(trade_id='L1', pnl=-50.0), _trade(trade_id='L2', pnl=-25.0, entry_time=_NOW - timedelta(minutes=90), exit_time=_NOW - timedelta(minutes=60))]
    result = run_full_metrics_engine(trades, candles=_candles(), health_snapshots=[_health()])
    assert result['performance']['winning_trades'] == 0
    assert result['performance']['losing_trades'] == 2
    assert result['performance']['expectancy'] < 0


def test_metrics_engine_detects_duplicates_and_latency() -> None:
    trades = [
        _trade(trade_id='D1', signal_time=_NOW - timedelta(minutes=35), execution_time=_NOW - timedelta(minutes=30)),
        _trade(trade_id='D1', signal_time=_NOW - timedelta(minutes=35), execution_time=_NOW - timedelta(minutes=29)),
    ]
    result = run_full_metrics_engine(trades, candles=_candles(), health_snapshots=[_health()])
    assert result['performance']['total_trades'] == 1
    assert result['execution']['average_signal_to_execution_latency_sec'] > 0


def test_metrics_engine_drawdown_and_stale_data() -> None:
    trades = [
        _trade(trade_id='M1', pnl=100.0, entry_time=_NOW - timedelta(hours=5), exit_time=_NOW - timedelta(hours=4)),
        _trade(trade_id='M2', pnl=-250.0, entry_time=_NOW - timedelta(hours=3), exit_time=_NOW - timedelta(hours=2)),
        _trade(trade_id='M3', pnl=50.0, entry_time=_NOW - timedelta(hours=2), exit_time=_NOW - timedelta(hours=1)),
    ]
    result = run_full_metrics_engine(trades, candles=_candles(stale=True), health_snapshots=[_health()])
    assert result['risk']['max_drawdown'] > 0
    assert result['system_health']['stale_data_detected'] is True


def test_compute_setup_quality_score_returns_weighted_percent() -> None:
    score = compute_setup_quality_score(_trade())
    assert 0.0 <= score <= 100.0
    assert score > 70.0


def test_readiness_report_pass() -> None:
    trades = []
    for index in range(120):
        trades.append(_trade(
            trade_id=f'P{index}',
            strategy='breakout',
            pnl=30.0 if index % 3 else -10.0,
            entry_time=_NOW - timedelta(minutes=90 + index),
            exit_time=_NOW - timedelta(minutes=60 + index),
            signal_time=_NOW - timedelta(minutes=91 + index),
            execution_time=_NOW - timedelta(minutes=89 + index),
        ))
    result = run_full_metrics_engine(trades, candles=_candles(), health_snapshots=[_health() for _ in range(5)])
    assert result['readiness']['overall_status'] == 'PASS'


def test_readiness_report_fail_on_duplicates_and_health() -> None:
    trades = [_trade(trade_id='X1', pnl=-20.0), _trade(trade_id='X2', pnl=-15.0, duplicate_blocked=True, rejection_reason='duplicate_trade')]
    result = run_full_metrics_engine(trades, candles=_candles(stale=True), health_snapshots=[_health(ok=False, error_message='broker down')])
    assert result['readiness']['overall_status'] == 'FAIL'
    assert 'stale_data_detected' in result['readiness']['failed_checks']



def test_coerce_trade_records_backfills_strict_fields_for_legacy_rows() -> None:
    frame = coerce_trade_records([
        {
            'trade_id': 'LEGACY-1',
            'symbol': 'nifty',
            'strategy_name': 'breakout',
            'side': 'buy',
            'entry_time': _NOW - timedelta(minutes=45),
            'exit_time': _NOW - timedelta(minutes=30),
            'entry_price': 100.0,
            'exit_price': 101.5,
            'stop_loss': 99.0,
            'target_price': 103.0,
            'quantity': 10,
            'pnl': 15.0,
            'validation_status': 'PASS',
            'validation_score': 8.2,
            'validation_reasons': '[]',
            'zone_score': 82.0,
            'freshness_score': 78.0,
            'move_away_score': 80.0,
            'rejection_strength': 74.0,
            'structure_clarity': 77.0,
        },
        {
            'trade_id': 'LEGACY-2',
            'symbol': 'nifty',
            'strategy_name': 'breakout',
            'side': 'sell',
            'entry_time': _NOW - timedelta(minutes=25),
            'exit_time': _NOW - timedelta(minutes=10),
            'entry_price': 101.0,
            'exit_price': 102.0,
            'stop_loss': 102.5,
            'target_price': 99.0,
            'quantity': 10,
            'pnl': -10.0,
            'validation_status': 'FAIL',
            'validation_score': 4.4,
            'validation_reasons': 'weak_zone_score, retest_not_confirmed',
        },
    ], deduplicate=False)

    assert frame.loc[0, 'strict_validation_score'] == 8
    assert frame.loc[0, 'rejection_reason'] == ''
    assert bool(frame.loc[0, 'execution_allowed']) is True
    assert frame.loc[0, 'zone_score_components']['zone_score'] == 82.0
    assert frame.loc[0, 'validation_log']['strict_validation_score'] == 8
    assert frame.loc[1, 'strict_validation_score'] == 4
    assert frame.loc[1, 'rejection_reason'] == 'weak_zone_score, retest_not_confirmed'
    assert bool(frame.loc[1, 'execution_allowed']) is False
    assert frame.loc[1, 'validation_log']['rejection_reason'] == 'weak_zone_score, retest_not_confirmed'


