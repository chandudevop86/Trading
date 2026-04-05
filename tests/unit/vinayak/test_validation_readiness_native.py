from unittest.mock import patch
from vinayak.validation.engine import validate_trade
from vinayak.analytics.readiness import evaluate_readiness
from vinayak.validation.trade_evaluation import build_trade_evaluation_summary


def test_native_validation_summary_shape_matches_workspace_expectations() -> None:
    rows = [
        {
            'strategy': 'VINAYAK_PAPER',
            'symbol': '^NSEI',
            'execution_type': 'PAPER',
            'side': 'BUY',
            'timestamp': '2026-04-02 09:20:00',
            'entry_time': '2026-04-02 09:20:00',
            'exit_time': '2026-04-02 10:00:00',
            'entry_price': 100.0,
            'exit_price': 102.0,
            'pnl': 200.0,
            'trade_status': 'CLOSED',
            'execution_status': 'FILLED',
            'duplicate_reason': '',
            'validation_error': '',
            'validation_status': 'PASS',
            'validation_reasons': [],
        },
        {
            'strategy': 'VINAYAK_PAPER',
            'symbol': '^NSEI',
            'execution_type': 'PAPER',
            'side': 'SELL',
            'timestamp': '2026-04-02 10:15:00',
            'entry_time': '2026-04-02 10:15:00',
            'exit_time': '2026-04-02 10:45:00',
            'entry_price': 101.0,
            'exit_price': 102.0,
            'pnl': -100.0,
            'trade_status': 'CLOSED',
            'execution_status': 'FILLED',
            'duplicate_reason': '',
            'validation_error': '',
            'validation_status': 'PASS',
            'validation_reasons': [],
        },
    ]

    summary = build_trade_evaluation_summary(rows, strategy_name='VINAYAK_PAPER')
    readiness = evaluate_readiness(rows, rows)

    assert 'clean_trades' in summary
    assert 'expectancy_per_trade' in summary
    assert 'profit_factor' in summary
    assert 'pass_fail_status' in summary
    assert 'go_live_status' in summary
    assert 'promotion_status' in summary
    assert 'warnings' in summary
    assert 'pass_fail_reasons' in summary

    assert 'verdict' in readiness
    assert 'reasons' in readiness
    assert 'validation_pass_rate' in readiness
    assert 'top_rejection_reasons' in readiness



def test_validation_engine_returns_scorecard_and_rejection_log() -> None:
    candles = [
        {'timestamp': '2026-04-02 09:15:00', 'open': 100.0, 'high': 101.0, 'low': 99.0, 'close': 100.5, 'volume': 1000, 'vwap': 100.2},
        {'timestamp': '2026-04-02 09:20:00', 'open': 100.5, 'high': 101.2, 'low': 100.1, 'close': 101.0, 'volume': 1200, 'vwap': 100.6},
    ]
    result = validate_trade(
        {
            'symbol': 'NIFTY',
            'strategy_name': 'DEMAND_SUPPLY',
            'side': 'BUY',
            'entry_price': 101.0,
            'stop_loss': 99.5,
            'target_price': 104.0,
            'zone_score': 80.0,
            'freshness_score': 85.0,
            'move_away_score': 82.0,
            'cleanliness_score': 76.0,
            'retest_confirmed': True,
            'retest_score': 78.0,
            'rejection_strength': 74.0,
            'structure_clarity': 81.0,
            'vwap_alignment': True,
            'trend_alignment': True,
        },
        candles,
    )

    assert 'strict_validation_score' in result['metrics']
    assert 'setup_quality_score' in result['metrics']
    assert 'rejection_log' in result
    assert result['decision'] == 'PASS'

def test_readiness_uses_clean_trades_only_for_edge_metrics() -> None:
    rows = [
        {
            'strategy': 'VINAYAK_PAPER',
            'symbol': '^NSEI',
            'execution_type': 'PAPER',
            'side': 'BUY',
            'timestamp': '2026-04-02 09:20:00',
            'entry_time': '2026-04-02 09:20:00',
            'exit_time': '2026-04-02 10:00:00',
            'entry_price': 100.0,
            'exit_price': 102.0,
            'pnl': 200.0,
            'trade_status': 'CLOSED',
            'execution_status': 'FILLED',
            'validation_status': 'PASS',
            'validation_reasons': [],
            'rejection_reason': '',
            'strict_validation_score': 8,
        },
        {
            'strategy': 'VINAYAK_PAPER',
            'symbol': '^NSEI',
            'execution_type': 'PAPER',
            'side': 'SELL',
            'timestamp': '2026-04-02 10:15:00',
            'entry_time': '2026-04-02 10:15:00',
            'exit_time': '2026-04-02 10:45:00',
            'entry_price': 101.0,
            'exit_price': 80.0,
            'pnl': 9999.0,
            'trade_status': 'REJECTED',
            'execution_status': 'REJECTED',
            'validation_status': 'FAIL',
            'validation_reasons': ['weak_zone_score'],
            'rejection_reason': 'weak_zone_score',
            'strict_validation_score': 4,
        },
    ]

    readiness = evaluate_readiness(rows, rows)

    assert readiness['clean_trade_metrics_only'] is True
    assert readiness['clean_trade_count'] == 1
    assert readiness['metrics']['clean_trade_count'] == 1
    assert readiness['expectancy'] == 200.0
    assert readiness['edge_report']['clean_trade_count'] == 1
    assert readiness['top_rejection_reasons']['weak_zone_score'] >= 1

def test_readiness_exposes_regime_and_walkforward_reports() -> None:
    rows = [
        {
            'strategy': 'VINAYAK_PAPER',
            'symbol': '^NSEI',
            'execution_type': 'PAPER',
            'side': 'BUY',
            'timestamp': '2026-04-02 09:20:00',
            'entry_time': '2026-04-02 09:20:00',
            'exit_time': '2026-04-02 10:00:00',
            'entry_price': 100.0,
            'exit_price': 102.0,
            'pnl': 200.0,
            'trade_status': 'CLOSED',
            'execution_status': 'FILLED',
            'validation_status': 'PASS',
            'validation_reasons': [],
            'rejection_reason': '',
            'strict_validation_score': 8,
            'trend_bias': 'BULLISH',
            'atr_pct': 0.45,
        },
        {
            'strategy': 'VINAYAK_PAPER',
            'symbol': '^NSEI',
            'execution_type': 'PAPER',
            'side': 'SELL',
            'timestamp': '2026-04-02 10:15:00',
            'entry_time': '2026-04-02 10:15:00',
            'exit_time': '2026-04-02 10:45:00',
            'entry_price': 101.0,
            'exit_price': 100.0,
            'pnl': 100.0,
            'trade_status': 'CLOSED',
            'execution_status': 'FILLED',
            'validation_status': 'PASS',
            'validation_reasons': [],
            'rejection_reason': '',
            'strict_validation_score': 8,
            'market_state': 'QUIET',
            'atr_pct': 0.2,
        },
    ]

    readiness = evaluate_readiness(rows, rows)

    assert 'regime_report' in readiness
    assert 'walkforward_report' in readiness
    assert 'regime_consistency_score' in readiness
    assert 'walkforward_windows' in readiness
    assert 'oos_status' in readiness
    assert 'overfit_risk_score' in readiness
    assert 'dominant_regime' in readiness['regime_report']
    assert 'oos_status' in readiness['walkforward_report']

def test_readiness_hard_blocks_oos_fail_and_high_overfit() -> None:
    rows = [
        {
            'strategy': 'VINAYAK_PAPER',
            'symbol': '^NSEI',
            'execution_type': 'PAPER',
            'side': 'BUY',
            'timestamp': '2026-04-02 09:20:00',
            'entry_time': '2026-04-02 09:20:00',
            'exit_time': '2026-04-02 10:00:00',
            'entry_price': 100.0,
            'exit_price': 102.0,
            'pnl': 200.0,
            'trade_status': 'CLOSED',
            'execution_status': 'FILLED',
            'validation_status': 'PASS',
            'validation_reasons': [],
            'rejection_reason': '',
            'strict_validation_score': 8,
        }
    ]

    with patch('vinayak.analytics.readiness.build_trade_evaluation_summary', return_value={
        'clean_trades': 120,
        'pass_fail_status': 'PASS',
        'go_live_status': 'LIVE_READY',
        'promotion_status': 'REJECT',
        'paper_readiness_summary': 'blocked by oos',
        'oos_status': 'OOS_FAIL',
        'overfit_risk_score': 8.2,
        'overfit_risk_label': 'HIGH',
        'regime_consistency_score': 6.2,
        'regime_consistency_label': 'DECENT',
        'walkforward_windows': 5,
        'oos_pass_rate': 20.0,
        'pass_fail_reasons': [],
    }):
        readiness = evaluate_readiness(rows, rows, config={'min_trades': 1})

    assert readiness['verdict'] == 'NOT_READY'
    assert 'OOS_FAIL' in readiness['reasons']
    assert 'OVERFIT_RISK_HIGH' in readiness['reasons']
    assert readiness['threshold_status']['oos_pass'] is False
    assert readiness['threshold_status']['overfit_risk_ok'] is False


def test_readiness_caps_borderline_oos_to_paper_only() -> None:
    rows = [
        {
            'strategy': 'VINAYAK_PAPER',
            'symbol': '^NSEI',
            'execution_type': 'PAPER',
            'side': 'BUY',
            'timestamp': '2026-04-03 09:15:00',
            'entry_time': '2026-04-03 09:15:00',
            'exit_time': '2026-04-03 09:20:00',
            'entry_price': 100.0,
            'exit_price': 101.0,
            'pnl': 10.0,
            'trade_status': 'CLOSED',
            'execution_status': 'FILLED',
            'validation_status': 'PASS',
            'validation_reasons': [],
            'rejection_reason': '',
            'strict_validation_score': 8,
        }
    ]

    with patch('vinayak.analytics.readiness.build_trade_evaluation_summary', return_value={
        'clean_trades': 120,
        'pass_fail_status': 'PASS',
        'go_live_status': 'LIVE_READY',
        'promotion_status': 'BACKTEST_OK',
        'paper_readiness_summary': 'borderline oos',
        'oos_status': 'OOS_BORDERLINE',
        'overfit_risk_score': 4.0,
        'overfit_risk_label': 'LOW',
        'regime_consistency_score': 5.0,
        'regime_consistency_label': 'FRAGILE',
        'walkforward_windows': 4,
        'oos_pass_rate': 55.0,
        'pass_fail_reasons': [],
    }):
        readiness = evaluate_readiness(rows, rows, config={'min_trades': 1})

    assert readiness['verdict'] == 'PAPER_ONLY'
    assert 'OOS_BORDERLINE' in readiness['reasons'] or 'REGIME_CONSISTENCY_BORDERLINE' in readiness['reasons']


