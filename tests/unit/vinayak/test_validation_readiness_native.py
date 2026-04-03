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
